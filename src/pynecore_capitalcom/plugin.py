"""Capital.com integration for PyneCore over the REST v1 API.

Historical and live market data, plus live order execution, share one
REST session and one credential block. The execute path follows the
defensive-reconcile architecture described in
``docs/pynecore/plugin-system/broker/capitalcom-broker-research.md``:
every state transition is PERSIST-FIRST (BrokerStore write before the
REST call), so a process crash mid-dispatch leaves an auditable trail
that ``_recover_in_flight_submissions`` can replay on restart.

This module is the facade — the :class:`CapitalCom` class is composed
from concern-scoped mix-ins (``rest.py``, ``provider.py``,
``streaming.py``, ``execution.py``, ``activity.py``, ``bracket.py``,
``reconcile.py``, ``recovery.py``) plus a shared :class:`_CapitalComBase`
that declares the cross-mix-in surface for static analysers. The
``__init__`` body lives here because it is the only place that owns
*all* the instance state.
"""
import asyncio
import threading
from pathlib import Path
from typing import TYPE_CHECKING

from pynecore.types.ohlcv import OHLCV

from ._base import _CapitalComBase
from .activity import _ActivityMixin
from .bracket import _BracketMixin
from .config import CapitalComConfig
from .exceptions import (
    CapitalComError,
    InvalidStopDistanceError,
    InvalidStopMaxValueError,
    InvalidTakeProfitDistanceError,
    InvalidTakeProfitMaxValueError,
    OrderNotFoundError,
)
from .execution import _ExecutionMixin
from .helpers import _VALID_UNEXPECTED_CANCEL_POLICIES, _parse_opening_hours_segment
from .models import _ActivityCursor, _InstrumentRules, _activity_fingerprint
from .provider import _ProviderMixin
from .reconcile import _ReconcileMixin
from .recovery import _RecoveryMixin
from .rest import _RestSessionMixin
from .streaming import _StreamingMixin

if TYPE_CHECKING:
    pass

__all__ = [
    'CapitalCom',
    'CapitalComConfig',
    'CapitalComError',
    'InvalidStopDistanceError',
    'InvalidStopMaxValueError',
    'InvalidTakeProfitDistanceError',
    'InvalidTakeProfitMaxValueError',
    'OrderNotFoundError',
    # Re-exported for backwards compat with tests that import private
    # helpers from this module path:
    #   ``from pynecore_capitalcom.plugin import _activity_fingerprint``
    #   ``from pynecore_capitalcom.plugin import _parse_opening_hours_segment``
    '_activity_fingerprint',
    '_parse_opening_hours_segment',
]


class CapitalCom(
    _RecoveryMixin,
    _ReconcileMixin,
    _BracketMixin,
    _ActivityMixin,
    _ExecutionMixin,
    _StreamingMixin,
    _ProviderMixin,
    _RestSessionMixin,
    _CapitalComBase,
):
    """Capital.com plugin for PyneCore.

    Provides historical OHLCV and live WebSocket market data, and live
    order execution over the Capital.com REST v1 API. Data ingest and
    trading share a single REST session, so one credential block serves
    both roles.

    Only one-way position mode is supported; hedging-mode accounts are
    rejected at startup. Stop orders and server-side trailing stops are
    supported; stop-limit is not. Partial closes are emulated via
    opposite-direction posts (netting), and size changes require
    cancel-and-recreate — order size is not amendable.

    Idempotency is software-upheld via the unified
    :class:`~pynecore.core.broker.storage.BrokerStore` — the generic
    ``order_refs`` table indexes each server-side ``dealReference`` /
    ``dealId`` so the plugin can resolve an activity tail entry back to
    the originating ``client_order_id`` with a single indexed SELECT.
    Execution events are polled from REST (``/positions``,
    ``/workingorders``, ``/history/activity``) since there is no
    WebSocket order channel.
    """

    def __init__(self, *, symbol: str | None = None, timeframe: str | None = None,
                 ohlcv_dir: Path | None = None, config: object | None = None):
        """
        :param symbol: The symbol to get data for.
        :param timeframe: The timeframe in TradingView format.
        :param ohlcv_dir: The directory to save OHLCV data.
        :param config: Pre-loaded :class:`CapitalComConfig` instance.
        """
        super().__init__(symbol=symbol, timeframe=timeframe,
                         ohlcv_dir=ohlcv_dir, config=config)
        assert isinstance(self.config, CapitalComConfig), "CapitalComConfig is required"

        if self.config.on_unexpected_cancel not in _VALID_UNEXPECTED_CANCEL_POLICIES:
            raise ValueError(
                f"CapitalComConfig.on_unexpected_cancel must be one of "
                f"{sorted(_VALID_UNEXPECTED_CANCEL_POLICIES)}, got "
                f"{self.config.on_unexpected_cancel!r}",
            )
        # Promote to instance attribute so the base-class handler sees the
        # user-selected policy. The class-level default is ``"stop"``.
        self.on_unexpected_cancel = self.config.on_unexpected_cancel

        # REST session state
        self.security_token: str | None = None
        self.cst_token: str | None = None
        self.session_data: dict = {}
        self._session_token_expiry_ts: float = 0.0
        self._security_token_deadline: float = 0.0
        self._cst_token_deadline: float = 0.0
        self._session_lock = threading.RLock()
        # Bumped by every successful bootstrap login. The rotation handler
        # uses it to tell apart "another worker rotated tokens via a normal
        # response" (same generation — keep both rotations, last writer
        # wins) from "another worker created a fresh session" (generation
        # advanced — discard our late response, its rotation header
        # belongs to a now-dead session). A snapshot-token comparison
        # cannot distinguish those two cases.
        self._session_generation: int = 0

        # Live WebSocket streaming state
        self._ws = None
        self._last_bar_timestamp: int | None = None
        self._last_bar_ohlcv: OHLCV | None = None
        self._update_queue: asyncio.Queue | None = None
        self._listen_task: asyncio.Task | None = None
        self._ping_task: asyncio.Task | None = None
        self._feed_watchdog_task: asyncio.Task | None = None
        self._tick_volume: int = 0
        # Latest tick quote snapshot, updated by each ``quote`` event and
        # attached to every OHLCV emitted by ``watch_ohlcv`` so the spinner
        # and the per-bar OHLCV log can show ``bid``, ``ask`` and ``spread``.
        self._last_bid: float | None = None
        self._last_ask: float | None = None
        # Wall-clock of the last received WS payload (any destination —
        # ohlc.event, quote, ping pong, etc.). Used by the stale-feed
        # watchdog: when the connection is TCP-alive but the server stops
        # streaming market data, this stamp is the only signal we have.
        self._last_payload_ts: float = 0.0

        # Broker state
        self._account_preferences: dict | None = None
        self._activity_cursor = _ActivityCursor()
        self._last_auth_probe_ts: float = 0.0
        # Per-epic dealing-rules cache.  Capital.com rules are effectively
        # static during a trading session (lot step, min size); the plugin
        # refetches on explicit cache invalidation only.
        self._instrument_rules_cache: dict[str, _InstrumentRules] = {}
        # Monotonic counter bumped at the start of every :meth:`_poll_once`.
        # ``_process_activity`` stamps it alongside ``close_event_yielded_at``
        # so :meth:`_reconcile_snapshot` can tell apart the poll that
        # *created* the breadcrumb (must keep — same stale snapshot is
        # what observed the deal alive while ``/history/activity`` had
        # already reported its close) from a *later* poll where the deal
        # is still observed alive against a freshly fetched snapshot
        # (safe to clear — race did not resolve as a full close). Without
        # this gating the clear runs in the same poll that stamped, which
        # then routes the next-poll disappearance through
        # ``missing_pending_since`` and raises a false
        # :class:`UnexpectedCancelError`.
        self._current_poll_id: int = 0
