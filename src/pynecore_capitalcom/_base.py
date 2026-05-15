"""Type-only shared base class for the Capital.com plugin's mix-ins.

Every mix-in inherits from :class:`_CapitalComBase` so static analysers
(PyCharm, pyright) can resolve ``self.<attr>`` and Capital.com–private
cross-mix-in method calls without warnings. The class declares:

* Every instance attribute the constructor sets, as a class-level type
  annotation. The runtime ``__init__`` of the final :class:`CapitalCom`
  class assigns concrete values; the annotations exist purely for the
  type system.

* Capital.com–private method signatures that one mix-in calls on another
  (with ``...`` body). The implementation lives in whichever mix-in owns
  the concern; this class declares the surface so call sites type-check
  cleanly.

Methods inherited from :class:`BrokerPlugin` / :class:`LiveProviderPlugin`
/ :class:`ProviderPlugin` (``execute_entry``, ``watch_ohlcv``,
``connect``, ``update_symbol_info``, etc.) are intentionally *not* re-
declared here — the parent classes already define them, and re-declaring
with a slightly different signature would trigger ``reportInvalidOverride``
warnings.

Final MRO:
``CapitalCom → <every mix-in> → _CapitalComBase → BrokerPlugin →
LiveProviderPlugin → ProviderPlugin → Plugin → object``.
"""
import asyncio
import threading
from typing import TYPE_CHECKING, Any, AsyncIterator

from pynecore.core.broker.models import (
    ExchangeOrder,
    OrderEvent,
)
from pynecore.core.plugin.broker import BrokerPlugin
from pynecore.core.syminfo import SymInfo
from pynecore.types.ohlcv import OHLCV

from .config import CapitalComConfig
from .models import _ActivityCursor, _InstrumentRules

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow


class _CapitalComBase(BrokerPlugin[CapitalComConfig]):
    """Shared instance state + Capital.com–private cross-mix-in surface.

    See module docstring. Every Capital.com mix-in derives from this
    class so the type checker resolves ``self.<x>`` references uniformly.
    """

    plugin_name = "Capital.com"
    Config = CapitalComConfig
    timezone = 'US/Eastern'

    # Narrow the base ``ProviderPlugin.config: ConfigT | None`` — the
    # runtime ``__init__`` asserts the value is a ``CapitalComConfig``,
    # so every method can treat it as non-``None``.
    config: CapitalComConfig

    # --- REST/auth state ---
    security_token: str | None
    cst_token: str | None
    session_data: dict
    # Wall-clock when the current session tokens are estimated to expire
    # (unix seconds; 0.0 = no session yet). Set from the JWT ``exp`` claim
    # if the tokens parse as JWTs, else from a hardcoded fallback interval.
    _session_token_expiry_ts: float
    # Per-token deadlines. Recorded at the moment a token is issued so an
    # opaque token's hard cap survives partial rotations of the other
    # header — recomputing on every response would slide an unchanged
    # opaque deadline forward and let proactive refresh wait past expiry.
    _security_token_deadline: float
    _cst_token_deadline: float
    # Guards re-entrant ``create_session()`` calls when several worker
    # threads (``asyncio.to_thread`` callers) trip the proactive refresh
    # at the same time. Also serialises the rotation handler in
    # ``__call__`` so a slow response cannot roll back tokens issued by
    # a concurrent refresh. ``RLock`` because ``_refresh_session_if_stale``
    # holds the lock while ``create_session`` calls back into ``__call__``
    # which re-enters the lock for its own rotation step.
    _session_lock: threading.RLock
    # Monotonic counter incremented by each successful bootstrap login —
    # see the long ``__init__`` comment in ``plugin.py``.
    _session_generation: int

    # --- WebSocket state ---
    # ``_ws`` holds the active ``websockets`` client connection. The
    # concrete type changed between websockets 11 and 12 (legacy
    # ``WebSocketClientProtocol`` → ``asyncio.client.ClientConnection``);
    # ``Any`` keeps the annotation portable across versions.
    _ws: Any
    _last_bar_timestamp: int | None
    _last_bar_ohlcv: OHLCV | None
    _update_queue: asyncio.Queue | None
    _listen_task: asyncio.Task | None
    _ping_task: asyncio.Task | None
    _feed_watchdog_task: asyncio.Task | None
    _ohlc_watchdog_task: asyncio.Task | None
    _volume_backfill_task: asyncio.Task | None
    _raw_ohlc_queue: asyncio.Queue | None
    _tick_volume: int
    _last_bid: float | None
    _last_ask: float | None
    _last_payload_ts: float
    _last_ohlc_event_ts: float
    _last_quote_event_ts: float
    _last_bar_open_ts: float

    # --- Caches & cursors ---
    _account_preferences: dict | None
    _activity_cursor: _ActivityCursor
    _last_auth_probe_ts: float
    _instrument_rules_cache: dict[str, _InstrumentRules]
    _current_poll_id: int

    # --- Session calendar cache ---
    # Latest ``SymInfo`` returned by ``update_symbol_info``. Used by the
    # streaming watchdogs to suppress active WS-close calls (REST recovery
    # + ohlc-stale reconnect) while the market is in a known-closed
    # window. ``None`` until ``update_symbol_info`` has run at least once
    # — watchdogs treat that as 24/7 fallback to preserve legacy
    # behaviour during early init or for plugins that never call it.
    _sym_info: SymInfo | None

    # ------------------------------------------------------------------
    # Capital.com–private cross-mix-in method surface.
    # Implementation lives in one of the mix-ins; declared here so other
    # mix-ins can call ``self.<name>(...)`` without analyser warnings.
    # ------------------------------------------------------------------

    # --- REST core (rest.py) ---
    async def _call(self, endpoint: str, *, data: dict | None = None,
                    method: str = 'post') -> dict: ...

    def create_session(self) -> None: ...

    async def get_preferences(self) -> dict: ...

    async def assert_one_way_mode(self) -> None: ...

    # --- Provider / market data (provider.py) ---
    def get_market_details(self, search_term: str | None = None,
                           symbols: list[str] | None = None) -> dict: ...

    def get_single_market_details(self) -> dict: ...

    async def _fetch_market(self, epic: str) -> tuple[_InstrumentRules, float | None]: ...

    async def _get_instrument_rules(self, epic: str) -> _InstrumentRules: ...

    async def _get_current_mid_price(self, epic: str) -> float | None: ...

    # --- Streaming (streaming.py) ---
    async def _send(self, destination: str, payload: dict | None = None,
                    correlation_id: str = "1") -> None: ...

    async def _listen_loop(self) -> None: ...

    async def _ping_loop(self) -> None: ...

    async def _feed_watchdog_loop(self) -> None: ...

    def _on_ohlc_event(self, payload: dict) -> OHLCV | None: ...

    def _synth_from_quote(self) -> OHLCV | None: ...

    def _extra_fields(self) -> dict[str, float] | None: ...

    # --- Execution (execution.py) ---
    async def _execute_close_partial(self, *args: Any, **kwargs: Any) -> ExchangeOrder: ...

    def _row_to_exchange_order(self, row: 'OrderRow', *args: Any,
                               **kwargs: Any) -> ExchangeOrder: ...

    # --- Activity polling (activity.py) ---
    def _poll_once(self) -> AsyncIterator[OrderEvent]:
        if False:
            yield  # pragma: no cover
        ...

    def _process_activity(self, *args: Any,
                          **kwargs: Any) -> AsyncIterator[OrderEvent]:
        if False:
            yield  # pragma: no cover
        ...

    def _activity_to_event(self, *args: Any, **kwargs: Any) -> OrderEvent | None: ...

    def _find_active_entry_row(self, *args: Any, **kwargs: Any) -> 'OrderRow | None': ...

    def _find_bracket_leg_row(self, *args: Any, **kwargs: Any) -> 'OrderRow | None': ...

    # --- Bracket lifecycle (bracket.py) ---
    def _close_bracket_after_natural_close(self, entry_row: 'OrderRow') -> None: ...

    def _rollback_bracket_legs(self, *args: Any, **kwargs: Any) -> None: ...

    def _mark_bracket_legs_disposition_unknown(self, *args: Any, **kwargs: Any) -> None: ...

    @staticmethod
    def _levels_match(expected: object, actual: object,
                      *args: Any, **kwargs: Any) -> bool: ...

    def _resolve_bracket_leg_disposition(self, *args: Any, **kwargs: Any) -> bool: ...

    def _record_bracket_resolution(self, *args: Any, **kwargs: Any) -> None: ...

    async def _trailing_activation_monitor(self, *args: Any, **kwargs: Any) -> Any: ...

    # --- Reconcile (reconcile.py) ---
    def _reconcile_snapshot(self, *args: Any,
                            **kwargs: Any) -> AsyncIterator[OrderEvent]:
        if False:
            yield  # pragma: no cover
        ...

    def _missing_pending_tracker(self, *args: Any,
                                 **kwargs: Any) -> AsyncIterator[OrderEvent]:
        if False:
            yield  # pragma: no cover
        ...

    async def _maybe_raise_unexpected_cancel(self, row: 'OrderRow') -> None: ...

    # --- Recovery (recovery.py) ---
    async def _load_activity_cursor_from_events(self) -> None: ...

    async def _recover_in_flight_submissions(self) -> None: ...

    async def _recover_submitted_row(self, *args: Any, **kwargs: Any) -> None: ...

    async def _recover_server_ref_seen_row(self, *args: Any, **kwargs: Any) -> None: ...

    def _retire_startup_orphans(self, *args: Any, **kwargs: Any) -> None: ...
