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
import collections
import threading
from abc import ABC
from collections.abc import Iterable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, AsyncIterator, Callable, Protocol, cast

from pynecore.core.broker.exceptions import OrderSkippedByPlugin
from pynecore.core.broker.models import (
    EntryIntent,
    ExchangeOrder,
    ExitIntent,
    OrderEvent,
)
from pynecore.core.plugin.broker import BrokerPlugin
from pynecore.core.syminfo import SymInfo
from pynecore.types.ohlcv import OHLCV

from .config import CapitalComConfig
from .helpers import _size_from_units
from .models import (
    _ActivityCursor,
    _ForeignExitBlock,
    _HeldEditVerdict,
    _InstrumentRules,
    _OwnCloseHandle,
    _ProtectionWrite,
)

if TYPE_CHECKING:
    from pynecore.core.broker.disappearance import DisappearanceTracker
    from pynecore.core.broker.protection_guard import ProtectionLevelGuard, ProtectionLevels
    from pynecore.core.broker.models import DispatchEnvelope, PositionLeg
    from pynecore.core.broker.storage import OrderRow, RunContext


@dataclass(frozen=True, slots=True)
class _QuoteSnapshot:
    """Immutable per-frame quote state carried through both FIFO queues."""

    bar_open_s: int | None
    cumulative_volume: int
    bid: float | None
    ask: float | None


class CapitalComHookCollaborator(Protocol):
    """Public collaborator surface used by standalone dispatch/recovery hooks."""

    store_ctx: 'RunContext | None'

    async def call_api(
            self, endpoint: str, *, data: dict | None = None,
            method: str = 'post',
    ) -> dict: ...

    async def submit_exclusive_partial_close(
            self, *, coid: str, symbol: str, expected_direction: str,
            target_deal_ids: frozenset[str], body: dict,
            intent_qty: float, lot_step: float,
    ) -> tuple[dict, list[dict]]: ...

    async def submit_market_entry(
            self, *, coid: str, symbol: str, side: str, body: dict,
    ) -> dict: ...

    async def instrument_rules(self, epic: str) -> _InstrumentRules: ...

    def quantize_size(self, qty: float, rules: _InstrumentRules) -> float: ...

    def mark_bracket_legs_disposition_unknown(
            self, *, intent: ExitIntent, parent_coid: str, deal_id: str,
            tp_coid: str | None, sl_coid: str | None, reason: str,
            stage: str, deal_reference: str | None = None,
    ) -> None: ...

    def protection_write(
            self, deal_id: str, body: dict,
    ) -> AbstractContextManager[_ProtectionWrite]: ...

    def own_close(
            self, deal_ids: Iterable[str], *, full: bool, size: float | None,
    ) -> AbstractContextManager[_OwnCloseHandle]: ...

    def working_order_write(
            self, row: 'OrderRow', level: float, *, previous_level: float | None,
    ) -> AbstractContextManager[_ProtectionWrite]: ...

    def report_unattributed_opposite(
            self, *, coid: str, intent_key: str, symbol: str,
            opposite_deal_ids: list[str], attributable: bool, context: dict,
    ) -> None: ...

    def stamp_own_cancel_accepted(self, coid: str, sent_at: float) -> None: ...

    def own_cancel(self, coid: str) -> AbstractContextManager[float]: ...


class _CapitalComBase(BrokerPlugin[CapitalComConfig], ABC):
    """Shared instance state + Capital.com–private cross-mix-in surface.

    See module docstring. Every Capital.com mix-in derives from this
    class so the type checker resolves ``self.<x>`` references uniformly.
    """

    plugin_name = "Capital.com"
    Config = CapitalComConfig

    # Fills come from the polled activity feed (every 1.5 s). The journal shows
    # a healthy worst case of about 15 s, but the venue has stamped an activity
    # row 61 s after its deal (demo, 2026-07-10), and a rate-limited poll backs
    # off up to 30 s on top of that.
    fill_report_grace_s: float = 120.0
    timezone = 'US/Eastern'

    @property
    def _capital_config(self) -> CapitalComConfig:
        """Return the configuration validated by the final plugin constructor."""
        return cast(CapitalComConfig, self.config)

    async def call_api(
            self, endpoint: str, *, data: dict | None = None,
            method: str = 'post',
    ) -> dict:
        """Call the Capital.com REST transport for a standalone hook."""
        return await self._call(endpoint, data=data, method=method)

    async def submit_exclusive_partial_close(
            self, *, coid: str, symbol: str, expected_direction: str,
            target_deal_ids: frozenset[str], body: dict,
            intent_qty: float, lot_step: float,
    ) -> tuple[dict, list[dict]]:
        """Submit one account-serialized partial close after a final snapshot."""
        return await self._submit_exclusive_partial_close(
            coid=coid,
            symbol=symbol,
            expected_direction=expected_direction,
            target_deal_ids=target_deal_ids,
            body=body,
            intent_qty=intent_qty,
            lot_step=lot_step,
        )

    async def submit_market_entry(
            self, *, coid: str, symbol: str, side: str, body: dict,
    ) -> dict:
        """POST one market entry; on a netting account never against a foreign deal.

        A hedging account opens every POST as its own deal, so it goes out
        as is. On a netting account an opposite deal is netted FIFO, so the
        POST is sent only after a final snapshot under the account write lock
        shows no foreign deal on the side it would reduce.
        """
        if self._hedging_enabled or self.store_ctx is None:
            return await self._call('positions', data=body, method='post')
        return await self._submit_netting_safe_entry(
            coid=coid, symbol=symbol, side=side, body=body,
        )

    async def instrument_rules(self, epic: str) -> _InstrumentRules:
        """Return cached or freshly fetched instrument rules for a hook."""
        return await self._get_instrument_rules(epic)

    @staticmethod
    def quantize_size(qty: float, rules: _InstrumentRules) -> float:
        """Quantize a hook quantity on the venue lot grid."""
        if rules.lot_step <= 0.0:
            return qty
        units = round(qty / rules.lot_step)
        return _size_from_units(units, rules.lot_step)

    def mark_bracket_legs_disposition_unknown(
            self, *, intent: ExitIntent, parent_coid: str, deal_id: str,
            tp_coid: str | None, sl_coid: str | None, reason: str,
            stage: str, deal_reference: str | None = None,
    ) -> None:
        """Expose bracket ambiguity bookkeeping to standalone hooks."""
        self._mark_bracket_legs_disposition_unknown(
            intent=intent,
            parent_coid=parent_coid,
            deal_id=deal_id,
            tp_coid=tp_coid,
            sl_coid=sl_coid,
            reason=reason,
            stage=stage,
            deal_reference=deal_reference,
        )

    def protection_write(
            self, deal_id: str, body: dict,
    ) -> AbstractContextManager[_ProtectionWrite]:
        """Record one own ``PUT /positions/{deal_id}`` for a standalone hook."""
        return self._protection_write(deal_id, body)

    def own_close(
            self, deal_ids: Iterable[str], *, full: bool, size: float | None,
    ) -> AbstractContextManager[_OwnCloseHandle]:
        """Record an own close of ``deal_ids`` for a standalone hook."""
        return self._own_close(deal_ids, full=full, size=size)

    def working_order_write(
            self, row: 'OrderRow', level: float, *, previous_level: float | None,
    ) -> AbstractContextManager[_ProtectionWrite]:
        """Record one own ``PUT /workingorders/{dealId}`` for a standalone hook."""
        return self._working_order_write(row, level, previous_level=previous_level)

    def report_unattributed_opposite(
            self, *, coid: str, intent_key: str, symbol: str,
            opposite_deal_ids: list[str], attributable: bool, context: dict,
    ) -> None:
        """Report opposite deals an own partial close cannot be shown to own, for a hook."""
        self._report_unattributed_opposite(
            coid=coid, intent_key=intent_key, symbol=symbol,
            opposite_deal_ids=opposite_deal_ids, attributable=attributable,
            context=context,
        )

    def stamp_own_cancel_accepted(self, coid: str, sent_at: float) -> None:
        """Record an accepted own working-order ``DELETE`` for a standalone hook."""
        self._stamp_own_cancel_accepted(coid, sent_at)

    def own_cancel(self, coid: str) -> AbstractContextManager[float]:
        """Record an own working-order ``DELETE`` before it leaves, for a standalone hook."""
        return self._own_cancel(coid)

    if TYPE_CHECKING:
        def __call__(
                self,
                endpoint: str,
                *,
                data: dict | None = None,
                method: str = 'post',
                _level: int = 0,
        ) -> dict: ...

        def get_historical_prices(
                self,
                time_from: datetime | None = None,
                time_to: datetime | None = None,
                limit: int = 1000,
        ) -> dict: ...

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
    # Protects token, expiry, and generation snapshots and updates. Network
    # calls and bootstrap-lock acquisition must never happen while held.
    _session_lock: threading.RLock
    # Coordinates direct, proactive, and reactive login attempts. The
    # condition lets queued callers wait without holding ``_session_lock``;
    # the completion counter distinguishes a successful preceding login from
    # a failed attempt that the next caller must retry.
    _session_refresh_lock: threading.RLock
    _session_refresh_condition: threading.Condition
    _session_login_active: bool
    _session_login_completion: int
    # Monotonic counter incremented by each successful bootstrap login —
    # see the long ``__init__`` comment in ``plugin.py``.
    _session_generation: int

    # --- WebSocket state ---
    # ``_ws`` holds the active ``websockets`` client connection. The
    # concrete type changed between websockets 11 and 12 (legacy
    # ``WebSocketClientProtocol`` → ``asyncio.client.ClientConnection``);
    # ``Any`` keeps the annotation portable across versions.
    _ws: Any
    _ws_session_invalid: bool
    # Bar OPEN of the most recent emitted closed bar, in epoch SECONDS —
    # the axis every REST call, session-calendar check and watchdog
    # deadline in this plugin runs on. The emitted
    # :class:`~pynecore.types.ohlcv.OHLCV` carries the same instant in
    # milliseconds, so the two must never be compared directly.
    _last_bar_timestamp: int | None
    _last_bar_ohlcv: OHLCV | None
    _update_queue: asyncio.Queue | None
    _listen_task: asyncio.Task | None
    _ping_task: asyncio.Task | None
    _feed_watchdog_task: asyncio.Task | None
    _ohlc_watchdog_task: asyncio.Task | None
    _volume_backfill_task: asyncio.Task | None
    _raw_ohlc_queue: asyncio.Queue | None
    _last_bid: float | None
    _last_ask: float | None
    _last_payload_ts: float
    _last_ohlc_event_ts: float
    _last_quote_event_ts: float
    _last_bar_open_ts: float

    # --- WS-primary volume state ---
    # Per-bar quote tick buckets keyed by bar OPEN time (seconds, UTC).
    # The listener increments a bucket on every ``quote`` event using the
    # event's own ``timestamp`` (ms) so reconnects do not contaminate
    # already-closed bars.
    _ws_quote_buckets: dict[int, int]
    # Wallclock (epoch seconds) at the most recent successful WS handshake.
    # Bars whose OPEN predates this stamp are partial-coverage and MUST
    # fall back to REST instead of contributing to the rolling baseline.
    _ws_coverage_started_at: float
    # Rolling history of the last N "good" full-coverage WS quote counts.
    # Used to compute a median for the low-ratio fallback trigger AND for
    # the REST-failed estimate. REST-substituted bars are NOT appended.
    _ws_volume_baseline: collections.deque[int]
    # Consecutive count of full-coverage bars where REST confirmed the WS
    # value was bogus (zero or <low_ratio * median). At the configured
    # threshold the worker closes the WS so live_runner reconnects.
    _ws_bad_bar_streak: int
    # One-shot flag for the missing-``timestamp``-on-quote warning so the
    # log is not flooded if Capital.com regresses the payload shape.
    # Reset on every successful ``connect()``.
    _ws_quote_timestamp_warned: bool

    # --- Caches & cursors ---
    _account_preferences: dict | None
    _activity_cursor: _ActivityCursor
    _instrument_rules_cache: dict[str, _InstrumentRules]
    _current_poll_id: int
    # Epoch time the last successful activity read was issued; the next
    # poll's ``lastPeriod`` reaches back past it (``_activity_lookback_s``).
    _activity_polled_at: float
    # Lazily-built core disappearance tracker (reconcile.py owns it).
    _disappearance: 'DisappearanceTracker | None'
    # Lazily-built protection-level guard (intervention.py owns it).
    _protection: 'ProtectionLevelGuard | None'
    # Last ``/positions`` size seen per guarded position key; a change marks
    # a partial close whose effect on the bracket is re-baselined.
    _protection_sizes: dict[str, float]
    # Own protection writes per guarded position key: how many are in flight
    # and when the last one was sent or settled (wall clock). A size-change
    # adoption only takes a snapshot read after both.
    _protection_in_flight: dict[str, int]
    _protection_written_at: dict[str, float]
    # Own writes per guard key (position or resting order) whose outcome is
    # not back yet: a verdict held on the key waits for them to settle.
    _own_writes_in_flight: dict[str, int]
    # Deals whose stop the core native fail-safe manager writes.
    _failsafe_deal_ids: set[str]
    # Manual-close report ids this process already sent to the policy.
    _manual_close_reports_sent: set[str]
    # Lower-bound samples of the venue clock minus the local clock, one per
    # activity read: ``(local wall read time, monotonic read time,
    # newest dateUTC - local wall read time)``.
    _venue_clock_samples: collections.deque[tuple[float, float, float]]
    # Own close requests this process sent: wall-clock send time -> monotonic
    # send time.
    _own_close_sends: dict[float, float]
    # Accepted level-edit activities (``EDIT_STOP_AND_LIMIT``, ``WORKING_ORDER``
    # ``MODIFIED``) per deal: ``dealId`` -> ``{edit identity: (venue time,
    # source, wall time first seen)}``.
    _level_edits: dict[str, dict[str, tuple[float, str, float]]]
    # Closes only a resting own working order could explain, waiting for a
    # later working-order read: close identity -> (wall time queued, record).
    _netting_pending: dict[str, tuple[float, dict]]
    # Attributes the bot never sets seen once on its resting order (a second
    # read decides): ``dealId`` -> attribute signature; and the edits already
    # reported, ``(dealId, signature)``.
    _working_attributes_seen: dict[str, tuple]
    _working_attributes_reported: set[tuple[str, tuple]]
    # Confirmed changes of the bot's levels whose author the activity feed
    # may not show yet, by change identity; and how long after a change an
    # activity read must start before it is trusted to show the author.
    _held_edit_verdicts: dict[str, _HeldEditVerdict]
    _level_edit_settle_s: float
    # Snapshots of a key whose levels verdict is held, read after the change
    # was confirmed: the start of the newest read, and levels other than the
    # verdict's that reads showed in a row (levels, consecutive reads).
    _held_level_views: dict[str, float]
    _held_level_changes: dict[str, tuple['ProtectionLevels', int]]
    # Start of the snapshot read the guard is judging, for its report hook.
    _judged_read_started_at: float | None

    # --- Foreign exposure (foreign.py) ---
    # Foreign deals already logged once.
    _foreign_deals_logged: set[str]
    # ``(reason, key, foreign deal ids)`` refusals already reported.
    _foreign_blocks_reported: set[tuple]
    # The last poll's ``/positions`` rows by ``dealId`` and its read start.
    _positions_snapshot: tuple[float, dict[str, dict]] | None
    # Every deal a ``/positions`` read showed recently: ``dealId`` ->
    # ``(createdDateUTC epoch, direction, last seen at)``.
    _seen_position_deals: dict[str, tuple[float, str, float]]
    # Own partial exits deals this run did not open block, by intent key; and
    # the ones whose block latched.
    _foreign_exit_blocks: dict[str, _ForeignExitBlock]
    _foreign_exit_latched: set[str]
    # Own working orders retired because their execution only netted a
    # foreign deal, by the order's ``dealId`` (loaded lazily from the audit
    # events, see ``activity.py``); and the deals such an order opened with
    # the rest of its size that were already reported.
    _netting_retired_orders: dict[str, dict] | None
    _residual_deals_reported: set[str]

    # --- Account mode ---
    # ``True`` once ``connect()`` reads ``hedgingMode`` from the account
    # preferences. Drives the one-way emulation path: ``connect()`` sets
    # ``position_port = self`` so the core OneWayEmulator decomposes
    # close/reversal/bracket over the per-dealId rows; state reads
    # aggregate through the core emulator helpers. A one-way (netting)
    # account keeps the direct execute path unchanged.
    _hedging_enabled: bool

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

    def _call_serialized_write(
            self, endpoint: str, *, data: dict | None = None,
            method: str = 'post',
    ) -> dict: ...

    async def _submit_exclusive_partial_close(
            self, *, coid: str, symbol: str, expected_direction: str,
            target_deal_ids: frozenset[str], body: dict,
            intent_qty: float, lot_step: float,
    ) -> tuple[dict, list[dict]]: ...

    async def _submit_netting_safe_entry(
            self, *, coid: str, symbol: str, side: str, body: dict,
    ) -> dict: ...

    def create_session(self) -> None: ...

    def _perform_session_login(self) -> None: ...

    async def get_preferences(self) -> dict: ...

    async def _detect_account_mode(self) -> None: ...

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

    def _synth_from_quote(self, quote: _QuoteSnapshot) -> OHLCV | None: ...

    def _extra_fields(self) -> dict[str, float] | None: ...

    # --- Execution (execution.py) ---
    # PositionPort transport surface — real bodies on the execution mix-in;
    # declared here so the assembled plugin structurally satisfies the
    # protocol and ``_detect_account_mode`` can assign
    # ``self.position_port = self`` on a hedging-mode account.
    async def fetch_raw_positions(self, symbol: str) -> 'list[PositionLeg]': ...

    async def get_volume_quantizer(
            self, symbol: str,
    ) -> 'Callable[[float], int]': ...

    async def close_leg(
            self, symbol: str, leg_id: str, volume: int, coid: str,
    ) -> None: ...

    async def reject_out_of_range(
            self, envelope: 'DispatchEnvelope', qty: float,
    ) -> None: ...

    async def place_leg(
            self, envelope: 'DispatchEnvelope', qty: float,
    ) -> list[ExchangeOrder]: ...

    async def amend_bracket(
            self, symbol: str, leg_id: str, *,
            side: str, tp_price: float | None, sl_price: float | None,
            trail_offset: float | None, coid: str,
    ) -> None: ...

    async def _execute_close_partial(self, *args: Any, **kwargs: Any) -> ExchangeOrder: ...

    @staticmethod
    def _row_to_exchange_order(
            row: 'OrderRow', intent: EntryIntent,
    ) -> ExchangeOrder: ...

    # --- Activity polling (activity.py) ---
    def _poll_once(self) -> AsyncIterator[OrderEvent]: ...

    def _process_activity(
            self,
            *args: Any,
            **kwargs: Any,
    ) -> AsyncIterator[OrderEvent]: ...

    def _activity_to_event(self, *args: Any, **kwargs: Any) -> OrderEvent | None: ...

    def _find_active_entry_row(self, *args: Any, **kwargs: Any) -> 'OrderRow | None': ...

    def _find_bracket_leg_row(self, *args: Any, **kwargs: Any) -> 'OrderRow | None': ...

    def _retire_netted_journal_exposure(
            self, closed_row: 'OrderRow', retired_exposure: float,
    ) -> None: ...

    def _find_promoted_deal_id(
            self, *args: Any, **kwargs: Any,
    ) -> str | None: ...

    def _promote_working_row_to_position(
            self, *args: Any, **kwargs: Any,
    ) -> None: ...

    def _note_residual_after_netting_retirement(
            self, wo_id: str, deal_id: str, activity_ts: float,
            positions_by_deal: dict[str, dict] | None, details: dict,
    ) -> bool: ...

    # --- Bracket lifecycle (bracket.py) ---
    def _close_bracket_after_natural_close(self, entry_row: 'OrderRow') -> None: ...

    def _rollback_bracket_legs(self, *args: Any, **kwargs: Any) -> None: ...

    def _mark_bracket_legs_disposition_unknown(self, *args: Any, **kwargs: Any) -> None: ...

    @staticmethod
    def _levels_match(
            expected: object, actual: object, tol: float = 1e-6,
    ) -> bool: ...

    def _resolve_bracket_leg_disposition(self, *args: Any, **kwargs: Any) -> None: ...

    def _record_bracket_resolution(self, *args: Any, **kwargs: Any) -> None: ...

    async def _trailing_activation_monitor(self, *args: Any, **kwargs: Any) -> Any: ...

    # --- External intervention (intervention.py) ---
    def _protection_write(
            self, deal_id: str, body: dict,
    ) -> AbstractContextManager[_ProtectionWrite]: ...

    def _own_close(
            self, deal_ids: Iterable[str], *, full: bool, size: float | None,
    ) -> AbstractContextManager[_OwnCloseHandle]: ...

    def _own_close_earliest(self, sent_at: float) -> float: ...

    def _unconfirmed_reverse_leg(
            self, positions: Iterable[dict], *, coid: str, intent_key: str | None, symbol: str,
            direction: str, target_deal_id: str, size: float, lot_step: float,
            sent_at: float, done_at: float | None,
    ) -> str | None: ...

    def _forget_protection(self, deal_id: str) -> None: ...

    def _claim_failsafe_protection(self, deal_ids: Iterable[str]) -> None: ...

    async def _observe_protection(
            self, positions_by_deal: dict[str, dict],
            read_started_at: float | None,
    ) -> None: ...

    def _stamp_protection_adopt(self, coid: str, after: float) -> None: ...

    async def _venue_protection_is_own(self, deal_id: str, pos: dict) -> bool: ...

    def _rewrite_extras(self, coid: str, mutate: Callable[[dict], Any]) -> None: ...

    def _stamp_own_cancel_accepted(self, coid: str, sent_at: float) -> None: ...

    def _own_cancel(self, coid: str) -> AbstractContextManager[float]: ...

    @staticmethod
    def _own_cancel_explains(row: 'OrderRow', activity_ts: float) -> bool: ...

    @staticmethod
    def _venue_end_of_working_order(activity: dict) -> str | None: ...

    def _note_working_order_end(self, row: 'OrderRow', activity: dict) -> bool: ...

    def _own_close_cursor_retired(self, row: 'OrderRow', activity: dict) -> bool: ...

    def _attribute_close_activity(
            self, row: 'OrderRow', activity: dict,
            positions_by_deal: dict[str, dict] | None = None,
    ) -> str: ...

    def _report_manual_close(self, row: 'OrderRow', activity: dict) -> None: ...

    def _send_manual_close_report(self, report: dict) -> None: ...

    def _send_pending_manual_close_reports(self) -> None: ...

    def _reload_netting_pending(self) -> None: ...

    def _resolve_netting_pending(
            self, working_by_deal: dict[str, dict], read_started_at: float | None,
    ) -> None: ...

    def _note_venue_clock(self, activities: Iterable[dict], read_at: float) -> None: ...

    def _note_level_edit(self, activity: dict) -> None: ...

    def _resolve_held_edit_verdicts(self) -> None: ...

    def _reload_held_edit_verdicts(self) -> None: ...

    def _defer_protection_write(self, deal_id: str, *, intent_key: str, symbol: str) -> None: ...

    def _note_own_protection_write(self, deal_id: str, sent_at: float, done_at: float) -> None: ...

    def _own_write_in_flight(self, key: str) -> AbstractContextManager[None]: ...

    def _venue_time_floor(self, local_ts: float) -> float: ...

    def _working_order_write(
            self, row: 'OrderRow', level: float, *, previous_level: float | None,
    ) -> AbstractContextManager[_ProtectionWrite]: ...

    def _forget_working_order_guard(self, deal_id: str | None) -> None: ...

    async def _observe_working_orders(
            self, working_by_deal: dict[str, dict],
            read_started_at: float | None,
    ) -> None: ...

    # --- Foreign exposure (foreign.py) ---
    def _owned_deal_ids(self, positions: Iterable[dict]) -> set[str] | None: ...

    def _foreign_deals(
            self, positions: Iterable[dict], symbol: str, *, direction: str | None = None,
    ) -> list[dict]: ...

    def _note_foreign_deals(self, positions: Iterable[dict], symbol: str) -> None: ...

    def _remember_positions_snapshot(
            self, positions_by_deal: dict[str, dict], read_started_at: float,
    ) -> None: ...

    def _cached_foreign_deals(self, symbol: str, direction: str) -> list[dict]: ...

    def _exit_blockers(
            self, positions: Iterable[dict], symbol: str, direction: str,
            target_deal_ids: Iterable[str],
    ) -> list[dict]: ...

    def _cached_exit_blockers(
            self, symbol: str, direction: str, target_deal_ids: Iterable[str],
    ) -> list[dict]: ...

    def _cached_deal_units(self, deal_ids: set[str], lot_step: float) -> int | None: ...

    def _other_run_deal_ids(self, symbol: str, deal_ids: Iterable[str]) -> list[str]: ...

    @staticmethod
    def _partial_close_block(foreign: list[dict]) -> str: ...

    def _foreign_block_reported(self, reason: str, key: str, foreign: list[dict]) -> bool: ...

    def _foreign_exit_cached_refusal(
            self, reason: str, key: str, foreign: list[dict],
    ) -> bool: ...

    def _foreign_exit_latch_due(self, intent_key: str) -> bool: ...

    def _foreign_exit_unblocked(self, intent_key: str) -> None: ...

    def _foreign_exit_skip(
            self, *, reason: str, intent_key: str, symbol: str, foreign: list[dict],
            write: dict, fresh: bool, direction: str, targets: Iterable[str],
    ) -> OrderSkippedByPlugin: ...

    def _promotion_conflict(
            self, side: str, deal_id: str, activity_ts: float,
            positions_by_deal: dict[str, dict] | None,
    ) -> str | None: ...

    def _report_own_order_netted_foreign_deal(
            self, wo_row: 'OrderRow', wo_id: str, deal_id: str, conflict: str,
            positions_by_deal: dict[str, dict] | None,
    ) -> None: ...

    def _report_foreign_block(
            self, *, reason: str, key: str, symbol: str, foreign: list[dict],
            write: dict, latch: bool,
    ) -> str: ...

    def _foreign_block_skip(
            self, *, reason: str, intent_key: str, symbol: str, foreign: list[dict],
            write: dict, latch: bool,
    ) -> OrderSkippedByPlugin: ...

    def _report_unattributed_opposite(
            self, *, coid: str, intent_key: str, symbol: str,
            opposite_deal_ids: list[str], attributable: bool, context: dict,
    ) -> None: ...

    # --- Reconcile (reconcile.py) ---
    def _reconcile_snapshot(
            self,
            *args: Any,
            **kwargs: Any,
    ) -> AsyncIterator[OrderEvent]: ...

    def _missing_pending_tracker(
            self,
            *args: Any,
            **kwargs: Any,
    ) -> AsyncIterator[OrderEvent]: ...

    def _disappearance_tracker(self) -> 'DisappearanceTracker': ...

    _connection_generation: int

    # --- Recovery (recovery.py) ---
    async def _load_activity_cursor_from_events(self) -> None: ...

    async def _recover_in_flight_submissions(self) -> None: ...

    async def _recover_submitted_row(self, *args: Any, **kwargs: Any) -> None: ...

    async def _recover_server_ref_seen_row(self, *args: Any, **kwargs: Any) -> None: ...

    def _retire_startup_orphans(self, *args: Any, **kwargs: Any) -> None: ...
