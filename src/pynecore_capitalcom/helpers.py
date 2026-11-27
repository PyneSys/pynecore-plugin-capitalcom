"""Module-level constants and stateless helpers.

Bundles three loosely related concerns that share one trait — none of them
touch class state, all are pure functions / constants:

* Endpoint constants (live + demo REST hosts, WS URL, prefix).
* Lookup tables (timeframe map, asset-type label map).
* Error-code frozensets used by ``_RestSessionMixin._map_exception``.
* Measured TradingView session anchors of the epics whose TradingView
  session no Capital.com boundary marks.
* Pure helpers: password encryption, opening-hours parser and weekly
  template repair, error-code +
  numeric-value extractors, ISO timestamp parser, order-type-from-row
  resolver.

Imported by ``_base.py`` and many mix-ins; depends only on stdlib and
``exceptions.py``.
"""
import json
import math
import os
from base64 import standard_b64decode, standard_b64encode, urlsafe_b64decode
from collections import Counter
from collections.abc import Container, Mapping
from datetime import UTC, datetime, time
from decimal import Decimal
from time import time as epoch_time
from typing import TYPE_CHECKING, Literal

from Crypto.Cipher import PKCS1_v1_5
from Crypto.PublicKey import RSA

from pynecore.core.broker.models import OrderType, PositionLeg

from .exceptions import CapitalComError

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow, RunContext


URL = 'https://api-capital.backend-capital.com'
URL_DEMO = 'https://demo-api-capital.backend-capital.com'

WS_URL = 'wss://api-streaming-capital.backend-capital.com/connect'

ENDPOINT_PREFIX = '/api/v1/'


def rest_url(demo: bool, override: str = "") -> str:
    """Return the REST base URL for the demo or live system.

    A per-process override (``PYNE_CAPITALCOM_REST_URL``) wins over the
    config override, which wins over the venue hosts.

    :param demo: ``True`` for the demo host, ``False`` for live.
    :param override: The config's ``rest_url`` field.
    :return: Base URL without the endpoint prefix.
    """
    return (os.environ.get("PYNE_CAPITALCOM_REST_URL") or override
            or (URL_DEMO if demo else URL)).rstrip("/")


def ws_url(override: str = "") -> str:
    """Return the streaming URL, honouring the same override order as :func:`rest_url`.

    :param override: The config's ``ws_url`` field.
    :return: WebSocket URL.
    """
    return os.environ.get("PYNE_CAPITALCOM_WS_URL") or override or WS_URL

TIMEFRAMES = {
    '1': 'MINUTE',
    '5': 'MINUTE_5',
    '15': 'MINUTE_15',
    '30': 'MINUTE_30',
    '60': 'HOUR',
    '240': 'HOUR_4',
    '1D': 'DAY',
    '1W': 'WEEK'
}

TIMEFRAMES_INV = {v: k for k, v in TIMEFRAMES.items()}

TYPES: dict[str, Literal['forex', 'crypto', 'stock', 'index']] = {
    'CURRENCIES': 'forex',
    'CRYTOCURRENCIES': 'crypto',
    'SHARES': 'stock',
    'INDICES': 'index',
}

# TradingView's own session start of every trading day, per epic, for the epics
# whose TradingView session no Capital.com opening-hours boundary marks: epic ->
# (timezone of the table, ((weekday, open time), ...)), weekdays 0=Mon..6=Sun in
# that timezone. ``update_symbol_info`` converts the entries to the SymInfo
# timezone. An epic missing here gets no explicit anchors, and PyneCore derives
# them from the session starts (right for the index, FX, metal and energy CFDs,
# whose TradingView session starts on a quarter hour at or just before the
# Capital.com reopening).
#
# MEASURED from TradingView on 2026-11-27: the chart session info (session string,
# timezone, trading days) of each symbol, checked against the start of its
# intraday bar grid on 2-240 minute charts.
# - CAPITALCOM:AU200: session ``0930-0700`` Australia/Sydney, Mon-Fri. The overnight
#   session opening Monday 09:30 belongs to TradingView's Tuesday trading day; the
#   open instants are Mon..Fri 09:30 Sydney. Capital.com reopens at 22:50 UTC and
#   pauses 05:30-06:10 UTC, and neither restarts TradingView's grid.
# - CAPITALCOM:AAPL, CAPITALCOM:TSLA: session ``0930-1600`` America/New_York,
#   Mon-Fri, while Capital.com trades these share CFDs from 01:00 UTC.
# - CAPITALCOM:BTCUSD: session ``1700-1700`` America/New_York, every day. Neither the
#   daily 22:00-22:05 UTC pause nor the Saturday maintenance reopening restarts the
#   grid.
_WEEKDAYS_MON_FRI = (0, 1, 2, 3, 4)
TRADINGVIEW_SESSION_ANCHORS: dict[str, tuple[str, tuple[tuple[int, time], ...]]] = {
    'AU200': ('Australia/Sydney', tuple((d, time(9, 30)) for d in _WEEKDAYS_MON_FRI)),
    'AAPL': ('America/New_York', tuple((d, time(9, 30)) for d in _WEEKDAYS_MON_FRI)),
    'TSLA': ('America/New_York', tuple((d, time(9, 30)) for d in _WEEKDAYS_MON_FRI)),
    'BTCUSD': ('America/New_York', tuple((d, time(17, 0)) for d in range(7))),
}


def _is_session_error(code: str) -> bool:
    """Return True when ``code`` says the venue session behind our tokens is gone.

    The venue publishes no error-code list and the session family keeps
    growing (``error.security.client-token-missing``, ``error.null.client.token``,
    ``error.invalid.session.token``; ``error.security.session-expired`` first
    appeared 212 lab cycles in), so the class is matched by shape instead of
    enumerated: every ``error.security.*`` code plus anything naming the
    session or the client token. A permanent credential problem is not in
    this class (``_AUTH_ERROR_CODES``, checked first by the classifier), and a
    misclassified permanent ``error.security.*`` code stays bounded: the
    reactive re-login retries at most three times and surfaces the bootstrap
    error itself.
    """
    return (
        code.startswith('error.security.')
        or 'session' in code
        or 'client.token' in code
        or 'client-token' in code
    )


_AUTH_ERROR_CODES = frozenset({
    'error.null.api.key',
    'error.invalid.details',
    'error.invalid.accountId',
})

_NOT_FOUND_DEAL_ID_CODE = 'error.not-found.dealId'
_NOT_FOUND_DEAL_REF_CODE = 'error.not-found.dealReference'
_INVALID_STOP_MIN_PREFIX = 'error.invalid.stoploss.minvalue'
_INVALID_STOP_MAX_PREFIX = 'error.invalid.stoploss.maxvalue'
_INVALID_TP_MIN_PREFIX = 'error.invalid.takeprofit.minvalue'
_INVALID_TP_MAX_PREFIX = 'error.invalid.takeprofit.maxvalue'
_INVALID_LEVERAGE_CODE = 'error.invalid.leverage.value'

# ``rejectReason`` tokens / substrings on a REJECTED confirm that mean the
# account cannot fund or risk-clear the order (insufficient balance, margin,
# leverage cap, or a generic risk-engine veto). These map to the typed
# :class:`InsufficientMarginError` — a non-terminal reject the runner survives
# (see its docstring) so the strategy can respond on a later bar.
_FUNDS_REJECT_REASON_SUBSTRINGS = ('MARGIN', 'LEVERAGE', 'FUND', 'RISK', 'BALANCE')
# Capital.com phrases a session-pause reject as free text, e.g. ``Rejected.
# BTCUSD is currently closed. Timetable in place: UTC; Mon - 21:00, 21:05 -; ...``
# (measured live), both as a POST/DELETE ``errorCode`` and as a confirm
# ``rejectReason``.
_MARKET_CLOSED_REJECT_SUBSTRINGS = ('CURRENTLY CLOSED', 'MARKET IS CLOSED', 'MARKET CLOSED',
                                    'MARKET.CLOSED', 'TRADING HOURS')
# A ``PUT /positions/{dealId}`` on a position the venue already closed (its
# stop or limit fired) is answered with a REJECTED confirm carrying this
# ``rejectReason`` rather than an HTTP 404 (measured live, 2026-10-30).
_POSITION_GONE_REJECT_REASONS = ('POSITION_NOT_FOUND',)


# --- Internal tuning constants ---------------------------------------------
#
# These are intentionally NOT exposed in :class:`CapitalComConfig` (and
# therefore not in ``workdir/config/plugins/capitalcom.toml``). They are
# rate-limit / cache / WS-feed heuristics tuned once for Capital.com's
# specific server behaviour and are not something an end user can
# meaningfully reason about. If real operator intent ever surfaces to
# override one, promote it through the config writer's hidden-field
# mechanism — not by reintroducing a docstring'd dataclass field.

# Seconds between ``GET /positions`` + ``GET /workingorders`` polls.
# Capital.com's 10 req/s budget easily accommodates 1s cadence, but 1.5s
# leaves headroom for the snapshot + activity tail + any ad-hoc reconcile
# calls the sync engine issues.
_POLL_INTERVAL_S = 1.5

#: Lookback the activity poll asks for beyond the time since its last
#: successful read: the venue publishes an activity row up to a minute after
#: the deal it stamps.
_ACTIVITY_LAG_S = 60

#: How long an own entry deal may sit in ``/positions`` with its confirmed
#: fill unbooked before the snapshot books it in place of the deal's
#: ``POSITION`` activity. Measured on 5896 activity rows (2026-10-29 ..
#: 2026-11-12): first seen a median 1.5 s and a p99 4.6 s after the deal, so
#: twice the p99 leaves the activity its normal path.
_UNANNOUNCED_FILL_GRACE_S = 10.0

#: Largest ``lastPeriod`` ``GET /history/activity`` accepts (one day).
_ACTIVITY_MAX_PERIOD_S = 86400

#: Shortest time between two activity lookups of one inherited entry
#: submission the engine holds (``reconcile.py``,
#: ``_resolve_inherited_submissions``). Each lookup is one extra
#: ``/history/activity`` read beside the poll's own three reads per
#: :data:`_POLL_INTERVAL_S`; at this spacing the three conclusive absences the
#: resolver needs come from reads at least 30 s apart, and a held row costs
#: one extra read per ten polls.
_INHERITED_LOOKUP_INTERVAL_S = 15.0

#: ``orders.extras`` key stamped on a parked (``disposition_unknown``) entry
#: row when a cancel intent reached it before the venue handed back a deal
#: id. The parked-submission resolver honours it the moment the id lands:
#: a working order is DELETEd right away, a filled market entry makes the
#: cancel a no-op. Value: ``{'cancel_coid': ..., 'intent_key': ...}``.
EXTRAS_KEY_CANCEL_REQUESTED = 'cancel_requested'

#: ``orders.extras`` key stamped on a working entry row whose deal id was
#: learned only after the venue may already have executed it: a parked or
#: inherited submission taken over (``reconcile.py``,
#: ``_take_over_parked_submission``). The execution's activity may have been
#: read while no row owned the order, so until the fill is booked every
#: snapshot reconcile that misses the order looks for the position that
#: back-links it. Value: epoch seconds of the take-over.
EXTRAS_KEY_TAKEN_OVER_AT = 'taken_over_at'

#: ``orders.extras`` key stamped on a taken-over entry row (``reconcile.py``,
#: ``_take_over_parked_submission``) whose position the ``/positions``
#: snapshot shows at a size other than the submitted quantity. Such a fill is
#: not booked: the position may have been reduced before the snapshot, and no
#: venue evidence tells which of the close activities that may still reach the
#: row the snapshot already shows. The row stays unbooked, the engine keeps
#: the entry held, a stamped working row is exempt from the disappearance
#: tracker (its absence from ``/workingorders`` is the execution, not a
#: cancel), the activity poll books none of the row's activities (no
#: promotion, fill or cancel through its order id or a position back-linking
#: it), the recovery pass of a reconnect or a fresh process neither confirms,
#: moves nor retires the row (``recovery.py``, ``_hold_unproven_take_over``),
#: an engine cancel or amend of the entry is not sent and does not settle
#: (``execution.py``, ``_hold_take_over_dispatch``: the cancel reports not
#: landed, the amend returns the row unchanged, nothing is re-sent), the
#: in-run lookup of an inherited row never reads it as absent (``recovery.py``,
#: ``_inherited_submission_verdict``). Its deal came from the bot's own order:
#: it is own exposure in every position read (``foreign.py``,
#: ``_held_take_over_refs``), while no close or leg close is sent to it before
#: the fill is booked (its close activity could not be attributed to the bot).
#: The stamp is the bot's own trouble, written with one warning: the poll proves
#: the fill from the venue's activity history and books it, then the deal's
#: later closes (``order_history.py``, ``_resolve_unproven_take_overs``).
#: Value: ``{'deal_id': ..., 'size': ..., 'at': ...}``.
EXTRAS_KEY_TAKE_OVER_UNPROVEN = 'take_over_unproven'

#: ``orders.extras`` key on an entry row handed to the engine's held-execution
#: recovery (``order_history.py``, ``_hand_over_to_engine``): the submission
#: landed on a deal that is closed since, so the stream will never deliver its
#: execution. The engine took the row over
#: (:meth:`~pynecore.core.plugin.broker.BrokerPlugin.request_execution_hold`),
#: books the deal's opening from
#: :meth:`~pynecore_capitalcom.order_history._OrderHistoryMixin.fetch_order_execution`
#: and the venue's flat book as an external reduction. While the stamp stands
#: the deal's absence from the venue is no disappearance (``reconcile.py``,
#: ``_is_disappearance_exempt``), and once the engine no longer holds the
#: order the bot tears the row down (``_settle_execution_holds``).
#: Value: ``{'deal_id': ..., 'at': ...}``.
EXTRAS_KEY_EXECUTION_HELD = 'execution_held'

#: ``orders.extras`` key on a position row: the own closes sent against the
#: position, each ``{'full', 'size', 'sent_at', 'done_at', 'matched'}``, plus
#: ``'coid'`` and ``'leg_size'`` (the whole leg's venue size) on a core leg
#: close. A close activity one of them explains is the bot's own, not an
#: operator's.
EXTRAS_KEY_OWN_CLOSES = 'own_close_expected'

#: ``orders.extras`` key on a position row: the ``coid`` of the own close whose
#: fill carries the core's client order id (a core leg close, or a
#: ``held_execution_close``) and whose DELETE the venue accepted, written in the
#: same transaction that marks the row ``closing``. The close fill booked from
#: the position's disappearance reports it as its client order id.
EXTRAS_KEY_CLOSE_LEG_DISPATCHED = 'close_leg_dispatched_coid'

#: ``orders.extras`` key on a position row a ``held_execution_close`` targets:
#: ``{'coid': ..., 'pine_id': ..., 'size': ...}``, the close's client order id,
#: its Pine id and the deal's venue size the close was sized against, written
#: before the DELETE leaves (``execution.py``, ``_execute_held_execution_close``).
#: A close fill of the row reported under that ``coid`` carries that Pine id
#: (:func:`_held_close_label`), so the engine books it as the held-execution
#: close it is, capped at what its book holds.
EXTRAS_KEY_HELD_CLOSE = 'held_execution_close'

#: ``orders.extras`` key on a position row the plugin seeded for a deal of an
#: entry the engine holds for its execution evidence that no live journal row
#: tracks, so a ``held_execution_close`` can close it and the close's activity
#: routes to a row (``execution.py``, ``_held_close_row``). Its
#: ``filled_qty`` stays ``0.0``: the row books no exposure of the run, the
#: engine's book does not hold the execution. Value: epoch seconds of the seed.
EXTRAS_KEY_HELD_CLOSE_SEEDED = 'held_execution_close_seeded'

#: ``orders.extras`` key on a position row: epoch seconds since the core
#: native fail-safe manager writes the position's stop. The protection guard
#: leaves such a position to the manager.
EXTRAS_KEY_FAILSAFE_OWNED = 'native_failsafe_owned_at'

#: ``orders.extras`` key on a position row: the next ``/positions`` read that
#: starts after this epoch second becomes the position's accepted protection
#: (set after a partial close).
EXTRAS_KEY_PROTECTION_ADOPT_AFTER = 'protection_adopt_after'

#: ``orders.extras`` key on a position row: epoch seconds since the core native
#: fail-safe manager owned the position's protection while it had a software
#: partial-bracket leg. The guard forgot the bot's levels then; once the leg is
#: gone, the venue's protection is the bot's (the manager's, or the bot's own
#: unguarded writes) and becomes the accepted baseline again.
EXTRAS_KEY_PROTECTION_HANDOVER = 'protection_failsafe_handover_at'

#: ``orders.extras`` key on a working-order row: the foreign deal the order
#: netted when it executed, ``{'deal_id': ..., 'conflict': ...}``. The order's
#: fill is then not the run's exposure.
EXTRAS_KEY_NETTED_FOREIGN_DEAL = 'netted_foreign_deal'

#: ``orders.extras`` key on a position row: the held execution of an own
#: working order that netted a foreign deal and opened this deal with the rest,
#: while its opened size or a price is unknown. Nothing of the execution is
#: booked meanwhile, and the row's ``filled_qty`` stays ``0``.
#: ``{'executed_qty', 'fill_price', 'fill_id', 'netted_deal_id', 'since'}``
#: plus, as the evidence arrives, ``'opened_qty'`` (size proven by the deal's
#: opening activity), ``'listed'`` (``{'size', 'at'}``, first ``/positions``
#: listing of the current size) and ``'held_closes'`` (close identity ->
#: ``{'size', 'seen_at', 'sizeless_id', 'date_utc'}``).
#:
#: ===================  ==========================================  =====================
#: State                Event                                       Next
#: ===================  ==========================================  =====================
#: not held             execution, opened size unknown              size unknown
#: not held             execution, size proven, no price            size proven
#: size unknown         sized opening activity, no price            size proven
#: size unknown         sized opening activity, priced              booked (key dropped)
#: size unknown         listing unchanged for ``_ACTIVITY_LAG_S``,  booked (listed + held
#:                      every held close sized and seen before            closes)
#:                      the read
#: size proven          priced opening activity or listing          booked (key dropped)
#: either held state    close of the deal                           same, close recorded
#: ===================  ==========================================  =====================
#:
#: A held close is deferred without consuming its fingerprint, and every
#: activity read reaches back to it, so it is booked from its activity once
#: the entry is. Booking drops the key in the transaction that raises
#: ``filled_qty``.
EXTRAS_KEY_NETTED_RESIDUAL_PENDING = 'netted_residual_pending'

#: ``orders.extras`` key on a position row: the closes a held partly netted
#: entry deferred, still unbooked after the entry was, and the fills held
#: behind an entry deferred for its price (:data:`EXTRAS_KEY_ENTRY_PRICE_PENDING`),
#: still unbooked, ``{close_id: dateUTC}``.
#: The activity read keeps reaching back to them until each close's identity
#: is booked (:data:`EXTRAS_KEY_BOOKED_CLOSES`); booking the close drops it.
EXTRAS_KEY_DEFERRED_CLOSES = 'deferred_close_dates'

#: ``orders.extras`` key on a position row: the ``dateUTC`` of its entry
#: activity, deferred because no source priced it, while the entry is not
#: booked. Every later fill of the deal (a reduction or close) is deferred
#: behind it, so the book never reduces an entry it has not received; the
#: activity read keeps reaching back to the entry, and the first booking of the
#: entry (``entry_filled_at``) drops the key.
EXTRAS_KEY_ENTRY_PRICE_PENDING = 'entry_price_pending'

#: ``orders.extras`` key on a position or working-order row: the ``dateUTC`` of
#: its entry's execution activity, kept once the entry is booked. The booking
#: stamp (``entry_filled_at``) is the local time of the booking, which trails
#: the execution by however late its priced activity was read; what the venue
#: did at a given time is judged by this date.
EXTRAS_KEY_ENTRY_EXECUTED_UTC = 'entry_executed_date_utc'

#: ``orders.extras`` key on a position row: the partial-close commands whose
#: reduction the run-owned exposure cursor (``filled_qty``) already reflects.
EXTRAS_KEY_RETIRED_CLOSE_COMMANDS = 'partial_close_retired_commands'

#: ``orders.extras`` key on a position row: the close activities already
#: attributed for the position, ``{close_id: verdict}`` (see
#: :func:`_close_identity`). A re-issued activity of the same close — the venue
#: republishes a level-less row with its level filled in, under a new
#: fingerprint — reuses the verdict instead of being judged (and booked) again.
EXTRAS_KEY_CLOSE_VERDICTS = 'close_verdicts'

#: ``orders.extras`` key on a working-order row: the level the bot placed the
#: order at. The working-order guard judges the venue's ``orderLevel`` against
#: it (or against ``amended_level`` once an own amend landed).
EXTRAS_KEY_ORDER_LEVEL = 'order_level'

#: ``orders.extras`` key on a position row: level-independent identities of
#: the close fills already booked for the position (see :func:`_close_identity`
#: in ``models.py``). A copy of a booked close is not booked again.
EXTRAS_KEY_BOOKED_CLOSES = 'booked_close_ids'

#: Booked close identities kept per position row.
BOOKED_CLOSES_RETAINED = 64

#: ``orders.extras`` key on a working-order row: epoch seconds the bot sent
#: the ``DELETE /workingorders/{dealId}`` the venue accepted (not "already
#: gone"). The order was still resting when the venue took the request.
EXTRAS_KEY_OWN_CANCEL_SENT_AT = 'own_cancel_accepted_sent_at'

#: ``orders.extras`` key on a working-order row: the epoch seconds of every own
#: ``DELETE /workingorders/{dealId}`` sent and not refused, recorded before the
#: request leaves. The venue stamps the bot's own cancel ``source=USER`` like
#: an operator's; one of these explains it.
EXTRAS_KEY_OWN_CANCEL_REQUESTS = 'own_cancel_requested_at'

#: ``orders.extras`` key on a working-order row: the cancel or expiry of the
#: resting order the activity feed showed, ``{'by', 'status', 'source',
#: 'dateUTC'}``; ``by`` is ``'venue'`` (a ``SYSTEM`` cancel, an expiry),
#: ``'dealer'`` (the dealing desk) or ``'client'`` (a ``USER`` cancel). The
#: disappearance verdict retires an order the venue or its dealing desk ended
#: as the venue's, never as an unexpected cancel.
EXTRAS_KEY_WORKING_ORDER_END = 'working_order_end'

#: Key on an own-close record (:data:`EXTRAS_KEY_OWN_CLOSES`): the run's
#: exposure cursor already dropped by this partial close, so a process that
#: adopts the venue size after a restart must not book its activity as a fill.
OWN_CLOSE_CURSOR_RETIRED = 'cursor_retired'

#: Key on a core leg close's own-close record (one carrying a ``'coid'``,
#: :data:`EXTRAS_KEY_OWN_CLOSES`): the wall-clock start of a ``/positions``
#: read that still listed the deal after the DELETE could no longer land, so
#: the DELETE did not close it. A later disappearance is not that close.
OWN_CLOSE_LEG_OUTLIVED = 'leg_close_outlived_at'

#: ``orders.extras`` key on a working-order row: the order attributes the bot
#: never sets that an operator resume accepted as the venue showed them,
#: ``{'attributes': [[field, value], ...], 'resume_ts_ms': ...}``.
EXTRAS_KEY_WORKING_ACCEPTED_ATTRIBUTES = 'working_order_accepted_attributes'

#: ``orders.extras`` key on a position or working-order row: the bot's own
#: writes of the row's protection or order level that the venue did not
#: refuse, by protection key, ``{key: [[sent_at, done_at], ...]}`` (wall
#: clock, newest last). The venue stamps each one with a ``USER`` edit
#: activity, like someone else's edit; one record explains one such activity.
EXTRAS_KEY_OWN_LEVEL_WRITES = 'own_level_writes'

#: ``orders.extras`` key on a position or working-order row: the confirmed
#: changes of the row's levels or attributes whose author is still being
#: read from the activity feed, ``{held id: record}``. A restarted process
#: picks them up and decides them like the process that confirmed them.
EXTRAS_KEY_HELD_EDIT_VERDICTS = 'held_edit_verdicts'

#: ``orders.extras`` key on a position or working-order row: per protection
#: key, the venue time of the venue or dealing-desk edit a verdict accepted.
#: That edit, and every older one, explains no later change.
EXTRAS_KEY_LEVEL_EDITS_CONSUMED = 'level_edits_consumed_until'

#: ``orders.extras`` key on a pending trailing stop leg a bracket amend wrote:
#: the amend command's COID. The activation monitor journals it with the
#: activation, so the amend's verdict counts only an activation of its own
#: pending trail, never one of an earlier amend under the same leg row.
EXTRAS_KEY_TRAIL_COMMAND = 'trail_command_coid'


def _trail_activation_key(leg_coid: str, command_coid: str | None) -> str:
    """The deferred-park key of an accepted trailing activation whose journal write failed.

    :param leg_coid: The pending trailing stop leg's COID.
    :param command_coid: The amend that wrote the pending trail
        (:data:`EXTRAS_KEY_TRAIL_COMMAND`), ``None`` for one no amend wrote.
    :return: The key under which the activation's journal transaction waits.
    """
    return f'trail-activation:{leg_coid}:{command_coid or ""}'

#: Close verdicts (``_InterventionMixin._attribute_close_activity``): who
#: closed (part of) a bot position.
CLOSE_VERDICT_OWN = 'own'
CLOSE_VERDICT_OWN_NETTING = 'own_netting'
CLOSE_VERDICT_AMBIGUOUS = 'ambiguous_netting'
CLOSE_VERDICT_DEALER = 'dealer'
CLOSE_VERDICT_FOREIGN = 'foreign'
CLOSE_VERDICT_VENUE = 'venue'
CLOSE_VERDICT_UNJUDGED = 'unjudged'
#: Only a resting own working order could explain the close; a later
#: working-order read decides between ``foreign`` and ``ambiguous_netting``.
CLOSE_VERDICT_NETTING_PENDING = 'netting_pending'

#: Verdicts whose reduction nothing of the run's own explains: the closed size
#: leaves the run's exposure cursor.
EXTERNAL_REDUCTION_VERDICTS = frozenset({
    CLOSE_VERDICT_FOREIGN, CLOSE_VERDICT_DEALER, CLOSE_VERDICT_VENUE,
})

#: Who made an activity, by its ``source`` (:func:`_activity_actor`).
ACTOR_CLIENT = 'client'
ACTOR_DEALER = 'dealer'
ACTOR_VENUE = 'venue'


def _activity_actor(source: object) -> str:
    """Who made an activity, read from its ``source``.

    The venue documents ``USER``, ``DEALER``, ``SYSTEM``, ``TP``, ``SL`` and
    ``CLOSE_OUT`` (live rows carry ``CLOSE``, ``MARGIN`` and ``STOP_OUT`` too).
    Only ``USER`` is a client's request: the bot's own requests and anyone
    else's (the web platform, another API client) carry it alike, so only an
    own-request record tells them apart. ``DEALER`` is the broker's dealing
    desk, a person at the broker acting on the account: never another client,
    so never an intervention, but a decision the operator is told about.
    Every other source, and a missing one, is the venue's own mechanics.

    :param source: The activity's ``source`` field.
    :return: :data:`ACTOR_CLIENT`, :data:`ACTOR_DEALER` or :data:`ACTOR_VENUE`.
    """
    normalized = source.upper() if isinstance(source, str) else ''
    if normalized == 'USER':
        return ACTOR_CLIENT
    if normalized == 'DEALER':
        return ACTOR_DEALER
    return ACTOR_VENUE

# TTL on cached ``_InstrumentRules`` entries. Capital.com widens the
# bracket-distance minimum during volatile sessions; unbounded
# caching would silently disable the pre-check. Five minutes bounds the
# staleness to one news-event window without flooding the rate-limited
# markets endpoint.
_INSTRUMENT_RULES_TTL_S = 300.0

# Rolling window of full-coverage WS quote counts used by the volume
# sanity checks. Drives the low-ratio outlier trigger and the
# median-estimate emitted when REST also fails. 20 bars stays responsive
# to session-level liquidity changes without being swayed by a single
# quiet bar.
_WS_VOLUME_BASELINE_BARS = 20

# Minimum baseline samples required before low-ratio and median-estimate
# paths activate. During warmup the worker emits raw WS counts; REST
# still fires for ``ws_volume == 0`` and partial-coverage cases.
_WS_VOLUME_MIN_BASELINE_BARS = 5

# Multiplier applied to the rolling median when deciding whether a
# full-coverage WS volume is low enough to warrant a REST sanity check.
# 0.20 → a bar below 20% of the median triggers REST.
_WS_VOLUME_LOW_RATIO = 0.20

# Consecutive REST-confirmed bad full-coverage bars after which the
# worker forces a WS reconnect. Two in a row implies the quote feed has
# gone silent while the OHLC subscription still ticks.
_WS_VOLUME_BAD_BAR_RECONNECT_THRESHOLD = 2


def encrypt_password(password: str, encryption_key: str, timestamp: int | None = None):
    if timestamp is None:
        timestamp = int(epoch_time())
    payload = password + '|' + str(timestamp)
    payload = standard_b64encode(payload.encode('ascii'))
    public_key = RSA.importKey(standard_b64decode(encryption_key.encode('ascii')))
    cipher = PKCS1_v1_5.new(public_key)
    ciphertext = standard_b64encode(cipher.encrypt(payload)).decode()
    return ciphertext


def _parse_opening_hours_segment(oh: str) -> tuple[time, time] | None:
    """Parse one Capital.com ``openingHours`` segment into ``(start, end)``.

    Each per-day entry has the shape ``"HH:MM - HH:MM"`` where either side may
    be ``"00:00"`` to denote midnight (open from / closed at the day boundary).
    Whitespace around the dash and around the times is optional.

    The two times are returned as :class:`datetime.time` values in the *source*
    timezone reported by Capital — the caller is responsible for converting
    them to ``self.timezone``. ``time(0, 0)`` represents midnight on either
    side; the caller uses that to decide whether to emit a session_start /
    session_end marker (midnight is the day boundary, not a real transition).

    Returns ``None`` for empty input or any segment that cannot be parsed
    (closed-day markers, multi-segment lines with stray dashes, malformed
    time strings) — invalid segments are silently skipped by the caller.
    """
    parts = oh.split('-')
    if len(parts) != 2:
        return None
    raw_start = parts[0].strip()
    raw_end = parts[1].strip()
    # A bare ``"-"`` (or any segment where one side is empty after
    # stripping) is a closed-day marker, NOT a 24h session. Defaulting an
    # empty side to ``time(0, 0)`` would round-trip to ``(00:00, 00:00)``,
    # which the caller treats as a 24h shape — silently flipping the
    # market open all day. Both sides must be explicit times; reject
    # the segment otherwise so the caller's ``parsed is None`` skip
    # fires.
    if not raw_start or not raw_end:
        return None
    try:
        start = time.fromisoformat(raw_start)
        end = time.fromisoformat(raw_end)
    except ValueError:
        return None
    return start, end


OPENING_HOURS_DAYS = ('mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun')


def _seconds_of_day(t: time, *, end: bool = False) -> int:
    """Seconds since midnight of an opening-hours time; an ``end`` at 00:00 is 24:00."""
    sec = t.hour * 3600 + t.minute * 60 + t.second
    return 86400 if end and sec == 0 else sec


def _format_opening_hours_row(row: list[tuple[time, time]]) -> list[str]:
    """Format parsed segments back into Capital.com's ``"HH:MM - HH:MM"`` shape."""
    def fmt(t: time) -> str:
        return t.strftime('%H:%M:%S' if t.second else '%H:%M')
    return [f"{fmt(start)} - {fmt(end)}" for start, end in row]


def _regular_weekly_close_day(regular: list[tuple[time, time]],
                              friday: list[tuple[time, time]]) -> list[tuple[time, time]] | None:
    """The regular shape of the weekly close day of a near-24h market in a holiday week.

    With a daily break in the regular day (a reopening segment that runs to midnight
    after a gap), the week closes at the daily close: Capital.com's normal Friday is
    the regular day without its reopening (MEASURED on the week of 2026-11-16 from
    MINUTE prices: US30 22:00, GOLD 21:59, OIL_CRUDE 22:00, EURUSD 22:00 and AU200
    21:00 UTC, each its Monday-Thursday daily close). A Friday that closes earlier,
    or does not trade, is a holiday early close and gets that shape. Without a daily
    break the weekly close time cannot be derived, so the Friday keeps its own close
    and only takes the regular day's segments before it (an opening delayed by the
    previous day's holiday closure goes back to midnight).

    :param regular: The regular weekday segments, opening and running to midnight.
    :param friday: The Friday segments of the fetched week.
    :return: The Friday segments to use, or ``None`` to keep the fetched ones.
    """
    midnight = time(0, 0)
    if (len(regular) >= 2 and regular[-1][0] != midnight
            and regular[-2][1] != midnight
            and _seconds_of_day(regular[-2][1]) < _seconds_of_day(regular[-1][0])):
        daily_close = _seconds_of_day(regular[-2][1])
        if friday and _seconds_of_day(friday[-1][1], end=True) >= daily_close:
            return None
        return regular[:-1]
    if not friday or friday[-1][1] == midnight:
        return None
    close = _seconds_of_day(friday[-1][1])
    clipped: list[tuple[time, time]] = []
    for start, end in regular:
        if _seconds_of_day(start) >= close:
            break
        clipped.append((start, end if _seconds_of_day(end, end=True) <= close else friday[-1][1]))
    return clipped


def _regular_week_opening_hours(opening_hours: Mapping) -> tuple[dict, list[str]]:
    """Keep a holiday-week exception out of the weekly opening-hours template.

    Capital.com's ``openingHours`` describe the CURRENT week, and the plugin turns them
    into the template of every week. Fetched in a holiday week, one day's exception
    would repeat weekly: in the Thanksgiving week of 2026 the US index CFDs list
    Thursday 21:00-23:00 UTC closed instead of the 22:00-22:05 pause, a 19:30 Thursday
    close for metals and energy, a Friday early close, and the share CFDs closed from
    Thursday 05:00 to Friday 01:00. The API offers no other week and no regular hours.

    HEURISTIC, applied only to near-24h markets (the regular day opens at and runs to
    midnight) and only where the weekdays that must be alike show a strict majority
    shape: Tuesday, Wednesday and Thursday always, Monday when the market trades into
    it from Sunday, and Friday when the market also trades on Saturday. A deviating
    one of those days takes the majority shape. Only in such a week, which has
    proved to hold an exception, a five-day market's Friday that closes before the
    daily close takes the regular weekly-close shape too (see
    :func:`_regular_weekly_close_day`). Weekends, daytime markets, a Friday-only
    exception and a week without a clear majority stay as fetched.

    :param opening_hours: Capital.com's ``instrument.openingHours`` mapping.
    :return: The opening hours to build the template from (the input itself when
             nothing deviates) and the day keys that were replaced.
    """
    midnight = time(0, 0)
    rows: dict[str, list[tuple[time, time]]] = {}
    for day in OPENING_HOURS_DAYS:
        parsed = (_parse_opening_hours_segment(oh) for oh in opening_hours.get(day) or [])
        rows[day] = [p for p in parsed if p is not None]

    def runs_to_midnight(row: list[tuple[time, time]]) -> bool:
        return bool(row) and row[-1][1] == midnight

    voters = ['tue', 'wed', 'thu']
    if runs_to_midnight(rows['sun']):
        voters.insert(0, 'mon')
    seven_day = bool(rows['sat'])
    if seven_day:
        voters.append('fri')
    shape, votes = Counter(tuple(rows[d]) for d in voters).most_common(1)[0]
    regular = list(shape)
    if votes * 2 <= len(voters) or not regular or regular[0][0] != midnight \
            or not runs_to_midnight(regular):
        return dict(opening_hours), []
    replaced = [d for d in voters if rows[d] != regular]
    if not replaced:
        return dict(opening_hours), []
    result = dict(opening_hours)
    for day in replaced:
        result[day] = _format_opening_hours_row(regular)
    if not seven_day:
        friday = _regular_weekly_close_day(regular, rows['fri'])
        if friday is not None and friday != rows['fri']:
            result['fri'] = _format_opening_hours_row(friday)
            replaced.append('fri')
    return result, replaced


def _extract_error_code(exc: CapitalComError) -> str:
    """Pull the ``error.*`` code out of a :class:`CapitalComError` message.

    The provider formats errors as ``"API error occured: <code>"`` — we
    split on the prefix so callers get the raw code back. Returns the
    empty string when the format does not match (defensive, since the
    formatter could change).
    """
    message = str(exc)
    prefix = "API error occured: "
    if message.startswith(prefix):
        return message[len(prefix):].split(':', 1)[0].strip()
    return message.split(':', 1)[0].strip()


def _extract_reject_reason(confirm: dict) -> str:
    """Pull the rejection reason out of a Capital.com confirm payload.

    Capital.com reports the reason under ``rejectReason`` (e.g.
    ``"RISK_CHECK"``, ``"INSUFFICIENT_FUNDS"``). The legacy ``reason`` key is
    kept as a fallback in case an alternate payload shape ever carries it.
    Returns ``"unknown"`` when neither is present.
    """
    reason = confirm.get('rejectReason') or confirm.get('reason')
    return str(reason) if reason else 'unknown'


def _is_funds_reject(reason: str) -> bool:
    """True when a Capital.com reject reason means the account cannot fund /
    risk-clear the order (insufficient balance, margin, leverage cap or a
    generic risk-engine veto), so the caller should raise the non-terminal
    :class:`~pynecore.core.broker.exceptions.InsufficientMarginError` rather than a
    plain reject."""
    reason_uc = reason.upper()
    return any(token in reason_uc for token in _FUNDS_REJECT_REASON_SUBSTRINGS)


def _is_market_closed_reject(reason: str) -> bool:
    """True when a Capital.com reject reason means the instrument is not
    tradable right now (scheduled session pause / halt), so the caller should
    raise the non-terminal :class:`~pynecore.core.broker.exceptions.MarketClosedError`
    rather than a plain reject — nothing landed and the venue reopens on its own."""
    reason_uc = reason.upper()
    return any(token in reason_uc for token in _MARKET_CLOSED_REJECT_SUBSTRINGS)


_CROSSED_WORKING_ORDER_CODES: dict[str, str] = {
    'STOP': 'error.validation.stop.price',
    'LIMIT': 'error.validation.limit.price',
}


def _is_crossed_working_order_reject(error_code: str | None, order_type: str | None) -> bool:
    """True when the venue refused a working-order entry because its level is
    already crossed by the current quote.

    Measured on the Capital.com demo (2026-11-23, ETHUSD): a working order
    carries no minimum distance — a BUY STOP one tick above the ask is
    accepted — but a STOP on the triggered side of the quote (a BUY STOP at
    or below the ask, a SELL STOP at or above the bid) is refused with
    ``error.validation.stop.price``, and a marketable LIMIT (a BUY LIMIT above
    the ask, a SELL LIMIT below the bid) with ``error.validation.limit.price``.
    Each code belongs to its own order type, so a code of the other type is
    not read as a crossed level.

    :param error_code: The ``errorCode`` the venue answered the POST with.
    :param order_type: The ``type`` field of the ``workingorders`` body.
    """
    return (error_code is not None and order_type is not None
            and _CROSSED_WORKING_ORDER_CODES.get(order_type) == error_code)


def _is_position_gone_reject(reason: str) -> bool:
    """True when a Capital.com reject reason means the targeted position no
    longer exists on the venue, so a write that only clears its bracket has
    nothing left to clear."""
    return reason.strip().upper() in _POSITION_GONE_REJECT_REASONS


def _order_type_from_row(row: 'OrderRow', activity_type: str) -> OrderType:
    """Pick an :class:`OrderType` for an event based on row + activity context.

    Bracket legs carry their intended type through ``row.extras.leg_kind``
    (``'tp'`` → LIMIT, ``'sl'`` → STOP / TRAILING_STOP). Working-order
    rows store the type under ``extras.order_type``. Everything else
    falls back to MARKET — the default for a position fill.
    """
    extras = row.extras or {}
    leg_kind = extras.get('leg_kind')
    if leg_kind == 'tp':
        return OrderType.LIMIT
    if leg_kind == 'sl':
        return OrderType.TRAILING_STOP if row.trailing_stop else OrderType.STOP
    stored = extras.get('order_type')
    if stored:
        try:
            return OrderType(stored)
        except ValueError:
            pass
    if activity_type == 'WORKING_ORDER':
        return OrderType.LIMIT
    return OrderType.MARKET


def _wire_float(value: object, *, finite: bool = False) -> float | None:
    """Parse an untyped wire value as a float.

    Booleans are rejected even though :class:`bool` is an :class:`int`
    subclass. When ``finite`` is true, NaN and infinities are rejected as
    invalid monetary / price / size inputs.
    """
    if value is None or isinstance(value, bool):
        return None
    if not isinstance(value, (str, bytes, bytearray, int, float, Decimal)):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if finite and not math.isfinite(parsed):
        return None
    return parsed


def _level_on_tick_grid(level: float, tick: float) -> bool:
    """Whether ``level`` sits on the ``tick`` price grid, within float noise.

    An unknown grid (``tick`` not positive) counts every level as on it.
    """
    if tick <= 0.0 or not math.isfinite(tick):
        return True
    steps = level / tick
    return abs(steps - round(steps)) <= 1e-6


def _venue_rendered_level(echoed: object, requested: float, tick: float) -> float | None:
    """The level a venue answer echoes for a requested order level, when it is that level.

    A level between two ticks is kept on the venue at one of its grid
    neighbours, which are less than a tick away from it; an echo further
    away is not this request's rendering.

    :param echoed: The level the venue answer carries (a confirm's ``level``).
    :param requested: The level the request sent.
    :param tick: The instrument's price tick.
    :return: The echoed level, or ``None`` when it is missing or not the
        request's rendering.
    """
    level = _wire_float(echoed, finite=True)
    if level is None or level <= 0.0 or tick <= 0.0 or not math.isfinite(tick):
        return None
    return level if abs(level - requested) < tick * (1.0 - 1e-9) else None


def _wire_int(value: object) -> int | None:
    """Parse an untyped wire value as an exact integer."""
    parsed = _wire_float(value, finite=True)
    if parsed is None or not parsed.is_integer():
        return None
    return int(parsed)


def _wire_id(value: object) -> str | None:
    """Return a non-empty string identifier from an untyped wire value."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (str, int)):
        parsed = str(value).strip()
        return parsed or None
    return None


#: ``ENTRY_BLOCK`` cause prefix of an own exit a foreign deal keeps blocked.
CAUSE_FOREIGN_EXIT_BLOCK = 'foreign_exit_block:'

#: ``ENTRY_BLOCK`` cause prefix of an own working order that netted a foreign deal.
CAUSE_NETTED_FOREIGN_DEAL = 'own_working_order_netted_foreign_deal:'

#: ``ENTRY_BLOCK`` cause prefix of a deal a retired own working order opened unbooked.
CAUSE_RESIDUAL_UNBOOKED = 'own_working_order_residual_unbooked:'


def _foreign_exit_cause(intent_key: str) -> str:
    """The ``ENTRY_BLOCK`` cause of an own exit a foreign deal keeps blocked."""
    return f"{CAUSE_FOREIGN_EXIT_BLOCK}{intent_key}"


def _netted_foreign_deal_cause(client_order_id: str) -> str:
    """The ``ENTRY_BLOCK`` cause of an own working order that netted a foreign deal."""
    return f"{CAUSE_NETTED_FOREIGN_DEAL}{client_order_id}"


def _residual_deal_cause(deal_id: str) -> str:
    """The ``ENTRY_BLOCK`` cause of a deal a retired own working order opened unbooked."""
    return f"{CAUSE_RESIDUAL_UNBOOKED}{deal_id}"


def _held_close_label(
        extras: Mapping, client_order_id: str | None,
) -> tuple[str, float | None] | None:
    """The ``held_execution_close`` a close fill of a position row reports, if it is one.

    A close fill is the held-execution close's own only when it is reported
    under that close's client order id (the own-close record of its DELETE,
    or the ``closing`` stamp
    :data:`EXTRAS_KEY_CLOSE_LEG_DISPATCHED`), which the row's
    :data:`EXTRAS_KEY_HELD_CLOSE` stamp names. Any other close of the row
    keeps the row's own labels.

    :param extras: The position row's ``extras``.
    :param client_order_id: The client order id the close fill reports.
    :return: The close's Pine id and the deal's venue size it was sized
        against (``None`` when the stamp holds none), or ``None``.
    """
    stamp = extras.get(EXTRAS_KEY_HELD_CLOSE)
    if not isinstance(stamp, Mapping) or not client_order_id:
        return None
    coid = _wire_id(stamp.get('coid'))
    pine_id = _wire_id(stamp.get('pine_id'))
    if coid is None or pine_id is None or coid != client_order_id:
        return None
    size = _wire_float(stamp.get('size'), finite=True)
    return pine_id, size if size is not None and size > 0.0 else None


def _extract_numeric_value(exc: CapitalComError) -> float:
    """Pull the trailing numeric ``X`` out of Capital.com's
    ``error.invalid.<leg>.<bound>: X`` messages.

    Stop-distance / take-profit-distance rejects (``minvalue``) follow
    the pattern ``error.invalid.stoploss.minvalue: 0.00050`` — the
    trailing value is the dynamic *distance* minimum. Maxvalue rejects
    (``maxvalue``) use the same shape but the trailing value is an
    *absolute price level* the bracket level cannot cross. The parser
    is identical for both — the *meaning* of the value differs by
    error code, which the typed exception captures.

    Returns ``0.0`` on parse failure so the caller gets a defined
    value without a raise — the typed exception is already the
    signal; the numeric payload is advisory.
    """
    message = str(exc)
    _, _, tail = message.rpartition(':')
    try:
        return float(tail.strip())
    except ValueError:
        return 0.0


def _extract_jwt_expiry(token: str | None) -> float | None:
    """Extract the ``exp`` claim from a JWT, or ``None`` if not parseable.

    Capital.com session tokens (``CST`` / ``X-SECURITY-TOKEN``) MAY be
    JWTs carrying their lifetime in the payload; the format is
    ``header.payload.signature`` where each segment is URL-safe base64
    without padding, and the ``exp`` claim is unix seconds.

    Returns ``None`` when the token is empty, not three dot-separated
    segments, the payload is not valid base64+JSON, or no numeric
    ``exp`` claim is present — the caller then falls back to a
    hardcoded refresh interval.
    """
    if not token:
        return None
    parts = token.split('.')
    if len(parts) != 3:
        return None
    payload_b64 = parts[1]
    payload_b64 += '=' * (-len(payload_b64) % 4)
    try:
        payload_bytes = urlsafe_b64decode(payload_b64)
        payload = json.loads(payload_bytes)
    except (ValueError, json.JSONDecodeError):
        return None
    exp = payload.get('exp') if isinstance(payload, dict) else None
    if isinstance(exp, (int, float)) and not isinstance(exp, bool):
        return float(exp)
    return None


def _parse_iso_timestamp(value: str) -> float:
    """Best-effort ISO-8601 → unix-seconds parser.

    Capital.com stamps ``createdDateUTC`` as ``YYYY-MM-DDTHH:MM:SS.sss``
    without a trailing ``Z``; :func:`datetime.fromisoformat` accepts that
    directly on Python 3.11+. Returns ``0.0`` on parse failure so the
    caller gets a defined value without a raise — timestamps here are
    advisory and never load-bearing for order identity.
    """
    if not value:
        return 0.0
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.timestamp()
    except ValueError:
        return 0.0


def _activity_watermark_floor(watermark: str) -> str:
    """Oldest ``dateUTC`` an activity row may carry and still be processed.

    The venue publishes an activity row up to :data:`_ACTIVITY_LAG_S` after
    the deal it stamps, so a row can surface after a newer row already moved
    the watermark. Rows inside that lag band are left to fingerprint dedup;
    only rows older than the band are skipped by date.

    :param watermark: The cursor's ``last_date_utc``.
    :return: The watermark minus the publication lag, in the venue's
        ``YYYY-MM-DDTHH:MM:SS.sss`` format; the watermark itself when it
        does not parse.
    """
    ts = _parse_iso_timestamp(watermark)
    if ts <= 0.0:
        return watermark
    floor = datetime.fromtimestamp(ts - _ACTIVITY_LAG_S, tz=UTC)
    return floor.strftime('%Y-%m-%dT%H:%M:%S.') + f"{floor.microsecond // 1000:03d}"


def _position_legs_from_payload(
        payload: Mapping, symbol: str,
) -> list[PositionLeg]:
    """Parse a ``/positions`` payload into symbol-scoped :class:`PositionLeg`\\ s.

    Shared by ``fetch_raw_positions`` (fresh GET) and the startup
    adoption pass (already-fetched snapshot): one leg per Capital.com
    position row on ``symbol`` with ZERO aggregation. Rows on any other
    epic are dropped — the endpoint is account-wide but a run trades ONE
    epic, and a foreign instrument's leg must never reach a store-row
    seeding path. Sorted by ``(open_time, leg_id)`` so the FIFO close
    order is deterministic and replay-stable across polls
    (``createdDateUTC`` has millisecond resolution; the dealId tiebreak
    covers same-millisecond fills).
    """
    legs: list[PositionLeg] = []
    for row in payload.get('positions') or []:
        market = row.get('market') or {}
        if market.get('epic') != symbol:
            continue
        position = row.get('position') or {}
        direction = (position.get('direction') or '').upper()
        if direction not in ('BUY', 'SELL'):
            continue
        deal_id = position.get('dealId')
        if not deal_id:
            continue
        legs.append(PositionLeg(
            leg_id=str(deal_id),
            # Post-filter the epic IS ``symbol``; stamping the leg from the
            # wire value (not the parameter) keeps the core seeding
            # helper's foreign-symbol guard armed should the filter above
            # ever regress.
            symbol=str(market.get('epic')),
            side='buy' if direction == 'BUY' else 'sell',
            qty=float(position.get('size', 0.0)),
            entry_price=float(position.get('level', 0.0)),
            open_time=_parse_iso_timestamp(
                position.get('createdDateUTC') or '',
            ),
            unrealized_pnl=float(position.get('upl', 0.0)),
        ))
    legs.sort(key=lambda leg: (leg.open_time, leg.leg_id))
    return legs


def _size_from_units(units: int, lot_step: float) -> float:
    """Convert a lot-step unit count back to a JSON-safe ``size`` value.

    ``units * lot_step`` in binary floats produces artifacts
    (``7 * 0.01 == 0.07000000000000001``) that would be serialized
    verbatim into the request body. Rounding to the lot step's own
    decimal precision restores the exact grid value.
    """
    exponent = Decimal(str(lot_step)).normalize().as_tuple().exponent
    decimals = max(0, -int(exponent))
    return round(units * lot_step, decimals)


#: Partial-close commands remembered per position row as already retired.
_RETIRED_CLOSE_COMMANDS_RETAINED = 64


def _mark_own_close_cursor_retired(extras: dict, sent_at: object) -> None:
    """Flag the own-close record sent at ``sent_at`` as retired from the exposure cursor.

    Called in the same write that lowers the target row's cursor for the
    close, so the flag and the cursor never disagree.

    :param extras: The target row's extras, mutated in place.
    :param sent_at: The ``sent_at`` of the own-close record (the partial-close
        command's ``own_close_sent_at``); nothing happens without one.
    """
    if not isinstance(sent_at, (int, float)) or isinstance(sent_at, bool):
        return
    for record in extras.get(EXTRAS_KEY_OWN_CLOSES) or ():
        if isinstance(record, dict) and record.get('sent_at') == sent_at:
            record[OWN_CLOSE_CURSOR_RETIRED] = True


def _retire_parked_partial_close_exposure(
        store_ctx: 'RunContext', command: 'OrderRow', units: int, lot_step: float,
        *, path: str,
) -> None:
    """Lower the target's exposure cursor for a partial close proven filled.

    The journal's ``filled_qty`` of a position row doubles as the run-owned
    exposure cursor. It is lowered here once per command, whichever path
    proves the reduction: the dispatch (before the journal makes the command
    terminal), the poll's resolver of a parked (``disposition_unknown``)
    close, or startup recovery.

    :param store_ctx: The run's journal.
    :param command: The partial-close command row.
    :param units: The reduction in lot units (the command's ``intent_units``).
    :param lot_step: The instrument's lot step.
    :param path: The settling path, for the audit event.
    """
    extras = command.extras or {}
    with store_ctx.transaction():
        target: OrderRow | None = None
        target_coid = extras.get('target_coid')
        if isinstance(target_coid, str):
            target = store_ctx.get_order(target_coid)
        if target is None:
            target_deal_id = _wire_id(extras.get('target_deal_id'))
            if target_deal_id is not None:
                target = store_ctx.find_by_ref('deal_id', target_deal_id)
        if target is None or target.closed_ts_ms is not None or units <= 0:
            return
        target_extras = dict(target.extras or {})
        retired = [str(c) for c in target_extras.get(EXTRAS_KEY_RETIRED_CLOSE_COMMANDS) or ()]
        if command.client_order_id in retired:
            return
        closed_qty = _size_from_units(units, lot_step)
        remaining = max(0.0, target.filled_qty - closed_qty)
        target_extras[EXTRAS_KEY_RETIRED_CLOSE_COMMANDS] = [
            *retired[-_RETIRED_CLOSE_COMMANDS_RETAINED + 1:], command.client_order_id,
        ]
        target_extras['partial_close_retired_at'] = epoch_time()
        _mark_own_close_cursor_retired(target_extras, extras.get('own_close_sent_at'))
        store_ctx.upsert_order(
            target.client_order_id, filled_qty=remaining, extras=target_extras,
        )
        store_ctx.log_event(
            'partial_close_journal_retired',
            client_order_id=target.client_order_id,
            exchange_order_id=target.exchange_order_id,
            payload={
                'retired_exposure': closed_qty,
                'remaining_exposure': remaining,
                'command': command.client_order_id,
                'path': path,
            },
        )


def _settle_vanished_full_close_targets(
        store_ctx: 'RunContext', command: 'OrderRow', positions_by_deal: Mapping[str, object],
        *, path: str,
) -> tuple[list[str], list[str], list[str]]:
    """Judge a full-close command's targets against a positions snapshot.

    A target the snapshot no longer lists was swept: the command's DELETE
    landed even if what came after it was never journaled. Each live row of
    a swept target that is not ``closing`` yet is moved there with its
    ``close_dispatched`` audit, in one transaction, whether or not the other
    targets are gone too: the snapshot's vanished-close pass then books each
    close fill, as it does after a fully journaled dispatch, and a target
    still listed does not hold back the accounting of one that is gone. One
    judgement for startup recovery and for the poll's resolver of a parked
    close.

    :param store_ctx: The run's journal.
    :param command: The full-close command row (``extras['targets']``).
    :param positions_by_deal: The authoritative ``/positions`` snapshot by deal id.
    :param path: The settling path, for the audit events.
    :return: The targets still listed, the targets swept, and the client order
        ids of the target rows moved to ``closing``.
    """
    targets = [str(deal) for deal in (command.extras or {}).get('targets') or ()]
    still_alive = [deal for deal in targets if deal in positions_by_deal]
    swept = [deal for deal in targets if deal not in positions_by_deal]
    mirrored: list[str] = []
    if not swept:
        return still_alive, swept, mirrored
    with store_ctx.transaction():
        for row in list(store_ctx.iter_live_orders()):
            if (row.exchange_order_id in swept
                    and row.client_order_id != command.client_order_id
                    and row.state not in ('closing', 'closed')):
                store_ctx.set_order_state(row.client_order_id, 'closing')
                store_ctx.log_event(
                    'close_dispatched',
                    client_order_id=row.client_order_id,
                    exchange_order_id=row.exchange_order_id,
                    intent_key=command.intent_key,
                    payload={'recovery_path': path},
                )
                mirrored.append(row.client_order_id)
    return still_alive, swept, mirrored


def _mirror_amended_level(store_ctx: 'RunContext', target_coid: object, level: float) -> None:
    """Mirror an own amend's level onto its working order's row (``amended_level``).

    The dispatch writes it after the venue took the amend; a settlement that
    proves the amend from venue data writes it when the dispatch could not.
    Without it, the working-order guard seeding its baseline from the row
    would take the order's own new level for someone else's edit.

    :param store_ctx: The run's journal.
    :param target_coid: The amended working order's client order id.
    :param level: The level the venue holds the order at.
    """
    if not target_coid:
        return
    with store_ctx.transaction():
        target = store_ctx.get_order(str(target_coid))
        if target is None or target.closed_ts_ms is not None:
            return
        extras = dict(target.extras or {})
        extras['amended_level'] = level
        store_ctx.upsert_order(target.client_order_id, extras=extras)


def _modify_entry_snapshot_verdict(
        store_ctx: 'RunContext', command: 'OrderRow', working_by_deal: Mapping[str, dict],
) -> tuple[str, str | None, float | None]:
    """Judge a modify-entry command from the ``/workingorders`` snapshot.

    The amend landed when the target's working order holds the requested
    ``new_level``. One judgement for startup recovery and for the poll's
    resolver of a parked amend.

    :param store_ctx: The run's journal.
    :param command: The modify-entry command row.
    :param working_by_deal: The authoritative ``/workingorders`` snapshot by deal id.
    :return: The verdict path (``modify_entry_no_target``,
        ``modify_entry_target_vanished``, ``modify_entry_snapshot_match`` or
        ``modify_entry_snapshot_level_mismatch``), the target's deal id and the
        level the venue holds the order at.
    """
    extras = command.extras or {}
    new_level = _wire_float(extras.get('new_level'), finite=True)
    target_coid = extras.get('target_coid')
    target = store_ctx.get_order(str(target_coid)) if target_coid else None
    target_deal_id = str(target.exchange_order_id) if target and target.exchange_order_id else None
    if target_deal_id is None:
        return 'modify_entry_no_target', None, None
    working = working_by_deal.get(target_deal_id)
    if working is None:
        return 'modify_entry_target_vanished', target_deal_id, None
    level = _wire_float((working.get('workingOrderData') or {}).get('orderLevel'), finite=True)
    if new_level is not None and level is not None and abs(level - new_level) < 1e-9:
        return 'modify_entry_snapshot_match', target_deal_id, level
    return 'modify_entry_snapshot_level_mismatch', target_deal_id, level


def _modify_exit_dispatch(store_ctx: 'RunContext', coid: str) -> dict | None:
    """The ``dispatch_submitted`` audit payload of the modify-exit command ``coid``.

    Written with the command row, before its PUT: it names the leg rows the
    amend writes (``tp_coid`` / ``sl_coid``) and the COIDs a leg the amend
    adds is created under (``envelope_tp_coid`` / ``envelope_sl_coid``).

    :param store_ctx: The run's journal.
    :param coid: The command's client order id.
    :return: The payload, or ``None`` when none is journaled.
    """
    return next((
        payload for _, event_coid, _, payload
        in store_ctx.iter_events_by_kind_for_run_id('dispatch_submitted')
        if event_coid == coid
    ), None)


def _own_trail_activation_sent(
        store_ctx: 'RunContext', parent_coid: str, activation_price: float,
        command_coid: str, unjournaled: Container[str],
) -> bool:
    """Whether the bot's activation monitor sent the pending trailing stop an amend wrote.

    The monitor journals ``trailing_activation_triggered`` for the stop leg
    once the venue accepted its ``trailingStop`` PUT, with the leg's
    activation price and the amend that wrote the pending trail
    (:data:`EXTRAS_KEY_TRAIL_COMMAND`). An amend reuses the leg row of the
    one before it, so only the amend's own COID tells its activation from
    an earlier one at the same price. An accepted activation whose journal
    write is still waiting to commit counts the same way, while the running
    process holds it (``unjournaled``).

    :param store_ctx: The run's journal.
    :param parent_coid: The position's entry COID.
    :param activation_price: The pending trailing stop's activation price.
    :param command_coid: The amend command's COID.
    :param unjournaled: The deferred-park keys (:func:`_trail_activation_key`)
        of accepted activations whose journal write has not committed yet.
    :return: ``True`` when a live stop leg of the position has such an
        activation of the amend's pending trail journaled at ``activation_price``,
        or waiting to be journaled.
    """
    legs = {
        row.client_order_id for row in store_ctx.iter_live_orders()
        if (row.extras or {}).get('leg_kind') == 'sl'
        and (row.extras or {}).get('parent_coid') == parent_coid
    }
    if not legs:
        return False
    if any(_trail_activation_key(leg, command_coid) in unjournaled for leg in legs):
        return True
    for _, coid, _, payload in store_ctx.iter_events_by_kind_for_run_id(
            'trailing_activation_triggered'):
        price = _wire_float(payload.get('activation_price'), finite=True)
        if (coid in legs and payload.get(EXTRAS_KEY_TRAIL_COMMAND) == command_coid
                and price is not None and abs(price - activation_price) < 1e-9):
            return True
    return False


def _modify_exit_snapshot_verdict(
        store_ctx: 'RunContext', command: 'OrderRow', positions_by_deal: Mapping[str, dict],
        *, unjournaled_activations: Container[str] = (),
) -> tuple[str, str | None, dict]:
    """Judge a modify-exit command from the ``/positions`` snapshot.

    The amend landed when the target position carries the requested bracket:
    ``new_tp`` as its ``profitLevel``, and ``new_sl`` as its ``stopLevel`` or
    ``new_trail`` as its native trailing distance. A pending trailing stop
    (``new_trail_price`` with ``new_trail``) is local until the activation
    monitor sends it, so the venue must show no stop at all, or the native
    trailing distance ``new_trail`` once the journal holds the monitor's
    accepted activation of the pending trail this amend wrote, at
    ``new_trail_price`` (:func:`_own_trail_activation_sent`). One judgement
    for startup recovery and for the poll's resolver of a parked amend.

    :param store_ctx: The run's journal.
    :param command: The modify-exit command row.
    :param positions_by_deal: The authoritative ``/positions`` snapshot by deal id.
    :param unjournaled_activations: The running process's accepted trailing
        activations whose journal write has not committed yet
        (:func:`_trail_activation_key`); a restart has none.
    :return: The verdict path (``modify_exit_no_target``,
        ``modify_exit_target_vanished``, ``modify_exit_snapshot_match`` or
        ``modify_exit_snapshot_mismatch``), the target's deal id and the
        levels the venue holds, with the expected ones on a mismatch.
    """
    extras = command.extras or {}
    new_tp = _wire_float(extras.get('new_tp'), finite=True)
    new_sl = _wire_float(extras.get('new_sl'), finite=True)
    new_trail = _wire_float(extras.get('new_trail'), finite=True)
    new_trail_price = _wire_float(extras.get('new_trail_price'), finite=True)
    pending_trail = new_trail is not None and new_trail_price is not None
    target_coid = extras.get('target_coid')
    target = store_ctx.get_order(str(target_coid)) if target_coid else None
    target_deal_id = str(target.exchange_order_id) if target and target.exchange_order_id else None
    if target_deal_id is None:
        return 'modify_exit_no_target', None, {}
    pos = positions_by_deal.get(target_deal_id)
    if pos is None:
        return 'modify_exit_target_vanished', target_deal_id, {}
    pos_data = pos.get('position') or {}
    profit_level = _wire_float(pos_data.get('profitLevel'))
    stop_level = _wire_float(pos_data.get('stopLevel'))
    stop_distance = _wire_float(pos_data.get('stopDistance'))
    trailing_stop = bool(pos_data.get('trailingStop'))
    held = {'profit_level': profit_level, 'stop_level': stop_level,
            'stop_distance': stop_distance, 'trailing_stop': trailing_stop}

    if new_tp is None:
        tp_matches = profit_level is None
    else:
        tp_matches = profit_level is not None and abs(profit_level - new_tp) < 1e-9
    if new_sl is None and new_trail is None:
        sl_matches = stop_level is None and not trailing_stop
    elif new_trail is not None:
        if pending_trail:
            sl_matches = (stop_level is None and not trailing_stop) or (
                trailing_stop and stop_distance is not None
                and abs(stop_distance - new_trail) < 1e-9
                and new_trail_price is not None
                and _own_trail_activation_sent(
                    store_ctx, str(target_coid), new_trail_price, command.client_order_id,
                    unjournaled_activations))
        else:
            sl_matches = (trailing_stop and stop_distance is not None
                          and abs(stop_distance - new_trail) < 1e-9)
    else:
        sl_matches = (new_sl is not None and stop_level is not None
                      and abs(stop_level - new_sl) < 1e-9)
    if tp_matches and sl_matches:
        return 'modify_exit_snapshot_match', target_deal_id, held
    return 'modify_exit_snapshot_mismatch', target_deal_id, {
        **held, 'expected_tp': new_tp, 'expected_sl': new_sl, 'expected_trail': new_trail,
        'expected_trail_price': new_trail_price, 'pending_trail': pending_trail,
    }
