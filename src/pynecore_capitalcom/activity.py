"""Activity-poll surface: ``watch_orders`` and the activity → event mapper.

Capital.com has no WebSocket order channel, so ``watch_orders`` is a
polling AsyncIterator that fuses three REST endpoints per cadence tick:

1. ``GET /history/activity`` — causal log of every fill / cancel /
   expiry. Cross-restart-safe dedup via SHA-1 fingerprints persisted
   in BrokerStore.
2. ``GET /positions`` + ``GET /workingorders`` — snapshot reconcile
   (delegated to ``_reconcile_snapshot`` in ``reconcile.py``).
3. ``_missing_pending_tracker`` — drives the core disappearance
   tracker's grace protocol for bot-owned rows that vanish without a
   corresponding cancel event (dual signal + ``on_unexpected_cancel``
   policy: ``stop`` latches the external-intervention quarantine, with
   :class:`UnexpectedCancelError` as its fail-safe signal when no
   quarantine sink is wired).

State touched: ``_activity_cursor`` (read/write — fingerprint dedup
set, watermark, external-activity log dedup), ``_current_poll_id``
(monotonic stamp on each poll cycle for breadcrumb race detection in
the reconciler).
"""
import asyncio
import logging
import math
from abc import ABC
from contextlib import nullcontext
from dataclasses import replace
from time import time as epoch_time
from typing import TYPE_CHECKING, AsyncIterator, Callable

import httpx

from pynecore.core.broker.exceptions import (
    BrokerError,
    ExchangeConnectionError,
    ExchangeRateLimitError,
)
from pynecore.core.broker.models import (
    ExchangeOrder,
    LegType,
    OrderEvent,
    OrderStatus,
)
from pynecore.core.broker.store_helpers import (
    CLOSE_LEG_STATE_PENDING,
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    EXTRAS_KEY_CLOSE_LEG_ID,
    iter_active_close_legs,
    update_close_leg_state,
)
from pynecore.core.plugin import override

from ._base import _CapitalComBase
from .delivery import (
    HOLD_BEHIND_QUEUED_FILL,
    HOLD_ENTRY_PRICE_PENDING,
    HOLD_SIZE_UNKNOWN,
    activity_size,
    book_entry,
    book_fill,
    delivery_pending,
    enqueue_entry,
    enqueue_fill,
    enqueue_identities,
    entry_pending_since,
    hold_reason,
    is_delivered,
    mark_delivered,
    queue_dates,
    queued_fills,
    settle_undelivered,
    sizeless_close_identity,
)
from .exceptions import CapitalComError
from .helpers import (
    ACTOR_CLIENT,
    ACTOR_DEALER,
    ACTOR_VENUE,
    CLOSE_VERDICT_FOREIGN,
    CLOSE_VERDICT_UNJUDGED,
    EXTERNAL_REDUCTION_VERDICTS,
    EXTRAS_KEY_ENTRY_EXECUTED_UTC,
    EXTRAS_KEY_NETTED_FOREIGN_DEAL,
    EXTRAS_KEY_NETTED_RESIDUAL_PENDING,
    EXTRAS_KEY_PROTECTION_ADOPT_AFTER,
    EXTRAS_KEY_TAKE_OVER_UNPROVEN,
    _ACTIVITY_LAG_S,
    _ACTIVITY_MAX_PERIOD_S,
    _POLL_INTERVAL_S,
    _activity_actor,
    _activity_watermark_floor,
    _held_close_label,
    _order_type_from_row,
    _residual_deal_cause,
    _parse_iso_timestamp,
    _wire_float,
)
from .models import _activity_fingerprint, _close_identity

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow

logger = logging.getLogger(__name__)


#: Failed polls between two reminders of an order-poll outage (~5 minutes at
#: the poll cadence).
_FAILED_POLL_REMINDER_EVERY = 200

#: Activity sources that only ever close a deal.
_CLOSING_SOURCES = frozenset({'TP', 'SL', 'CLOSE_OUT', 'CLOSE', 'MARGIN', 'STOP_OUT'})

#: The audit event of each :func:`~pynecore_capitalcom.delivery.hold_reason`.
_HOLD_AUDIT_EVENTS = {
    HOLD_ENTRY_PRICE_PENDING: 'activity_held_entry_price_pending',
    HOLD_SIZE_UNKNOWN: 'activity_deferred_close_size_unknown',
    HOLD_BEHIND_QUEUED_FILL: 'activity_held_behind_owed_fill',
}


def _wire_price(value: object) -> float:
    """A wire price as a positive finite float, ``0.0`` when it is no usable price.

    NaN, infinities, non-positive and unparseable values all read as missing,
    so every price source of an activity falls through to the next one and an
    entry or close no source prices is deferred, never booked at that value.
    """
    parsed = _wire_float(value, finite=True)
    return parsed if parsed is not None and parsed > 0.0 else 0.0


def _is_opening_activity(activity: dict, side: str, deal_id: str) -> bool:
    """Whether ``activity`` is the opening of the own deal ``deal_id`` of ``side``.

    A ``POSITION`` activity of the deal in the deal's own trade direction
    (``details.direction`` is the trade direction, measured 2026-07-10) and
    from no closing source.

    :param activity: A ``/history/activity`` row.
    :param side: The deal's side (``'buy'`` / ``'sell'``).
    :param deal_id: The deal.
    """
    details = activity.get('details') or {}
    return (str(activity.get('type') or '') == 'POSITION'
            and str(activity.get('dealId') or '') == deal_id
            and str(details.get('direction') or '').upper() == ('BUY' if side == 'buy' else 'SELL')
            and str(activity.get('source') or '').upper() not in _CLOSING_SOURCES)


def _note_failed_poll(failed_polls: int, error: Exception) -> int:
    """Log a failed order poll and return the new failure streak.

    The poll is the only order channel the venue has, so its outage must show
    in the log; at the poll cadence a plain network outage would repeat the
    same line dozens of times a minute. The first failure of a streak warns,
    then one reminder per :data:`_FAILED_POLL_REMINDER_EVERY` failures; the
    rest log at DEBUG.

    :param failed_polls: The streak before this failure.
    :param error: What failed the poll.
    :return: The streak including this failure.
    """
    streak = failed_polls + 1
    log = (logger.warning if streak == 1 or streak % _FAILED_POLL_REMINDER_EVERY == 0
           else logger.debug)
    log("Capital.com order poll failed (transient, %d consecutive): %s",
        streak, str(error) or type(error).__name__)
    return streak


class _ActivityMixin(_CapitalComBase, ABC):
    """Activity-poll mix-in: the ``watch_orders`` AsyncIterator + helpers."""

    @override
    async def watch_orders(self) -> AsyncIterator[OrderEvent]:
        """Emulated order event stream (Capital.com has no WebSocket channel).

        The outer loop polls three REST endpoints per cadence tick and
        fuses them into a single event stream:

        1. ``GET /history/activity`` — causal: every executed fill /
           cancel / expiry is reported here.  Cross-restart-safe dedup
           via SHA-1 content fingerprint.
        2. ``GET /positions`` / ``GET /workingorders`` — authoritative
           snapshots. Reconcile detects entry fills (working → position
           transition), own entry fills no activity announced, and
           bot-owned orders that silently vanished (``missing_pending``
           grace window + ``on_unexpected_cancel`` policy).
        3. Trailing activation monitor — client-side state machine
           (``pending → activating → active``) that implements Pine
           ``trail_price`` activation threshold on top of Capital's
           always-on native trailing.

        Rate limits (3 GET per tick @ :data:`_POLL_INTERVAL_S` cadence)
        sit comfortably below Capital's 10 req/s account budget; 429s
        still trigger an exponential backoff.
        """
        cadence = _POLL_INTERVAL_S
        consecutive_429 = 0
        failed_polls = 0
        while True:
            failure: Exception | None = None
            try:
                async for ev in self._poll_once():
                    yield ev
                consecutive_429 = 0
            except asyncio.CancelledError:
                raise
            except ExchangeRateLimitError as e:
                consecutive_429 += 1
                failed_polls = _note_failed_poll(failed_polls, e)
                backoff = min(30.0, cadence * (2 ** min(consecutive_429, 5)))
                await asyncio.sleep(backoff)
                continue
            except (ExchangeConnectionError,
                    httpx.TimeoutException, httpx.RequestError,
                    ConnectionError) as e:
                # Single-cycle hiccup — retry next tick.
                failure = e
            except CapitalComError as e:
                mapped = self._map_exception(e)
                if mapped is not None and not isinstance(
                        mapped, (ExchangeConnectionError, ExchangeRateLimitError)):
                    raise mapped from e
                failure = e
            if failure is not None:
                failed_polls = _note_failed_poll(failed_polls, failure)
            elif failed_polls:
                logger.warning(
                    "Capital.com order poll recovered after %d failed poll(s)", failed_polls)
                failed_polls = 0
            await asyncio.sleep(cadence)

    async def _poll_once(self) -> AsyncIterator[OrderEvent]:
        """One poll cycle: fetch the three endpoints and emit fused events.

        The order of the sub-generators matters.  Activity comes first
        because it is *causal* — when an order fills, the fill hits
        ``/history/activity`` before ``/positions`` updates.  Snapshot
        reconcile then covers the cases activity alone cannot pin down
        (working → position transitions, missing pending, unannounced fills).
        The trailing monitor follows since it depends on the current
        positions snapshot bid/offer. The bot's own repairs of its troubles
        (:mod:`~pynecore_capitalcom.order_history`) run last, against the
        same snapshot.
        """
        # Bump *before* fetching the snapshots so any defer-stamp written
        # by :meth:`_process_activity` is keyed to the snapshot that
        # caused the defer; :meth:`_reconcile_snapshot` then refuses to
        # clear breadcrumbs whose poll-id matches the current cycle.
        self._current_poll_id += 1
        # Every own protection write recorded before this instant may be
        # missing from the snapshot; the protection guard skips those.
        positions_read_started_at = epoch_time()
        positions_resp = await self._call('positions', method='get')
        working_resp = await self._call('workingorders', method='get')
        polled_at = epoch_time()
        activity_resp = await self._call(
            'history/activity',
            data={'lastPeriod': self._activity_lookback_s(polled_at),
                  'detailed': 'true'},
            method='get',
        )
        activity_read_at = epoch_time()

        positions_by_deal: dict[str, dict] = {}
        for row in positions_resp.get('positions') or []:
            pos = row.get('position') or {}
            did = pos.get('dealId')
            if did:
                positions_by_deal[str(did)] = row
        self._remember_positions_snapshot(positions_by_deal, positions_read_started_at)
        self._note_foreign_deals(positions_by_deal.values(), self.symbol or '')

        working_by_deal: dict[str, dict] = {}
        for wo in working_resp.get('workingOrders') or []:
            data = wo.get('workingOrderData') or {}
            did = data.get('dealId')
            if did:
                working_by_deal[str(did)] = wo

        async for ev in self._process_activity(
                activity_resp.get('activities') or [],
                positions_by_deal, read_at=activity_read_at):
            yield ev
        # The read counts as successful only once its whole batch is
        # processed: a batch whose booking rolled back must come back in the
        # next read, so the lookback keeps reaching back past this read.
        self._activity_polled_at = polled_at
        async for ev in self._reconcile_snapshot(
                positions_by_deal, working_by_deal,
                read_started_at=positions_read_started_at):
            yield ev
        await self._trailing_activation_monitor(positions_by_deal)
        async for ev in self._missing_pending_tracker(
                working_by_deal, positions_by_deal):
            yield ev
        # The bot's own way out of its troubles: a held take-over's fill
        # proven from the venue's history, and every ENTRY_BLOCK cause whose
        # condition the snapshot shows gone (or the bot itself ends).
        async for ev in self._resolve_unproven_take_overs(positions_by_deal):
            yield ev
        await self._settle_own_troubles(positions_by_deal)

    def _activity_lookback_s(self, now: float) -> int:
        """``lastPeriod`` covering every activity since the last successful read.

        A fixed window loses whatever the venue logged while the poll could
        not read: a fill that happened during an outage longer than the window
        never reaches the engine (measured live: cycle 276, 2026-10-13 — a 76s
        REST outage swallowed a market entry's fill, and the next opposite
        entry netted against it). Reaching back past the last successful read
        recovers those rows; fingerprint dedup and the date watermark drop the
        ones already processed.

        The read also reaches back to the oldest activity some durable
        pending state still waits for (:meth:`_oldest_pending_activity_ts`):
        such an activity is booked only after a later read, and a window
        anchored at the last read would no longer return it by then.

        :param now: Epoch time of the read about to be issued.
        :return: Lookback in whole seconds, capped at the venue maximum.
        """
        since_last = 0.0
        if self._activity_polled_at > 0.0:
            since_last = max(0.0, now - self._activity_polled_at)
        oldest_pending = self._oldest_pending_activity_ts()
        if oldest_pending is not None:
            since_last = max(since_last, now - oldest_pending)
        return min(_ACTIVITY_MAX_PERIOD_S, math.ceil(since_last) + _ACTIVITY_LAG_S)

    def _oldest_pending_activity_ts(self) -> float | None:
        """Venue time of the oldest activity a durable pending state still waits for.

        The one place every pending state that waits for an activity read
        anchors the lookback, so no transition between those states can
        shrink the window while an activity is still needed:

        * a held partly netted entry
          (:data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_NETTED_RESIDUAL_PENDING`)
          waits for its deal's opening activity, republished with the size or
          price it still lacks; its record keeps the ``dateUTC`` of the
          activity that revealed the execution;
        * a close deferred while that entry is held (its ``held_closes``);
        * every entry and fill queued on a row's delivery queue
          (:func:`~pynecore_capitalcom.delivery.queue_dates`): an entry no
          source priced, the fills held behind it or behind an earlier queued
          fill, and the closes of a held partly netted entry after the entry
          was booked, each until it is delivered.

        An activity older than the venue's maximum lookback can no longer be
        read and does not count.

        :return: Epoch seconds, ``None`` when nothing waits for an activity.
        """
        if self.store_ctx is None:
            return None
        readable_since = epoch_time() - _ACTIVITY_MAX_PERIOD_S
        oldest: float | None = None
        for row in self.store_ctx.iter_live_orders():
            extras = row.extras or {}
            dates: list[str] = []
            pending = extras.get(EXTRAS_KEY_NETTED_RESIDUAL_PENDING)
            if isinstance(pending, dict):
                dates.append(str(pending.get('date_utc') or ''))
                dates.extend(str(close.get('date_utc') or '')
                             for close in (pending.get('held_closes') or {}).values())
            dates.extend(queue_dates(extras))
            for date_utc in dates:
                ts = _parse_iso_timestamp(date_utc)
                if ts > readable_since and (oldest is None or ts < oldest):
                    oldest = ts
        return oldest

    def _pending_activity_keys(self) -> tuple[set[str], set[str]]:
        """What the durable pending states of :meth:`_oldest_pending_activity_ts` wait for.

        :return: The deals of held partly netted entries (their opening
            activity) and of entries deferred for their price (the entry and
            the fills held behind it; a row moved onto a re-keyed deal counts
            the deal it was moved from too), and the identities
            (:func:`~pynecore_capitalcom.models._close_identity`) of the closes
            still deferred, held or after their entry was booked. A close
            deferred without its size is keyed by its sizeless identity, which
            the sized copy matches through
            :func:`~pynecore_capitalcom.delivery.sizeless_close_identity`.
        """
        held_deals: set[str] = set()
        close_ids: set[str] = set()
        if self.store_ctx is None:
            return held_deals, close_ids
        for row in self.store_ctx.iter_live_orders():
            extras = row.extras or {}
            pending = extras.get(EXTRAS_KEY_NETTED_RESIDUAL_PENDING)
            if isinstance(pending, dict):
                if row.exchange_order_id:
                    held_deals.add(row.exchange_order_id)
                close_ids.update(pending.get('held_closes') or {})
            if entry_pending_since(extras) is not None:
                # A row moved onto a re-keyed deal keeps the deferred entry
                # activity under the deal it was read on.
                held_deals.update(
                    str(deal) for deal in (row.exchange_order_id,
                                           extras.get('promoted_from_working_order_id'))
                    if deal)
            close_ids.update(queued_fills(extras))
        return held_deals, close_ids

    async def _process_activity(
            self, activities: list[dict],
            positions_by_deal: dict[str, dict] | None = None,
            *,
            read_at: float | None = None,
    ) -> AsyncIterator[OrderEvent]:
        """Convert activity rows to events, with fingerprint-based dedup.

        External activity (i.e. ``dealId`` not owned by this bot) is
        logged as ``external_activity_ignored`` and **not** emitted —
        see the reference-plugin external-order policy in the broker
        plugin plan. The sync engine would otherwise try to reconcile
        orders it never placed, which is an invariant violation.

        :param positions_by_deal: Optional snapshot from the same poll
            cycle's ``GET /positions`` response, keyed by ``dealId``.
            Forwarded to :meth:`_activity_to_event` as a fallback source
            for the fill price — Capital.com leaves the activity row's
            ``level`` empty (= ``0.0``) on some market entries, and the
            position snapshot is the authoritative open price for those
            cases.
        :param read_at: Local time right after the activity read returned;
            the batch samples the venue clock against it (see
            :meth:`_note_venue_clock`). ``None`` leaves the clock unsampled.
        """
        if read_at is not None:
            self._note_venue_clock(activities, read_at)
        cursor = self._activity_cursor
        activities_sorted = sorted(
            activities, key=lambda x: x.get('dateUTC') or '',
        )
        # Cursor watermark guard: once we defer a row in this batch — a
        # closing fill without a price, or a row no bot-owned order matches
        # yet — no further activity in the same batch may advance
        # ``last_date_utc``. The batch is sorted ascending by ``dateUTC``,
        # so any row after the first defer is at-or-after the deferred
        # timestamp; advancing cursor past that timestamp would make the
        # next polls' date guard drop the deferred row once it falls out of
        # the publication-lag band, silently desyncing the strategy.
        deferred_in_batch = False
        # Rows inside the publication-lag band below the watermark are
        # still processed (fingerprint dedup drops the ones already seen):
        # the venue can publish a row after a newer one moved the watermark.
        watermark_floor = (
            _activity_watermark_floor(cursor.last_date_utc)
            if cursor.last_date_utc else ''
        )
        # An activity a durable pending state still waits for passes the
        # watermark: a read that omitted it may have let a newer row move the
        # watermark past it, and the lookback reaches back to it for exactly
        # this read (:meth:`_oldest_pending_activity_ts`). A close deferred
        # without its size is matched by the sized copy's sizeless identity.
        # Fingerprint dedup and the booked-close identities still drop a copy
        # already booked.
        pending_deals, pending_closes = (
            self._pending_activity_keys() if watermark_floor else (set(), set()))
        # Every fingerprint in this batch, collected BEFORE the watermark
        # guard: the end-of-poll retention below must keep any fingerprint
        # that a future poll could still consult, and consultable rows are
        # exactly those the venue still returns in its rolling window.
        batch_fingerprints: set[str] = set()
        for index, a in enumerate(activities_sorted):
            date_utc = a.get('dateUTC') or ''
            deal_id = str(a.get('dealId') or '')
            fingerprint = _activity_fingerprint(a)
            batch_fingerprints.add(fingerprint)

            # Who edited a deal's stop / limit levels decides whether the
            # protection guard reports the edit; remembered even for a row
            # an earlier process already processed, and for one below the
            # watermark: a read after an outage serves it again, and the
            # edit stays the evidence of a verdict still to be decided.
            self._note_level_edit(a)
            if (watermark_floor and date_utc < watermark_floor
                    and deal_id not in pending_deals
                    and _close_identity(a) not in pending_closes
                    and sizeless_close_identity(a) not in pending_closes):
                continue
            if fingerprint in cursor.seen_fingerprints:
                continue

            row: 'OrderRow | None' = None
            # The opened size of a partly netted own working order this
            # activity's deal belongs to, when the bridge below promoted it.
            bridge_fill_qty: float | None = None
            # A taken-over entry whose fill the snapshot could not prove
            # (:data:`EXTRAS_KEY_TAKE_OVER_UNPROVEN`) that this activity belongs
            # to, directly or through the order's back-link.
            held: 'OrderRow | None' = None
            if deal_id and self.store_ctx is not None:
                row = self.store_ctx.find_by_ref('deal_id', deal_id)

            if (row is None and deal_id and self.store_ctx is not None
                    and str(a.get('type') or '') == 'POSITION'):
                # Working-order → position bridge. A stop/limit entry's
                # fill opens the position under a NEW dealId; the POSITION
                # activity announces it with a ``details.workingOrderId``
                # back-link to the order's old dealId (measured 2026-08-16:
                # order ...c2ab filled into position ...c471). Without the
                # bridge this activity reads as external, the row keeps its
                # old ref — absent from both /workingorders and /positions
                # — and the disappearance tracker confirms a FALSE
                # unexpected cancel on a filled entry after the grace
                # window (quarantine + orphaned venue position).
                #
                # The back-link alone does not make the deal the order's: on
                # a netting account an executing working order reduces the
                # oldest opposite deal, and that deal's activity may name the
                # order too. A deal of the other direction, or one that
                # existed before the order executed, is a deal the order
                # netted — never promoted, never booked against the order's
                # row; the order's own execution is retired unbooked
                # (:meth:`_retire_order_netted_into_foreign_deal`). An order
                # that already lives under another deal owns no further deal
                # either.
                wo_id = str((a.get('details') or {}).get('workingOrderId') or '')
                if wo_id and wo_id != deal_id:
                    wo_row = self.store_ctx.find_by_ref('deal_id', wo_id)
                    activity_ts = _parse_iso_timestamp(date_utc)
                    conflict = (
                        self._promotion_conflict(
                            wo_row.side, deal_id, activity_ts, positions_by_deal,
                        )
                        if wo_row is not None else None
                    )
                    if (wo_row is not None
                            and (wo_row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN)
                            is not None):
                        # Neither promoted nor reported: the order's execution
                        # is unproven, and the row stays as it is.
                        held = wo_row
                    elif wo_row is None:
                        # The order's execution may have been retired unbooked
                        # as one that only netted a foreign deal (its row is
                        # closed, its refs gone); a deal it opened with the
                        # rest surfaced only now.
                        self._note_residual_after_netting_retirement(
                            wo_id, deal_id, activity_ts, positions_by_deal,
                            a.get('details') or {},
                        )
                    elif conflict is not None:
                        self._report_own_order_netted_foreign_deal(
                            wo_row, wo_id, deal_id, conflict, positions_by_deal,
                        )
                    elif wo_row.exchange_order_id == wo_id:
                        # A position row is the same shape one step later:
                        # the venue ran a MARKET entry through its
                        # working-order machinery, the confirm named the
                        # executed order and the position opened under this
                        # fresh ``dealId`` (measured 2026-08-28, cycle 84,
                        # and cycle 442, where the bracket PUT and the
                        # defensive DELETE both went to the order's id).
                        if (wo_row.extras or {}).get('kind') in (
                                ENTRY_KIND_WORKING, ENTRY_KIND_POSITION):
                            bridge_fill_qty = self._bridge_working_order_deal(
                                wo_row, deal_id, a, activities_sorted, positions_by_deal,
                                fingerprint,
                            )
                        row = (self.store_ctx.find_by_ref('deal_id', deal_id)
                               or wo_row)

            if (row is not None
                    and (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None):
                held = row
            if held is not None:
                # A taken-over entry whose fill the snapshot could not prove
                # is held until the bot proves the fill from the venue's
                # history (``order_history.py``): none of its activities is
                # booked meanwhile. Its execution would book a fill whose
                # later reduction the snapshot already showed, a close would
                # reduce a fill never booked, and a cancel would release the
                # held entry for a second send. Like an unmatched row, the
                # activity is deferred, not consumed, and audited once; the
                # proof queues the deal's closes, which this loop then books
                # in order.
                deferred_in_batch = True
                if (self.store_ctx is not None
                        and fingerprint not in cursor.external_logged_fingerprints):
                    cursor.external_logged_fingerprints.add(fingerprint)
                    self.store_ctx.log_event(
                        'activity_held_take_over_unproven',
                        client_order_id=held.client_order_id,
                        exchange_order_id=deal_id or None,
                        intent_key=held.intent_key,
                        payload={'activity': a, 'fingerprint': fingerprint},
                    )
                continue

            if row is None:
                # Two cases land here:
                #   (a) Genuinely external activity (manual broker action,
                #       another bot using the same account, etc.).
                #   (b) Race against our own ``execute_entry``: the activity
                #       arrived before ``add_ref('deal_id', …)`` finished
                #       writing the link row.
                # We can't tell them apart from this poll alone, so we keep
                # the activity *retryable* — do NOT add the fingerprint to
                # ``seen_fingerprints`` and do NOT advance ``last_date_utc``.
                # The next poll re-evaluates: if the bot's confirm step has
                # caught up, the row attaches and the fill emits normally.
                # Truly external rows fall out of Capital.com's 60s rolling
                # window without further noise; the
                # ``external_logged_fingerprints`` set keeps the audit log
                # one-shot per row within the session. A later matched row
                # in this batch must not move the watermark past this one,
                # or the date guard drops it once its reference lands.
                deferred_in_batch = True
                if (self.store_ctx is not None
                        and fingerprint not in cursor.external_logged_fingerprints):
                    cursor.external_logged_fingerprints.add(fingerprint)
                    self.store_ctx.log_event(
                        'external_activity_ignored',
                        exchange_order_id=deal_id or None,
                        payload={'activity': a, 'fingerprint': fingerprint},
                    )
                continue

            # Row is matched — promote out of the external-logged set in
            # case a previous poll caught the race window. Dedup commit
            # and cursor advance happen after the deferral guard below.
            cursor.external_logged_fingerprints.discard(fingerprint)

            position_snapshot = (
                positions_by_deal.get(deal_id)
                if positions_by_deal and deal_id else None
            )
            event = self._activity_to_event(a, row, position_snapshot)
            if (bridge_fill_qty is not None and event is not None
                    and event.event_type == 'filled' and event.leg_type == LegType.ENTRY):
                event = self._residual_fill(event, bridge_fill_qty)

            # An own execution held until the size of the deal it opened is
            # known (``EXTRAS_KEY_NETTED_RESIDUAL_PENDING``): the deal's opening
            # activity, when it states the size, books that size; any other
            # entry-side row is the replay of the held execution. A close of
            # the deal waits for the held entry to be booked first — reducing
            # exposure never booked would walk the engine's book across — and,
            # like an unmatched row, is deferred, not consumed, and audited
            # once. The held record remembers it: the deal's ``/positions``
            # size is net of it.
            pending_settle_size: float | None = None
            if ((row.extras or {}).get(EXTRAS_KEY_NETTED_RESIDUAL_PENDING) is not None
                    and event is not None and event.event_type == 'filled'):
                if event.leg_type != LegType.ENTRY:
                    deferred_in_batch = True
                    self._hold_close_of_netted_residual(row, a, activity_size(a))
                    if (self.store_ctx is not None
                            and fingerprint not in cursor.held_logged_fingerprints):
                        cursor.held_logged_fingerprints.add(fingerprint)
                        self.store_ctx.log_event(
                            'activity_held_netted_residual_pending',
                            client_order_id=row.client_order_id,
                            exchange_order_id=deal_id or None,
                            intent_key=row.intent_key,
                            payload={'activity': a, 'fingerprint': fingerprint},
                        )
                    continue
                if _is_opening_activity(a, row.side, row.exchange_order_id or ''):
                    # A copy stating no size still prices a size an earlier
                    # copy proved.
                    held = (row.extras or {})[EXTRAS_KEY_NETTED_RESIDUAL_PENDING]
                    proven = (_wire_float(held.get('opened_qty'), finite=True)
                              if isinstance(held, dict) else None)
                    pending_settle_size = activity_size(a) or proven
                    price_held = ((_wire_float(held.get('fill_price'), finite=True)
                                   if isinstance(held, dict) else None) or 0.0) > 0.0
                    priced = price_held or (
                        _wire_float(event.fill_price, finite=True) or 0.0) > 0.0
                    if pending_settle_size is None or not priced:
                        # An opening that leaves the held entry without its
                        # size or price is deferred, not consumed: the venue
                        # republishes it with the missing ``details`` field
                        # under the same fingerprint (which reads neither the
                        # ``details`` size nor level), and that copy books the
                        # entry. A size or a price it proves is kept on the
                        # held record, so complementary copies settle it.
                        deferred_in_batch = True
                        details = a.get('details') or {}
                        level = _wire_float(a.get('level') or details.get('level'),
                                            finite=True)
                        proof: dict = {}
                        if pending_settle_size is not None and proven is None:
                            proof['opened_qty'] = pending_settle_size
                        if level is not None and level > 0.0 and not price_held:
                            proof['fill_price'] = level
                        if proof:
                            self._update_netted_residual(
                                row, lambda record: record.update(proof))
                        if (self.store_ctx is not None
                                and fingerprint not in cursor.held_logged_fingerprints):
                            cursor.held_logged_fingerprints.add(fingerprint)
                            self.store_ctx.log_event(
                                'activity_held_netted_residual_opening_incomplete',
                                client_order_id=row.client_order_id,
                                exchange_order_id=deal_id or None,
                                intent_key=row.intent_key,
                                payload={'activity': a, 'fingerprint': fingerprint},
                            )
                        continue

            # The deal's delivery queue (:mod:`~pynecore_capitalcom.delivery`)
            # decides whether this reduction or close is the next deliverable
            # fill of the deal. A held fill is deferred, not consumed, and
            # audited once; a fill held behind the entry or behind an earlier
            # queued fill is queued itself, so the read keeps reaching back to
            # it, past the watermark, until it is booked in order.
            hold = (hold_reason(row.extras or {}, a)
                    if (event is not None and event.event_type == 'filled'
                        and event.leg_type != LegType.ENTRY) else None)
            if hold is not None:
                deferred_in_batch = True
                if hold != HOLD_SIZE_UNKNOWN and enqueue_fill(dict(row.extras or {}), a):
                    self._rewrite_extras(
                        row.client_order_id, lambda extras: enqueue_fill(extras, a))
                if (self.store_ctx is not None
                        and fingerprint not in cursor.held_logged_fingerprints):
                    cursor.held_logged_fingerprints.add(fingerprint)
                    self.store_ctx.log_event(
                        _HOLD_AUDIT_EVENTS[hold],
                        client_order_id=row.client_order_id,
                        exchange_order_id=deal_id or None,
                        intent_key=row.intent_key,
                        payload={'activity': a, 'fingerprint': fingerprint},
                    )
                continue

            # DEFER guard: a closing-leg fill with no resolvable price
            # (Capital.com sometimes leaves ``level`` empty on closes
            # whose position is already gone from ``/positions`` by the
            # time we poll). Yielding ``fill_price=0`` lets
            # ``BrokerPosition.record_fill`` ignore the event AND the
            # post-yield ``natural_close_at`` stamp would suppress
            # reconcile recovery, leaving the strategy internally open
            # while the broker is closed. Skip the dedup commit + cursor
            # advance so the next poll re-evaluates with a fresh
            # ``positions_by_deal`` snapshot. If the activity rolls out
            # of Capital.com's 60s history window before a price
            # surfaces, :meth:`_reconcile_snapshot` still detects the
            # vanished deal and routes through the missing-pending grace
            # window (UnexpectedCancelError), which is preferable to a
            # silent desync.
            if (event is not None
                    and event.event_type == 'filled'
                    and (event.fill_price or 0.0) <= 0.0
                    and event.leg_type in (
                        LegType.TAKE_PROFIT, LegType.STOP_LOSS,
                        LegType.TRAILING_STOP, LegType.CLOSE,
                    )):
                # Distinguish vanished-deal cases (snapshot reconcile's
                # vanished-deal recovery handles them) from manual
                # partial closes where the deal is still alive: a
                # ``LegType.CLOSE`` activity (USER / DEALER on an
                # already-filled entry) with ``level=0`` while
                # ``/positions`` still carries the deal is a partial
                # reduction that snapshot reconcile cannot detect —
                # :meth:`_reconcile_snapshot` books no fill from a deal's
                # size change, so once the activity rolls out of
                # Capital.com's 60s history window the local position
                # would stay larger than the broker forever. Resolve
                # the price with the live mid as a proxy and let the
                # event flow through. Bracket TP/SL/TRAILING_STOP and
                # full-close activities keep the original defer: their
                # broker leg is gone post-fill, vanished-deal recovery
                # via :meth:`_reconcile_snapshot` reaches them, and
                # the bracket-leg level fallback in
                # :meth:`_activity_to_event` already covers the
                # in-window case.
                deal_alive_with_size = False
                if (event.leg_type == LegType.CLOSE
                        and deal_id and positions_by_deal
                        and deal_id in positions_by_deal):
                    pos_snap = (
                        positions_by_deal[deal_id].get('position') or {}
                    )
                    deal_alive_with_size = (
                        float(pos_snap.get('size') or 0.0) > 0.0
                    )
                proxy_price: float | None = None
                if deal_alive_with_size:
                    try:
                        proxy_price = await self._get_current_mid_price(
                            row.symbol,
                        )
                    except (httpx.TimeoutException, httpx.RequestError,
                            ConnectionError, ExchangeConnectionError,
                            CapitalComError, BrokerError):
                        proxy_price = None
                if proxy_price is not None and _wire_price(proxy_price) > 0.0:
                    # Re-emit the event with the proxy price; the
                    # downstream yield path stamps the close-event
                    # breadcrumb, persists ``entry_filled_at``, etc.
                    # Live mid is not the exact close mid (no REST
                    # endpoint exposes it after the fact), but it is
                    # close enough to keep
                    # :meth:`BrokerPosition.record_fill` from
                    # ignoring the reduction — the alternative is a
                    # silent desync. Carry the source activity's
                    # ``size`` as ``fill_qty`` so the engine reduces
                    # by exactly the manual close amount.
                    event = OrderEvent(
                        order=event.order,
                        event_type=event.event_type,
                        fill_price=proxy_price,
                        fill_qty=event.fill_qty,
                        journal_fill_qty=event.journal_fill_qty,
                        timestamp=event.timestamp,
                        pine_id=event.pine_id,
                        from_entry=event.from_entry,
                        leg_type=event.leg_type,
                        fee=event.fee,
                        fee_currency=event.fee_currency,
                        # Same source activity row => same canonical fingerprint.
                        fill_id=event.fill_id,
                    )
                    if self.store_ctx is not None:
                        self.store_ctx.log_event(
                            'partial_manual_close_proxy_priced',
                            exchange_order_id=deal_id,
                            client_order_id=row.client_order_id,
                            payload={'fingerprint': fingerprint,
                                     'dateUTC': date_utc,
                                     'deal_id': deal_id,
                                     'type': a.get('type'),
                                     'status': a.get('status'),
                                     'source': a.get('source'),
                                     'proxy_price': proxy_price},
                        )
                else:
                    deferred_in_batch = True
                    if self.store_ctx is not None:
                        self.store_ctx.log_event(
                            'activity_close_deferred_no_price',
                            exchange_order_id=deal_id,
                            client_order_id=row.client_order_id,
                            payload={'fingerprint': fingerprint,
                                     'dateUTC': date_utc,
                                     'deal_id': deal_id,
                                     'type': a.get('type'),
                                     'status': a.get('status'),
                                     'source': a.get('source')},
                        )
                    # Who closed the position does not depend on the price:
                    # judge the close now (once per close, the verdict is
                    # kept on the row) and report someone else's close at
                    # once. The booking waits for a priced copy of the
                    # activity; if none ever comes, the vanished deal is
                    # left to the disappearance verdict.
                    if (event.leg_type == LegType.CLOSE
                            and (row.extras or {}).get('natural_close_at') is None
                            and self._attribute_close_activity(
                                row, a, positions_by_deal) == CLOSE_VERDICT_FOREIGN):
                        self._report_manual_close(row, a)
                    continue

            # A sizeless copy of a close whose sized copy superseded it
            # (:meth:`_hold_close_of_netted_residual`) is dropped without
            # consuming its fingerprint: the fingerprint does not read the
            # ``details`` size, so the sized copy shares it and still books.
            if (event is not None and event.event_type == 'filled'
                    and event.leg_type != LegType.ENTRY
                    and activity_size(a) is None
                    and is_delivered(row.extras or {}, a)):
                continue

            # Idempotent entry-fill replay guard. An ENTRY fill whose
            # matched row already carries ``entry_filled_at`` was
            # ALREADY accounted for — either earlier in this session
            # (the first emit stamps it, and the venue's 60s rolling
            # window keeps re-serving the same activity) or in a prior
            # process incarnation whose durable row persisted the
            # stamp. On restart the engine's startup reconcile adopts
            # the venue position, which already INCLUDES this entry
            # fill; re-emitting it would drive
            # :meth:`BrokerPosition.record_fill` to double-count the
            # exposure (the observed 100 -> 200 desync). Capital.com
            # opens a fresh ``dealId`` for every entry, so a genuine
            # second entry matches a different row that carries no
            # ``entry_filled_at`` — this guard only ever fires on a
            # replay of the same accounted fill. The activity stays
            # durable (``activity_processed`` logged below, cursor advanced
            # above); only the position-affecting yield is dropped.
            # Robust to the venue backfilling the activity ``level``
            # between ingest and restart (which changes the content-
            # addressed fingerprint and would otherwise defeat
            # ``seen_fingerprints`` dedup).
            entry_fill_already_recorded = (
                event is not None
                and event.event_type == 'filled'
                and event.leg_type == LegType.ENTRY
                and (row.extras or {}).get('entry_filled_at') is not None
                and pending_settle_size is None
            )
            # The engine recovered the entry's execution from the venue's
            # history itself (``fetch_order_execution``, read in
            # ``order_history.py``) before this stream delivered it: its
            # receipt books the entry. The activity still settles the row as
            # the stream's own copy would (promotion, entry stamp, exposure
            # cursor), but its fill is not yielded a second time.
            engine_recovered = (
                event is not None and not entry_fill_already_recorded
                and event.event_type == 'filled'
                and event.leg_type == LegType.ENTRY
                and pending_settle_size is None
                and self._engine_booked_entry_qty(row) > 0.0
            )
            # An entry fill no source prices (activity, snapshot, confirm)
            # is deferred like an unpriced close: the book drops a fill at a
            # zero price, and consuming the activity would stamp the entry
            # booked while the engine never received it. A priced copy of the
            # activity books it, and once the activity has rolled out of the
            # history window the snapshot fallback
            # (:meth:`~_ReconcileMixin._unannounced_entry_fill`) does. The
            # deferral is recorded on the row: every later fill of the deal
            # waits behind the entry, across polls and restarts.
            if (event is not None
                    and event.event_type == 'filled'
                    and event.leg_type == LegType.ENTRY
                    and _wire_price(event.fill_price) <= 0.0
                    and not entry_fill_already_recorded
                    and not engine_recovered
                    and pending_settle_size is None):
                deferred_in_batch = True
                if enqueue_entry(dict(row.extras or {}), date_utc):
                    self._rewrite_extras(
                        row.client_order_id,
                        lambda extras: enqueue_entry(extras, date_utc),
                    )
                if (self.store_ctx is not None
                        and fingerprint not in cursor.held_logged_fingerprints):
                    cursor.held_logged_fingerprints.add(fingerprint)
                    self.store_ctx.log_event(
                        'activity_entry_deferred_no_price',
                        client_order_id=row.client_order_id,
                        exchange_order_id=deal_id or None,
                        intent_key=row.intent_key,
                        payload={'activity': a, 'fingerprint': fingerprint},
                    )
                continue
            # Closing-leg twin of the guard above. A row stamped
            # ``natural_close_at`` with its exposure cursor retired
            # already delivered its close fill — either from an
            # earlier activity or from the snapshot reconcile's
            # vanished-``closing``-row path, which prices the fill
            # from the DELETE's confirm when the venue publishes the
            # close activity late or not at all. Capital.com never
            # reuses a dealId, so a later closing activity on such a
            # row can only be that same close surfacing again;
            # yielding it would walk the engine's flat book into a
            # phantom opposite position.
            is_close_fill = (
                event is not None
                and event.event_type == 'filled'
                and event.leg_type in (
                    LegType.TAKE_PROFIT, LegType.STOP_LOSS,
                    LegType.TRAILING_STOP, LegType.CLOSE,
                )
            )
            # The venue may re-issue a close under a new fingerprint (a
            # level-less row republished with its level): the close's
            # level-independent identity, recorded when it was booked,
            # suppresses the copy.
            close_id = _close_identity(a)
            close_fill_already_recorded = is_close_fill and (
                ((row.extras or {}).get('natural_close_at') is not None
                 and row.filled_qty <= 0.0)
                or is_delivered(row.extras or {}, a)
            )
            # Attribute a USER / DEALER close before its effects land: the
            # bot's own DELETE / reduce POST carries the same source as an
            # operator's close, and only the own-close record written before
            # that request tells them apart. Someone else's close is booked
            # like any other, then reported. The report it owes is persisted
            # in the same transaction as the dedup commit: a process that
            # dies after the commit must still find the report on restart,
            # where the deduplicated activity no longer replays.
            close_verdict = CLOSE_VERDICT_UNJUDGED
            # The dedup commit and every journal write the booking makes
            # before its event is yielded (the entry stamp, the exposure
            # cursor, the close teardown or partial-close retirement) are one
            # transaction: a process that dies in between must not find the
            # activity consumed while its effect on the journal is missing. The
            # in-memory cursor follows only the committed transaction: a failed
            # one leaves the activity for the next poll of this process.
            suppress_fill = False
            with (self.store_ctx.transaction() if self.store_ctx is not None
                  else nullcontext()):
                if self.store_ctx is not None:
                    self.store_ctx.log_event(
                        'activity_processed',
                        exchange_order_id=deal_id,
                        client_order_id=row.client_order_id,
                        payload={'fingerprint': fingerprint, 'dateUTC': date_utc,
                                 'deal_id': deal_id,
                                 'type': a.get('type'), 'status': a.get('status'),
                                 'source': a.get('source')},
                    )
                    if (event is not None
                            and not entry_fill_already_recorded
                            and not close_fill_already_recorded
                            and event.event_type == 'filled'
                            and event.leg_type == LegType.CLOSE):
                        close_verdict = self._attribute_close_activity(
                            row, a, positions_by_deal,
                        )
                    if is_close_fill and not close_fill_already_recorded:
                        self._rewrite_extras(
                            row.client_order_id,
                            lambda extras: book_fill(extras, a, close_id),
                        )
                    # Working-order fill (WORKING_ORDER EXECUTED): promote the
                    # row to its position identity BEFORE yielding the fill,
                    # in the dedup commit's transaction: a process that dies
                    # after the commit must find the promotion (and an
                    # execution held for its size) on restart, where the
                    # deduplicated activity no longer replays. The new dealId
                    # comes from the same-instant POSITION activity in this
                    # batch, or from the same-poll ``/positions`` snapshot
                    # (the position payload carries ``workingOrderId``).
                    # ``stamp_entry_filled=True`` marks the fill as accounted
                    # so the POSITION activity's own ENTRY-fill replay is
                    # suppressed by the guard above on a later poll. When
                    # neither source shows the new deal yet, still clear the
                    # missing-pending premise and stamp the fill — the bridge
                    # branch or the next poll's snapshot completes the ref
                    # migration within the grace window.
                    if (event is not None
                            and event.event_type == 'filled'
                            and event.leg_type == LegType.ENTRY
                            and not entry_fill_already_recorded
                            and (row.extras or {}).get('kind') == 'working'):
                        event = self._promote_executed_working_order(
                            row, event, activities_sorted, positions_by_deal,
                            date_utc=date_utc,
                        )
                    if event is not None and pending_settle_size is not None:
                        event = self._settle_netted_residual(
                            row, pending_settle_size, fill_price=event.fill_price,
                            fill_id=event.fill_id,
                        )
                if event is not None:
                    # Stamp the entry row before yielding its first
                    # filled-as-ENTRY activity so :meth:`_activity_to_event`
                    # can recognise subsequent ``USER`` / ``DEALER`` activities
                    # for the same dealId as manual closes (Capital.com nets
                    # per dealId — without the stamp the close routes as an
                    # entry add). Persisting BEFORE the yield, in the
                    # transaction of the ``activity_processed`` log, is
                    # critical: a crash or cancellation before the consumer
                    # resumes would otherwise dedupe the entry on restart but
                    # leave ``entry_filled_at`` missing, misclassifying any
                    # later manual close as ``LegType.ENTRY``.
                    if (event.event_type == 'filled'
                            and event.leg_type == LegType.ENTRY
                            and (row.extras or {}).get('kind') == 'position'
                            and not (row.extras or {}).get('entry_filled_at')
                            and self.store_ctx is not None):
                        refreshed = self.store_ctx.get_order(row.client_order_id)
                        base_extras = dict((refreshed or row).extras or {})
                        book_entry(base_extras, epoch_time(), date_utc)
                        self.store_ctx.upsert_order(
                            row.client_order_id, extras=base_extras,
                        )
                    # The journal ``filled_qty`` doubles as the run-owned
                    # exposure cursor (K3 cycle-end book + startup adoption
                    # clamp read the signed sum of live rows' cursors), and the
                    # activity stream is the ONLY fill source for a working
                    # entry whose promote raced ahead of the snapshot pass:
                    # once the row is ``kind='position'`` the snapshot's
                    # working→position stamp can never run — so
                    # without this write the cursor would stay at zero forever
                    # (measured 2026-08-21, cycle 51: engine and venue both
                    # -0.01, journal owned 0 → K3 MISMATCH).
                    if (event.event_type == 'filled'
                            and event.leg_type == LegType.ENTRY
                            and not entry_fill_already_recorded
                            and self.store_ctx is not None):
                        refreshed = self.store_ctx.get_order(row.client_order_id)
                        if refreshed is not None:
                            cursor_fill = min(
                                refreshed.qty,
                                max(refreshed.filled_qty,
                                    float(event.fill_qty or 0.0)),
                            )
                            if cursor_fill > refreshed.filled_qty:
                                self.store_ctx.upsert_order(
                                    row.client_order_id, filled_qty=cursor_fill,
                                )
                    # Stamp the close-event breadcrumb / natural-close teardown
                    # BEFORE yielding, for the same reason as the entry stamp
                    # above, in the transaction of the ``activity_processed``
                    # log: a crash or cancellation after the yield would
                    # otherwise dedupe the activity on restart but leave the
                    # breadcrumb missing. Next poll's vanished deal would
                    # then fall into ``missing_pending_since`` /
                    # :class:`UnexpectedCancelError` instead of the intended
                    # natural-close teardown (or, for the still-present-deal
                    # race, the snapshot would lack the breadcrumb that
                    # ``_reconcile_snapshot`` relies on to upgrade to a
                    # teardown without waiting for the grace window).
                    suppress_fill = False
                    if entry_fill_already_recorded:
                        suppress_fill = True
                        if self.store_ctx is not None:
                            self.store_ctx.log_event(
                                'entry_fill_replay_suppressed',
                                client_order_id=row.client_order_id,
                                exchange_order_id=deal_id,
                                payload={
                                    'reason': 'entry_already_accounted',
                                    'fingerprint': fingerprint,
                                    'dateUTC': date_utc,
                                    'deal_id': deal_id,
                                    'symbol': row.symbol,
                                    'side': row.side,
                                },
                            )
                    elif engine_recovered:
                        suppress_fill = True
                        if self.store_ctx is not None:
                            self.store_ctx.log_event(
                                'entry_fill_recovered_by_engine',
                                client_order_id=row.client_order_id,
                                exchange_order_id=deal_id,
                                payload={'fingerprint': fingerprint, 'dateUTC': date_utc,
                                         'deal_id': deal_id},
                            )
                    elif close_fill_already_recorded:
                        suppress_fill = True
                        if self.store_ctx is not None:
                            self.store_ctx.log_event(
                                'close_fill_replay_suppressed',
                                client_order_id=row.client_order_id,
                                exchange_order_id=deal_id,
                                payload={
                                    'reason': 'close_already_accounted',
                                    'fingerprint': fingerprint,
                                    'dateUTC': date_utc,
                                    'deal_id': deal_id,
                                    'symbol': row.symbol,
                                    'side': row.side,
                                },
                            )
                    if (not suppress_fill
                            and event.event_type == 'filled'
                            and event.leg_type in (
                                LegType.TAKE_PROFIT,
                                LegType.STOP_LOSS,
                                LegType.TRAILING_STOP,
                                LegType.CLOSE,
                            )):
                        # Bracket / manual close handling: the same-poll
                        # ``positions_by_deal`` snapshot decides whether
                        # this fill actually retired the position.
                        #   * Deal absent / size <= 0 → full close. Tear
                        #     down the entry + bracket rows so
                        #     :meth:`_reconcile_snapshot` does not stamp
                        #     ``missing_pending_since`` on the now-gone
                        #     dealId next poll.
                        #   * Deal still present at size > 0 → either a
                        #     PARTIAL close (``USER`` / ``DEALER`` reduced
                        #     the deal — the OCA bracket stays attached
                        #     to the remaining exposure) or a race where
                        #     ``/history/activity`` is ahead of
                        #     ``/positions`` in this same poll. Stamping
                        #     ``natural_close_at`` here would orphan the
                        #     live exposure: ``execute_exit`` / ``modify_exit``
                        #     and reconcile would skip the still-open
                        #     remainder. Leave a ``close_event_yielded_at``
                        #     breadcrumb instead — :meth:`_reconcile_snapshot`
                        #     reads it on a subsequent poll: if the deal has
                        #     since vanished, that's the race resolving and
                        #     the row is upgraded to a teardown without
                        #     waiting for the missing-pending grace window
                        #     (which would otherwise raise a false
                        #     :class:`UnexpectedCancelError`).
                        #   * Deal absent, but a later close of the same deal
                        #     follows in this batch (an own partial close and
                        #     someone else's close of the rest) → not this
                        #     close's teardown: it is booked like a partial, and
                        #     the last close tears down. Tearing down at the
                        #     first would turn the later, different close into
                        #     a suppressed replay — never booked, never judged.
                        #   * Deal absent, but the deal's delivery queue still
                        #     owes a readable fill
                        #     (:func:`~pynecore_capitalcom.delivery.delivery_pending`:
                        #     a read returned the closes in separate responses)
                        #     → booked like a partial too: the teardown would
                        #     settle the queued fill unbooked, and the last
                        #     delivered close tears down.
                        deal_remaining: float | None = None
                        if (deal_id and positions_by_deal
                                and deal_id in positions_by_deal):
                            pos_snap = (
                                positions_by_deal[deal_id].get('position') or {}
                            )
                            deal_remaining = float(pos_snap.get('size') or 0.0)
                        booked_row = (self.store_ctx.get_order(row.client_order_id)
                                      if self.store_ctx is not None else None) or row
                        if ((deal_remaining is None or deal_remaining <= 0.0)
                                and not self._later_close_in_batch(
                                    activities_sorted, index, deal_id, row)
                                and not delivery_pending(booked_row.extras or {})):
                            self._tear_down_natural_close(row)
                        elif self.store_ctx is not None:
                            refreshed = self.store_ctx.get_order(row.client_order_id)
                            if refreshed is not None:
                                mark_extras = dict(refreshed.extras or {})
                                mark_extras['close_event_yielded_at'] = epoch_time()
                                # Tie the breadcrumb to the current poll cycle so
                                # :meth:`_reconcile_snapshot` does not clear it
                                # against the same stale snapshot that triggered
                                # the defer here. See ``_current_poll_id`` in
                                # ``__init__`` for the rationale.
                                mark_extras['close_event_yielded_at_poll_id'] = (
                                    self._current_poll_id
                                )
                                # A reduction may change the bracket the venue
                                # keeps on the residual: the next snapshot read
                                # after it becomes the accepted protection.
                                mark_extras[EXTRAS_KEY_PROTECTION_ADOPT_AFTER] = (
                                    epoch_time()
                                )
                                # The run's share of the deal shrinks by what
                                # someone else (or the venue) took: the
                                # ``filled_qty`` exposure cursor follows it, as it
                                # does after an own partial close. An own close
                                # already lowered it at dispatch; an own netting
                                # entry offsets it with its own cursor.
                                remaining: float | None = None
                                if (event.leg_type == LegType.CLOSE
                                        and close_verdict in EXTERNAL_REDUCTION_VERDICTS):
                                    remaining = max(
                                        0.0, refreshed.filled_qty - float(event.fill_qty or 0.0),
                                    )
                                    mark_extras['partial_close_retired_at'] = epoch_time()
                                if remaining is None:
                                    self.store_ctx.upsert_order(
                                        row.client_order_id, extras=mark_extras,
                                    )
                                else:
                                    self.store_ctx.upsert_order(
                                        row.client_order_id, filled_qty=remaining,
                                        extras=mark_extras,
                                    )
                                    self.store_ctx.log_event(
                                        'partial_close_journal_retired',
                                        client_order_id=row.client_order_id,
                                        exchange_order_id=deal_id,
                                        payload={
                                            'retired_exposure': float(event.fill_qty or 0.0),
                                            'remaining_exposure': remaining,
                                            'source': a.get('source'),
                                            'verdict': close_verdict,
                                        },
                                    )
                # Who ended a resting bot order is recorded in the dedup
                # commit's transaction too: the deduplicated activity never
                # replays, and without the record the disappearance verdict
                # would take the venue's cancel for someone else's.
                venue_ended = (
                    event is not None
                    and event.event_type in ('cancelled', 'rejected')
                    and (row.extras or {}).get('kind') == 'working'
                    and self._note_working_order_end(row, a)
                )
            cursor.seen_fingerprints[fingerprint] = date_utc
            if (not deferred_in_batch
                    and (not cursor.last_date_utc
                         or date_utc > cursor.last_date_utc)):
                cursor.last_date_utc = date_utc
            report_close = close_verdict == CLOSE_VERDICT_FOREIGN

            if event is not None:
                if suppress_fill:
                    # The activity is already durable (``activity_processed``
                    # logged, cursor advanced) and the entry/bracket rows were
                    # torn down above — only the double-counting fill is
                    # dropped.
                    continue
                if venue_ended and event.event_type == 'cancelled':
                    # The venue itself ended the resting order: the engine
                    # would take the cancel for someone else's. The
                    # disappearance verdict retires the row as the venue's.
                    continue
                if not report_close:
                    yield event
                    continue
                # The close is already durable (dedup committed, exposure
                # retired), so a replay after a restart is suppressed: the
                # report must also run when the consumer abandons this
                # generator at the yield instead of resuming it.
                try:
                    yield event
                finally:
                    self._report_manual_close(row, a)

        # End-of-poll pruning — both dedup structures stay bounded by the
        # venue's rolling window instead of growing for the whole session.
        # Safe by construction: the watermark guard above skips any row
        # older than the watermark's publication-lag floor before its
        # fingerprint is consulted, and an external row that left the
        # rolling window can never be returned by the venue again.
        if cursor.last_date_utc:
            prune_floor = _activity_watermark_floor(cursor.last_date_utc)
            stale = [fp for fp, d in cursor.seen_fingerprints.items()
                     if d < prune_floor]
            for fp in stale:
                del cursor.seen_fingerprints[fp]
        cursor.external_logged_fingerprints &= batch_fingerprints
        cursor.held_logged_fingerprints &= batch_fingerprints

    def _activity_to_event(
            self, activity: dict, row: 'OrderRow',
            position_snapshot: dict | None = None,
    ) -> OrderEvent | None:
        """Map a Capital.com activity row to an :class:`OrderEvent`.

        Returns ``None`` when the row does not correspond to an order-
        lifecycle transition the sync engine needs (e.g. a ``UPDATE``
        without size change).

        :param position_snapshot: Optional ``GET /positions`` row for the
            same ``dealId``, used to recover the fill price when the
            activity row's ``level`` field is empty / zero.  Capital.com
            occasionally returns market-entry activity rows without the
            execution price — without this fallback the resulting
            :class:`OrderEvent` carries ``fill_price=0.0`` and
            :meth:`BrokerPosition.record_fill` rejects it.

            Fill-price resolution chain (first non-zero wins):

            1. ``activity.level`` — the normal happy path.
            2. ``position_snapshot['position']['level']`` — recovers
               the OPEN price when the activity row leaves it blank
               but the position is already open in the same poll's
               snapshot. **Gated to non-closing activities**: the
               snapshot's ``level`` is the position's *open* price, so
               using it for a close would emit a close event at the
               entry price and silently corrupt P&L.
            3. Bracket TP/SL leg fallback (closing legs only): the
               leg row's persisted ``tp_level`` / ``sl_level``.
            4. ``row.extras['confirm_level']`` — the price the confirm
               step reported at :meth:`execute_entry` time, persisted
               specifically for the race where the fill lands
               *between* the poll's ``/positions`` and
               ``/history/activity`` fetches, so neither carries it.
               Gated to non-closing activities (the entry's
               confirm_level is the open price, not a close price).
            5. None of the above resolved a price for a closing leg →
               :meth:`_process_activity`'s defer guard skips the
               yield and the dedup commit so the next poll re-tries.
        """
        activity_type = (activity.get('type') or '').upper()
        status = (activity.get('status') or '').upper()
        source = (activity.get('source') or '').upper()
        # The real ``detailed=true`` payload carries size / level / direction
        # inside ``details`` only (measured 2026-07-10 — there is NO top-level
        # ``size``/``direction``); the top-level reads are kept first for
        # backward compatibility with any legacy payload shape.
        details = activity.get('details') or {}
        size = activity_size(activity) or row.qty
        level = _wire_price(activity.get('level') or details.get('level'))

        # A closing leg on a position-attached bracket: the activity
        # references the entry row's dealId (TP/SL share the entry's
        # dealId on Capital.com), so the entry row's ``confirm_level``
        # is the *open* price — wrong for the close fallback. The
        # corresponding bracket row's ``tp_level`` / ``sl_level`` is
        # the right source instead.
        #
        # Manual close (``DELETE /positions/{dealId}`` from the web UI
        # / mobile app / dealer intervention) lands on the entry row
        # with the same ``dealId`` as the entry fill — Capital.com nets
        # per dealId. Distinguishing entry-fill from manual-close
        # requires per-row state: the first POSITION/EXECUTED activity
        # routed for the row stamps ``extras['entry_filled_at']``
        # (done by :meth:`_process_activity` after the event yields);
        # subsequent activities for the same row are necessarily
        # closes. Without this disambiguation, ``USER`` / ``DEALER``
        # close activities would route as :class:`LegType.ENTRY` and
        # ``BrokerPosition.record_fill`` would ADD to the existing
        # position instead of reducing it.
        row_extras = row.extras or {}
        row_leg_kind = row_extras.get('leg_kind')
        row_kind = row_extras.get('kind', 'position')
        entry_already_filled = bool(row_extras.get('entry_filled_at'))
        # Defensive fallback for the "activity rolled out before stamp"
        # race. ``entry_filled_at`` is stamped just-in-time by
        # :meth:`_process_activity` right before yielding the entry-fill
        # event; the ``activity_processed`` log + cursor advance
        # committed earlier in that block are durable, so a crash
        # between :meth:`execute_entry`'s confirm and the next poll's
        # stamp leaves the row at ``state='confirmed'`` /
        # ``filled_qty > 0`` / ``kind='position'`` with no breadcrumb.
        # If Capital.com's 60s activity window rolls past the entry
        # before the process restarts and resumes polling, the
        # entry-fill activity simply never replays — the breadcrumb
        # stays absent forever. Without this fallback a subsequent
        # ``USER`` / ``DEALER`` manual close on the same dealId would
        # fall through as :class:`LegType.ENTRY` and
        # ``BrokerPosition.record_fill`` would ADD to the local
        # position instead of reducing it.
        #
        # Resolution: a ``confirmed`` (or ``closing``) row with
        # ``filled_qty > 0`` and ``kind == 'position'`` is by definition
        # an already-filled entry; the only question is whether the
        # *current* activity is the entry's first fill or a separate
        # trade. ``row.created_ts_ms`` settles that — entry-fill
        # activities arrive within Capital.com's 60s activity window of
        # row creation, anything older than the window is necessarily
        # a separate trade and therefore a close.
        if (not entry_already_filled
                and row_kind == 'position'
                and row.state in ('confirmed', 'closing')
                and row.filled_qty > 0
                and row.created_ts_ms):
            activity_iso = activity.get('dateUTC') or ''
            if activity_iso:
                activity_ts = _parse_iso_timestamp(activity_iso)
                if (activity_ts > 0.0
                        and activity_ts > (row.created_ts_ms / 1000.0) + 60.0):
                    entry_already_filled = True
        # Deterministic close discriminator (primary, MEASURED 2026-07-10):
        # ``details.direction`` is the TRADE direction — BUY on the open
        # activity of a long, SELL on its close — and on Capital.com a
        # same-direction add on an existing dealId is impossible (every
        # open creates a fresh deal), so a POSITION activity carrying the
        # row's own direction is the entry's fill and an opposite-direction
        # one is a reduce/close. This must NOT be left to the time
        # heuristics above: the feed stamps the entry-fill activity up to
        # 60+ s after the deal (observed 61.4 s), which tripped the
        # ``created_ts_ms + 60`` fallback and routed the bot's own entry
        # fill as a manual close — a sign-flip in the engine's position
        # accounting. Deliberately details-only: no top-level ``direction``
        # exists in the measured payload, and inventing semantics for one
        # would risk reading a position-direction field as a trade
        # direction. The heuristics remain the tie-breaker when the
        # direction is absent.
        activity_direction = (details.get('direction') or '').upper()
        is_same_direction: bool | None = None
        if row_kind == 'position' and activity_direction in ('BUY', 'SELL'):
            row_direction = 'BUY' if row.side == 'buy' else 'SELL'
            is_same_direction = activity_direction == row_direction
        is_explicit_close_source = source in (
            'TP', 'SL', 'CLOSE_OUT', 'CLOSE', 'MARGIN', 'STOP_OUT',
        )
        closes_filled_entry = (
            row_kind == 'position'
            and row_leg_kind not in ('tp', 'sl')
            and (
                is_same_direction is False
                or (is_same_direction is None and entry_already_filled)
            )
        )
        # Who made the row is the shared source reading the close attribution
        # uses (:func:`_activity_actor`): a client or the dealing desk
        # closing by hand, or the venue itself.
        actor = _activity_actor(source)
        is_manual_close = closes_filled_entry and actor in (ACTOR_CLIENT, ACTOR_DEALER)
        # A venue-side action (``SYSTEM``, or any source but a client's or
        # the dealing desk's) is never an operator's or another bot's, but
        # unlike ``CLOSE_OUT`` the source alone does not say "close": only
        # the same direction / fill-state evidence the manual path uses makes
        # it one, so an entry-side ``SYSTEM`` row still routes as the entry
        # fill.
        is_system_close = closes_filled_entry and actor == ACTOR_VENUE
        is_closing_leg = (
            activity_type == 'POSITION'
            and status in ('EXECUTED', 'ACCEPTED')
            and (
                is_explicit_close_source
                or row_leg_kind in ('tp', 'sl')
                or is_manual_close
                or is_system_close
            )
        )

        # Snapshot fallback is gated to NON-closing activities. A
        # ``/positions`` snapshot's ``level`` field is the position's
        # OPEN price, which is the correct fill price for a market
        # entry whose activity row left ``level`` blank, but the WRONG
        # price for a close — using it would emit a close event at the
        # entry price and silently corrupt P&L. Closing legs with
        # ``level=0`` fall through to the bracket-leg fallback below
        # (TP/SL only) or :meth:`_process_activity`'s defer guard.
        if (level <= 0.0
                and position_snapshot is not None
                and not is_closing_leg):
            pos_data = position_snapshot.get('position') or {}
            fallback_level = _wire_price(pos_data.get('level'))
            if fallback_level > 0.0:
                level = fallback_level
        # Bracket TP/SL fill fallback: ONLY when the closing event truly
        # comes from a bracket leg — explicit ``source in ('TP', 'SL')`` or
        # the row itself is a stored bracket leg. Generic closes
        # (``CLOSE``/``CLOSE_OUT``/``MARGIN``/``STOP_OUT``, venue-side
        # ``SYSTEM`` and manual ``USER``/``DEALER`` closes) execute at the
        # prevailing market price, NOT at the bracket SL level — falling
        # back to the still-attached SL leg's stored level would misprice
        # the fill and corrupt P&L. When the activity ``level`` is missing for those
        # cases, leave it at 0; :meth:`_process_activity` defers the
        # event (no yield, no dedup commit, no ``natural_close_at``
        # stamp) so the next poll re-evaluates with a fresh
        # ``positions_by_deal`` snapshot. If the activity rolls out of
        # Capital.com's 60s history window before a price surfaces,
        # :meth:`_reconcile_snapshot` still detects the vanished deal
        # and routes through the missing-pending grace window
        # (``UnexpectedCancelError``) — preferable to silently emitting
        # a zero-priced close that ``BrokerPosition.record_fill`` would
        # ignore while the row gets stamped closed.
        is_bracket_leg_close = (
            is_closing_leg
            and (source in ('TP', 'SL') or row_leg_kind in ('tp', 'sl'))
        )
        if level <= 0.0 and is_bracket_leg_close:
            leg_kind_for_fallback = (
                'tp' if source == 'TP' or row_leg_kind == 'tp' else 'sl'
            )
            leg_row = self._find_bracket_leg_row(row, leg_kind_for_fallback)
            if leg_row is not None:
                leg_level = (leg_row.tp_level if leg_kind_for_fallback == 'tp'
                             else leg_row.sl_level)
                if _wire_price(leg_level) > 0.0:
                    level = _wire_price(leg_level)
        if level <= 0.0 and not is_closing_leg:
            # Entry-leg last-resort fallback: the price the confirm step
            # reported at :meth:`execute_entry` time, persisted into
            # ``extras`` (see the PERSIST dealId block in
            # :meth:`execute_entry`). Covers the poll-cycle race where
            # the fill lands between the ``/positions`` snapshot and
            # ``/history/activity`` fetches, so neither carries the
            # price. Closing legs deliberately skip this — for them the
            # entry's confirm_level is the open price, not the close
            # price.
            confirm_level = _wire_price((row.extras or {}).get('confirm_level'))
            if confirm_level > 0.0:
                level = confirm_level
        now_ts = epoch_time()

        # Closing-leg side is the OPPOSITE of the matched entry row's
        # side. Capital.com's ``direction`` field on a TP/SL close
        # activity reports the POSITION direction (``'BUY'`` for a long
        # being closed), NOT the closing trade direction — verified
        # live 2026-05-01. Without this flip
        # ``BrokerPosition.record_fill`` reads ``side='buy'`` on a
        # long-closing fill, computes a positive ``signed_delta``, and
        # ADDS to the existing long instead of reducing it.
        if is_closing_leg:
            side = 'sell' if row.side == 'buy' else 'buy'
        else:
            side = row.side
        order_type = _order_type_from_row(row, activity_type)
        leg_kind = row_leg_kind

        leg_type: LegType | None = None
        event_type: str | None = None

        if activity_type == 'POSITION':
            if status in ('EXECUTED', 'ACCEPTED'):
                if source == 'TP' or leg_kind == 'tp':
                    leg_type = LegType.TAKE_PROFIT
                    event_type = 'filled'
                elif source == 'SL' or leg_kind == 'sl':
                    leg_type = (LegType.TRAILING_STOP if row.trailing_stop
                                else LegType.STOP_LOSS)
                    event_type = 'filled'
                elif source in ('CLOSE_OUT', 'CLOSE', 'MARGIN', 'STOP_OUT'):
                    # Unambiguous close sources — the bracket leg lookup
                    # already ran for TP/SL above; everything else is
                    # an exchange-side close (margin, stop-out, manual
                    # via Close button) on the entry row.
                    leg_type = LegType.CLOSE
                    event_type = 'filled'
                elif is_system_close:
                    # Venue-side close of a filled bot entry — booked
                    # like ``CLOSE_OUT``, never an operator intervention.
                    leg_type = LegType.CLOSE
                    event_type = 'filled'
                elif is_manual_close:
                    # USER/DEALER on an already-filled entry row =
                    # manual close (web UI / mobile / dealer).
                    leg_type = LegType.CLOSE
                    event_type = 'filled'
                else:
                    leg_type = LegType.ENTRY
                    event_type = 'filled'
            elif status in ('REJECTED',):
                event_type = 'rejected'
            elif status in ('CANCELLED', 'DELETED', 'EXPIRED'):
                event_type = 'cancelled'
        elif activity_type == 'WORKING_ORDER':
            if status in ('ACCEPTED', 'CREATED'):
                leg_type = LegType.ENTRY
                event_type = 'created'
            elif status == 'EXECUTED':
                leg_type = LegType.ENTRY
                event_type = 'filled'
            elif status in ('EXPIRED', 'REJECTED', 'DELETED', 'CANCELLED'):
                event_type = 'cancelled' if status != 'REJECTED' else 'rejected'

        if event_type is None:
            return None

        # The close of a core leg close (``close_leg``'s DELETE, stamped
        # ``USER`` like a manual close) reports the coid the core gave it, so
        # the core tells a hedging account's netting closes from the
        # strategy's closes; any other close reports the position row's own.
        # A ``held_execution_close``'s close fill (its own-close record keeps
        # the close's coid the same way) carries that close's Pine id, so the
        # engine books it as the held-execution close, capped at its book.
        client_order_id = row.client_order_id
        pine_id = row.pine_entry_id
        if leg_type is LegType.CLOSE and event_type == 'filled' and actor == ACTOR_CLIENT:
            client_order_id = self._own_close_leg_coid(row, activity) or client_order_id
            held_close = _held_close_label(row.extras or {}, client_order_id)
            if held_close is not None:
                pine_id = held_close[0]

        exch_order = ExchangeOrder(
            id=row.exchange_order_id or '',
            symbol=row.symbol,
            side=side,
            order_type=order_type,
            qty=row.qty,
            filled_qty=size if event_type == 'filled' else row.filled_qty,
            remaining_qty=max(0.0, row.qty - size) if event_type == 'filled' else row.qty,
            price=None,
            stop_price=None,
            average_fill_price=level if event_type == 'filled' else None,
            status=(OrderStatus.FILLED if event_type == 'filled'
                    else OrderStatus.CANCELLED if event_type == 'cancelled'
                    else OrderStatus.REJECTED if event_type == 'rejected'
                    else OrderStatus.OPEN),
            timestamp=now_ts,
            fee=0.0,
            fee_currency='',
            reduce_only=(leg_kind in ('tp', 'sl')),
            client_order_id=client_order_id,
        )
        return OrderEvent(
            order=exch_order,
            event_type=event_type,
            fill_price=level if event_type == 'filled' else None,
            fill_qty=size if event_type == 'filled' else None,
            journal_fill_qty=size if event_type == 'filled' else None,
            timestamp=now_ts,
            pine_id=pine_id,
            from_entry=row.from_entry,
            leg_type=leg_type,
            # Capital.com exposes no stable activity/execution id, so the
            # content-addressed activity fingerprint (the same key
            # :meth:`_process_activity` dedups on) is the canonical per-fill
            # id. The engine's duplicate-fill gate keys on it as a backstop
            # for a row redelivered past the local ``seen_fingerprints`` set
            # (e.g. cross-restart before the cursor persists). Snapshot
            # reconcile emissions carry no fingerprint and stay ``None``,
            # relying on the ``filled_qty`` cursor watermark instead.
            fill_id=_activity_fingerprint(activity),
        )

    def _bridge_working_order_deal(
            self, wo_row: 'OrderRow', deal_id: str, activity: dict,
            activities: list[dict], positions_by_deal: dict[str, dict] | None,
            fingerprint: str,
    ) -> float | None:
        """Promote an own order onto the deal a ``POSITION`` activity says it opened.

        The bridge leaves the entry fill to this activity (no
        ``entry_filled_at`` stamp). When the order also netted a foreign deal,
        only the opened deal's size is the run's: the size the opening
        activity states. Without one, the execution is held until the size is
        known (:meth:`_hold_netted_residual`).

        :param wo_row: The order's row, still on the order's ``dealId``.
        :param deal_id: The deal the activity names.
        :param activity: The ``POSITION`` activity back-linking the order.
        :param activities: The activity batch.
        :param positions_by_deal: The poll's ``/positions`` rows by ``dealId``.
        :param fingerprint: The activity's fingerprint, the held fill's id.
        :return: The opened size to book for a partly netted order, else ``None``.
        """
        netted = self._netted_foreign_deal(wo_row, activities, positions_by_deal)
        if netted is None:
            self._promote_working_row_to_position(wo_row, deal_id, stamp_entry_filled=False)
            return None
        netted_deal_id, conflict = netted
        self._report_own_order_netted_foreign_deal(
            wo_row, wo_row.exchange_order_id or '', netted_deal_id, conflict, positions_by_deal,
        )
        opened = self._netted_residual_size(wo_row.side, deal_id, [activity])
        date_utc = str(activity.get('dateUTC') or '')
        if opened is None:
            details = activity.get('details') or {}
            self._hold_netted_residual(
                wo_row, deal_id, netted_deal_id, executed=wo_row.qty,
                fill_price=_wire_float(activity.get('level') or details.get('level'),
                                       finite=True),
                fill_id=fingerprint, date_utc=date_utc,
            )
            return None
        if self._opened_deal_price(wo_row.side, deal_id, [activity], positions_by_deal) <= 0.0:
            # The size is proven but no price is: held with the size, booked
            # by the first read that prices it (:meth:`_settle_netted_residual`).
            self._hold_netted_residual(
                wo_row, deal_id, netted_deal_id, executed=wo_row.qty,
                fill_price=None, fill_id=fingerprint, opened_qty=opened, date_utc=date_utc,
            )
            return None
        self._promote_working_row_to_position(wo_row, deal_id, stamp_entry_filled=False)
        return opened

    def _promote_executed_working_order(
            self, row: 'OrderRow', event: OrderEvent, activities: list[dict],
            positions_by_deal: dict[str, dict] | None, *, date_utc: str,
    ) -> OrderEvent | None:
        """Move an executed own working order onto the deal it opened, before its fill is booked.

        :param row: The working-order row.
        :param event: The ``filled`` ENTRY event of its ``WORKING_ORDER EXECUTED`` activity.
        :param activities: The activity batch.
        :param positions_by_deal: The poll's ``/positions`` rows by ``dealId``.
        :param date_utc: The execution activity's ``dateUTC``.
        :return: The event to book: ``event``, the residual of a partly netted
            execution, the ``rejected`` entry of one that only netted a foreign
            deal, or ``None`` while a partly netted execution's opened size is
            unknown.
        """
        assert self.store_ctx is not None
        promoted_deal_id = self._find_promoted_deal_id(row, activities, positions_by_deal)
        netted = self._netted_foreign_deal(row, activities, positions_by_deal)
        if netted is not None and not promoted_deal_id:
            return self._retire_order_netted_into_foreign_deal(
                row, event, netted, positions_by_deal,
            )
        if promoted_deal_id:
            if netted is None:
                self._promote_working_row_to_position(
                    row, promoted_deal_id, stamp_entry_filled=True,
                    executed_date_utc=date_utc,
                )
                return event
            # A held execution promotes the row itself.
            residual = self._book_residual_of_partly_netted_order(
                row, event, netted, promoted_deal_id, activities, positions_by_deal,
                date_utc=date_utc,
            )
            if residual is not None:
                self._promote_working_row_to_position(
                    row, promoted_deal_id, stamp_entry_filled=True,
                    executed_date_utc=date_utc,
                )
            return residual
        refreshed = self.store_ctx.get_order(row.client_order_id)
        base_extras = dict((refreshed or row).extras or {})
        base_extras['entry_filled_at'] = epoch_time()
        if date_utc:
            base_extras[EXTRAS_KEY_ENTRY_EXECUTED_UTC] = date_utc
        base_extras.pop('missing_pending_since', None)
        self.store_ctx.upsert_order(row.client_order_id, extras=base_extras)
        return event

    def _later_close_in_batch(
            self, activities: list[dict], index: int, deal_id: str, row: 'OrderRow',
    ) -> bool:
        """Whether an unprocessed close of ``deal_id`` follows ``activities[index]``.

        :param activities: The batch, sorted by ``dateUTC``.
        :param index: The position of the close being booked.
        :param deal_id: Its deal.
        :param row: The bot position row the deal belongs to.
        """
        seen = self._activity_cursor.seen_fingerprints
        current = _activity_fingerprint(activities[index])
        for later in activities[index + 1:]:
            fingerprint = _activity_fingerprint(later)
            if (str(later.get('dealId') or '') != deal_id
                    or fingerprint == current or fingerprint in seen):
                continue
            event = self._activity_to_event(later, row)
            if (event is not None and event.event_type == 'filled'
                    and event.leg_type in (LegType.TAKE_PROFIT, LegType.STOP_LOSS,
                                           LegType.TRAILING_STOP, LegType.CLOSE)):
                return True
        return False

    def _find_promoted_deal_id(
            self,
            row: 'OrderRow',
            activities: list[dict],
            positions_by_deal: dict[str, dict] | None,
    ) -> str | None:
        """The NEW position dealId a filled working order promoted into.

        Two sources, in evidence order: the batch's POSITION activity
        whose ``details.workingOrderId`` back-links to the row's old
        dealId, then the same-poll ``/positions`` snapshot (the position
        payload carries ``workingOrderId`` directly).

        A candidate already owned by a DIFFERENT row is rejected:
        Capital.com opens a fresh dealId for every entry, so a matching
        activity that carries another row's deal is venue noise, not the
        promotion target (measured 2026-08-23, cycle 61: the same-instant
        POSITION activity of an unrelated TP-closed deal matched the
        back-link and the promote rewrote the working row onto the DEAD
        dealId — never present in ``/positions`` again, so the
        disappearance tracker confirmed a false unexpected cancel and
        quarantined the run). With no clean candidate the caller defers;
        the next poll's snapshot back-link completes the migration
        within the grace window.

        A candidate the order cannot have opened is rejected too: a deal of
        the other direction, or one created well before the activity naming
        it (:meth:`_promotion_conflict`) — a netting account reports the deal
        an executing working order reduced with the same back-link.
        """
        old_id = row.exchange_order_id or ''
        if not old_id:
            return None

        def owned_by_other(candidate: str) -> bool:
            if self.store_ctx is None:
                return False
            owner = self.store_ctx.find_by_ref('deal_id', candidate)
            return (owner is not None
                    and owner.client_order_id != row.client_order_id)

        for a in activities:
            if (str(a.get('type') or '') == 'POSITION'
                    and str((a.get('details') or {}).get('workingOrderId')
                            or '') == old_id):
                new_id = str(a.get('dealId') or '')
                if (new_id and new_id != old_id
                        and not owned_by_other(new_id)
                        and self._promotion_conflict(
                            row.side, new_id,
                            _parse_iso_timestamp(str(a.get('dateUTC') or '')),
                            positions_by_deal) is None):
                    return new_id
        for deal_id, raw in (positions_by_deal or {}).items():
            pos = raw.get('position') or {}
            if (str(pos.get('workingOrderId') or '') == old_id
                    and deal_id != old_id
                    and not owned_by_other(deal_id)
                    and self._promotion_conflict(
                        row.side, deal_id, 0.0, positions_by_deal) is None):
                return deal_id
        return None

    def _netted_foreign_deal(
            self,
            row: 'OrderRow',
            activities: list[dict],
            positions_by_deal: dict[str, dict] | None,
    ) -> tuple[str, str] | None:
        """The foreign deal an executed own working order netted instead of opening one.

        On a netting account an executing working order reduces the oldest
        opposite deal, and the venue names the order on that deal too. The
        evidence, in order: the netted deal the row already recorded (the
        deal's activity came first), the batch's ``POSITION`` activity
        back-linking the order, the same-poll ``/positions`` snapshot. Only a
        deal no own row tracks and that the order cannot have opened
        (:meth:`_promotion_conflict`) counts.

        :return: ``(dealId, conflict)``, or ``None`` without such evidence.
        """
        store_ctx = self.store_ctx
        old_id = row.exchange_order_id or ''
        if store_ctx is None or not old_id:
            return None
        fresh = store_ctx.get_order(row.client_order_id) or row
        recorded = (fresh.extras or {}).get(EXTRAS_KEY_NETTED_FOREIGN_DEAL)
        if isinstance(recorded, dict) and recorded.get('deal_id'):
            return str(recorded['deal_id']), str(recorded.get('conflict') or '')

        candidates: list[tuple[str, float]] = [
            (str(a.get('dealId') or ''), _parse_iso_timestamp(str(a.get('dateUTC') or '')))
            for a in activities
            if (str(a.get('type') or '') == 'POSITION'
                and str((a.get('details') or {}).get('workingOrderId') or '') == old_id)
        ]
        candidates.extend(
            (deal_id, 0.0) for deal_id, raw in (positions_by_deal or {}).items()
            if str((raw.get('position') or {}).get('workingOrderId') or '') == old_id
        )
        for deal_id, activity_ts in candidates:
            if (not deal_id or deal_id == old_id
                    or store_ctx.find_by_ref('deal_id', deal_id) is not None):
                continue
            conflict = self._promotion_conflict(
                row.side, deal_id, activity_ts, positions_by_deal)
            if conflict is not None:
                return deal_id, conflict
        return None

    def _retire_order_netted_into_foreign_deal(
            self,
            row: 'OrderRow',
            event: OrderEvent,
            netted: tuple[str, str],
            positions_by_deal: dict[str, dict] | None,
    ) -> OrderEvent:
        """Retire an own working order whose execution only netted a foreign deal.

        The order executed, but into someone else's deal: the run holds no
        exposure from it. Booking its fill would give the engine a position
        the venue never shows the run, so the row is retired instead, with
        nothing booked on the exposure cursor, and the engine gets the entry
        back as ``rejected`` (no fill, no quarantine of its own). The netting
        itself is reported once (ERROR, audit event, ``ENTRY_BLOCK``). The
        retirement is remembered by the order's ``dealId``
        (:meth:`_netting_retirements`): a deal the order opened with the rest
        of its size may surface only later.

        :param row: The own working-order row.
        :param event: The ``filled`` ENTRY event of the order's execution.
        :param netted: ``(dealId, conflict)`` of the netted foreign deal.
        :param positions_by_deal: The poll's ``/positions`` rows by ``dealId``.
        :return: The ``rejected`` event to yield in place of the fill.
        """
        assert self.store_ctx is not None
        wo_id = row.exchange_order_id or ''
        deal_id, conflict = netted
        self._report_own_order_netted_foreign_deal(
            row, wo_id, deal_id, conflict, positions_by_deal,
        )
        fresh = self.store_ctx.get_order(row.client_order_id)
        if fresh is not None and fresh.closed_ts_ms is None:
            retired = {'client_order_id': row.client_order_id, 'side': row.side,
                       'symbol': row.symbol, 'netted_deal_id': deal_id}
            self._netting_retirements()[wo_id] = retired
            self.store_ctx.log_event(
                'own_working_order_retired_netted_foreign_deal',
                client_order_id=row.client_order_id,
                exchange_order_id=wo_id,
                payload={'netted_deal_id': deal_id, 'conflict': conflict,
                         'side': row.side, 'symbol': row.symbol,
                         'executed_qty': event.fill_qty,
                         'executed_price': event.fill_price},
            )
            self.store_ctx.close_order(row.client_order_id)
        order = event.order
        return OrderEvent(
            order=ExchangeOrder(
                id=order.id, symbol=order.symbol, side=order.side,
                order_type=order.order_type, qty=order.qty,
                filled_qty=0.0, remaining_qty=order.qty,
                price=order.price, stop_price=order.stop_price,
                average_fill_price=None, status=OrderStatus.REJECTED,
                timestamp=order.timestamp, fee=0.0, fee_currency='',
                reduce_only=order.reduce_only, client_order_id=order.client_order_id,
            ),
            event_type='rejected',
            fill_price=None,
            fill_qty=None,
            timestamp=event.timestamp,
            pine_id=event.pine_id,
            from_entry=event.from_entry,
            leg_type=event.leg_type,
        )

    def _book_residual_of_partly_netted_order(
            self,
            row: 'OrderRow',
            event: OrderEvent,
            netted: tuple[str, str],
            promoted_deal_id: str,
            activities: list[dict],
            positions_by_deal: dict[str, dict] | None,
            *,
            date_utc: str,
    ) -> OrderEvent | None:
        """The fill of an own working order that netted a foreign deal and opened one.

        On a netting account an order larger than the foreign deal it nets
        consumes that deal and opens a deal of its own with the rest. Only the
        rest is the run's exposure: the ``WORKING_ORDER EXECUTED`` fill is cut
        to the opened deal's size, so the engine never books exposure the
        venue does not show the run. The netting itself is reported once
        (ERROR, audit event, ``ENTRY_BLOCK``).

        While neither the batch nor the snapshot tells the opened size, nothing
        is booked: the execution is kept on the row
        (:data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_NETTED_RESIDUAL_PENDING`)
        until the deal's size is known (:meth:`_settle_netted_residual`).

        :param row: The own working-order row.
        :param event: The ``filled`` ENTRY event of the order's whole execution.
        :param netted: ``(dealId, conflict)`` of the netted foreign deal.
        :param promoted_deal_id: The deal the order opened.
        :param activities: The activity batch.
        :param positions_by_deal: The poll's ``/positions`` rows by ``dealId``.
        :param date_utc: The execution activity's ``dateUTC``.
        :return: The event to book (the residual fill), or ``None`` while the
            opened size is unknown.
        """
        assert self.store_ctx is not None
        wo_id = row.exchange_order_id or ''
        deal_id, conflict = netted
        self._report_own_order_netted_foreign_deal(
            row, wo_id, deal_id, conflict, positions_by_deal,
        )
        opened = self._netted_residual_size(row.side, promoted_deal_id, activities)
        executed = float(event.fill_qty or 0.0)
        self.store_ctx.log_event(
            'own_working_order_partly_netted_foreign_deal',
            client_order_id=row.client_order_id,
            exchange_order_id=promoted_deal_id,
            payload={'working_order': wo_id, 'netted_deal_id': deal_id,
                     'conflict': conflict, 'executed_qty': executed,
                     'opened_qty': opened},
        )
        if opened is None:
            self._hold_netted_residual(
                row, promoted_deal_id, deal_id, executed=executed,
                fill_price=event.fill_price, fill_id=event.fill_id, date_utc=date_utc,
            )
            return None
        price = (_wire_float(event.fill_price, finite=True)
                 or self._opened_deal_price(row.side, promoted_deal_id, activities,
                                            positions_by_deal))
        if price <= 0.0:
            self._hold_netted_residual(
                row, promoted_deal_id, deal_id, executed=executed,
                fill_price=None, fill_id=event.fill_id, opened_qty=opened, date_utc=date_utc,
            )
            return None
        return self._residual_fill(event, opened, fill_price=price)

    @staticmethod
    def _opened_deal_price(
            side: str, deal_id: str, activities: list[dict],
            positions_by_deal: dict[str, dict] | None,
    ) -> float:
        """The open price of the deal a partly netted own order opened, ``0.0`` when unknown.

        The deal's opening activity's level, else its ``/positions`` row's
        open level.

        :param side: The order's side (``'buy'`` / ``'sell'``).
        :param deal_id: The opened deal.
        :param activities: The activity batch.
        :param positions_by_deal: The poll's ``/positions`` rows by ``dealId``.
        """
        for activity in activities:
            if _is_opening_activity(activity, side, deal_id):
                details = activity.get('details') or {}
                level = _wire_float(activity.get('level') or details.get('level'), finite=True)
                if level is not None and level > 0.0:
                    return level
        position = ((positions_by_deal or {}).get(deal_id) or {}).get('position') or {}
        level = _wire_float(position.get('level'), finite=True)
        return level if level is not None and level > 0.0 else 0.0

    @staticmethod
    def _residual_fill(event: OrderEvent, opened: float, *,
                       fill_price: float | None = None) -> OrderEvent:
        """``event`` cut to the ``opened`` size of the deal a partly netted order opened.

        :param event: The ``filled`` ENTRY event of the order's execution.
        :param opened: The opened deal's size; at or above the executed size
            (nothing was netted after all) the size stays unchanged.
        :param fill_price: The price to book in place of the event's own,
            ``None`` to keep it.
        """
        order = event.order
        if fill_price is not None and fill_price != event.fill_price:
            order = replace(order, average_fill_price=fill_price)
            event = replace(event, order=order, fill_price=fill_price)
        if opened <= 0.0 or opened >= float(event.fill_qty or 0.0):
            return event
        return replace(
            event,
            order=replace(order, filled_qty=opened,
                          remaining_qty=max(0.0, order.qty - opened)),
            fill_qty=opened,
            journal_fill_qty=opened,
        )

    @staticmethod
    def _netted_residual_size(side: str, deal_id: str, activities: list[dict]) -> float | None:
        """The size of the deal a partly netted own working order opened, if an activity states it.

        Only the deal's opening activity counts: a ``POSITION`` activity of the
        deal in the order's own trade direction and from no closing source. A
        close of the deal states the size it took, not what the order opened.
        A ``/positions`` row is no such evidence: it states the size left after
        every reduction it already reflects, which
        :meth:`_settle_netted_residual_from_snapshot` accounts for.

        :param side: The order's side (``'buy'`` / ``'sell'``).
        :param deal_id: The opened deal.
        :param activities: The activity batch.
        :return: The size, or ``None`` when no opening activity states a positive one.
        """
        for activity in activities:
            if _is_opening_activity(activity, side, deal_id):
                opened = activity_size(activity)
                if opened is not None:
                    return opened
        return None

    def _hold_netted_residual(
            self, row: 'OrderRow', deal_id: str, netted_deal_id: str, *,
            executed: float, fill_price: float | None, fill_id: str | None,
            date_utc: str, opened_qty: float | None = None,
    ) -> None:
        """Keep the execution of a partly netted own order unbooked until its size and price are known.

        The order netted a foreign deal and opened ``deal_id`` with the rest,
        but how much it opened is not known yet: booking the whole execution
        would give the run the exposure the foreign deal consumed. A size
        already proven without a price (``opened_qty``) is held the same way:
        a fill without a valid price would be discarded by the engine. The row
        stays on the opened deal with the execution recorded under
        :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_NETTED_RESIDUAL_PENDING`;
        the deal's sized opening activity or the ``/positions`` reads that list
        it book exactly the opened size (:meth:`_settle_netted_residual`,
        :meth:`_settle_netted_residual_from_snapshot`). The record is durable,
        so a restart in between still books it once; the run-owned exposure
        cursor stays at zero meanwhile, so a startup adoption does not count
        the deal either.

        :param row: The own working-order row, before its promotion.
        :param deal_id: The deal the order opened.
        :param netted_deal_id: The foreign deal it netted.
        :param executed: The size of the whole execution.
        :param fill_price: The execution's price, ``None`` when unknown.
        :param fill_id: The execution's fill id.
        :param date_utc: The ``dateUTC`` of the activity that revealed the
            execution; the activity read reaches back to it while the
            execution is held (:meth:`_oldest_pending_activity_ts`).
        :param opened_qty: The opened deal's size when an opening activity
            proved it, ``None`` when unknown.
        """
        assert self.store_ctx is not None
        pending = {'executed_qty': executed, 'fill_price': fill_price, 'fill_id': fill_id,
                   'netted_deal_id': netted_deal_id, 'since': epoch_time(),
                   'date_utc': date_utc}
        if opened_qty is not None:
            pending['opened_qty'] = opened_qty
        with self.store_ctx.transaction():
            self._rewrite_extras(
                row.client_order_id,
                lambda extras: extras.__setitem__(EXTRAS_KEY_NETTED_RESIDUAL_PENDING, pending),
            )
            self.store_ctx.log_event(
                'netted_residual_size_pending',
                client_order_id=row.client_order_id,
                exchange_order_id=deal_id,
                payload={'working_order': row.exchange_order_id, **pending},
            )
            self._promote_working_row_to_position(
                row, deal_id, stamp_entry_filled=True, executed_date_utc=date_utc,
            )
        if opened_qty is None:
            logger.warning(
                "own working order %s (%s) netted foreign deal %s and opened deal %s of "
                "unknown size: nothing of its execution of %s is booked until the deal's "
                "size is known",
                row.exchange_order_id, row.symbol, netted_deal_id, deal_id, executed,
            )
        else:
            logger.warning(
                "own working order %s (%s) netted foreign deal %s and opened deal %s of "
                "%s at an unknown price: nothing is booked until a read prices the deal",
                row.exchange_order_id, row.symbol, netted_deal_id, deal_id, opened_qty,
            )

    def _update_netted_residual(self, row: 'OrderRow', mutate: Callable[[dict], None]) -> None:
        """Apply ``mutate`` to the held execution record of ``row``, if it still has one."""

        def apply(extras: dict) -> None:
            pending = extras.get(EXTRAS_KEY_NETTED_RESIDUAL_PENDING)
            if isinstance(pending, dict):
                pending = dict(pending)
                mutate(pending)
                extras[EXTRAS_KEY_NETTED_RESIDUAL_PENDING] = pending

        self._rewrite_extras(row.client_order_id, apply)

    def _hold_close_of_netted_residual(
            self, row: 'OrderRow', activity: dict, size: float | None,
    ) -> None:
        """Remember a close of a held partly netted deal, deferred until the entry is booked.

        The ``/positions`` size of the deal is net of such a close, so the
        snapshot settlement adds it back (:meth:`_settle_netted_residual_from_snapshot`);
        the close itself is then booked from its activity like any other.

        A close read without its size and again with it is one close: the
        sized copy (same deal, ``dateUTC``, type, status and source) replaces
        the sizeless record and keeps its first sighting, and the sizeless
        copy's identity is marked delivered
        (:func:`~pynecore_capitalcom.delivery.mark_delivered`), so only the
        sized copy is booked once the entry is.

        :param row: The held position row.
        :param activity: The deferred close activity.
        :param size: The size it closed, ``None`` when the activity states none
            (the snapshot then cannot tell the opened size).
        """
        close_id = _close_identity(activity)
        sizeless_id = sizeless_close_identity(activity)
        date_utc = str(activity.get('dateUTC') or '')
        now = epoch_time()

        def remember(extras: dict) -> None:
            pending = extras.get(EXTRAS_KEY_NETTED_RESIDUAL_PENDING)
            if not isinstance(pending, dict):
                return
            pending = dict(pending)
            held = dict(pending.get('held_closes') or {})
            superseded = False
            if size is None:
                if any(close.get('sizeless_id') == close_id and close_id != key
                       for key, close in held.items()):
                    superseded = True
                elif close_id not in held:
                    held[close_id] = {'size': None, 'seen_at': now, 'sizeless_id': close_id,
                                      'date_utc': date_utc}
            elif close_id not in held:
                provisional = held.pop(sizeless_id, None)
                seen_at = now
                if provisional is not None:
                    superseded = True
                    seen_at = min(now, float(provisional.get('seen_at') or now))
                held[close_id] = {'size': size, 'seen_at': seen_at, 'sizeless_id': sizeless_id,
                                  'date_utc': date_utc}
            pending['held_closes'] = held
            extras[EXTRAS_KEY_NETTED_RESIDUAL_PENDING] = pending
            if superseded:
                mark_delivered(extras, sizeless_id)

        self._rewrite_extras(row.client_order_id, remember)

    def _settle_netted_residual(
            self, row: 'OrderRow', opened: float, *, fill_price: float | None,
            fill_id: str | None,
    ) -> OrderEvent | None:
        """Book the held execution of a partly netted own order once its size and price are known.

        Without a valid price the fill would be discarded by the engine while
        the journal already counted it: the proven size is kept on the held
        record instead (``opened_qty``), and a later priced activity or
        ``/positions`` read books it.

        :param row: The position row carrying
            :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_NETTED_RESIDUAL_PENDING`.
        :param opened: The opened deal's size.
        :param fill_price: The price to book when the held execution has none.
        :param fill_id: The fill id to book when the held execution has none.
        :return: The ENTRY fill of the opened size, ``None`` when nothing is
            held or the price is still unknown.
        """
        assert self.store_ctx is not None
        coid = row.client_order_id
        current = self.store_ctx.get_order(coid) or row
        pending = (current.extras or {}).get(EXTRAS_KEY_NETTED_RESIDUAL_PENDING)
        if not isinstance(pending, dict) or opened <= 0.0:
            return None
        price = (_wire_float(pending.get('fill_price'), finite=True)
                 or _wire_float(fill_price, finite=True) or 0.0)
        if price <= 0.0:
            if _wire_float(pending.get('opened_qty'), finite=True) is None:
                self._update_netted_residual(
                    current, lambda held: held.__setitem__('opened_qty', opened))
            return None
        executed = float(pending.get('executed_qty') or current.qty)
        booked = min(opened, executed, current.qty)
        now_ts = epoch_time()
        with self.store_ctx.transaction():
            extras = dict(current.extras or {})
            extras.pop(EXTRAS_KEY_NETTED_RESIDUAL_PENDING, None)
            held_closes = pending.get('held_closes') or {}
            if held_closes:
                # The held closes join the deal's delivery queue and are
                # booked from their activities after the entry: the read keeps
                # reaching back to them until they are. Each close is queued
                # under its sizeless identity too, so a copy stating no size
                # stays deferred in whichever order the copies arrive; booking
                # the close settles both identities.
                for close_id, close in held_closes.items():
                    enqueue_identities(
                        extras,
                        tuple(dict.fromkeys((close_id, str(close.get('sizeless_id') or close_id)))),
                        str(close.get('date_utc') or ''))
            self.store_ctx.upsert_order(
                coid, filled_qty=max(current.filled_qty, booked), extras=extras)
            self.store_ctx.log_event(
                'netted_residual_booked',
                client_order_id=coid,
                exchange_order_id=current.exchange_order_id,
                payload={'opened_qty': opened, 'booked_qty': booked,
                         'executed_qty': executed, 'fill_price': price,
                         'netted_deal_id': pending.get('netted_deal_id')},
            )
        return OrderEvent(
            order=ExchangeOrder(
                id=current.exchange_order_id or '', symbol=current.symbol,
                side=current.side, order_type=_order_type_from_row(current, 'WORKING_ORDER'),
                qty=current.qty, filled_qty=booked,
                remaining_qty=max(0.0, current.qty - booked),
                price=None, stop_price=None, average_fill_price=price,
                status=OrderStatus.FILLED, timestamp=now_ts, fee=0.0, fee_currency='',
                reduce_only=False, client_order_id=coid,
            ),
            event_type='filled',
            fill_price=price,
            fill_qty=booked,
            journal_fill_qty=booked,
            timestamp=now_ts,
            pine_id=current.pine_entry_id,
            from_entry=current.from_entry,
            leg_type=LegType.ENTRY,
            fill_id=pending.get('fill_id') or fill_id,
        )

    def _settle_netted_residual_from_snapshot(
            self, row: 'OrderRow', pos: dict, *, now_ts: float, read_started_at: float,
    ) -> OrderEvent | None:
        """Book a held partly netted execution from the opened deal's ``/positions`` rows.

        A size an opening activity already proved (``opened_qty``) is booked
        at the listed open level. Otherwise the listed size is the opened size
        less every reduction the read reflects, and a reduction's activity can
        be published after the read. A deal's size only ever shrinks, so a size
        unchanged since the first listing means every reduction this read
        reflects happened before the first listing. The size is taken only once
        this poll's reads started at least
        :data:`~pynecore_capitalcom.helpers._ACTIVITY_LAG_S` after that first
        listing: the venue publishes an activity row within that lag, and this
        poll's activity read, processed before the snapshot, has held every
        such close. A close first read in this poll may postdate the snapshot
        read and keeps the execution held one more poll. The opened size is
        then the listed size plus the held closes; those closes are booked
        from their activities afterwards like any other. A held close that
        states no size leaves the opened size unknown, and the execution stays
        held.

        :param row: A row carrying
            :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_NETTED_RESIDUAL_PENDING`.
        :param pos: The row's deal in this poll's ``/positions`` read.
        :param now_ts: Wall-clock time of this reconcile pass, after its reads.
        :param read_started_at: Local time the poll's snapshot reads started;
            the poll's activity read follows them.
        :return: The ENTRY fill, or ``None`` while the execution stays held.
        """
        pending = (row.extras or {}).get(EXTRAS_KEY_NETTED_RESIDUAL_PENDING)
        position = pos.get('position') or {}
        if (not isinstance(pending, dict)
                or str(position.get('direction') or '').upper()
                != ('BUY' if row.side == 'buy' else 'SELL')):
            return None
        level = _wire_float(position.get('level'), finite=True)
        proven = _wire_float(pending.get('opened_qty'), finite=True)
        if proven is not None:
            return self._settle_netted_residual(row, proven, fill_price=level, fill_id=None)
        listed = _wire_float(position.get('size'), finite=True)
        if listed is None or listed <= 0.0:
            return None
        first = pending.get('listed') or {}
        if _wire_float(first.get('size'), finite=True) != listed:
            self._update_netted_residual(
                row, lambda held: held.__setitem__('listed', {'size': listed, 'at': now_ts}))
            return None
        held_closes = list((pending.get('held_closes') or {}).values())
        if (read_started_at - float(first.get('at') or now_ts) < _ACTIVITY_LAG_S
                or any(float(close.get('seen_at') or 0.0) >= read_started_at
                       or _wire_float(close.get('size'), finite=True) is None
                       for close in held_closes)):
            return None
        opened = listed + sum(float(close['size']) for close in held_closes)
        return self._settle_netted_residual(row, opened, fill_price=level, fill_id=None)

    def _netting_retirements(self) -> dict[str, dict]:
        """Own working orders retired because their execution only netted a foreign deal.

        A retired order's row is closed and its refs are gone, yet a deal the
        order opened with the rest of its size can surface after the
        retirement (the venue publishes it a poll later, or the bot restarts
        in between). Loaded once per process from the run's audit events
        (every instance of the logical run), together with the residual deals
        already reported, and kept up to date by
        :meth:`_retire_order_netted_into_foreign_deal`.

        :return: The order's ``dealId`` -> ``{'client_order_id', 'side',
            'symbol', 'netted_deal_id'}``; empty without a journal.
        """
        retired = self._netting_retired_orders
        if retired is not None:
            return retired
        store_ctx = self.store_ctx
        if store_ctx is None:
            return {}
        retired = {}
        for _, coid, wo_id, payload in store_ctx.iter_events_by_kind_for_run_id(
                'own_working_order_retired_netted_foreign_deal'):
            if wo_id:
                retired[wo_id] = {'client_order_id': coid, 'side': payload.get('side'),
                                  'symbol': payload.get('symbol'),
                                  'netted_deal_id': payload.get('netted_deal_id')}
        self._residual_deals_reported.update(
            deal_id for _, _, deal_id, _ in store_ctx.iter_events_by_kind_for_run_id(
                'own_working_order_residual_unbooked')
            if deal_id
        )
        self._netting_retired_orders = retired
        return retired

    def _note_residual_after_netting_retirement(
            self, wo_id: str, deal_id: str, activity_ts: float,
            positions_by_deal: dict[str, dict] | None, details: dict,
    ) -> bool:
        """Report a deal a retired own working order opened with the rest of its size.

        The deal back-links the order (``workingOrderId``), is not the
        foreign deal the order netted, and is one the order can have opened
        (:meth:`_promotion_conflict`). The venue holds exposure the bot's own
        order opened, but the order's execution was retired unbooked and the
        engine took the entry back as rejected: the deal is in no book and
        unprotected, and nobody asked for it. ERROR, an
        ``own_working_order_residual_unbooked`` audit event and an
        ``ENTRY_BLOCK`` quarantine naming the deal under the cause
        :func:`~pynecore_capitalcom.helpers._residual_deal_cause`, once per
        deal for the run. The bot closes the deal itself (the safe end of
        exposure nobody booked) and releases the cause once the venue no
        longer lists it
        (:meth:`~pynecore_capitalcom.order_history._OrderHistoryMixin._close_unbooked_residuals`).

        :param wo_id: The ``dealId`` of the order the deal back-links.
        :param deal_id: The deal.
        :param activity_ts: Venue time of the activity naming the deal, ``0``
            when the deal comes from a ``/positions`` read.
        :param positions_by_deal: The ``/positions`` rows by ``dealId``.
        :param details: The activity's ``details`` or the deal's ``position``
            object, for the report.
        :return: Whether the deal is such a residual (reported now or earlier).
        """
        store_ctx = self.store_ctx
        retired = self._netting_retirements().get(wo_id)
        if store_ctx is None or retired is None or deal_id == retired.get('netted_deal_id'):
            return False
        side = str(retired.get('side') or '')
        if (side not in ('buy', 'sell')
                or self._promotion_conflict(side, deal_id, activity_ts,
                                            positions_by_deal) is not None):
            return False
        if deal_id in self._residual_deals_reported:
            return True
        self._residual_deals_reported.add(deal_id)
        self._foreign_deals_logged.add(deal_id)
        position = ((positions_by_deal or {}).get(deal_id) or {}).get('position') or {}
        direction = position.get('direction') or details.get('direction')
        size = position.get('size', details.get('size'))
        symbol = retired.get('symbol') or self.symbol
        message = (
            f"own working order {wo_id} ({symbol}) netted foreign deal "
            f"{retired.get('netted_deal_id')} and also opened deal {deal_id} "
            f"({direction} {size}): its execution was retired unbooked, so the deal is "
            f"in no book and unprotected; nobody asked for this exposure, so the bot "
            f"closes the deal itself, and new entries are blocked until it is gone"
        )
        logger.error("%s", message)
        context = {'working_order': wo_id, 'deal_id': deal_id, 'symbol': symbol,
                   'netted_deal_id': retired.get('netted_deal_id'),
                   'direction': direction, 'size': size, 'details': details}
        store_ctx.log_event(
            'own_working_order_residual_unbooked',
            client_order_id=retired.get('client_order_id'),
            exchange_order_id=deal_id,
            payload=context,
        )
        self._latch_own_trouble(message, context, _residual_deal_cause(deal_id))
        return True

    def _promote_working_row_to_position(
            self, wo_row: 'OrderRow', new_deal_id: str, *,
            stamp_entry_filled: bool, executed_date_utc: str | None = None,
    ) -> None:
        """Migrate a filled working-order row onto its position dealId.

        Adds the new ``deal_id`` ref (the old ref stays, so in-window
        WORKING_ORDER activity replays still resolve to this row),
        rewrites ``exchange_order_id`` so the disappearance tracker and
        every deal-targeting call follow the live position, flips the
        row's kind to ``position``, and drops any ``missing_pending_since``
        stamp — the fill disproves the missing premise, and the tracker's
        stamp-version guard then discards an in-flight grace-expiry
        cancel verdict. ``stamp_entry_filled`` marks the entry fill as
        accounted (the caller is about to yield it, or already has);
        the bridge path leaves it unset so the POSITION activity's own
        ENTRY fill still emits when the WORKING_ORDER EXECUTED activity
        was never seen. ``executed_date_utc``, the ``dateUTC`` of the
        execution activity when one was read, is kept with that stamp
        (:data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_ENTRY_EXECUTED_UTC`).

        A pending core close-leg row naming the old deal is retargeted to the
        new one in the same transaction: its restart replay would otherwise
        find the old id absent from ``/positions`` and settle the close
        without sending it to the position that is still open.
        """
        store_ctx = self.store_ctx
        assert store_ctx is not None
        old_deal_id = wo_row.exchange_order_id
        # The resting order's level guard ends with the order; its persisted
        # levels share the row with the position's protection to come.
        self._forget_working_order_guard(old_deal_id)
        refreshed = store_ctx.get_order(wo_row.client_order_id)
        base_extras = dict((refreshed or wo_row).extras or {})
        base_extras['kind'] = 'position'
        base_extras['promoted_from_working_order_id'] = old_deal_id
        base_extras.pop('missing_pending_since', None)
        if stamp_entry_filled and not base_extras.get('entry_filled_at'):
            base_extras['entry_filled_at'] = epoch_time()
            if executed_date_utc:
                base_extras[EXTRAS_KEY_ENTRY_EXECUTED_UTC] = executed_date_utc
        with store_ctx.transaction():
            store_ctx.add_ref(
                wo_row.client_order_id, 'deal_id', new_deal_id,
            )
            store_ctx.upsert_order(
                wo_row.client_order_id,
                exchange_order_id=new_deal_id,
                extras=base_extras,
            )
            if old_deal_id and old_deal_id != new_deal_id:
                for leg_row in list(iter_active_close_legs(store_ctx)):
                    if (leg_row.extras or {}).get(EXTRAS_KEY_CLOSE_LEG_ID) == old_deal_id:
                        update_close_leg_state(
                            store_ctx, coid=leg_row.client_order_id,
                            new_state=CLOSE_LEG_STATE_PENDING,
                            extras_patch={EXTRAS_KEY_CLOSE_LEG_ID: new_deal_id},
                        )
        # The promoted deal is the run's from here on; a read may not list it
        # yet (see ``identity.py``).
        self._note_entry_fill_unlisted(wo_row.client_order_id)
        store_ctx.log_event(
            'working_order_promoted_to_position',
            client_order_id=wo_row.client_order_id,
            exchange_order_id=new_deal_id,
            payload={
                'working_order_deal_id': old_deal_id,
                'position_deal_id': new_deal_id,
                'stamp_entry_filled': stamp_entry_filled,
            },
        )

    def _tear_down_natural_close(self, row: 'OrderRow') -> None:
        """Tear down a position row whose deal a close retired, in one transaction.

        Stamps the row and its bracket legs ``natural_close_at``
        (:meth:`~_BracketMixin._close_bracket_after_natural_close`) and drops
        its exposure cursor (:meth:`_retire_netted_journal_exposure`)
        together: the snapshot reconcile skips a stamped row, so a process
        that died between the two writes would keep the dead exposure
        counting as run-owned for good.

        The teardown drops the deal's delivery queue unbooked in the same
        transaction (:func:`~pynecore_capitalcom.delivery.settle_undelivered`).
        Every runtime caller tears down only once the queue owes no readable
        fill (:func:`~pynecore_capitalcom.delivery.delivery_pending`), so what
        it settles at runtime is only what the venue no longer serves. The
        startup retirement of a vanished deal settles whatever is queued: the
        engine adopts the venue's book then.

        :param row: The position row whose deal is gone.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return
        retired_exposure = row.filled_qty
        with store_ctx.transaction():
            self._close_bracket_after_natural_close(row)
            self._retire_netted_journal_exposure(row, retired_exposure)
            current = store_ctx.get_order(row.client_order_id) or row
            if settle_undelivered(dict(current.extras or {}), epoch_time()) is None:
                return
            settled: dict = {}

            def settle(extras: dict) -> None:
                settled.update(settle_undelivered(extras, epoch_time()) or {})

            self._rewrite_extras(row.client_order_id, settle)
            store_ctx.log_event(
                'owed_fills_settled_by_close',
                client_order_id=row.client_order_id,
                exchange_order_id=row.exchange_order_id,
                payload=settled,
            )

    def _retire_netted_journal_exposure(
            self, closed_row: 'OrderRow', retired_exposure: float,
    ) -> None:
        """Drop a fully-closed position row's journal exposure to zero.

        The journal's ``filled_qty`` doubles as the run-owned exposure cursor:
        the startup adoption clamp (``_durable_owned_signed_size``) and the
        cycle-end reconciliation both compute the run's net as the signed sum
        of live rows' ``filled_qty``. A naturally-closed entry row is
        deliberately NOT physically closed
        (:meth:`_close_bracket_after_natural_close` keeps it live as the
        ``modify_exit`` lookup anchor), so without this write its dead
        exposure would keep counting.

        Called from the natural-close teardown, i.e. AFTER the closing fill
        has been emitted, so no event payload is derived from the value
        written here. Every dispatched entry is a pure add — the engine's
        close-then-open reversal protocol never nets an entry against
        opposite exposure — so the closed row is the ONLY row this close
        touches; other live rows' cursors are authoritative as written.
        """
        if self.store_ctx is None:
            return
        refreshed = self.store_ctx.get_order(closed_row.client_order_id)
        if refreshed is not None and refreshed.filled_qty != 0.0:
            self.store_ctx.upsert_order(
                closed_row.client_order_id, filled_qty=0.0,
            )
            self.store_ctx.log_event(
                'journal_exposure_retired',
                client_order_id=closed_row.client_order_id,
                exchange_order_id=closed_row.exchange_order_id,
                payload={'retired_exposure': retired_exposure},
            )

    def _find_active_entry_rows(self, pine_entry_id: str | None) -> list['OrderRow']:
        """Every live, ACTIVE position row opened under ``pine_entry_id``, oldest first.

        Capital.com opens each market entry as its own deal, so one Pine
        entry id holds several position rows once the script pyramids under
        that id (``strategy.entry('L1')`` re-issued while ``L1`` is open). A
        whole-row ``strategy.exit(from_entry='L1')`` covers every open trade
        of the id, so :meth:`execute_exit` and :meth:`modify_exit` protect
        each of these rows.

        After a TP/SL natural close the entry row stays live (so further
        exit lookups for the same entry keep working) but carries an
        ``extras['natural_close_at']`` flag — that row is closed on the
        broker's side and is not returned. Rows still closing, never
        confirmed or without a deal id are not returned either.

        :param pine_entry_id: The Pine entry id the rows were opened under.
        :return: The rows in creation order, the venue's FIFO order.
        """
        if self.store_ctx is None:
            return []
        rows: list['OrderRow'] = []
        for row in self.store_ctx.iter_live_orders():
            if row.state != 'confirmed':
                continue
            if row.pine_entry_id != pine_entry_id:
                continue
            if not row.exchange_order_id:
                continue
            extras = row.extras or {}
            if extras.get('kind') != 'position':
                continue
            if extras.get('natural_close_at') is not None:
                continue
            rows.append(row)
        rows.sort(key=lambda entry_row: entry_row.created_ts_ms)
        return rows

    def _find_bracket_leg_row(
            self, entry_row: 'OrderRow', leg_kind: str,
    ) -> 'OrderRow | None':
        """Look up the bracket TP or SL row attached to ``entry_row``.

        ``execute_exit`` registers each bracket leg under
        ``extras['parent_deal_id'] == entry_row.exchange_order_id``
        with ``extras['leg_kind']`` of ``'tp'`` or ``'sl'``.

        ``find_by_ref('bracket_deal_id', deal_id)`` cannot be used —
        ``add_ref`` UNIQUEs on ``(run_instance_id, ref_type, ref_value)``,
        so the SL ``add_ref`` clobbers the TP one in
        :meth:`execute_exit`. Linear scan over live orders for the
        symbol is acceptable: a realistic one-way Pine strategy has
        well under 10 live rows at any time.
        """
        if self.store_ctx is None or not entry_row.exchange_order_id:
            return None
        for r in self.store_ctx.iter_live_orders():
            rextras = r.extras or {}
            if (rextras.get('parent_deal_id') == entry_row.exchange_order_id
                    and rextras.get('leg_kind') == leg_kind):
                return r
        return None

