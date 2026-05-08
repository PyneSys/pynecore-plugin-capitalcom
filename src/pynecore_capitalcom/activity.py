"""Activity-poll surface: ``watch_orders`` and the activity → event mapper.

Capital.com has no WebSocket order channel, so ``watch_orders`` is a
polling AsyncIterator that fuses three REST endpoints per cadence tick:

1. ``GET /history/activity`` — causal log of every fill / cancel /
   expiry. Cross-restart-safe dedup via SHA-1 fingerprints persisted
   in BrokerStore.
2. ``GET /positions`` + ``GET /workingorders`` — snapshot reconcile
   (delegated to ``_reconcile_snapshot`` in ``reconcile.py``).
3. ``_missing_pending_tracker`` — separately raises
   :class:`UnexpectedCancelError` for bot-owned rows that vanish
   without a corresponding cancel event.

State touched: ``_activity_cursor`` (read/write — fingerprint dedup
set, watermark, external-activity log dedup), ``_current_poll_id``
(monotonic stamp on each poll cycle for breadcrumb race detection in
the reconciler).
"""
import asyncio
from time import time as epoch_time
from typing import TYPE_CHECKING, AsyncIterator

import httpx

from pynecore.core.broker.exceptions import (
    BrokerError,
    ExchangeConnectionError,
    ExchangeRateLimitError,
    UnexpectedCancelError,
)
from pynecore.core.broker.models import (
    ExchangeOrder,
    LegType,
    OrderEvent,
    OrderStatus,
)
from pynecore.core.plugin import override

from ._base import _CapitalComBase
from .exceptions import CapitalComError
from .helpers import _order_type_from_row, _parse_iso_timestamp
from .models import _activity_fingerprint

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow


class _ActivityMixin(_CapitalComBase):
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
           transition), partial fills (cumulative size decrease), and
           bot-owned orders that silently vanished (``missing_pending``
           grace window + ``on_unexpected_cancel`` policy).
        3. Trailing activation monitor — client-side state machine
           (``pending → activating → active``) that implements Pine
           ``trail_price`` activation threshold on top of Capital's
           always-on native trailing.

        Rate limits (3 GET per tick @ 1.5 s default cadence = 2 req/s)
        sit comfortably below Capital's 10 req/s account budget; 429s
        still trigger an exponential backoff.
        """
        cadence = max(0.5, float(self.config.poll_interval_seconds))
        consecutive_429 = 0
        while True:
            try:
                async for ev in self._poll_once():
                    yield ev
                consecutive_429 = 0
            except asyncio.CancelledError:
                raise
            except ExchangeRateLimitError:
                consecutive_429 += 1
                backoff = min(30.0, cadence * (2 ** min(consecutive_429, 5)))
                await asyncio.sleep(backoff)
                continue
            except (ExchangeConnectionError,
                    httpx.TimeoutException, httpx.RequestError,
                    ConnectionError):
                # Single-cycle hiccup — retry next tick.
                pass
            except CapitalComError as e:
                mapped = self._map_exception(e)
                if mapped is not None and not isinstance(
                        mapped, (ExchangeConnectionError, ExchangeRateLimitError)):
                    raise mapped from e
            await asyncio.sleep(cadence)

    async def _poll_once(self) -> AsyncIterator[OrderEvent]:
        """One poll cycle: fetch the three endpoints and emit fused events.

        The order of the sub-generators matters.  Activity comes first
        because it is *causal* — when an order fills, the fill hits
        ``/history/activity`` before ``/positions`` updates.  Snapshot
        reconcile then covers the cases activity alone cannot pin down
        (working → position transitions, missing pending, partial fill).
        The trailing monitor runs last since it depends on the current
        positions snapshot bid/offer.
        """
        # Bump *before* fetching the snapshots so any defer-stamp written
        # by :meth:`_process_activity` is keyed to the snapshot that
        # caused the defer; :meth:`_reconcile_snapshot` then refuses to
        # clear breadcrumbs whose poll-id matches the current cycle.
        self._current_poll_id += 1
        positions_resp = await self._call('positions', method='get')
        working_resp = await self._call('workingorders', method='get')
        last_period = max(60, int(self.config.poll_interval_seconds * 10))
        activity_resp = await self._call(
            'history/activity',
            data={'lastPeriod': last_period, 'detailed': 'true'},
            method='get',
        )

        positions_by_deal: dict[str, dict] = {}
        for row in positions_resp.get('positions') or []:
            pos = row.get('position') or {}
            did = pos.get('dealId')
            if did:
                positions_by_deal[str(did)] = row

        working_by_deal: dict[str, dict] = {}
        for wo in working_resp.get('workingOrders') or []:
            data = wo.get('workingOrderData') or {}
            did = data.get('dealId')
            if did:
                working_by_deal[str(did)] = wo

        async for ev in self._process_activity(
                activity_resp.get('activities') or [],
                positions_by_deal):
            yield ev
        async for ev in self._reconcile_snapshot(
                positions_by_deal, working_by_deal):
            yield ev
        await self._trailing_activation_monitor(positions_by_deal)
        async for ev in self._missing_pending_tracker(
                working_by_deal, positions_by_deal):
            yield ev

    async def _process_activity(
            self, activities: list[dict],
            positions_by_deal: dict[str, dict] | None = None,
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
        """
        cursor = self._activity_cursor
        activities_sorted = sorted(
            activities, key=lambda x: x.get('dateUTC') or '',
        )
        # Cursor watermark guard: once we defer a row in this batch, no
        # further activity in the same batch may advance ``last_date_utc``.
        # The batch is sorted ascending by ``dateUTC``, so any row after
        # the first defer is at-or-after the deferred timestamp; advancing
        # cursor past that timestamp would make the next poll's
        # ``date_utc < cursor.last_date_utc`` guard permanently drop the
        # deferred row and silently desync the strategy.
        deferred_in_batch = False
        for a in activities_sorted:
            date_utc = a.get('dateUTC') or ''
            deal_id = str(a.get('dealId') or '')
            fingerprint = _activity_fingerprint(a)

            if cursor.last_date_utc and date_utc < cursor.last_date_utc:
                continue
            if fingerprint in cursor.seen_fingerprints:
                continue

            row: 'OrderRow | None' = None
            if deal_id and self.store_ctx is not None:
                row = self.store_ctx.find_by_ref('deal_id', deal_id)

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
                # one-shot per row within the session.
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
                # :meth:`_reconcile_snapshot`'s partial-fill check
                # ``row.filled_qty + 1e-9 < cumulative`` is unreachable
                # once the entry is fully filled (``row.filled_qty ==
                # row.qty``), so once the activity rolls out of
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
                if proxy_price is not None and proxy_price > 0.0:
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
                        timestamp=event.timestamp,
                        pine_id=event.pine_id,
                        from_entry=event.from_entry,
                        leg_type=event.leg_type,
                        fee=event.fee,
                        fee_currency=event.fee_currency,
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
                    continue

            cursor.seen_fingerprints.add(fingerprint)
            if (not deferred_in_batch
                    and (not cursor.last_date_utc
                         or date_utc > cursor.last_date_utc)):
                cursor.last_date_utc = date_utc

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

            if event is not None:
                # Stamp the entry row before yielding its first
                # filled-as-ENTRY activity so :meth:`_activity_to_event`
                # can recognise subsequent ``USER`` / ``DEALER`` activities
                # for the same dealId as manual closes (Capital.com nets
                # per dealId — without the stamp the close routes as an
                # entry add). Persisting BEFORE the yield is critical:
                # the ``activity_processed`` log + cursor advance above
                # are already durable, so a crash or cancellation between
                # this point and the consumer's resumption would dedupe
                # the entry on restart but leave ``entry_filled_at``
                # missing, misclassifying any later manual close as
                # ``LegType.ENTRY``.
                if (event.event_type == 'filled'
                        and event.leg_type == LegType.ENTRY
                        and (row.extras or {}).get('kind') == 'position'
                        and not (row.extras or {}).get('entry_filled_at')
                        and self.store_ctx is not None):
                    refreshed = self.store_ctx.get_order(row.client_order_id)
                    base_extras = dict((refreshed or row).extras or {})
                    base_extras['entry_filled_at'] = epoch_time()
                    self.store_ctx.upsert_order(
                        row.client_order_id, extras=base_extras,
                    )
                # Stamp the close-event breadcrumb / natural-close teardown
                # BEFORE yielding, for the same reason as the entry stamp
                # above: the ``activity_processed`` log + cursor advance
                # are already durable at this point, so a crash or
                # cancellation between the yield and the legacy post-yield
                # branch would dedupe the activity on restart but leave
                # the breadcrumb missing. Next poll's vanished deal would
                # then fall into ``missing_pending_since`` /
                # :class:`UnexpectedCancelError` instead of the intended
                # natural-close teardown (or, for the still-present-deal
                # race, the snapshot would lack the breadcrumb that
                # ``_reconcile_snapshot`` relies on to upgrade to a
                # teardown without waiting for the grace window).
                if (event.event_type == 'filled'
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
                    deal_remaining: float | None = None
                    if (deal_id and positions_by_deal
                            and deal_id in positions_by_deal):
                        pos_snap = (
                            positions_by_deal[deal_id].get('position') or {}
                        )
                        deal_remaining = float(pos_snap.get('size') or 0.0)
                    if deal_remaining is None or deal_remaining <= 0.0:
                        self._close_bracket_after_natural_close(row)
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
                            self.store_ctx.upsert_order(
                                row.client_order_id, extras=mark_extras,
                            )
                yield event

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
        size = float(activity.get('size') or row.qty)
        level = float(activity.get('level') or 0.0)

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
        is_explicit_close_source = source in (
            'TP', 'SL', 'CLOSE_OUT', 'CLOSE', 'MARGIN', 'STOP_OUT',
        )
        is_manual_close = (
            row_kind == 'position'
            and row_leg_kind not in ('tp', 'sl')
            and entry_already_filled
            and source in ('USER', 'DEALER')
        )
        is_closing_leg = (
            activity_type == 'POSITION'
            and status in ('EXECUTED', 'ACCEPTED')
            and (
                is_explicit_close_source
                or row_leg_kind in ('tp', 'sl')
                or is_manual_close
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
            fallback_level = float(pos_data.get('level') or 0.0)
            if fallback_level > 0.0:
                level = fallback_level
        # Bracket TP/SL fill fallback: ONLY when the closing event truly
        # comes from a bracket leg — explicit ``source in ('TP', 'SL')`` or
        # the row itself is a stored bracket leg. Generic closes
        # (``CLOSE``/``CLOSE_OUT``/``MARGIN``/``STOP_OUT`` and manual
        # ``USER``/``DEALER`` closes) execute at the prevailing market
        # price, NOT at the bracket SL level — falling back to the still-
        # attached SL leg's stored level would misprice the fill and
        # corrupt P&L. When the activity ``level`` is missing for those
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
                if leg_level is not None and float(leg_level) > 0.0:
                    level = float(leg_level)
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
            confirm_level = float((row.extras or {}).get('confirm_level') or 0.0)
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
            client_order_id=row.client_order_id,
        )
        return OrderEvent(
            order=exch_order,
            event_type=event_type,
            fill_price=level if event_type == 'filled' else None,
            fill_qty=size if event_type == 'filled' else None,
            timestamp=now_ts,
            pine_id=row.pine_entry_id,
            from_entry=row.from_entry,
            leg_type=leg_type,
        )

    def _find_active_entry_row(
            self, symbol: str, pine_entry_id: str | None,
    ) -> 'OrderRow | None':
        """Locate the live, ACTIVE entry row for ``pine_entry_id``.

        Used by :meth:`execute_exit` and :meth:`modify_exit` to find
        the parent position. After a TP/SL natural close the entry row
        stays live (so further modify_exit lookups for the same entry
        keep working) but carries an ``extras['natural_close_at']``
        flag — that row is closed on the broker's side and must NOT
        be returned. Pine reusing the same ``pine_entry_id`` on a new
        entry produces two rows with the same id; this helper picks
        the active one by skipping flagged rows. If multiple active
        rows match (should not happen in one-way mode) the most
        recently created wins.
        """
        if self.store_ctx is None:
            return None
        best: 'OrderRow | None' = None
        for row in self.store_ctx.iter_live_orders(symbol=symbol):
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
            if best is None or row.created_ts_ms > best.created_ts_ms:
                best = row
        return best

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
        for r in self.store_ctx.iter_live_orders(symbol=entry_row.symbol):
            rextras = r.extras or {}
            if (rextras.get('parent_deal_id') == entry_row.exchange_order_id
                    and rextras.get('leg_kind') == leg_kind):
                return r
        return None

