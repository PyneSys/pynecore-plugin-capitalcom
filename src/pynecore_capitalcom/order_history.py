"""The bot's own way out of its troubles: the venue's history, read again on demand.

The bot runs unattended: an ``ENTRY_BLOCK`` the plugin latches is the bot's
own trouble, and the plugin ends it by itself (PyneCore broker plugin
authoring guide, section 2c). Every repair here reads
``GET /history/activity?lastPeriod=86400&detailed=true``: the venue's
activity log, 600 s by default and at most 86 400 s back (research dossier
§4). Its ``detailed`` rows carry the size, level, trade direction and
``workingOrderId`` back-link in ``details`` (measured 2026-07-10), and the
venue gives every fill a fresh ``dealId`` (a working order's fill opens one
that back-links the order, dossier §8).

* **The execution of one order**
  (:meth:`_OrderHistoryMixin.fetch_order_execution`), the engine's recovery
  of an execution its stream never delivered. An entry's execution is its
  deal's opening activity, returned as the fill the stream books from that
  row, under the same fill id (the activity fingerprint): a later stream copy
  of an execution the engine booked this way is settled on the row without
  being yielded again (:meth:`_OrderHistoryMixin._engine_booked_entry_qty`).
  A working order the venue ended without an execution (its ``WORKING_ORDER``
  activity ``DELETED`` / ``EXPIRED`` / ``REJECTED``, no execution of it in
  the history) is the same verdict the stream draws from that row. A partial
  close's execution is the close activity its own-close record explains.
* **A take-over whose fill the snapshot could not prove**
  (:data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_TAKE_OVER_UNPROVEN`,
  :meth:`_OrderHistoryMixin._resolve_unproven_take_overs`): the deal's
  opening activity proves what the submission filled, and the deal's later
  closes are queued so the activity poll books them in order.
* **The ENTRY_BLOCK causes the plugin latched**
  (:meth:`_OrderHistoryMixin._settle_own_troubles`): a residual deal no book
  holds is closed by the bot, and each cause is released once its condition
  is gone.

The venue reports no commission on an activity or a confirm (the confirm
carries ``status``, ``dealStatus``, ``reason``, ``dealId``, ``affectedDeals``,
``level``, ``size`` and ``direction``, dossier §4), so every execution read
here is ``fee_reported=False``. The history holds at most the last 24 hours,
and its completeness and order are not documented (dossier §9 #8): an order
missing from it is never proven absent, so ``order=None`` is never answered,
and a read that finds nothing conclusive answers ``None`` (asked again).
"""
import logging
from abc import ABC
from collections.abc import AsyncIterator, Iterable, Sequence
from dataclasses import replace
from time import time as epoch_time
from typing import TYPE_CHECKING

import httpx

from pynecore.core.broker.disappearance import MISSING_PENDING_EXTRA
from pynecore.core.broker.exceptions import BrokerError
from pynecore.core.broker.journal import DispatchJournal, ReconcileOutcome
from pynecore.core.broker.models import (
    ExchangeOrder,
    LegType,
    OrderEvent,
    OrderExecution,
    OrderStatus,
    OrderType,
)
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    EXTRAS_KEY_LEG_KIND,
    KIND_PARTIAL_CLOSE,
    mark_confirmed_with_fill,
)
from pynecore.core.plugin import override

from ._base import _CapitalComBase
from .activity import _is_opening_activity, _wire_price
from .delivery import (
    activity_size,
    book_entry,
    book_fill,
    delivery_pending,
    enqueue_entry,
    enqueue_fill,
    entry_executed,
    entry_pending_since,
)
from .exceptions import CapitalComError, OrderNotFoundError
from .helpers import (
    CAUSE_NETTED_FOREIGN_DEAL,
    EXTRAS_KEY_CANCEL_REQUESTED,
    EXTRAS_KEY_EXECUTION_HELD,
    EXTRAS_KEY_NETTED_RESIDUAL_PENDING,
    EXTRAS_KEY_OWN_CLOSES,
    EXTRAS_KEY_TAKE_OVER_UNPROVEN,
    _ACTIVITY_LAG_S,
    _ACTIVITY_MAX_PERIOD_S,
    _order_type_from_row,
    _parse_iso_timestamp,
    _residual_deal_cause,
    _wire_float,
    _wire_id,
)
from .models import _activity_fingerprint, _close_identity

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow

logger = logging.getLogger(__name__)

#: Least time between two history reads that prove held take-overs.
_TAKE_OVER_PROOF_INTERVAL_S = 30.0

#: How long the size of a held take-over's deal must stay unchanged before it
#: is booked from ``/positions`` alone (its opening is older than the history):
#: twice the venue's measured activity publication lag, so every close the
#: size already shows is in the history read that marks it delivered.
_TAKE_OVER_SIZE_STABLE_S = 2.0 * _ACTIVITY_LAG_S

#: Least time between two closes of the same unbooked residual deal.
_RESIDUAL_CLOSE_RETRY_S = 30.0

#: Activity statuses of an executed fill.
_FILL_STATUSES = frozenset({'EXECUTED', 'ACCEPTED'})

#: The order status a ``WORKING_ORDER`` activity of an order the venue ended
#: without executing it reports.
_WORKING_ORDER_END_STATUSES = {
    'DELETED': OrderStatus.CANCELLED,
    'CANCELLED': OrderStatus.CANCELLED,
    'EXPIRED': OrderStatus.EXPIRED,
    'REJECTED': OrderStatus.REJECTED,
}


def _same_size(a: float, b: float) -> bool:
    """Whether two venue sizes are equal up to float noise."""
    return abs(a - b) <= 1e-9 * max(1.0, abs(a), abs(b))


def _best_copy(copies: Sequence[dict]) -> dict | None:
    """The copy of one activity that states the most: size and level, else size, else any.

    The venue republishes an activity with a field it lacked (a level, a
    size); the latest copy stating both wins.
    """
    def priced(activity: dict) -> bool:
        details = activity.get('details') or {}
        return _wire_price(activity.get('level') or details.get('level')) > 0.0

    for accept in ((lambda a: activity_size(a) is not None and priced(a)),
                   (lambda a: activity_size(a) is not None),
                   (lambda _a: True)):
        chosen = [copy for copy in copies if accept(copy)]
        if chosen:
            return chosen[-1]
    return None


def _deal_openings(activities: Iterable[dict], side: str, deal_id: str) -> list[dict]:
    """The executed opening activities of the own deal ``deal_id`` of ``side``, oldest first."""
    return [a for a in activities
            if _is_opening_activity(a, side, deal_id)
            and str(a.get('status') or '').upper() in _FILL_STATUSES]


def _deal_closes(activities: Iterable[dict], side: str, deal_id: str) -> list[dict]:
    """The executed ``POSITION`` activities of ``deal_id`` that are not its opening."""
    return [a for a in activities
            if _wire_id(a.get('dealId')) == deal_id
            and str(a.get('type') or '') == 'POSITION'
            and str(a.get('status') or '').upper() in _FILL_STATUSES
            and not _is_opening_activity(a, side, deal_id)]


class _OrderHistoryMixin(_CapitalComBase, ABC):
    """Venue history reads and the repairs of the bot's own ``ENTRY_BLOCK`` troubles."""

    async def _read_order_history(self) -> list[dict]:
        """Every activity the venue still keeps, oldest first.

        ``GET /history/activity`` with the maximum ``lastPeriod`` (86 400 s)
        and ``detailed=true`` (research dossier §4).

        :return: The activity rows, sorted by ``dateUTC``.
        """
        response = await self._call(
            'history/activity',
            data={'lastPeriod': _ACTIVITY_MAX_PERIOD_S, 'detailed': 'true'},
            method='get',
        )
        return sorted(response.get('activities') or [],
                      key=lambda activity: str(activity.get('dateUTC') or ''))

    @override
    async def fetch_order_execution(
            self, order_id: str | None, client_order_id: str | None,
    ) -> OrderExecution | None:
        """Read what the venue executed of one own order, from its activity history.

        The order is looked up in the journal (the client order id across
        every process of the run, else the order id as the row's ``dealId``,
        ``dealReference``, the working order a row was promoted from, or the
        deal of a held take-over). Then:

        * **An entry** (market or working order): its deal's opening activity
          in the history (the deal the row holds, or a fresh deal whose
          ``workingOrderId`` back-links the order and that the order can have
          opened, :meth:`~pynecore_capitalcom.foreign._ForeignExposureMixin._promotion_conflict`)
          is the execution: the order ``FILLED`` with the opened size at the
          price the stream books it at, and the execution as the stream's
          fill, under the activity fingerprint the stream uses as fill id. An
          opened size other than the order's quantity (an execution that
          netted another deal, or a partial fill, research dossier §9 #4)
          answers ``None``: the activity poll settles those. Without an
          opening, a working order the venue ended (``WORKING_ORDER``
          ``DELETED`` / ``CANCELLED`` / ``EXPIRED`` / ``REJECTED``, or a
          ``POSITION`` ``REJECTED``) with no execution of it in the history
          and none in the journal answers that status with nothing executed,
          the verdict the stream draws from that row.
        * **A partial close** (the netting reduce ``POST``): its own-close
          record matched, or a close activity of its target that the record
          explains, gives what it executed; a close with none answers
          ``None``, an absence proves nothing.
        * Anything else, or no conclusive record: ``None``.

        The venue reports no commission: ``fee_reported`` is ``False``. Never
        ``order=None``: the history covers 24 hours, and its completeness is
        not documented (research dossier §9 #8), so it never proves an order
        absent. Read-only.

        :param order_id: The venue order id (a ``dealId``), when known.
        :param client_order_id: The client order id, when known.
        :return: The venue's record, or ``None`` when it cannot tell now.
        """
        if self.store_ctx is None:
            return None
        row = self._execution_row(order_id, client_order_id)
        if row is None:
            return None
        extras = row.extras or {}
        if extras.get('kind') == KIND_PARTIAL_CLOSE:
            return await self._partial_close_execution(row)
        if (extras.get(EXTRAS_KEY_LEG_KIND) is None
                and extras.get('kind') in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING)):
            return await self._entry_execution(row, order_id)
        return None

    def _execution_row(self, order_id: str | None,
                       client_order_id: str | None) -> 'OrderRow | None':
        """The journal row of the order :meth:`fetch_order_execution` asks for."""
        store_ctx = self.store_ctx
        assert store_ctx is not None
        if client_order_id:
            row = store_ctx.get_order_for_run_id(client_order_id)
            if row is not None:
                return row
        if not order_id:
            return None
        for ref_type in ('deal_id', 'deal_reference'):
            row = store_ctx.find_by_ref(ref_type, order_id)
            if row is not None:
                return row
        for row in store_ctx.iter_live_orders():
            extras = row.extras or {}
            unproven = extras.get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) or {}
            if order_id in (_wire_id(extras.get('promoted_from_working_order_id')),
                            _wire_id(unproven.get('deal_id'))):
                return row
        return None

    async def _entry_execution(self, row: 'OrderRow',
                               order_id: str | None) -> OrderExecution | None:
        """An own entry's execution from the history (:meth:`fetch_order_execution`)."""
        activities = await self._read_order_history()
        snapshot = self._positions_snapshot[1] if self._positions_snapshot is not None else {}
        extras = row.extras or {}
        order_ids = {order_ref for order_ref in (
            order_id, row.exchange_order_id,
            _wire_id(extras.get('promoted_from_working_order_id')),
            _wire_id((extras.get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) or {}).get('deal_id')),
        ) if order_ref}
        reported_id = order_id or row.exchange_order_id or ''
        opening = self._order_opening(row, order_ids, activities, snapshot)
        if opening is not None:
            return self._opening_execution(row, opening, reported_id, snapshot)
        if entry_executed(extras) or row.filled_qty > 0.0:
            # The journal holds an execution whose opening the history no
            # longer shows: its figures cannot be read again.
            return None
        status = self._order_end_status(order_ids, activities)
        if status is None:
            return None
        return OrderExecution(
            order=ExchangeOrder(
                id=reported_id, symbol=row.symbol, side=row.side,
                order_type=_order_type_from_row(
                    row, 'WORKING_ORDER' if extras.get('kind') == ENTRY_KIND_WORKING
                    else 'POSITION'),
                qty=row.qty, filled_qty=0.0, remaining_qty=row.qty,
                price=None, stop_price=None,
                average_fill_price=None, status=status,
                timestamp=epoch_time(), fee=0.0, fee_currency='',
                reduce_only=False, client_order_id=row.client_order_id,
            ),
            fills=None,
            fee_reported=False,
        )

    def _order_opening(self, row: 'OrderRow', order_ids: set[str], activities: list[dict],
                       snapshot: dict[str, dict]) -> dict | None:
        """The opening activity of the deal an own entry's order opened, if the history shows it.

        The deal is one the row holds (``order_ids``), or a deal whose first
        activity in the history is an opening in the order's direction that
        back-links the order (``workingOrderId``), that a ``/positions`` read
        listed or the plugin remembers, and that the order can have opened
        (:meth:`~pynecore_capitalcom.foreign._ForeignExposureMixin._promotion_conflict`):
        a deal the order only netted existed before it, or runs the other way.
        Two deals that both qualify answer ``None``.
        """
        first_is_opening: dict[str, bool] = {}
        copies: dict[str, list[dict]] = {}
        for activity in activities:
            if str(activity.get('type') or '') != 'POSITION':
                continue
            deal_id = _wire_id(activity.get('dealId'))
            if deal_id is None:
                continue
            opening = (_is_opening_activity(activity, row.side, deal_id)
                       and str(activity.get('status') or '').upper() in _FILL_STATUSES)
            linked = _wire_id((activity.get('details') or {}).get('workingOrderId'))
            if deal_id not in first_is_opening:
                first_is_opening[deal_id] = deal_id in order_ids or (
                    opening and linked in order_ids
                    and (deal_id in snapshot or deal_id in self._seen_position_deals)
                    and self._promotion_conflict(
                        row.side, deal_id,
                        _parse_iso_timestamp(str(activity.get('dateUTC') or '')),
                        snapshot) is None)
            if opening and first_is_opening[deal_id] and (deal_id in order_ids
                                                           or linked in order_ids):
                copies.setdefault(deal_id, []).append(activity)
        if len(copies) != 1:
            return None
        return _best_copy(next(iter(copies.values())))

    def _opening_execution(self, row: 'OrderRow', opening: dict, reported_id: str,
                           snapshot: dict[str, dict]) -> OrderExecution | None:
        """The execution an own entry's opening activity proves, as the stream books it."""
        size = activity_size(opening)
        if size is None or not _same_size(size, row.qty):
            return None
        deal_id = str(opening.get('dealId') or '')
        event = self._activity_to_event(opening, row, snapshot.get(deal_id))
        if event is None or event.event_type != 'filled' or event.leg_type is not LegType.ENTRY:
            return None
        price = _wire_price(event.fill_price)
        if price <= 0.0:
            return None
        executed_at = _parse_iso_timestamp(str(opening.get('dateUTC') or '')) or epoch_time()
        order = ExchangeOrder(
            id=reported_id, symbol=row.symbol, side=row.side,
            order_type=event.order.order_type,
            qty=row.qty, filled_qty=size, remaining_qty=0.0,
            price=None, stop_price=None, average_fill_price=price,
            status=OrderStatus.FILLED, timestamp=executed_at,
            fee=0.0, fee_currency='', reduce_only=False,
            client_order_id=row.client_order_id,
        )
        fill = replace(event, order=order, fill_price=price, fill_qty=size,
                       journal_fill_qty=size, timestamp=executed_at)
        return OrderExecution(order=order, fills=(fill,), fee_reported=False)

    @staticmethod
    def _order_end_status(order_ids: set[str], activities: list[dict]) -> OrderStatus | None:
        """How the venue ended an own order without executing it, if the history says so.

        :return: The status of the latest ending activity of the order, or
            ``None`` when there is none or the history shows the order
            executed (``WORKING_ORDER`` ``EXECUTED``).
        """
        ended: OrderStatus | None = None
        for activity in activities:
            if _wire_id(activity.get('dealId')) not in order_ids:
                continue
            kind = str(activity.get('type') or '')
            status = str(activity.get('status') or '').upper()
            if kind == 'WORKING_ORDER' and status == 'EXECUTED':
                return None
            if kind == 'WORKING_ORDER' and status in _WORKING_ORDER_END_STATUSES:
                ended = _WORKING_ORDER_END_STATUSES[status]
            elif kind == 'POSITION' and status == 'REJECTED':
                ended = OrderStatus.REJECTED
        return ended

    async def _partial_close_execution(self, row: 'OrderRow') -> OrderExecution | None:
        """What an own partial close executed (:meth:`fetch_order_execution`).

        The reduce ``POST`` leaves an own-close record on its target before it
        is sent (``own_close_sent_at`` on the command row): a matched record
        means a close activity of the record's size explained it; otherwise
        the close activities of the target the record explains
        (:meth:`~pynecore_capitalcom.intervention._InterventionMixin._fitting_own_closes`)
        are summed.
        """
        store_ctx = self.store_ctx
        assert store_ctx is not None
        extras = row.extras or {}
        target_coid = extras.get('target_coid')
        sent_at = _wire_float(extras.get('own_close_sent_at'), finite=True)
        if not isinstance(target_coid, str) or sent_at is None:
            return None
        target = store_ctx.get_order_for_run_id(target_coid)
        if target is None:
            return None
        record = next((r for r in (target.extras or {}).get(EXTRAS_KEY_OWN_CLOSES) or ()
                       if isinstance(r, dict)
                       and _wire_float(r.get('sent_at'), finite=True) == sent_at), None)
        if record is None:
            return None
        target_deal = _wire_id(extras.get('target_deal_id')) or target.exchange_order_id
        if record.get('matched'):
            executed = row.qty
        else:
            sizes: dict[str, float] = {}
            for activity in await self._read_order_history():
                if (_wire_id(activity.get('dealId')) != target_deal
                        or str(activity.get('type') or '') != 'POSITION'):
                    continue
                if not any(_wire_float(fit.get('sent_at'), finite=True) == sent_at
                           for fit in self._fitting_own_closes(target, activity)):
                    continue
                size = activity_size(activity)
                if size is None:
                    return None
                sizes[_close_identity(activity)] = size
            if not sizes:
                return None
            executed = min(row.qty, sum(sizes.values()))
        filled = _same_size(executed, row.qty)
        return OrderExecution(
            order=ExchangeOrder(
                id=target_deal or '', symbol=row.symbol, side=row.side,
                order_type=OrderType.MARKET, qty=row.qty, filled_qty=executed,
                remaining_qty=0.0 if filled else max(0.0, row.qty - executed),
                price=None, stop_price=None, average_fill_price=None,
                status=OrderStatus.FILLED if filled else OrderStatus.CANCELLED,
                timestamp=epoch_time(), fee=0.0, fee_currency='',
                reduce_only=True, client_order_id=row.client_order_id,
            ),
            fills=None,
            fee_reported=False,
        )

    def _engine_booked_entry_qty(self, row: 'OrderRow') -> float:
        """How much of an own entry the engine booked from fills this stream never yielded.

        The engine's durable receipts of entry fills
        (:meth:`~pynecore.core.broker.storage.RunContext.iter_entry_fill_receipts`)
        that it took itself (``booked``) and that belong to ``row``: by the
        row's client order id, or, for a receipt without one, by the row's
        deal or the working order it was promoted from. Called for a row whose
        entry this stream has not delivered: such a receipt is the engine's
        recovery of the execution from the venue (:meth:`fetch_order_execution`).

        :param row: An own entry row.
        :return: The booked quantity in the journal's unit, ``0.0`` for none.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return 0.0
        extras = row.extras or {}
        order_ids = {order_ref for order_ref in (
            row.exchange_order_id, _wire_id(extras.get('promoted_from_working_order_id')),
        ) if order_ref}
        booked = 0.0
        for receipt in store_ctx.iter_entry_fill_receipts():
            if not receipt.booked or receipt.qty <= 0.0:
                continue
            if (receipt.client_order_id == row.client_order_id
                    or (receipt.client_order_id is None and receipt.order_id in order_ids)):
                booked += receipt.qty
        return booked

    async def _resolve_unproven_take_overs(
            self, positions_by_deal: dict[str, dict],
    ) -> AsyncIterator[OrderEvent]:
        """Book the fill of every held take-over the venue's history proves.

        A take-over stamped
        :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_TAKE_OVER_UNPROVEN` holds
        a deal of another size than it submitted
        (:meth:`~pynecore_capitalcom.reconcile._ReconcileMixin._take_over_fill_proven`).
        At most every :data:`_TAKE_OVER_PROOF_INTERVAL_S` the history is read
        (:meth:`_read_order_history`), and per row:

        * **The deal's opening activity proves the fill**: an opening of the
          submitted size, priced by the activity or, failing that, by the
          deal's open level. The fill is booked (:meth:`_book_proven_take_over`)
          under the opening's fingerprint, and the deal's later closes in the
          history are queued on the row, so the activity poll books them in
          order, exactly once.
        * **The opening is older than the history** (the deal's
          ``createdDateUTC`` lies beyond the 24 hours the venue keeps): what
          ``/positions`` lists is booked once its size has stayed unchanged
          for :data:`_TAKE_OVER_SIZE_STABLE_S`, and the deal's closes in the
          history are settled as delivered (the size already shows them).
        * **The deal is gone and its opening no longer proves the fill**: a
          pending market submission is handed to the engine's held-execution
          recovery (:meth:`_hand_over_vanished_take_over`).
        * Otherwise the row stays held and is proven again later: an opening
          of another size (part of the order netted another deal, or the
          order filled partly), or an opening the history lacks while it
          should still hold it (dossier §9 #8).

        A failed read is logged and repeated on a later poll.

        :param positions_by_deal: The poll's ``/positions`` rows by ``dealId``.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return
        held = [row for row in store_ctx.iter_live_orders()
                if (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None]
        live = {row.client_order_id for row in held}
        for coid in [coid for coid in self._take_over_size_seen if coid not in live]:
            del self._take_over_size_seen[coid]
        for coid in [coid for coid in self._take_over_absent_since if coid not in live]:
            del self._take_over_absent_since[coid]
        if not held:
            return
        now = epoch_time()
        if now - self._take_over_proof_read_at < _TAKE_OVER_PROOF_INTERVAL_S:
            return
        self._take_over_proof_read_at = now
        try:
            activities = await self._read_order_history()
        except (BrokerError, CapitalComError, httpx.TimeoutException, httpx.RequestError,
                ConnectionError) as exc:
            logger.warning(
                "the venue history could not be read to prove %d held take-over(s) (%s); "
                "read again on a later poll", len(held), exc,
            )
            return
        for row in held:
            event = self._prove_take_over(row, activities, positions_by_deal, now)
            if event is not None:
                yield event

    def _prove_take_over(self, row: 'OrderRow', activities: list[dict],
                         positions_by_deal: dict[str, dict], now: float) -> OrderEvent | None:
        """Book one held take-over when the history or a stable snapshot proves its fill.

        See :meth:`_resolve_unproven_take_overs` for the proofs.

        :return: The ENTRY fill to yield, ``None`` when nothing is booked now
            or the engine booked the fill itself.
        """
        coid = row.client_order_id
        extras = row.extras or {}
        deal_id = _wire_id((extras.get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) or {}).get('deal_id'))
        if deal_id is None:
            return None
        position = (positions_by_deal.get(deal_id) or {}).get('position') or {}
        closes = _deal_closes(activities, row.side, deal_id)
        opening = _best_copy(_deal_openings(activities, row.side, deal_id))
        if opening is not None:
            self._take_over_size_seen.pop(coid, None)
            size = activity_size(opening)
            details = opening.get('details') or {}
            price = (_wire_price(opening.get('level') or details.get('level'))
                     or _wire_price(position.get('level')))
            if size is None or not _same_size(size, row.qty) or price <= 0.0:
                logger.debug("take-over %s: the opening of deal %s (size %s) does not prove "
                             "the submitted %s; still held", coid, deal_id, size, row.qty)
                return None
            return self._book_proven_take_over(
                row, deal_id, size, price, now=now,
                executed_date_utc=str(opening.get('dateUTC') or '') or None,
                fill_id=_activity_fingerprint(opening), queue=closes, delivered=(),
                proof='opening_activity',
            )
        if not position:
            self._take_over_size_seen.pop(coid, None)
            self._hand_over_vanished_take_over(row, deal_id, closes, now)
            return None
        self._take_over_absent_since.pop(coid, None)
        created = _parse_iso_timestamp(str(position.get('createdDateUTC') or ''))
        size = _wire_float(position.get('size'), finite=True)
        level = _wire_price(position.get('level'))
        if (created <= 0.0 or created > now - _ACTIVITY_MAX_PERIOD_S
                or size is None or size <= 0.0 or size > row.qty * (1.0 + 1e-9)
                or level <= 0.0
                or str(position.get('direction') or '').upper()
                != ('BUY' if row.side == 'buy' else 'SELL')):
            self._take_over_size_seen.pop(coid, None)
            return None
        seen = self._take_over_size_seen.get(coid)
        if seen is None or not _same_size(seen[0], size):
            self._take_over_size_seen[coid] = (size, now)
            return None
        if now - seen[1] < _TAKE_OVER_SIZE_STABLE_S:
            return None
        self._take_over_size_seen.pop(coid, None)
        return self._book_proven_take_over(
            row, deal_id, size, level, now=now, executed_date_utc=None, fill_id=None,
            queue=(), delivered=closes, proof='position_beyond_history',
        )

    def _hand_over_vanished_take_over(self, row: 'OrderRow', deal_id: str,
                                      closes: Sequence[dict], now: float) -> None:
        """Hand a held take-over whose deal is gone to the engine.

        The fill could not be proven from the deal's opening, and ``/positions``
        no longer lists the deal, so the stable-size booking beyond the history
        can never come. A close of the deal in the history proves it closed; an
        absence alone counts once it lasted :data:`_TAKE_OVER_SIZE_STABLE_S`
        (a ``/positions`` read may lag). Only a submission still waiting for
        the venue's answer, or a taken-over working order whose execution
        opened the deal, can be handed over
        (:meth:`~pynecore.core.broker.storage.RunContext.execution_hold_applies`).

        :param row: The held take-over row.
        :param deal_id: Its deal.
        :param closes: The deal's closes in the history.
        :param now: The pass's wall-clock time.
        """
        store_ctx = self.store_ctx
        assert store_ctx is not None
        if not store_ctx.execution_hold_applies(row):
            return
        if not closes:
            since = self._take_over_absent_since.setdefault(row.client_order_id, now)
            if now - since < _TAKE_OVER_SIZE_STABLE_S:
                return
        self._hand_over_to_engine(row, deal_id, None, closes,
                                  proof='closed_in_history' if closes else 'absent_from_positions')

    async def _hand_over_if_closed(self, row: 'OrderRow', deal_id: str,
                                   positions_by_deal: dict[str, dict]) -> bool:
        """Hand a landed market submission whose deal the venue no longer lists to the engine.

        The take-over found the deal missing from the poll's ``/positions``
        snapshot. That alone may be the eventually consistent read lagging the
        deal's opening, so the history (read at most every
        :data:`_TAKE_OVER_PROOF_INTERVAL_S` per row) must show a close of the
        deal the submission executed into: the deal it landed on, or, when the
        venue ran it through its working-order machinery (measured 2026-08-28,
        cycle 84), the fresh deal whose opening back-links it
        (:meth:`_execution_in_history`), which the snapshot does not list
        either. Then the stream will never deliver the submission's execution,
        and the engine's held-execution recovery owns it
        (:meth:`_hand_over_to_engine`).

        :param row: The landed submission, still waiting for the venue's answer.
        :param deal_id: The deal it landed on.
        :param positions_by_deal: The poll's ``/positions`` rows by ``dealId``.
        :return: Whether the engine was asked.
        """
        coid = row.client_order_id
        now = epoch_time()
        if now - self._closed_deal_read_at.get(coid, 0.0) < _TAKE_OVER_PROOF_INTERVAL_S:
            return False
        self._closed_deal_read_at[coid] = now
        try:
            activities = await self._read_order_history()
        except (BrokerError, CapitalComError, httpx.TimeoutException, httpx.RequestError,
                ConnectionError) as exc:
            logger.warning("the venue history could not be read to see whether deal %s of "
                           "submission %s closed (%s); read again on a later poll",
                           deal_id, coid, exc)
            return False
        executed = self._execution_in_history(row, deal_id, activities)
        if executed is not None and executed[0] != deal_id:
            fresh_deal, opening, closes = executed
            if not closes or fresh_deal in positions_by_deal:
                return False
            return self._hand_over_to_engine(row, fresh_deal, opening, closes,
                                             proof='closed_in_history')
        closes = _deal_closes(activities, row.side, deal_id)
        if not closes:
            return False
        return self._hand_over_to_engine(
            row, deal_id, _best_copy(_deal_openings(activities, row.side, deal_id)), closes,
            proof='closed_in_history')

    async def _hand_over_if_executed(self, row: 'OrderRow') -> bool:
        """Keep a vanished unfilled entry live, or hand it to the engine, when it executed.

        The disappearance verdict's last check before it calls an own entry
        whose order shows no execution cancelled: a resting order, or a market
        entry the venue accepted without opening it at once. The order may have
        executed into a deal that closed before any ``/positions`` read listed
        it, while the stream booked nothing of it: a taken-over order that
        executed before its id was known (its activities were read as someone
        else's), or one whose activities the poll never read. The history
        (:meth:`_execution_in_history`) decides:

        * no execution of the order: the cancel stands (``False``);
        * an execution into a deal another row owns: that row books it, and
          the cancel stands for this one (``False``);
        * an execution into a deal a fresh ``GET /positions/{dealId}`` probe
          shows live: the stream or the snapshot pass books it, and the row
          stays (``True``);
        * an execution into a deal the probe proves gone (a definitive
          not-found), with a close of it in the history or gone for
          :data:`_TAKE_OVER_SIZE_STABLE_S` since the row was first missed: the
          engine's held-execution recovery books it (:meth:`_hand_over_to_engine`),
          like an order that executed while no process ran (``True``);
        * otherwise the evidence is not complete yet (``True``).

        The history is read at most every :data:`_TAKE_OVER_PROOF_INTERVAL_S`
        per row; a failed read or probe keeps the row too. A row the engine
        cannot hold
        (:meth:`~pynecore.core.broker.storage.RunContext.execution_hold_applies`)
        is never handed over and never cancelled while the history shows its
        execution: it stays, logged ERROR.

        :param row: A live own entry row the venue no longer lists, with no
            execution read and no recorded end of its order.
        :return: ``True`` when the row stays live, ``False`` when the cancel
            stands.
        """
        store_ctx = self.store_ctx
        order_id = row.exchange_order_id
        if store_ctx is None or not order_id:
            return False
        coid = row.client_order_id
        now = epoch_time()
        if now - self._closed_deal_read_at.get(coid, 0.0) < _TAKE_OVER_PROOF_INTERVAL_S:
            return True
        self._closed_deal_read_at[coid] = now
        try:
            activities = await self._read_order_history()
        except (BrokerError, CapitalComError, httpx.TimeoutException, httpx.RequestError,
                ConnectionError) as exc:
            logger.warning("the venue history could not be read to see whether order %s of "
                           "entry row %s executed before it vanished (%s); read again on a "
                           "later pass", order_id, coid, exc)
            return True
        executed = self._execution_in_history(row, order_id, activities)
        if executed is None:
            self._closed_deal_read_at.pop(coid, None)
            return False
        deal_id, opening, closes = executed
        owner = store_ctx.find_by_ref('deal_id', deal_id)
        if owner is not None and owner.client_order_id != coid:
            self._closed_deal_read_at.pop(coid, None)
            return False
        try:
            await self.call_api(f'positions/{deal_id}', method='get')
        except OrderNotFoundError:
            missed_at = _wire_float((row.extras or {}).get(MISSING_PENDING_EXTRA), finite=True)
            if not closes and (missed_at is None or now - missed_at < _TAKE_OVER_SIZE_STABLE_S):
                return True
            if not store_ctx.execution_hold_applies(row):
                logger.error(
                    "entry row %s: its order %s executed into deal %s, closed since, and the "
                    "engine cannot hold the entry for it; the row stays live", coid, order_id,
                    deal_id,
                )
                return True
            self._hand_over_to_engine(
                row, deal_id, opening, closes,
                proof='executed_and_closed_between_polls' if closes
                else 'executed_and_absent_from_positions')
        except (BrokerError, CapitalComError, httpx.TimeoutException, httpx.RequestError,
                ConnectionError) as exc:
            logger.debug("order %s of entry row %s executed into deal %s, whose probe failed "
                         "(%s); asked again later", order_id, coid, deal_id, exc)
        else:
            logger.debug("order %s of entry row %s executed into deal %s, which is still "
                         "live; asked again later", order_id, coid, deal_id)
        return True

    def _hand_over_to_engine(self, row: 'OrderRow', deal_id: str, opening: dict | None,
                             closes: Sequence[dict], *, proof: str,
                             engine_adopts_closes: bool = False) -> bool:
        """Hand a landed submission whose deal is closed to the engine's held-execution recovery.

        One transaction prepares the row: the deal is mapped to it (so
        :meth:`fetch_order_execution` answers by the deal id with the
        opening's execution), it is stamped
        :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_EXECUTION_HELD` (its
        held take-over stamp goes), and the opening, when the history shows
        it, is queued at the head of the deal's delivery queue. The engine
        books the opening itself, under the activity fingerprint the stream
        uses, so a copy the stream reads later is a replay, and its receipt
        stamps the queued entry booked (:meth:`_settle_engine_booked_entries`).
        The deal's closes the history shows reach the book once, in order:

        * a running engine (``engine_adopts_closes`` false) gets them from the
          stream: each joins the deal's delivery queue behind the entry, and
          the next activity read delivers it, judged and booked like any
          close of an own deal, once the engine booked the entry (a close
          read earlier, a taken-over order's as someone else's, is read
          again); the book then matches the venue's flat position;
        * an engine yet to adopt the venue's book at its startup
          (``engine_adopts_closes`` true, a hand-over before its first sync)
          owns them: it adopts the venue's flat book as the close, so each is
          recorded delivered here, and a copy the stream reads later is a
          replay.

        Then, outside the transaction, the engine is asked
        (:meth:`_ask_execution_hold`); a refusal leaves the prepared row to
        the next pass (:meth:`_settle_execution_holds`).

        The deal is the order the engine reads: the deal a market
        submission landed on, or the one a working order's execution (or a
        market entry's, run through the venue's working-order machinery)
        opened, since the order's own id names no execution. A row the engine
        cannot hold
        (:meth:`~pynecore.core.broker.storage.RunContext.execution_hold_applies`)
        is left as it is.

        :param row: The landed submission.
        :param deal_id: The closed deal it landed on.
        :param opening: The deal's opening activity, ``None`` when the
            history no longer shows it.
        :param closes: The deal's closes in the history.
        :param proof: What proved the deal closed, for the audit event.
        :param engine_adopts_closes: Whether the engine has yet to adopt the
            venue's book at its startup, and so owns the deal's closes.
        :return: Whether the engine was asked.
        """
        store_ctx = self.store_ctx
        assert store_ctx is not None
        coid = row.client_order_id
        if not store_ctx.execution_hold_applies(row):
            return False
        owner = store_ctx.find_by_ref('deal_id', deal_id)
        if owner is not None and owner.client_order_id != coid:
            return False
        now = epoch_time()
        opened_at = str(opening.get('dateUTC') or '') if opening is not None else ''

        def stamp(extras: dict) -> None:
            extras[EXTRAS_KEY_EXECUTION_HELD] = {'deal_id': deal_id, 'at': now}
            extras.pop(EXTRAS_KEY_TAKE_OVER_UNPROVEN, None)
            if opened_at:
                enqueue_entry(extras, opened_at)
            for close in closes:
                if engine_adopts_closes:
                    book_fill(extras, close, _close_identity(close))
                else:
                    enqueue_fill(extras, close)

        with store_ctx.transaction():
            if owner is None:
                store_ctx.add_ref(coid, 'deal_id', deal_id)
            self._rewrite_extras(coid, stamp)
            store_ctx.log_event(
                'execution_handed_to_engine',
                client_order_id=coid,
                exchange_order_id=deal_id,
                intent_key=row.intent_key,
                payload={'proof': proof, 'opening_date_utc': opened_at or None,
                         'closes': len(closes),
                         'closes_adopted_by_engine': engine_adopts_closes},
            )
        if not engine_adopts_closes:
            # The closes were read already (a taken-over order's as someone
            # else's): the next activity read delivers them again, behind the
            # entry the engine books.
            for close in closes:
                self._activity_cursor.seen_fingerprints.pop(_activity_fingerprint(close), None)
        if not self._ask_execution_hold(coid, deal_id):
            return False
        self._closed_deal_read_at.pop(coid, None)
        self._take_over_absent_since.pop(coid, None)
        logger.warning(
            "entry row %s (intent %s): its order reached deal %s, which is closed since (%s): "
            "the stream will not deliver that execution, so the engine reads it from the "
            "venue and books it; new entries wait meanwhile", coid, row.intent_key, deal_id,
            proof,
        )
        return True

    def _ask_execution_hold(self, coid: str, order_id: str) -> bool:
        """Ask the engine to hold a handed-over row's entry for ``order_id``; record it asked.

        :meth:`~pynecore.core.plugin.broker.BrokerPlugin.request_execution_hold`,
        outside every store transaction. On ``True`` the row's
        :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_EXECUTION_HELD` stamp
        records the request, so :meth:`_settle_execution_holds` tells a
        request the engine has not taken yet from one it has settled.

        :return: Whether the request is stored.
        """
        if not self.request_execution_hold(coid, order_id):
            return False

        def asked(extras: dict) -> None:
            stamp = extras.get(EXTRAS_KEY_EXECUTION_HELD)
            if isinstance(stamp, dict):
                extras[EXTRAS_KEY_EXECUTION_HELD] = {**stamp, 'requested': True}

        self._rewrite_extras(coid, asked)
        return True

    def _settle_execution_holds(self, positions_by_deal: dict[str, dict]) -> None:
        """Ask again for, or tear down, every row handed to the engine's held-execution recovery.

        Per row stamped
        :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_EXECUTION_HELD`, by the
        order the stamp names:

        * not asked yet (an earlier request was refused): asked again
          (:meth:`_ask_execution_hold`);
        * asked, and the request still waits for the engine
          (:meth:`~pynecore.core.broker.storage.RunContext.execution_hold_requests`),
          or the engine holds the order
          (:meth:`~pynecore.core.broker.storage.RunContext.held_execution_order_ids`):
          left alone;
        * the deal's delivery queue still owes a fill
          (:func:`~pynecore_capitalcom.delivery.delivery_pending`, a close owed
          to a running engine): left to the stream, whose booking of the last
          close tears the row down;
        * otherwise the engine booked the execution (or released the entry
          as unexecuted) and adopted the venue's flat book: the row is torn
          down as closed (:meth:`_tear_down_natural_close`), its journal
          exposure and delivery queue with it, since the engine's book holds
          the deal's whole life. A deal ``/positions`` lists again is an
          ordinary own position: only the stamp goes.

        :param positions_by_deal: The poll's ``/positions`` rows by ``dealId``.
        """
        store_ctx = self.store_ctx
        assert store_ctx is not None
        held: frozenset[str] | None = None
        requests: dict[str, dict] | None = None
        for row in list(store_ctx.iter_live_orders()):
            stamp = (row.extras or {}).get(EXTRAS_KEY_EXECUTION_HELD)
            if not isinstance(stamp, dict) or not stamp.get('deal_id'):
                continue
            coid = row.client_order_id
            order_id = str(stamp['deal_id'])
            if not stamp.get('requested'):
                self._ask_execution_hold(coid, order_id)
                continue
            if held is None or requests is None:
                held = store_ctx.held_execution_order_ids()
                requests = store_ctx.execution_hold_requests()
            if coid in requests or order_id in held:
                continue
            if delivery_pending((store_ctx.get_order(coid) or row).extras or {}):
                # The deal's closes are owed to a running engine through the
                # stream, behind the entry it booked: the close booking the
                # last of them tears the row down.
                continue
            if order_id in positions_by_deal:
                self._rewrite_extras(coid, lambda current: current.pop(
                    EXTRAS_KEY_EXECUTION_HELD, None))
                continue
            with store_ctx.transaction():
                self._rewrite_extras(coid, lambda current: current.pop(
                    EXTRAS_KEY_EXECUTION_HELD, None))
                self._tear_down_natural_close(store_ctx.get_order(coid) or row)
                store_ctx.log_event(
                    'execution_hold_settled',
                    client_order_id=coid,
                    exchange_order_id=order_id,
                    intent_key=row.intent_key,
                    payload={'filled_qty': row.filled_qty},
                )

    def _execution_in_history(
            self, row: 'OrderRow', order_id: str, activities: list[dict],
    ) -> tuple[str, dict, list[dict]] | None:
        """The deal an own entry's order executed into, when the history shows its opening.

        The deal is the one the order landed on (``order_id`` itself: a market
        entry), or a fresh deal whose opening back-links the order
        (``details.workingOrderId``, dossier §8): a resting order's execution,
        or a market entry the venue ran through its working-order machinery
        (measured 2026-08-28, cycle 84). Exactly one such deal must show an
        executed opening in the order's direction; two answer ``None``.

        :param row: The own entry row.
        :param order_id: The venue order id the row's order has (its ``dealId``).
        :param activities: A ``/history/activity`` read.
        :return: ``(deal id, opening, closes)``, the closes possibly empty, or
            ``None`` when the history shows no single execution of the order.
        """
        openings: dict[str, list[dict]] = {}
        for activity in activities:
            deal_id = _wire_id(activity.get('dealId'))
            if (deal_id is not None
                    and (deal_id == order_id
                         or _wire_id((activity.get('details') or {}).get('workingOrderId'))
                         == order_id)
                    and _is_opening_activity(activity, row.side, deal_id)
                    and str(activity.get('status') or '').upper() in _FILL_STATUSES):
                openings.setdefault(deal_id, []).append(activity)
        if len(openings) != 1:
            return None
        deal_id, copies = next(iter(openings.items()))
        opening = _best_copy(copies)
        if opening is None:
            return None
        return deal_id, opening, _deal_closes(activities, row.side, deal_id)

    def _closed_execution_evidence(
            self, row: 'OrderRow', activities: list[dict], listed: dict[str, dict],
    ) -> tuple[str, dict, list[dict]] | None:
        """The closed deal an own entry's order executed into while no process ran.

        The order is gone from the venue's lists. The startup history proves
        both ends of its execution (:meth:`_execution_in_history`): the
        opening of the deal it executed into, and a close of that deal, which
        ``/positions`` no longer lists.

        :param row: The own entry row, still live, with its venue order id.
        :param activities: The startup ``/history/activity`` read.
        :param listed: The startup ``/positions`` rows by ``dealId``.
        :return: ``(deal id, opening, closes)``, or ``None`` when the history
            does not prove an execution into a deal closed since.
        """
        if not row.exchange_order_id:
            return None
        executed = self._execution_in_history(row, row.exchange_order_id, activities)
        if executed is None or not executed[2] or executed[0] in listed:
            return None
        return executed

    def _hand_over_offline_executions(
            self, executed: list[tuple['OrderRow', str, dict | None, list[dict]]], *,
            proof: str = 'executed_while_offline', engine_adopts_closes: bool = True,
    ) -> set[str]:
        """Hand the startup's unseen executions to the engine, before its first sync.

        After a restart nothing maps such an order to its entry, so no event
        of it reaches the entry, and a re-declared entry would go out again
        next to the execution: the engine holds the entry over the deal the
        execution opened
        (:meth:`~pynecore.core.plugin.broker.BrokerPlugin.request_execution_hold`),
        reads its opening (:meth:`fetch_order_execution`) and books it, and
        adopts the venue's flat book as its close (:meth:`_hand_over_to_engine`).

        :param executed: Per row, the deal its order executed into, that
            deal's opening (``None`` when the history cannot show it) and its
            closes (:meth:`_closed_execution_evidence`, or a landed submission
            whose deal no snapshot lists).
        :param proof: What proved it, for the audit event.
        :param engine_adopts_closes: Whether the engine has yet to adopt the
            venue's book (a fresh start), and so owns the deals' closes; a
            reconnect's running engine gets them from the stream
            (:meth:`_hand_over_to_engine`).
        :return: The client order ids prepared for the engine (asked or to be
            asked again).
        """
        prepared: set[str] = set()
        for row, deal_id, opening, closes in executed:
            self._hand_over_to_engine(row, deal_id, opening, closes, proof=proof,
                                      engine_adopts_closes=engine_adopts_closes)
            current = self.store_ctx.get_order(row.client_order_id) if self.store_ctx else None
            if current is not None and (current.extras or {}).get(EXTRAS_KEY_EXECUTION_HELD):
                prepared.add(row.client_order_id)
        return prepared

    def _book_proven_take_over(
            self, row: 'OrderRow', deal_id: str, size: float, price: float, *,
            now: float, executed_date_utc: str | None, fill_id: str | None,
            queue: Iterable[dict], delivered: Iterable[dict], proof: str,
    ) -> OrderEvent | None:
        """Book a held take-over's proven fill and end its hold, in one transaction.

        A pending market submission is confirmed filled on ``deal_id`` like a
        take-over the snapshot proved; a taken-over working order moves onto
        the deal. The stamp goes, the entry is stamped booked
        (``entry_filled_at``, the opening's ``dateUTC``), the closes in
        ``queue`` join the deal's fill queue and those in ``delivered`` are
        recorded booked (:mod:`~pynecore_capitalcom.delivery`), and the
        ``'attached'`` resolution settles the engine's hold on the entry. The
        fill is returned for the stream unless the engine has booked it
        already (:meth:`_engine_booked_entry_qty`).

        :param row: The held take-over row.
        :param deal_id: Its deal.
        :param size: The proven fill.
        :param price: Its price.
        :param now: The pass's wall-clock time.
        :param executed_date_utc: The opening's ``dateUTC``, when read.
        :param fill_id: The fill id (the opening's fingerprint), ``None`` for
            a fill booked from ``/positions``.
        :param queue: Closes of the deal still to be booked.
        :param delivered: Closes of the deal the booked size already shows.
        :param proof: The proof, for the audit event.
        :return: The ENTRY fill, or ``None``.
        """
        store_ctx = self.store_ctx
        assert store_ctx is not None
        coid = row.client_order_id
        queued = list(queue)
        settled = list(delivered)
        engine_booked = self._engine_booked_entry_qty(row) > 0.0
        market = (row.extras or {}).get('kind') == ENTRY_KIND_POSITION

        def settle(extras: dict) -> None:
            extras.pop(EXTRAS_KEY_TAKE_OVER_UNPROVEN, None)
            book_entry(extras, now, executed_date_utc)
            for close in queued:
                enqueue_fill(extras, close)
            for close in settled:
                book_fill(extras, close, _close_identity(close))

        with store_ctx.transaction():
            if market:
                mark_confirmed_with_fill(
                    store_ctx, coid=coid, exchange_id=deal_id, is_filled=True,
                    filled_qty=size, fill_price=price,
                )
            else:
                working_id = row.exchange_order_id
                self._promote_working_row_to_position(
                    row, deal_id, stamp_entry_filled=True, executed_date_utc=executed_date_utc,
                )
                DispatchJournal(store_ctx).apply_reconcile_outcome(
                    coid,
                    ReconcileOutcome(
                        kind='filled',
                        reason='working_promoted_position',
                        new_state='confirmed',
                        audit_event='working_to_position',
                        filled_qty=size,
                        audit_payload={'filled': size, 'working_order_id': working_id},
                        exchange_order_id=deal_id,
                    ),
                )
            self._rewrite_extras(coid, settle)
            store_ctx.record_resolution(coid, 'attached')
            store_ctx.log_event(
                'take_over_fill_proven',
                client_order_id=coid,
                exchange_order_id=deal_id,
                intent_key=row.intent_key,
                payload={'proof': proof, 'filled': size, 'fill_price': price,
                         'submitted': row.qty, 'fill_id': fill_id,
                         'queued_closes': len(queued), 'settled_closes': len(settled),
                         'engine_booked': engine_booked},
            )
            if market and (row.extras or {}).get(EXTRAS_KEY_CANCEL_REQUESTED) is not None:
                # Pine cancelled the entry while it was held; the market
                # entry had executed, so the cancel is a no-op.
                store_ctx.log_event(
                    'cancel_noop',
                    client_order_id=coid,
                    exchange_order_id=deal_id,
                    intent_key=row.intent_key,
                    payload={'reason': 'already_filled', 'pine_id': row.pine_entry_id,
                             'from_entry': row.from_entry},
                )
        logger.warning(
            "take-over %s (intent %s): the venue proves its fill (%s): %s at %s on deal %s; "
            "booked, %d later close(s) of the deal follow in order — the hold ended by itself",
            coid, row.intent_key, proof, size, price, deal_id, len(queued),
        )
        if engine_booked:
            return None
        return OrderEvent(
            order=ExchangeOrder(
                id=deal_id, symbol=row.symbol, side=row.side,
                order_type=OrderType.MARKET,
                qty=row.qty, filled_qty=size,
                remaining_qty=max(0.0, row.qty - size),
                price=None, stop_price=None,
                average_fill_price=price,
                status=OrderStatus.FILLED,
                timestamp=now, fee=0.0, fee_currency='',
                reduce_only=False, client_order_id=coid,
            ),
            event_type='filled',
            fill_price=price,
            fill_qty=size,
            journal_fill_qty=size,
            timestamp=now,
            pine_id=row.pine_entry_id,
            from_entry=row.from_entry,
            leg_type=LegType.ENTRY,
            fill_id=fill_id,
        )

    async def _settle_own_troubles(self, positions_by_deal: dict[str, dict]) -> None:
        """End every ``ENTRY_BLOCK`` cause of the bot's own trouble whose condition is gone.

        * ``own_working_order_netted_foreign_deal:<row>``: released once the
          order's execution is accounted: its row retired (it only netted the
          foreign deal, none of it is the run's) or its residual booked.
        * ``own_working_order_residual_unbooked:<deal>``: the bot closes the
          deal itself (:meth:`_close_unbooked_residuals`).
        * An entry the stream holds for its price while the engine booked its
          execution from the venue (:meth:`_engine_booked_entry_qty`) is
          stamped booked, so the deal's queued fills follow.
        * A row handed to the engine's held-execution recovery is torn down
          once the engine no longer holds its order (:meth:`_settle_execution_holds`).

        The ``foreign_exit_block:<intent>`` cause is released where the block
        ends (:meth:`~pynecore_capitalcom.foreign._ForeignExposureMixin._foreign_exit_unblocked`).

        :param positions_by_deal: The poll's ``/positions`` rows by ``dealId``.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return
        for cause in [cause for cause in self._own_trouble_causes
                      if cause.startswith(CAUSE_NETTED_FOREIGN_DEAL)]:
            row = store_ctx.get_order(cause[len(CAUSE_NETTED_FOREIGN_DEAL):])
            extras = (row.extras or {}) if row is not None else {}
            if row is None or row.closed_ts_ms is not None:
                self._release_own_trouble(
                    cause, "the order's execution was retired: it only netted the foreign "
                           "deal, none of it is the run's exposure")
            elif (extras.get('entry_filled_at') is not None
                  and extras.get(EXTRAS_KEY_NETTED_RESIDUAL_PENDING) is None):
                self._release_own_trouble(cause, "the order's own residual deal is booked")
        self._settle_engine_booked_entries()
        self._settle_execution_holds(positions_by_deal)
        await self._close_unbooked_residuals(positions_by_deal)

    def _settle_engine_booked_entries(self) -> None:
        """Stamp booked every entry queued for its price that the engine booked itself.

        A row handed to the engine's held-execution recovery
        (:data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_EXECUTION_HELD`) whose
        order executed into another deal (a working order's, or a market
        entry's run through the venue's working-order machinery) moves onto
        that deal in the same transaction: the deal's closes owed behind the
        entry are then read as the closes of an own position.
        """
        store_ctx = self.store_ctx
        assert store_ctx is not None
        for row in list(store_ctx.iter_live_orders()):
            extras = row.extras or {}
            if entry_pending_since(extras) is None:
                continue
            booked = self._engine_booked_entry_qty(row)
            if booked <= 0.0:
                continue
            now = epoch_time()
            handed = extras.get(EXTRAS_KEY_EXECUTION_HELD)
            executed_deal = (_wire_id(handed.get('deal_id'))
                             if isinstance(handed, dict) else None)
            with store_ctx.transaction():
                self._rewrite_extras(row.client_order_id,
                                     lambda current: book_entry(current, now))
                if executed_deal is not None and executed_deal != row.exchange_order_id:
                    self._promote_working_row_to_position(
                        store_ctx.get_order(row.client_order_id) or row, executed_deal,
                        stamp_entry_filled=True)
                cursor = min(row.qty, max(row.filled_qty, booked))
                if cursor > row.filled_qty:
                    store_ctx.upsert_order(row.client_order_id, filled_qty=cursor)
                store_ctx.log_event(
                    'entry_fill_recovered_by_engine',
                    client_order_id=row.client_order_id,
                    exchange_order_id=row.exchange_order_id,
                    intent_key=row.intent_key,
                    payload={'booked': booked, 'queued_since': entry_pending_since(extras)},
                )

    async def _close_unbooked_residuals(self, positions_by_deal: dict[str, dict]) -> None:
        """Close every deal a retired own working order opened that no book holds.

        Such a deal (:meth:`~pynecore_capitalcom.activity._ActivityMixin._note_residual_after_netting_retirement`)
        is the bot's own exposure that nobody asked for and nobody books: the
        safe end is to close it. While ``/positions`` lists it, its cause is
        latched (again in a later process, whose entry block ended with the
        restart) and the bot sends ``DELETE /positions/{dealId}``, which
        closes exactly that deal and can never open the other side (research
        dossier §4, §8), at most every :data:`_RESIDUAL_CLOSE_RETRY_S`. A
        stored external-intervention latch forbids the write: the deal is the
        operator's then. Once ``/positions`` no longer lists the deal its
        cause is released.

        :param positions_by_deal: The poll's ``/positions`` rows by ``dealId``.
        """
        store_ctx = self.store_ctx
        assert store_ctx is not None
        self._netting_retirements()
        now = epoch_time()
        for deal_id in sorted(self._residual_deals_reported):
            cause = _residual_deal_cause(deal_id)
            raw = positions_by_deal.get(deal_id)
            if raw is None:
                self._residual_close_sent_at.pop(deal_id, None)
                self._release_own_trouble(cause, f"deal {deal_id} is no longer on the venue")
                continue
            position = raw.get('position') or {}
            if cause not in self._own_trouble_causes:
                message = (
                    f"deal {deal_id} ({(raw.get('market') or {}).get('epic')} "
                    f"{position.get('direction')} {position.get('size')}) that a retired own "
                    f"working order opened is still open and in no book; the bot closes it "
                    f"itself, and new entries are blocked until it is gone"
                )
                logger.error("%s", message)
                self._latch_own_trouble(message, {'deal_id': deal_id,
                                                  'position': dict(position)}, cause)
            sent_at = self._residual_close_sent_at.get(deal_id)
            if sent_at is not None and now - sent_at < _RESIDUAL_CLOSE_RETRY_S:
                continue
            if store_ctx.intervention_latch_stored():
                continue
            self._residual_close_sent_at[deal_id] = now
            try:
                with self._own_close([deal_id], full=True, size=None):
                    await self._call(f'positions/{deal_id}', method='delete')
            except OrderNotFoundError:
                logger.warning("residual deal %s: the venue does not route its close (not "
                               "found); judged again on the next poll", deal_id)
                continue
            except (BrokerError, CapitalComError, httpx.TimeoutException, httpx.RequestError,
                    ConnectionError) as exc:
                logger.warning("residual deal %s: its close did not land (%s); repeated on a "
                               "later poll", deal_id, exc)
                continue
            store_ctx.log_event(
                'own_working_order_residual_closed',
                exchange_order_id=deal_id,
                payload={'position': dict(position)},
            )
            logger.warning("residual deal %s: closed by the bot (DELETE accepted); the entry "
                           "block ends once the venue no longer lists it", deal_id)
