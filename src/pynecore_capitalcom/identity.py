"""Deal identity of the run's own entries: which venue deal an entry row holds.

The confirm of a market entry names the deal the run books its fill against,
but that id is not always the position's. The venue can run a market ``POST``
through its working-order machinery: the confirm then names the executed
working order, and the position opens under a fresh ``dealId`` whose
``workingOrderId`` back-links the order (measured: capitalcom cycle 84, and
cycle 442, where the confirm named ``…4bbe`` and the position opened as
``…4bc1``). Neither ``PUT`` nor ``DELETE positions/{dealId}`` reaches the
position by the order's id. A fresh deal can also be missing from the venue's
views for a while: the bulk ``GET /positions`` omitted one past a 7.5 s grace
in cycle 81 and for over 30 s in cycle 442, and ``positions/{dealId}``
answered not-found for seconds after a ``POST``.

An entry whose fill the venue confirmed therefore holds its deal until the
venue shows that deal closed. A not-found or an absence never says so on its
own:

* **Rekeyed on the first evidence.** A ``POSITION`` activity or a
  ``/positions`` row that back-links the row's ``dealId`` moves the row onto the
  position's deal: the activity bridge in ``activity.py``, the snapshot pass in
  ``reconcile.py``, and :meth:`_DealIdentityMixin._resolve_own_deal` when a
  write on the deal answers not-found.
* **Materialising until listed.** The deal of a fill this process booked
  stays *materialising* (:meth:`_DealIdentityMixin._materialising`) until a
  ``/positions`` read lists it, for at most :data:`_MATERIALISATION_BOUND_S`.
  A not-found on a materialising deal is no evidence that the deal is gone: a
  bracket ``PUT`` waits for the next sync, a close ``DELETE`` parks, the
  disappearance tracker defers its verdict, and the run's position counts the
  deal from the journal (:meth:`_DealIdentityMixin._materialising_position_rows`).
"""
from abc import ABC
from collections.abc import Container, Iterable
from time import time as epoch_time
from typing import TYPE_CHECKING

from pynecore.core.broker.store_helpers import ENTRY_KIND_POSITION, EXTRAS_KEY_LEG_KIND

from ._base import _CapitalComBase
from .delivery import entry_pending_since
from .helpers import _wire_float, _wire_id
from .models import _DealVerdict

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow

#: How long after its fill a deal no ``/positions`` read has listed stays
#: materialising. It outlasts every wait that concludes on a missing deal: the
#: disappearance tracker's ``fill_report_grace_s`` (120 s), the engine's 120 s
#: confirmation of an external reduction and the 30 s defensive-close grace, so
#: none of them decides on a read that has not caught up. A deal still unlisted
#: after it is judged by the venue's word again.
_MATERIALISATION_BOUND_S = 300.0


class _DealIdentityMixin(_CapitalComBase, ABC):
    """Which deal an own entry row holds, and whether a missing one is gone."""

    def _note_entry_fill_unlisted(self, coid: str) -> None:
        """Start the materialising window of an entry fill this process booked.

        :param coid: The entry row's client order id.
        """
        self._unlisted_fills.setdefault(coid, epoch_time())

    def _materialising(self, row: 'OrderRow', listed: Container[str] = ()) -> bool:
        """Whether ``row``'s deal is a fill this process booked that no read listed yet.

        Only a filled, still ``confirmed`` position row whose fill this
        process booked within :data:`_MATERIALISATION_BOUND_S` qualifies. A
        deal a ``/positions`` read listed, the caller's (``listed``) or a
        recent poll's, has materialised: its later absence is a close. A row
        whose own close the venue accepted is ``closing`` and does not qualify.

        :param row: An own entry row.
        :param listed: The ``dealId`` values the caller's ``/positions`` read listed.
        """
        coid = row.client_order_id
        started = self._unlisted_fills.get(coid)
        if started is None:
            return False
        deal_id = row.exchange_order_id
        if (epoch_time() - started > _MATERIALISATION_BOUND_S
                or row.closed_ts_ms is not None
                or (deal_id is not None
                    and (deal_id in self._seen_position_deals or deal_id in listed))):
            del self._unlisted_fills[coid]
            return False
        extras = row.extras or {}
        return (
            deal_id is not None
            and row.state == 'confirmed'
            and row.filled_qty > 0.0
            and extras.get('kind') == ENTRY_KIND_POSITION
            and extras.get(EXTRAS_KEY_LEG_KIND) is None
            and extras.get('natural_close_at') is None
        )

    def _materialising_position_rows(self, positions: Iterable[dict], symbol: str) -> list[dict]:
        """``/positions``-shaped rows for the own deals a read does not list yet.

        One row per materialising entry of ``symbol`` that ``positions`` omits,
        built from the journal: the confirmed fill's side, size and level. The
        run holds these deals, and a read that has not caught up must not hide
        them from the run's position.

        :param positions: The ``/positions`` rows of the caller's read.
        :param symbol: The epic.
        """
        store_ctx = self.store_ctx
        if store_ctx is None or not self._unlisted_fills:
            return []
        listed = {
            deal_id for raw in positions
            if (deal_id := _wire_id((raw.get('position') or {}).get('dealId'))) is not None
        }
        rows: list[dict] = []
        for row in store_ctx.iter_live_orders():
            if row.symbol != symbol or not self._materialising(row, listed):
                continue
            rows.append({
                'market': {'epic': row.symbol},
                'position': {
                    'dealId': row.exchange_order_id,
                    'direction': 'BUY' if row.side == 'buy' else 'SELL',
                    'size': row.filled_qty,
                    'level': _wire_float((row.extras or {}).get('confirm_level'),
                                         finite=True) or 0.0,
                    'upl': 0.0,
                },
            })
        return rows

    async def _resolve_own_deal(
            self, row: 'OrderRow', deal_id: str,
    ) -> tuple[_DealVerdict, str]:
        """Judge an own entry's deal after a write on ``deal_id`` answered not-found.

        The evidence, in order: the journal row, which a back-link may have
        moved meanwhile; a fresh ``/positions`` read, which may list the deal;
        the deals that read and the recent ``POSITION`` activities back-link to
        ``deal_id`` (a hit moves the row onto that deal); last, whether the
        deal is still materialising. A transport fault on a read escapes to
        the caller.

        :param row: The entry row the write targeted.
        :param deal_id: The ``dealId`` the write used.
        :return: The verdict and the deal the row holds now.
        """
        store_ctx = self.store_ctx
        current = (store_ctx.get_order(row.client_order_id) if store_ctx is not None
                   else None) or row
        if current.exchange_order_id and current.exchange_order_id != deal_id:
            return _DealVerdict.REKEYED, current.exchange_order_id
        snapshot = await self._call('positions', method='get')
        positions_by_deal = {
            deal: raw for raw in (snapshot.get('positions') or [])
            if (deal := _wire_id((raw.get('position') or {}).get('dealId'))) is not None
        }
        if deal_id in positions_by_deal:
            return _DealVerdict.PRESENT, deal_id
        if store_ctx is not None:
            history = await self._call(
                'history/activity', data={'lastPeriod': 60, 'detailed': 'true'}, method='get',
            )
            promoted = self._find_promoted_deal_id(
                current, list(history.get('activities') or []), positions_by_deal,
            )
            if promoted:
                # An entry deferred for its price is not booked: its priced
                # activity or the snapshot fallback books it, before the
                # fills held behind it.
                self._promote_working_row_to_position(
                    current, promoted,
                    stamp_entry_filled=entry_pending_since(current.extras or {}) is None,
                )
                return _DealVerdict.REKEYED, promoted
        if self._materialising(current, positions_by_deal):
            return _DealVerdict.MATERIALISING, deal_id
        return _DealVerdict.GONE, deal_id
