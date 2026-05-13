"""Plugin-side dispatch hook implementations for the Core ``DispatchJournal``.

Each hook class is constructed once per dispatch by the matching
``execute_*`` / ``modify_*`` orchestrator in ``execution.py`` and hands
the persist-first state machine to
:class:`pynecore.core.broker.journal.DispatchJournal`. The journal owns
every ``upsert_order`` / ``add_ref`` / ``log_event`` / state transition
along the lifecycle; the hook only owns the wire format
(endpoint / body / response parsing) and the reject classification.

These hooks live in their own module (rather than inside ``execution.py``)
so the dispatch surface stays small and the ~2700-line execution mix-in
shrinks as each path migrates.
"""
from typing import TYPE_CHECKING

import httpx

from pynecore.core.broker.exceptions import (
    ExchangeConnectionError,
    ExchangeOrderRejectedError,
    InsufficientMarginError,
    OrderDispositionUnknownError,
)
from pynecore.core.broker.journal import ModifyEntryOutcome
from pynecore.core.broker.models import (
    EntryIntent,
    ExchangeOrder,
    OrderStatus,
    OrderType,
)

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow

    from .execution import _ExecutionMixin


class _CapitalComModifyEntryHooks:
    """Hook set for the working-order amend dispatch.

    Conforms structurally to
    :class:`pynecore.core.broker.journal.ModifyEntryDispatchHooks`. The
    journal calls hooks by method name (Protocol semantics) so no
    explicit ``isinstance`` is needed.

    Construction captures the per-dispatch wire data (endpoint, PUT
    body, target row). The amend target — the working order itself —
    lives outside the journal's command-row scope; the hook mutates
    its ``exchange_order_id``-keyed row directly via
    ``store.upsert_order`` after the confirm GET so the engine-facing
    :meth:`exchange_order_from_state` can build an
    :class:`ExchangeOrder` from it without a second REST call.
    """

    def __init__(
            self,
            *,
            plugin: '_ExecutionMixin',
            target_row: 'OrderRow',
            new_level: float,
            order_type: OrderType,
    ) -> None:
        self._plugin = plugin
        self._target_row = target_row
        self._new_level = new_level
        self._order_type = order_type

    async def submit_amend(
            self, *, coid: str, target_coid: str,
            old_intent: EntryIntent, new_intent: EntryIntent,
    ) -> ModifyEntryOutcome:
        """PUT ``/workingorders/{dealId}`` with the new level and confirm.

        Capital.com lets us atomically rewrite the level of a pending
        working order — cheaper and safer than cancel+create. ``size``
        and order-type changes are caught by the orchestrator's
        cancel+create fallback before this hook runs, so by the time
        we get here the only thing changing is the level.

        Network failures on either the PUT or the confirms GET map to
        :class:`OrderDispositionUnknownError` — recovery on next start
        re-evaluates against the broker's authoritative view.
        """
        del old_intent  # captured at construction time as target_row + new_level
        del target_coid

        deal_id = self._target_row.exchange_order_id
        body = {'level': self._new_level}

        try:
            put_resp = await self._plugin._call(  # type: ignore[attr-defined]
                f'workingorders/{deal_id}', data=body, method='put',
            )
        except (httpx.TimeoutException, httpx.RequestError,
                ConnectionError, ExchangeConnectionError) as net:
            raise OrderDispositionUnknownError(
                f"Capital PUT workingorders/{deal_id} ambiguous: {net}",
                client_order_id=coid,
                cause=net if isinstance(net, Exception) else None,
            ) from net

        deal_ref = put_resp.get('dealReference')
        if not deal_ref:
            raise OrderDispositionUnknownError(
                f"Capital PUT workingorders/{deal_id}: no dealReference "
                f"in response",
                client_order_id=coid,
            )

        try:
            confirm = await self._plugin._call(  # type: ignore[attr-defined]
                f'confirms/{deal_ref}', method='get',
            )
        except (httpx.TimeoutException, httpx.RequestError,
                ConnectionError, ExchangeConnectionError) as net:
            raise OrderDispositionUnknownError(
                f"Capital GET confirms/{deal_ref} ambiguous after "
                f"working-order amend: {net}",
                client_order_id=coid,
                cause=net if isinstance(net, Exception) else None,
            ) from net

        deal_status = (confirm.get('dealStatus') or '').upper()
        if deal_status == 'REJECTED':
            reason = confirm.get('reason') or 'unknown'
            reason_lc = reason.lower()
            if 'margin' in reason_lc or 'leverage' in reason_lc:
                raise InsufficientMarginError(
                    f"Capital reject on working-order amend: {reason}",
                )
            raise ExchangeOrderRejectedError(
                f"Capital confirm REJECTED on working-order amend: {reason}",
            )

        echoed_level_raw = confirm.get('level')
        if echoed_level_raw is None:
            echoed_level_raw = put_resp.get('level')
        echoed_level = float(echoed_level_raw) if echoed_level_raw is not None \
            else self._new_level

        # Mirror the new level onto the target row so the engine sees it
        # immediately. The target row lives outside the journal's
        # command-row scope, so the hook owns this write.
        if self._plugin.store_ctx is not None:
            existing = self._plugin.store_ctx.get_order(
                self._target_row.client_order_id,
            )
            merged_extras = dict((existing.extras or {}) if existing else {})
            merged_extras['amended_level'] = echoed_level
            self._plugin.store_ctx.upsert_order(
                self._target_row.client_order_id,
                extras=merged_extras,
            )

        return ModifyEntryOutcome(
            server_ref=str(deal_ref),
            new_level=echoed_level,
            raw=confirm,
        )

    def exchange_order_from_state(
            self, *, row: 'OrderRow', new_intent: EntryIntent,
            outcome: ModifyEntryOutcome,
    ) -> list[ExchangeOrder]:
        """Build the engine-facing :class:`ExchangeOrder` for the amended order.

        ``row`` is the journal's *command* row (the modify dispatch
        record); the engine wants the *target* working order back. Read
        the latest target row directly from the store so its echoed
        level (mirrored by :meth:`submit_amend`) is included.
        """
        del row, outcome
        target = (
            self._plugin.store_ctx.get_order(self._target_row.client_order_id)
            if self._plugin.store_ctx is not None else self._target_row
        )
        if target is None:
            target = self._target_row
        return [ExchangeOrder(
            id=target.exchange_order_id or '',
            symbol=target.symbol,
            side=target.side,
            order_type=new_intent.order_type,
            qty=target.qty,
            filled_qty=target.filled_qty,
            remaining_qty=max(0.0, target.qty - target.filled_qty),
            price=new_intent.limit,
            stop_price=new_intent.stop,
            average_fill_price=None,
            status=OrderStatus.OPEN,
            timestamp=target.updated_ts_ms / 1000.0,
            fee=0.0,
            fee_currency='',
            reduce_only=False,
            client_order_id=target.client_order_id,
        )]
