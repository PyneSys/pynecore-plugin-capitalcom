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
from time import time as epoch_time
from typing import TYPE_CHECKING

import httpx

from pynecore.core.broker.exceptions import (
    BrokerError,
    BrokerManualInterventionError,
    ExchangeConnectionError,
    ExchangeOrderRejectedError,
    InsufficientMarginError,
    OrderDispositionUnknownError,
)
from pynecore.core.broker.journal import (
    CancelOutcome,
    CancelReasonPath,
    CloseOutcome,
    ModifyEntryOutcome,
    ModifyExitOutcome,
)
from pynecore.core.broker.models import (
    CancelIntent,
    CloseIntent,
    EntryIntent,
    ExchangeOrder,
    ExitIntent,
    OrderStatus,
    OrderType,
)

from .exceptions import CapitalComError, OrderNotFoundError
from .helpers import _extract_reject_reason, _is_funds_reject, _parse_iso_timestamp
from .models import _bracket_leg_id

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow

    from .execution import _ExecutionMixin
    from .models import _InstrumentRules


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
            reason = _extract_reject_reason(confirm)
            if _is_funds_reject(reason):
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


class _CapitalComCancelHooks:
    """Hook set for the cancel dispatch's per-target sweep.

    Conforms structurally to
    :class:`pynecore.core.broker.journal.CancelDispatchHooks`. The
    journal owns the command-row lifecycle (``submitted`` → ``confirmed``
    + ``close_order``); this hook owns the wire format for every target
    type the per-target loop encounters:

    * **Working order** with an ``exchange_order_id`` → DELETE
      ``/workingorders/{dealId}``. ``OrderNotFoundError`` is absorbed
      as the benign already-gone path (the broker had already cancelled
      it, typically because the underlying market closed or another
      cancel landed first).
    * **Bracket leg** (``leg_kind in {'tp','sl'}``) with a matching
      ``from_entry`` cancel → PUT ``/positions/{parent_deal_id}`` with
      ``profitLevel: null`` or ``stopLevel: null``. Trailing SLs add
      ``trailingStop: False`` so the native trailing mechanism stops
      managing the parent.
    * **Bracket leg** picked up by an entry cancel (``from_entry is
      None``) → silent skip; TV semantics: ``strategy.cancel(entry_id)``
      after the entry filled must leave the protective bracket alive.
    * **Filled position** (``kind == 'position'``) → silent
      ``cancel_noop`` audit event; never DELETE a filled position from
      a cancel intent.
    * **Working order without an ``exchange_order_id`` yet** → mark the
      row ``cancel_pending``; the next reconciler / recovery pass
      retries once the id lands.

    The hook closes each target row it actively swept (DELETE landed,
    PUT-null landed, or broker reported already-gone). Skipped rows
    stay live. The :class:`CancelOutcome.reason_path` reflects the
    aggregate verdict: ``deleted`` if anything was actively swept,
    ``already_gone`` if every wire call hit a 404, ``noop`` only when
    no live row matched the intent at all.
    """

    def __init__(
            self,
            *,
            plugin: '_ExecutionMixin',
    ) -> None:
        self._plugin = plugin

    async def submit_cancel(
            self, *, coid: str, intent: CancelIntent,
            targets: list['OrderRow'],
    ) -> CancelOutcome:
        """Sweep every target row and return an aggregated outcome.

        The journal already persisted the command row plus the
        ``dispatch_submitted`` audit event before calling this method.
        On per-target completion the hook closes the target row and
        logs the matching ``cancelled`` / ``cancel_already_gone`` /
        ``cancel_noop`` event so the audit trail stays compatible with
        the pre-journal implementation. Network failures on a wire
        call raise :class:`OrderDispositionUnknownError`; the journal
        flips the command row to ``disposition_unknown`` and
        recovery on next start re-evaluates.
        """
        store_ctx = self._plugin.store_ctx
        applied: list[str] = []
        swept_count = 0
        already_gone_count = 0

        if not targets:
            return CancelOutcome(
                succeeded=True, reason_path='noop',
                cleared_legs=0, applied_target_coids=[],
            )

        for row in targets:
            if row.state not in ('submitted', 'server_ref_seen', 'confirmed'):
                continue

            extras = row.extras or {}
            leg_kind = extras.get('leg_kind')

            # Bracket leg: ``execute_exit`` stamps ``leg_kind`` ('tp'/'sl')
            # plus ``parent_deal_id`` and stores no ``exchange_order_id``
            # because the level is a position attribute, not an order. The
            # cancel must clear that one level on the parent position via
            # PUT — DELETE on the bracket row id is not a thing.
            #
            # Gate on ``intent.from_entry is not None``: bracket rows store
            # ``pine_entry_id = intent.from_entry`` (the entry id), so an
            # entry-side ``CancelIntent(pine_id='Long', from_entry=None)``
            # would otherwise match the bracket and clear protective exits
            # from a still-open position. Only ``strategy.cancel(exit_id)``
            # carries a non-None ``from_entry`` and should remove brackets.
            if leg_kind in ('tp', 'sl') and intent.from_entry is not None:
                parent_deal_id = extras.get('parent_deal_id')
                if not parent_deal_id:
                    if store_ctx is not None:
                        store_ctx.set_order_state(
                            row.client_order_id, 'cancel_pending',
                        )
                    continue
                # ``stopLevel: null`` / ``profitLevel: null`` — Capital.com
                # treats null as an explicit clear, leaving the unspecified
                # leg untouched so the sister bracket survives. A trailing
                # SL needs the ``trailingStop: False`` flag too: clearing
                # ``stopLevel`` alone does not disable the native trailing
                # mechanism, so without this the broker would keep the
                # trailing stop active even after the local row is closed.
                body: dict
                if leg_kind == 'tp':
                    body = {'profitLevel': None}
                else:
                    body = {'stopLevel': None}
                    if row.trailing_distance or extras.get('trail_offset'):
                        body['trailingStop'] = False
                target_already_gone = False
                try:
                    resp = await self._plugin._call(  # type: ignore[attr-defined]
                        f'positions/{parent_deal_id}',
                        data=body, method='put',
                    )
                    # Mirror the confirm round-trip ``execute_exit`` /
                    # ``modify_exit`` already perform after every bracket
                    # PUT — Capital.com's confirms endpoint is TTL-bounded
                    # and the activity stream uses it as the truth source.
                    new_ref = (resp or {}).get('dealReference') \
                        if isinstance(resp, dict) else None
                    if new_ref:
                        await self._plugin._call(  # type: ignore[attr-defined]
                            f'confirms/{new_ref}', method='get',
                        )
                except (httpx.TimeoutException, httpx.RequestError,
                        ConnectionError, ExchangeConnectionError) as net:
                    raise OrderDispositionUnknownError(
                        f"Capital PUT positions/{parent_deal_id} ambiguous "
                        f"during cancel sweep: {net}",
                        client_order_id=coid,
                        cause=net if isinstance(net, Exception) else None,
                    ) from net
                except OrderNotFoundError:
                    # Position already gone — bracket is gone with it.
                    target_already_gone = True
                    if store_ctx is not None:
                        store_ctx.log_event(
                            'cancel_already_gone',
                            client_order_id=row.client_order_id,
                            exchange_order_id=str(parent_deal_id),
                            intent_key=intent.intent_key,
                        )
                if store_ctx is not None:
                    store_ctx.close_order(row.client_order_id)
                    store_ctx.log_event(
                        'cancelled',
                        client_order_id=row.client_order_id,
                        exchange_order_id=str(parent_deal_id),
                        intent_key=intent.intent_key,
                        payload={'leg_kind': leg_kind,
                                 'cleared_via': 'put_position'},
                    )
                applied.append(row.client_order_id)
                if target_already_gone:
                    already_gone_count += 1
                else:
                    swept_count += 1
                continue
            if leg_kind in ('tp', 'sl'):
                # Bracket row picked up by an entry cancel (pine_entry_id
                # match). TV semantics: cancel(entry_id) is a no-op — the
                # protective exit must survive. Skip without touching the
                # broker or the local state.
                continue

            if not row.exchange_order_id:
                # No exchange id yet — recovery will clear this on the next
                # reconcile; marking ``cancel_pending`` prevents a duplicate
                # DELETE once the id finally lands.
                if store_ctx is not None:
                    store_ctx.set_order_state(
                        row.client_order_id, 'cancel_pending',
                    )
                continue

            kind = extras.get('kind', 'working')

            # Filled position: TV-verified semantics for ``strategy.cancel``
            # after the entry has filled is a no-op — never close the
            # position from a cancel intent. (Same rule as
            # ``Position._remove_order_by_id`` upstream; this is the
            # broker-side guard against a stale/ambiguous cancel intent
            # still reaching the plugin.)
            if kind == 'position':
                if store_ctx is not None:
                    store_ctx.log_event(
                        'cancel_noop',
                        client_order_id=row.client_order_id,
                        exchange_order_id=row.exchange_order_id,
                        intent_key=intent.intent_key,
                        payload={'reason': 'already_filled',
                                 'pine_id': intent.pine_id,
                                 'from_entry': intent.from_entry},
                    )
                continue

            # Working order: real DELETE.
            target_already_gone = False
            try:
                await self._plugin._call(  # type: ignore[attr-defined]
                    f'workingorders/{row.exchange_order_id}', method='delete',
                )
            except (httpx.TimeoutException, httpx.RequestError,
                    ConnectionError, ExchangeConnectionError) as net:
                raise OrderDispositionUnknownError(
                    f"Capital DELETE workingorders/{row.exchange_order_id} "
                    f"ambiguous during cancel sweep: {net}",
                    client_order_id=coid,
                    cause=net if isinstance(net, Exception) else None,
                ) from net
            except OrderNotFoundError:
                target_already_gone = True
                if store_ctx is not None:
                    store_ctx.log_event(
                        'cancel_already_gone',
                        client_order_id=row.client_order_id,
                        exchange_order_id=row.exchange_order_id,
                        intent_key=intent.intent_key,
                    )
            if store_ctx is not None:
                store_ctx.close_order(row.client_order_id)
                store_ctx.log_event(
                    'cancelled',
                    client_order_id=row.client_order_id,
                    exchange_order_id=row.exchange_order_id,
                    intent_key=intent.intent_key,
                )
            applied.append(row.client_order_id)
            if target_already_gone:
                already_gone_count += 1
            else:
                swept_count += 1

        reason_path: CancelReasonPath
        if swept_count > 0:
            reason_path = 'deleted'
        elif already_gone_count > 0:
            reason_path = 'already_gone'
        else:
            # Every matched row was a filled position, bracket-on-entry
            # cancel skip, or cancel_pending deferral — no wire call
            # produced a verdict, so report ``noop`` for the dispatch as
            # a whole. The audit chain still carries the per-row
            # ``cancel_noop`` events.
            reason_path = 'noop'

        return CancelOutcome(
            succeeded=True,
            reason_path=reason_path,
            cleared_legs=len(applied),
            applied_target_coids=applied,
        )

    def exchange_order_from_state(
            self, *, row: 'OrderRow', intent: CancelIntent,
            outcome: CancelOutcome,
    ) -> ExchangeOrder:
        """Synthesise a :class:`ExchangeOrder` for the cancel command row.

        The engine's cancel signature returns ``bool``, so this synthetic
        order is only used by callers that want a uniform object shape
        through the journal — the orchestrator in ``execute_cancel``
        discards it and returns ``True``.
        """
        del outcome
        return ExchangeOrder(
            id=row.client_order_id,
            symbol=intent.symbol,
            side=row.side,
            order_type=OrderType.MARKET,
            qty=row.qty,
            filled_qty=0.0,
            remaining_qty=0.0,
            price=None,
            stop_price=None,
            average_fill_price=None,
            status=OrderStatus.CANCELLED,
            timestamp=row.updated_ts_ms / 1000.0,
            fee=0.0,
            fee_currency='',
            reduce_only=False,
            client_order_id=row.client_order_id,
        )


class _CapitalComCloseHooks:
    """Hook set for the close dispatch.

    Conforms structurally to
    :class:`pynecore.core.broker.journal.CloseDispatchHooks`. Two wire
    shapes:

    * **Full close** — issue ``DELETE /positions/{dealId}`` for every
      target position. The DELETE is synchronous; no confirm GET, no
      ``dealReference``. The hook advances each target row to
      ``closing`` and logs a per-target ``close_dispatched`` event so
      the activity stream can later promote the row to ``closed``.
    * **Partial close** — Capital.com has no native partial-close
      endpoint, so the plugin emulates it by issuing an opposite-side
      ``POST /positions``. The POST is inherently racy against any
      unrelated opposite-side opener landing in the same instant; the
      hook protects with a pre- + post-snapshot reconciliation and a
      corrective ``DELETE`` only when the fresh row's ``createdDateUTC``
      falls within a ±3 s window of our POST. If the race cannot be
      confidently resolved the hook raises
      :class:`BrokerManualInterventionError` — the journal does NOT
      catch that (intentional: operator intervention required), so the
      propagation matches the legacy execute_close semantics.

    Only one of :meth:`submit_full_close` / :meth:`submit_partial_close`
    is invoked per dispatch — the orchestrator decides between them
    based on whether the intent's quantity matches the live position
    total, and passes the matching ``kind`` to
    :meth:`pynecore.core.broker.journal.DispatchJournal.run_close`.
    """

    def __init__(
            self,
            *,
            plugin: '_ExecutionMixin',
            rules: '_InstrumentRules',
    ) -> None:
        self._plugin = plugin
        self._rules = rules

    async def submit_full_close(
            self, *, coid: str, intent: CloseIntent,
            targets: list['OrderRow'],
    ) -> CloseOutcome:
        """DELETE every target position synchronously.

        Capital.com's ``DELETE /positions/{dealId}`` returns no
        ``dealReference`` and no confirm step is required — the broker
        echoes a 200 with no body once the position has been queued for
        closure. The hook mirrors each target row's state to ``closing``
        and emits a per-target ``close_dispatched`` audit event so the
        activity stream's subsequent ``CLOSED`` event can match the
        local row.

        Network errors raise :class:`BrokerManualInterventionError`
        rather than :class:`OrderDispositionUnknownError`: a parked
        close cannot be verified in-session because
        :meth:`get_open_orders` only returns working orders, not
        positions, so the parked intent would never resolve while a
        position may remain open. Halting hands the ambiguity to the
        operator; recovery on next start verifies the DELETE landed by
        checking each target ``dealId`` against the positions snapshot.
        """
        store_ctx = self._plugin.store_ctx
        applied_targets: list[str] = []
        for row in targets:
            try:
                await self._plugin._call(  # type: ignore[attr-defined]
                    f'positions/{row.exchange_order_id}', method='delete',
                )
            except (httpx.TimeoutException, httpx.RequestError,
                    ConnectionError, ExchangeConnectionError) as net:
                raise BrokerManualInterventionError(
                    f"Capital DELETE positions/{row.exchange_order_id} "
                    f"ambiguous during full close: {net}",
                    intent_key=intent.intent_key,
                    context={
                        'coid': coid,
                        'target_deal_id': row.exchange_order_id,
                        'symbol': intent.symbol,
                        'error': str(net),
                    },
                ) from net
            except OrderNotFoundError:
                # Target vanished between local snapshot and the DELETE
                # (broker-side natural close, manual intervention, or
                # another stale dispatch already swept it). The user's
                # close intent is satisfied either way — count the
                # target as swept so the journal's ``mark_closing``
                # path records it. Letting ``OrderNotFoundError`` (a
                # subclass of :class:`ExchangeOrderRejectedError`)
                # escape would mark the entire close dispatch
                # ``rejected`` — terminal, no recovery, stale position
                # row left behind. Mirrors the cancel hook's
                # ``already_gone`` semantics.
                if store_ctx is not None:
                    store_ctx.log_event(
                        'close_already_gone',
                        client_order_id=row.client_order_id,
                        exchange_order_id=row.exchange_order_id,
                        intent_key=intent.intent_key,
                    )
            if store_ctx is not None:
                store_ctx.set_order_state(row.client_order_id, 'closing')
                store_ctx.log_event(
                    'close_dispatched',
                    client_order_id=row.client_order_id,
                    exchange_order_id=row.exchange_order_id,
                    intent_key=intent.intent_key,
                )
            applied_targets.append(row.exchange_order_id or '')

        primary = targets[0]
        return CloseOutcome(
            mode='full',
            applied_targets=applied_targets,
            deal_reference=None,
            exchange_id=primary.exchange_order_id or '',
            filled_qty=intent.qty,
        )

    async def submit_partial_close(
            self, *, coid: str, intent: CloseIntent,
    ) -> CloseOutcome:
        """Emulated partial close via opposite-direction POST.

        Pre-snapshot the positions list to capture the live unit total
        and the set of ``dealId``\\ s that existed *before* the POST;
        these are needed both to detect a race-opened fresh leg and to
        give recovery on next start enough context to verify the dispatch
        even when the corrective-DELETE time window has elapsed.

        On network ambiguity the hook raises
        :class:`BrokerManualInterventionError`: the sync engine cannot
        verify a parked emulated-partial-close in-session
        (``get_open_orders`` does not see netted positions, and a
        second POST could compound the exposure), so the safe contract
        is to halt and let recovery on next start resolve the dispatch
        via the persisted ``deal_reference`` / pre-snapshot context.
        On an unresolvable race (post-snapshot has too many units AND
        no fresh-leg candidate falls within ±3 s of the POST) it also
        raises :class:`BrokerManualInterventionError`. The journal
        does not catch either — operator intervention is required.
        """
        store_ctx = self._plugin.store_ctx
        rules = self._rules

        pre_snap = await self._plugin._call('positions', method='get')  # type: ignore[attr-defined]
        pre_rows = [
            r for r in (pre_snap.get('positions') or [])
            if (r.get('market') or {}).get('epic') == intent.symbol
        ]
        pre_deal_ids = {
            (r.get('position') or {}).get('dealId') for r in pre_rows
        }
        pre_total_units = sum(
            round(float((r.get('position') or {}).get('size', 0.0))
                  / rules.lot_step) if rules.lot_step > 0 else 0
            for r in pre_rows
        )
        intent_units = (
            round(intent.qty / rules.lot_step) if rules.lot_step > 0 else 0
        )

        opposite_dir = 'SELL' if intent.side == 'sell' else 'BUY'
        quantized_qty = self._plugin._quantize_size(intent.qty, rules)  # type: ignore[attr-defined]
        body = {
            'epic': intent.symbol,
            'direction': opposite_dir,
            'size': quantized_qty,
        }

        # Persist the pre-POST snapshot delta context into the command
        # row so recovery on next start can verify the dispatch even
        # though the corrective-DELETE time window will have elapsed.
        if store_ctx is not None:
            existing = store_ctx.get_order(coid)
            merged_extras = dict((existing.extras or {}) if existing else {})
            merged_extras['pre_total_units'] = pre_total_units
            merged_extras['intent_units'] = intent_units
            store_ctx.upsert_order(coid, extras=merged_extras)

        try:
            post_resp = await self._plugin._call(  # type: ignore[attr-defined]
                'positions', data=body, method='post',
            )
        except (httpx.TimeoutException, httpx.RequestError,
                ConnectionError, ExchangeConnectionError) as net:
            raise BrokerManualInterventionError(
                f"Capital POST positions ambiguous during partial close: "
                f"{net}",
                intent_key=intent.intent_key,
                context={
                    'coid': coid,
                    'symbol': intent.symbol,
                    'qty': intent.qty,
                    'error': str(net),
                },
            ) from net

        deal_ref: str | None = post_resp.get('dealReference')
        # Mid-flow add_ref so recovery has the durable reference even if
        # this method raises between POST and journal return. The journal
        # will idempotently re-record the ref via ``record_close_server_ref``
        # once :meth:`submit_partial_close` returns successfully.
        if deal_ref and store_ctx is not None:
            store_ctx.add_ref(coid, 'deal_reference', deal_ref)

        if deal_ref:
            try:
                await self._plugin._call(  # type: ignore[attr-defined]
                    f'confirms/{deal_ref}', method='get',
                )
            except (httpx.TimeoutException, httpx.RequestError,
                    ConnectionError, ExchangeConnectionError) as net:
                raise BrokerManualInterventionError(
                    f"Capital GET confirms/{deal_ref} ambiguous after "
                    f"partial close POST: {net}",
                    intent_key=intent.intent_key,
                    context={
                        'coid': coid,
                        'deal_reference': deal_ref,
                        'symbol': intent.symbol,
                        'qty': intent.qty,
                        'error': str(net),
                    },
                ) from net
            except OrderNotFoundError:
                # Confirms TTL/lookup race: the POST landed and a
                # ``dealReference`` was returned, but the ``confirms``
                # endpoint cannot find it anymore (Capital.com's TTL
                # expired between POST and our GET). The post-snapshot
                # reconcile below still reconstructs the verdict via
                # the unit delta. Letting ``OrderNotFoundError`` (a
                # subclass of :class:`ExchangeOrderRejectedError`)
                # escape would mark this row ``rejected`` — terminal,
                # no recovery — even though the close almost certainly
                # netted. Fall through to the post-snapshot path.
                pass

        post_snap = await self._plugin._call('positions', method='get')  # type: ignore[attr-defined]
        post_rows = [
            r for r in (post_snap.get('positions') or [])
            if (r.get('market') or {}).get('epic') == intent.symbol
        ]
        post_total_units = sum(
            round(float((r.get('position') or {}).get('size', 0.0))
                  / rules.lot_step) if rules.lot_step > 0 else 0
            for r in post_rows
        )
        expected_post_units = pre_total_units - intent_units

        if post_total_units > expected_post_units:
            # Race window: our opposite POST opened a fresh row instead
            # of netting. Identify the freshly-opened leg by direction
            # + a ±3 s ``createdDateUTC`` band around now and issue a
            # corrective DELETE. If no candidate falls inside the band,
            # the race cannot be confidently resolved — halt.
            fresh_opposite: dict | None = None
            now_ts = epoch_time()
            for r in post_rows:
                pos_data = r.get('position') or {}
                if pos_data.get('dealId') in pre_deal_ids:
                    continue
                if (pos_data.get('direction') or '').upper() != opposite_dir:
                    continue
                created_at = _parse_iso_timestamp(
                    pos_data.get('createdDateUTC') or '',
                )
                if created_at and abs(created_at - now_ts) <= 3.0:
                    fresh_opposite = pos_data
                    break
            if fresh_opposite is not None:
                fresh_id = fresh_opposite.get('dealId')
                await self._plugin._call(  # type: ignore[attr-defined]
                    f'positions/{fresh_id}', method='delete',
                )
                if store_ctx is not None:
                    store_ctx.log_event(
                        'partial_close_corrective_delete',
                        client_order_id=coid,
                        exchange_order_id=fresh_id,
                        intent_key=intent.intent_key,
                        payload={'reason': 'race_reverse_leg_corrected'},
                    )
            else:
                raise BrokerManualInterventionError(
                    f"Partial close race detected but reverse leg cannot "
                    f"be confidently identified (expected "
                    f"{expected_post_units} units, have "
                    f"{post_total_units})",
                    intent_key=intent.intent_key,
                    context={
                        'coid': coid,
                        'pre_total_units': pre_total_units,
                        'post_total_units': post_total_units,
                        'intent_units': intent_units,
                        'symbol': intent.symbol,
                    },
                )

        return CloseOutcome(
            mode='partial',
            applied_targets=[deal_ref] if deal_ref else [],
            deal_reference=deal_ref,
            exchange_id=deal_ref or '',
            filled_qty=quantized_qty,
            raw=post_resp,
        )

    def exchange_order_from_state(
            self, *, row: 'OrderRow', intent: CloseIntent,
            outcome: CloseOutcome,
    ) -> ExchangeOrder:
        """Build the engine-facing :class:`ExchangeOrder` for the close.

        Full-close: the engine sees the first target's ``dealId`` as
        the synthetic order id. Partial-close: the ``dealReference``
        is used. Both branches report the close as fully filled — full
        close synchronously, partial close emulated via the opposite
        POST.
        """
        return ExchangeOrder(
            id=outcome.exchange_id or '',
            symbol=intent.symbol,
            side=intent.side,
            order_type=OrderType.MARKET,
            qty=outcome.filled_qty,
            filled_qty=outcome.filled_qty,
            remaining_qty=0.0,
            price=None,
            stop_price=None,
            average_fill_price=outcome.fill_price,
            status=OrderStatus.FILLED,
            timestamp=epoch_time(),
            fee=0.0,
            fee_currency='',
            reduce_only=True,
            client_order_id=row.client_order_id,
        )


class _CapitalComModifyExitHooks:
    """Hook set for the position bracket amend dispatch.

    Conforms structurally to
    :class:`pynecore.core.broker.journal.ModifyExitDispatchHooks`. The
    journal owns the entry-side command row's state machine
    (``submitted`` → ``server_ref_seen`` → ``confirmed`` /
    ``disposition_unknown`` / ``rejected``); this hook owns:

    * The PUT ``/positions/{dealId}`` body and the confirm GET wire
      format.
    * The disposition-unknown seeding for newly-added bracket leg rows
      on the ambiguous PUT/confirm path (the journal does not write leg
      rows — it does not know about them).
    * The post-success mirror that materialises the synthetic TP / SL
      leg rows under the parent dealId.

    The orchestrator (``execution.modify_exit``) performs the pre-flight
    (target row lookup, distance validation, PUT body construction,
    derivation of ``effective_*_coid`` / ``ambiguous_*_coid`` etc.) and
    passes the resulting derivations into this hook's constructor. That
    keeps the journal's contract pure (one ``submit_amend`` call, one
    ``mirror_bracket_legs`` call) while the plugin retains responsibility
    for every Capital.com-specific decision.

    When the PUT body is empty (a pure pending-trail seed amend), the
    orchestrator bypasses the journal entirely and calls
    :meth:`_apply_mirror` directly — there is no broker round-trip to
    journal in that case.
    """

    def __init__(
            self,
            *,
            plugin: '_ExecutionMixin',
            target_row: 'OrderRow',
            body: dict,
            existing_tp: 'OrderRow | None',
            existing_sl: 'OrderRow | None',
            tp_coid: str,
            sl_coid: str,
            effective_tp_coid: str,
            effective_sl_coid: str,
            trail_pending: bool,
            ambiguous_tp_coid: str | None,
            ambiguous_sl_coid: str | None,
            sl_in_body: bool,
            sl_clears_broker_active: bool,
            clears_broker_tp: bool,
    ) -> None:
        self._plugin = plugin
        self._target_row = target_row
        self._body = body
        self._existing_tp = existing_tp
        self._existing_sl = existing_sl
        self._tp_coid = tp_coid
        self._sl_coid = sl_coid
        self._effective_tp_coid = effective_tp_coid
        self._effective_sl_coid = effective_sl_coid
        self._trail_pending = trail_pending
        self._ambiguous_tp_coid = ambiguous_tp_coid
        self._ambiguous_sl_coid = ambiguous_sl_coid
        self._sl_in_body = sl_in_body
        self._sl_clears_broker_active = sl_clears_broker_active
        self._clears_broker_tp = clears_broker_tp

    async def submit_amend(
            self, *, coid: str, target_coid: str,
            old_intent: ExitIntent, new_intent: ExitIntent,
    ) -> ModifyExitOutcome:
        """PUT ``/positions/{dealId}`` with the new bracket and confirm.

        ``PUT /positions/{dealId}`` atomically rewrites ``profitLevel`` /
        ``stopLevel`` / ``trailingStop`` without a cancel window. Three
        outcome categories:

        * **Happy path:** PUT 200 → confirm GET 200 with
          ``dealStatus == 'ACCEPTED'``. Returns
          :class:`ModifyExitOutcome` carrying the broker-echoed levels in
          ``post_put_state``.
        * **Synchronous reject:** confirm GET returns
          ``dealStatus == 'REJECTED'``. The hook logs the rejection event
          and raises :class:`ExchangeOrderRejectedError`.
        * **Ambiguous (PUT timeout / confirm timeout / mapped broker
          error on confirm):** the broker may already have applied the
          amend while we cannot read back. The hook seeds any newly-added
          leg rows in ``disposition_unknown``, stamps
          ``force_disposition_rejected`` on the existing SL row for the
          pending-trail transition, refreshes existing leg rows to the
          attempted target levels, and finally raises
          :class:`OrderDispositionUnknownError`. The next reconcile
          snapshot promotes the legs against the broker's authoritative
          state.

        ``target_coid`` is identical to the constructor's
        ``target_row.client_order_id``; the argument is part of the
        Protocol contract.
        """
        del target_coid

        deal_id = self._target_row.exchange_order_id
        if deal_id is None:
            raise RuntimeError(
                f"_CapitalComModifyExitHooks.submit_amend: target row has "
                f"no exchange_order_id (coid={coid!r})"
            )

        try:
            resp = await self._plugin._call(  # type: ignore[attr-defined]
                f'positions/{deal_id}', data=self._body, method='put',
            )
        except (httpx.TimeoutException, httpx.RequestError,
                ConnectionError, ExchangeConnectionError) as net:
            # PUT outcome ambiguous: the broker may already have applied
            # the amend while local state still reflects the pre-amend
            # bracket. Seed/refresh the leg rows so the next reconcile
            # snapshot resolves them against authoritative broker state.
            self._seed_added_legs_disposition_unknown(new_intent)
            self._mark_existing_sl_force_rejected_on_pending_trail()
            self._persist_existing_legs_attempted_target(new_intent)
            self._plugin._mark_bracket_legs_disposition_unknown(  # type: ignore[attr-defined]
                intent=new_intent,
                parent_coid=self._target_row.client_order_id,
                deal_id=deal_id,
                tp_coid=self._ambiguous_tp_coid,
                sl_coid=self._ambiguous_sl_coid,
                reason=str(net), stage='put',
            )
            raise OrderDispositionUnknownError(
                f"Capital modify_exit PUT positions/{deal_id} "
                f"ambiguous: {net}",
                client_order_id=self._target_row.client_order_id,
                cause=net if isinstance(net, Exception) else None,
            ) from net

        new_ref = resp.get('dealReference')
        post_put_state: dict = {}
        if new_ref:
            try:
                modify_confirm = await self._plugin._call(  # type: ignore[attr-defined]
                    f'confirms/{new_ref}', method='get',
                )
            except (httpx.TimeoutException, httpx.RequestError,
                    ConnectionError, ExchangeConnectionError,
                    CapitalComError, BrokerError) as net:
                # PUT landed (got a dealReference) but the confirm read
                # failed. Same ambiguity as PUT-timeout: the broker may
                # already have applied the amend. Mapped broker errors
                # (404 OrderNotFoundError, 429 ExchangeRateLimitError,
                # etc.) on the confirm GET are treated identically to
                # network failures here.
                # Persist the server-allocated deal_reference on the
                # modify command row before raising so restart recovery
                # (_verdict_modify_exit) can resolve via confirms/{ref}
                # without falling back to snapshot matching.
                if self._plugin.store_ctx is not None:
                    self._plugin.store_ctx.add_ref(
                        coid, 'deal_reference', new_ref,
                    )
                self._seed_added_legs_disposition_unknown(new_intent)
                self._mark_existing_sl_force_rejected_on_pending_trail()
                self._persist_existing_legs_attempted_target(new_intent)
                self._plugin._mark_bracket_legs_disposition_unknown(  # type: ignore[attr-defined]
                    intent=new_intent,
                    parent_coid=self._target_row.client_order_id,
                    deal_id=deal_id,
                    tp_coid=self._ambiguous_tp_coid,
                    sl_coid=self._ambiguous_sl_coid,
                    reason=str(net), stage='confirm',
                    deal_reference=new_ref,
                )
                raise OrderDispositionUnknownError(
                    f"Capital modify_exit confirm {new_ref} "
                    f"ambiguous: {net}",
                    client_order_id=self._target_row.client_order_id,
                    cause=net if isinstance(net, Exception) else None,
                ) from net

            deal_status = (modify_confirm.get('dealStatus') or '').upper()
            if deal_status == 'REJECTED':
                reason = _extract_reject_reason(modify_confirm)
                if self._plugin.store_ctx is not None:
                    self._plugin.store_ctx.log_event(
                        'modify_exit_rejected',
                        client_order_id=self._target_row.client_order_id,
                        exchange_order_id=self._target_row.exchange_order_id,
                        intent_key=new_intent.intent_key,
                        payload={'reason': reason,
                                 'tp': new_intent.tp_price,
                                 'sl': new_intent.sl_price,
                                 'trail_offset': new_intent.trail_offset},
                    )
                raise ExchangeOrderRejectedError(
                    f"Capital position amend REJECTED: {reason}",
                )

            post_put_state = {
                'profit_level': modify_confirm.get('profitLevel'),
                'stop_level': modify_confirm.get('stopLevel'),
                'trailing_stop': modify_confirm.get('trailingStop'),
                'stop_distance': modify_confirm.get('stopDistance'),
            }
            raw_confirm = modify_confirm
        else:
            raw_confirm = None

        return ModifyExitOutcome(
            server_ref=str(new_ref) if new_ref else '',
            deal_status='ACCEPTED',
            rejected_reason=None,
            post_put_state=post_put_state,
            raw=raw_confirm,
        )

    def mirror_bracket_legs(
            self, *, target_row: 'OrderRow', new_intent: ExitIntent,
            outcome: ModifyExitOutcome,
    ) -> None:
        """Mirror the new TP / SL onto the parent entry row + leg rows.

        Invoked by the journal only on the happy path (after the
        command row has been ``confirmed`` + ``close_order``). Writes:

        * The parent entry row's risk snapshot
          (``set_risk`` + risk-clear ``upsert_order``).
        * Each TP / SL leg row: INSERT if absent, UPDATE if present,
          retire (close) if removed. Pending-trail extras are written /
          dropped symmetrically with the new intent's trail shape.
        * A ``modify_exit`` audit event tying the wire result back to
          the leg COIDs the engine sees.
        """
        del outcome
        self._apply_mirror(target_row=target_row, new_intent=new_intent)

    def _apply_mirror(
            self, *, target_row: 'OrderRow', new_intent: ExitIntent,
    ) -> None:
        """Body of :meth:`mirror_bracket_legs` minus the outcome arg.

        Called directly by the orchestrator on the body-less path
        (pending-trail seed amend with no broker round-trip), and by
        :meth:`mirror_bracket_legs` on the journal-happy-path.
        """
        if self._plugin.store_ctx is None:
            return

        new_i = new_intent
        old_target_row = self._target_row
        existing_tp = self._existing_tp
        existing_sl = self._existing_sl
        effective_tp_coid = self._effective_tp_coid
        effective_sl_coid = self._effective_sl_coid
        trail_pending = self._trail_pending
        deal_id = old_target_row.exchange_order_id or ''
        store_ctx = self._plugin.store_ctx
        # We need ``old_i`` for the retire branches; the constructor
        # captured the existing rows but not the prior intent. The
        # journal hands ``new_intent`` only; ``old_intent`` was passed to
        # ``submit_amend`` but not stored. Reconstruct the "had a TP
        # before" / "had an SL before" predicates from ``existing_*``:
        # those rows are the authoritative record of what the broker
        # currently has live, which is exactly what the retire branches
        # need.
        had_tp_before = (
            existing_tp is not None and existing_tp.closed_ts_ms is None
        )
        had_sl_before = (
            existing_sl is not None and existing_sl.closed_ts_ms is None
        )

        # ``trailing_stop`` MUST be passed even when False — ``set_risk``
        # treats ``None`` as "no change", so a prior ``True`` survives
        # an amend back to a fixed SL and ``_activity_to_event`` would
        # misclassify the fill on the next stop hit.
        store_ctx.set_risk(
            old_target_row.client_order_id,
            sl=new_i.sl_price, tp=new_i.tp_price,
            trailing_distance=new_i.trail_offset,
            trailing_stop=(
                new_i.trail_offset is not None and not trail_pending
            ),
        )
        # Removal-amend clears: ``set_risk`` filters ``None`` as "leave
        # unchanged", so an amend that REMOVES a protective leg leaves
        # the parent entry row's stale risk fields in BrokerStore.
        risk_clears: dict[str, float | None] = {}
        if had_tp_before and new_i.tp_price is None:
            risk_clears['tp_level'] = None
        if (existing_sl is not None
                and existing_sl.closed_ts_ms is None
                and existing_sl.sl_level is not None
                and new_i.sl_price is None):
            risk_clears['sl_level'] = None
        if (existing_sl is not None
                and existing_sl.closed_ts_ms is None
                and existing_sl.trailing_distance is not None
                and new_i.trail_offset is None):
            risk_clears['trailing_distance'] = None
        if risk_clears:
            store_ctx.upsert_order(
                old_target_row.client_order_id, **risk_clears,
            )

        # === TP leg mirror =================================================
        if new_i.tp_price is None and had_tp_before:
            # TP removal: retire the previously confirmed TP leg row.
            tp_existing = store_ctx.get_order(effective_tp_coid)
            if (tp_existing is not None
                    and tp_existing.closed_ts_ms is None):
                store_ctx.set_order_state(
                    effective_tp_coid, 'rejected',
                )
                store_ctx.close_order(effective_tp_coid)
                store_ctx.log_event(
                    'bracket_leg_retired_on_modify',
                    client_order_id=effective_tp_coid,
                    exchange_order_id=deal_id,
                    intent_key=new_i.intent_key,
                    payload={
                        'leg_kind': 'tp',
                        'reason': 'tp_removed',
                        'old_tp_price': (existing_tp.tp_level
                                         if existing_tp is not None else None),
                    },
                )
        if new_i.tp_price is not None:
            tp_existing = store_ctx.get_order(effective_tp_coid)
            if tp_existing is None or tp_existing.closed_ts_ms is not None:
                if tp_existing is not None:
                    store_ctx.reopen_order(effective_tp_coid)
                store_ctx.upsert_order(
                    effective_tp_coid,
                    symbol=new_i.symbol,
                    side=new_i.side,
                    qty=old_target_row.qty,
                    state='confirmed',
                    intent_key=new_i.intent_key + '\0TP',
                    from_entry=new_i.from_entry,
                    pine_entry_id=new_i.from_entry,
                    tp_level=float(new_i.tp_price),
                    extras={
                        'leg_kind': 'tp',
                        'parent_deal_id': deal_id,
                        'parent_coid': old_target_row.client_order_id,
                    },
                )
                store_ctx.add_ref(
                    effective_tp_coid, 'bracket_deal_id', deal_id,
                )
            else:
                # Live row exists — UPDATE under its own COID. ``state``
                # is forced back to ``confirmed`` because an earlier
                # ambiguous PUT/confirm could have flipped the row to
                # ``disposition_unknown``; this amend just succeeded, so
                # the leg is unambiguously attached.
                store_ctx.upsert_order(
                    effective_tp_coid,
                    state='confirmed',
                    tp_level=float(new_i.tp_price),
                )

        # === SL leg mirror =================================================
        if new_i.sl_price is not None or new_i.trail_offset is not None:
            sl_existing = store_ctx.get_order(effective_sl_coid)
            if sl_existing is None or sl_existing.closed_ts_ms is not None:
                if sl_existing is not None:
                    store_ctx.reopen_order(effective_sl_coid)
                sl_extras: dict = {
                    'leg_kind': 'sl',
                    'parent_deal_id': deal_id,
                    'parent_coid': old_target_row.client_order_id,
                }
                # Pending-trail leg: stash the activation threshold and
                # offset under the same keys
                # :meth:`_trailing_activation_monitor` reads. Without
                # ``trail_state='pending'`` the monitor would skip this
                # row and the protective trailing stop would never
                # attach.
                if trail_pending:
                    sl_extras['trail_activation_price'] = new_i.trail_price
                    sl_extras['trail_offset'] = new_i.trail_offset
                    sl_extras['trail_state'] = 'pending'
                store_ctx.upsert_order(
                    effective_sl_coid,
                    symbol=new_i.symbol,
                    side=new_i.side,
                    qty=old_target_row.qty,
                    state='confirmed',
                    intent_key=new_i.intent_key + '\0SL',
                    from_entry=new_i.from_entry,
                    pine_entry_id=new_i.from_entry,
                    sl_level=(float(new_i.sl_price)
                              if new_i.sl_price is not None else None),
                    trailing_distance=(float(new_i.trail_offset)
                                       if new_i.trail_offset is not None
                                       else None),
                    trailing_stop=(
                        new_i.trail_offset is not None and not trail_pending
                    ),
                    extras=sl_extras,
                )
                store_ctx.add_ref(
                    effective_sl_coid, 'bracket_deal_id', deal_id,
                )
            else:
                # ``trailing_stop`` MUST be re-stamped — amending a fixed
                # SL to native trailing (or vice versa) changes the leg's
                # classification, and :meth:`_activity_to_event` reads
                # ``row.trailing_stop`` off this stored row to decide
                # between ``LegType.STOP_LOSS`` and
                # ``LegType.TRAILING_STOP`` on the eventual fill.
                #
                # The pending-trail trio (``trail_state`` /
                # ``trail_activation_price`` / ``trail_offset`` extras)
                # must be kept in sync with ``trail_pending``.
                new_sl_extras = dict(sl_existing.extras or {})
                if trail_pending:
                    new_sl_extras['trail_activation_price'] = new_i.trail_price
                    new_sl_extras['trail_offset'] = new_i.trail_offset
                    new_sl_extras['trail_state'] = 'pending'
                else:
                    # Leaving pending trailing: drop ALL three trail
                    # extras — ``trail_offset`` MUST go too, otherwise
                    # :meth:`_resolve_bracket_leg_disposition` keeps
                    # treating this row as a trailing leg and the next
                    # ambiguous amend recovery would resolve a fixed
                    # stop against the parent's ``trailingStop`` /
                    # ``stopDistance`` instead of ``stopLevel``.
                    new_sl_extras.pop('trail_state', None)
                    new_sl_extras.pop('trail_activation_price', None)
                    new_sl_extras.pop('trail_offset', None)
                store_ctx.upsert_order(
                    effective_sl_coid,
                    state='confirmed',
                    sl_level=(float(new_i.sl_price)
                              if new_i.sl_price is not None else None),
                    trailing_distance=(float(new_i.trail_offset)
                                       if new_i.trail_offset is not None
                                       else None),
                    trailing_stop=(
                        new_i.trail_offset is not None and not trail_pending
                    ),
                    extras=new_sl_extras,
                )
        elif had_sl_before:
            # The amend removed the protective stop entirely (no fixed
            # SL, no trail in the new intent). All three prior shapes
            # need the SL leg row retired here — see the inline notes
            # in the original ``modify_exit`` for the rationale of each
            # reason code.
            if existing_sl is not None and existing_sl.sl_level is not None:
                reason = 'fixed_sl_removed_no_replacement'
            elif (existing_sl is not None
                    and existing_sl.extras is not None
                    and 'trail_activation_price' in (existing_sl.extras or {})):
                reason = 'pending_trail_removed_no_replacement'
            else:
                reason = 'native_trail_removed_no_replacement'
            sl_existing = store_ctx.get_order(effective_sl_coid)
            if (sl_existing is not None
                    and sl_existing.closed_ts_ms is None):
                store_ctx.set_order_state(
                    effective_sl_coid, 'rejected',
                )
                store_ctx.close_order(effective_sl_coid)
                store_ctx.log_event(
                    'bracket_leg_retired_on_modify',
                    client_order_id=effective_sl_coid,
                    exchange_order_id=deal_id,
                    intent_key=new_i.intent_key,
                    payload={
                        'leg_kind': 'sl',
                        'reason': reason,
                        'old_sl_price': (
                            existing_sl.sl_level
                            if existing_sl is not None else None
                        ),
                        'old_trail_offset': (
                            existing_sl.trailing_distance
                            if existing_sl is not None else None
                        ),
                        'old_trail_price': (
                            (existing_sl.extras or {}).get(
                                'trail_activation_price'
                            )
                            if existing_sl is not None else None
                        ),
                    },
                )

        store_ctx.log_event(
            'modify_exit',
            client_order_id=old_target_row.client_order_id,
            exchange_order_id=old_target_row.exchange_order_id,
            intent_key=new_i.intent_key,
            payload={'tp': new_i.tp_price, 'sl': new_i.sl_price,
                     'trail_offset': new_i.trail_offset,
                     'tp_coid': effective_tp_coid,
                     'sl_coid': effective_sl_coid,
                     'envelope_tp_coid': self._tp_coid,
                     'envelope_sl_coid': self._sl_coid},
        )

    def exchange_order_from_state(
            self, *, row: 'OrderRow', new_intent: ExitIntent,
            outcome: ModifyExitOutcome,
    ) -> list[ExchangeOrder]:
        """Build the synthetic TP / SL leg list the engine expects."""
        del row, outcome
        new_i = new_intent
        target_row = self._target_row
        deal_id = target_row.exchange_order_id or ''
        legs: list[ExchangeOrder] = []
        now_ts = epoch_time()
        if new_i.tp_price is not None:
            legs.append(ExchangeOrder(
                id=_bracket_leg_id(deal_id, 'tp'),
                symbol=new_i.symbol, side=new_i.side,
                order_type=OrderType.LIMIT,
                qty=target_row.qty, filled_qty=0.0,
                remaining_qty=target_row.qty,
                price=new_i.tp_price, stop_price=None,
                average_fill_price=None, status=OrderStatus.OPEN,
                timestamp=now_ts, fee=0.0, fee_currency='',
                reduce_only=True, client_order_id=self._effective_tp_coid,
            ))
        if new_i.sl_price is not None or new_i.trail_offset is not None:
            legs.append(ExchangeOrder(
                id=_bracket_leg_id(deal_id, 'sl'),
                symbol=new_i.symbol, side=new_i.side,
                order_type=(
                    OrderType.TRAILING_STOP
                    if new_i.trail_offset is not None else OrderType.STOP
                ),
                qty=target_row.qty, filled_qty=0.0,
                remaining_qty=target_row.qty,
                price=None, stop_price=new_i.sl_price,
                average_fill_price=None, status=OrderStatus.OPEN,
                timestamp=now_ts, fee=0.0, fee_currency='',
                reduce_only=True, client_order_id=self._effective_sl_coid,
            ))
        return legs

    # === Disposition-unknown seeding helpers ==============================
    #
    # These three helpers run on the ambiguous PUT/confirm path inside
    # :meth:`submit_amend` *before* the journal raises. They mutate the
    # leg rows and the existing SL row so the next reconcile snapshot
    # resolves them correctly against the broker's authoritative state.
    # The journal does not orchestrate these — bracket leg rows live
    # outside the entry-row scope for M4.

    def _seed_added_legs_disposition_unknown(
            self, new_intent: ExitIntent,
    ) -> None:
        """Seed leg rows for newly-added (not pre-existing) legs.

        ``modify_exit`` mirrors leg state AFTER the PUT, unlike
        ``execute_exit`` which persist-firsts. When the amend ADDs a leg
        that had no prior row, the ambiguous-PUT/confirm path has
        nothing to flip — the next snapshot reconcile would have no row
        to compare against the parent ``profitLevel`` / ``stopLevel``
        and the broker-attached new leg would stay invisible to
        BrokerStore. Pre-seed those rows here in ``disposition_unknown``
        so the reconcile snapshot owns promotion or rejection.

        Closed-row case: a previous attach for the same envelope could
        have closed the row. Treat closed rows as absent: reopen first,
        then upsert. Mirrors the post-PUT mirror's closed-row guard.
        """
        store_ctx = self._plugin.store_ctx
        if store_ctx is None:
            return
        new_i = new_intent
        target_row = self._target_row
        existing_tp = self._existing_tp
        existing_sl = self._existing_sl
        tp_coid = self._tp_coid
        sl_coid = self._sl_coid
        deal_id = target_row.exchange_order_id or ''
        sl_in_body = self._sl_in_body
        trail_pending = self._trail_pending

        if new_i.tp_price is not None and existing_tp is None:
            tp_existing = store_ctx.get_order(tp_coid)
            if (tp_existing is not None
                    and tp_existing.closed_ts_ms is not None):
                store_ctx.reopen_order(tp_coid)
            store_ctx.upsert_order(
                tp_coid,
                symbol=new_i.symbol,
                side=new_i.side,
                qty=target_row.qty,
                state='disposition_unknown',
                intent_key=new_i.intent_key + '\0TP',
                from_entry=new_i.from_entry,
                pine_entry_id=new_i.from_entry,
                tp_level=float(new_i.tp_price),
                extras={
                    'leg_kind': 'tp',
                    'parent_deal_id': deal_id,
                    'parent_coid': target_row.client_order_id,
                },
            )
            store_ctx.add_ref(tp_coid, 'bracket_deal_id', deal_id)
        if sl_in_body and existing_sl is None:
            sl_existing = store_ctx.get_order(sl_coid)
            if (sl_existing is not None
                    and sl_existing.closed_ts_ms is not None):
                store_ctx.reopen_order(sl_coid)
            store_ctx.upsert_order(
                sl_coid,
                symbol=new_i.symbol,
                side=new_i.side,
                qty=target_row.qty,
                state='disposition_unknown',
                intent_key=new_i.intent_key + '\0SL',
                from_entry=new_i.from_entry,
                pine_entry_id=new_i.from_entry,
                sl_level=(float(new_i.sl_price)
                          if new_i.sl_price is not None else None),
                trailing_distance=(float(new_i.trail_offset)
                                   if new_i.trail_offset is not None else None),
                trailing_stop=(
                    new_i.trail_offset is not None and not trail_pending
                ),
                extras={
                    'leg_kind': 'sl',
                    'parent_deal_id': deal_id,
                    'parent_coid': target_row.client_order_id,
                },
            )
            store_ctx.add_ref(sl_coid, 'bracket_deal_id', deal_id)
        elif (
            trail_pending
            and existing_sl is None
            and new_i.trail_offset is not None
            and new_i.trail_price is not None
        ):
            # Pending-trail SL is purely local — the activation monitor
            # PUTs it later when the threshold is crossed — so it never
            # appears in the PUT body and ``sl_in_body`` is False. On
            # an ambiguous PUT/confirm path the exception unwinds before
            # the post-PUT mirror inserts this row; without persisting
            # it here :meth:`_trailing_activation_monitor` has nothing
            # to activate.
            #
            # Persist with ``state='confirmed'`` (NOT
            # ``disposition_unknown``): the broker has no leg to resolve
            # here — the pending-trail row is a local bookkeeping anchor
            # — and :meth:`_resolve_bracket_leg_disposition` would see
            # ``trailingStop=False`` on the parent and wrongly mark this
            # row rejected.
            pending_extras: dict = {
                'leg_kind': 'sl',
                'parent_deal_id': deal_id,
                'parent_coid': target_row.client_order_id,
                'trail_activation_price': new_i.trail_price,
                'trail_offset': new_i.trail_offset,
                'trail_state': 'pending',
            }
            pending_existing = store_ctx.get_order(sl_coid)
            if (pending_existing is not None
                    and pending_existing.closed_ts_ms is not None):
                store_ctx.reopen_order(sl_coid)
            store_ctx.upsert_order(
                sl_coid,
                symbol=new_i.symbol,
                side=new_i.side,
                qty=target_row.qty,
                state='confirmed',
                intent_key=new_i.intent_key + '\0SL',
                from_entry=new_i.from_entry,
                pine_entry_id=new_i.from_entry,
                sl_level=None,
                trailing_distance=float(new_i.trail_offset),
                trailing_stop=False,
                extras=pending_extras,
            )
            store_ctx.add_ref(sl_coid, 'bracket_deal_id', deal_id)

    def _mark_existing_sl_force_rejected_on_pending_trail(self) -> None:
        """Stamp ``force_disposition_rejected`` on the existing SL row.

        When the amend transitions an existing broker-active SL (fixed
        or immediate native trail) into a pending-trail, the PUT body
        actively clears the broker-side stop. Marking the existing SL
        row as ``disposition_unknown`` alone is NOT enough — the next
        :meth:`_resolve_bracket_leg_disposition` compares the row's OLD
        ``sl_level`` / ``trailing_distance`` against the parent's actual
        ``stopLevel`` / ``trailingStop``, which only tells us whether
        the broker applied the clear. It does NOT tell us whether the
        new pending-trail intent was satisfied (the pending-trail leg
        is purely local — the activation monitor PUTs it later).

        Setting ``force_disposition_rejected`` makes the resolver record
        ``'rejected'`` for this transition regardless of broker
        comparison. The engine then restores ``_active_intents`` from
        the pre-modify snapshot and re-dispatches.
        """
        store_ctx = self._plugin.store_ctx
        if (store_ctx is None
                or not self._sl_clears_broker_active
                or self._existing_sl is None):
            return
        existing_extras = dict(self._existing_sl.extras or {})
        existing_extras['force_disposition_rejected'] = True
        store_ctx.upsert_order(
            self._existing_sl.client_order_id,
            extras=existing_extras,
        )

    def _persist_existing_legs_attempted_target(
            self, new_intent: ExitIntent,
    ) -> None:
        """Refresh existing leg rows to the attempted new target.

        :meth:`_mark_bracket_legs_disposition_unknown` only flips
        ``state='disposition_unknown'``; it does NOT update
        ``tp_level`` / ``sl_level`` / ``trailing_distance`` /
        ``trailing_stop``. The next
        :meth:`_resolve_bracket_leg_disposition` then compares the row's
        stale OLD level against the broker's actual levels, so a PUT
        that never reached Capital.com (broker still has the old level)
        looks like ``attached`` because old == old.

        Fix: write the new intent's target onto the existing leg row
        BEFORE flipping it to ``disposition_unknown``. The resolver then
        compares the attempted target against broker — broker applied
        → ``'attached'``; broker NOT applied → mismatch → ``'rejected'``
        → engine retries.
        """
        store_ctx = self._plugin.store_ctx
        if store_ctx is None:
            return
        new_i = new_intent
        existing_tp = self._existing_tp
        existing_sl = self._existing_sl
        trail_pending = self._trail_pending

        if existing_tp is not None:
            store_ctx.upsert_order(
                existing_tp.client_order_id,
                tp_level=(float(new_i.tp_price)
                          if new_i.tp_price is not None else None),
            )
        if existing_sl is not None:
            # Read the row DB-fresh: an earlier helper
            # (:meth:`_mark_existing_sl_force_rejected_on_pending_trail`)
            # may have just stamped ``force_disposition_rejected`` into
            # extras; the in-memory ``existing_sl`` snapshot was captured
            # before that write and would clobber the marker if used as
            # the merge base.
            cur = store_ctx.get_order(existing_sl.client_order_id)
            cur_extras = dict((cur.extras if cur is not None else None) or {})
            new_is_pending_trail = (
                new_i.trail_offset is not None
                and new_i.trail_price is not None
            )
            if not new_is_pending_trail:
                for key in (
                        'trail_state',
                        'trail_activation_price',
                        'trail_offset',
                ):
                    cur_extras.pop(key, None)
            store_ctx.upsert_order(
                existing_sl.client_order_id,
                sl_level=(float(new_i.sl_price)
                          if new_i.sl_price is not None else None),
                trailing_distance=(float(new_i.trail_offset)
                                   if new_i.trail_offset is not None else None),
                trailing_stop=(
                    new_i.trail_offset is not None and not trail_pending
                ),
                extras=cur_extras,
            )
