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
)
from pynecore.core.broker.models import (
    CancelIntent,
    CloseIntent,
    EntryIntent,
    ExchangeOrder,
    OrderStatus,
    OrderType,
)

from .exceptions import OrderNotFoundError
from .helpers import _parse_iso_timestamp

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
