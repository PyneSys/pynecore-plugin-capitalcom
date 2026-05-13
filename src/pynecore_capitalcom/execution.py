"""Execution surface: every ``execute_*`` and ``modify_*`` path.

This is the largest mix-in by line count — the bulk lives in
``modify_exit`` (~1000 lines) which encodes the full Capital.com
bracket-amend matrix (TP-only / SL-only / TP+SL / native trailing /
pending-trail / removal cases × ambiguous-recovery branches). A future
internal decomposition can split that one method without touching this
file's boundaries.

State touched: ``_instrument_rules_cache`` (read), ``_account_id``
(read), the BrokerStore through ``self.store_ctx`` (write — every
state transition is PERSIST-FIRST so a process crash mid-dispatch
leaves an auditable row that ``_recover_in_flight_submissions`` can
replay on restart).
"""
import asyncio
from time import time as epoch_time
from typing import TYPE_CHECKING

import httpx

from pynecore.core.broker.exceptions import (
    AuthenticationError,
    BracketAttachAfterFillRejectedError,
    BrokerError,
    BrokerManualInterventionError,
    ExchangeCapabilityError,
    ExchangeConnectionError,
    ExchangeOrderRejectedError,
    ExchangeRateLimitError,
    InsufficientMarginError,
    OrderDispositionUnknownError,
    OrderSkippedByPlugin,
)
from pynecore.core.broker.idempotency import (
    KIND_CANCEL,
    KIND_CLOSE,
    KIND_ENTRY,
    KIND_EXIT_SL,
    KIND_EXIT_TP,
    KIND_MODIFY_ENTRY,
    KIND_MODIFY_EXIT,
)
from pynecore.core.broker.journal import DispatchJournal
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
)
from pynecore.core.broker.models import (
    CancelIntent,
    CloseIntent,
    DispatchEnvelope,
    EntryIntent,
    ExchangeOrder,
    ExchangePosition,
    ExitIntent,
    LegType,
    OrderEvent,
    OrderStatus,
    OrderType,
)
from pynecore.core.plugin import override

from ._base import _CapitalComBase
from .exceptions import (
    CapitalComError,
    InvalidStopDistanceError,
    InvalidStopMaxValueError,
    InvalidTakeProfitDistanceError,
    InvalidTakeProfitMaxValueError,
)
from .helpers import _parse_iso_timestamp
from .models import _InstrumentRules, _bracket_leg_id

if TYPE_CHECKING:
    from pynecore.core.broker.journal import (
        ConfirmOutcome,
        ResumeOutcome,
        SubmitOutcome,
    )
    from pynecore.core.broker.storage import OrderRow


class _ExecutionMixin(_CapitalComBase):
    """Order execution mix-in: every ``execute_*`` and ``modify_*`` path."""

    @staticmethod
    def _validate_sl_distance(
            rules: _InstrumentRules,
            reference_price: float | None,
            sl_price: float,
    ) -> None:
        """Raise :class:`InvalidStopDistanceError` if ``sl_price`` is closer
        than the cached minimum distance to ``reference_price``.

        ``reference_price`` is the entry's confirmed fill level (the
        broker's actual anchor is the live mid quote, but the bracket is
        attached to a confirmed position so the entry price is the closest
        proxy available without an extra REST round-trip). Pre-rejecting
        here turns an obvious script bug ("offset less than min distance")
        into a logged skip instead of an exchange-side rejection. When
        ``reference_price`` is missing or non-positive the check is a
        no-op — the REST round-trip remains the authoritative gate.
        """
        if reference_price is None or reference_price <= 0.0:
            return
        if rules.min_stop_or_limit_distance <= 0.0:
            return
        distance = abs(reference_price - sl_price)
        if distance < rules.min_stop_or_limit_distance:
            raise InvalidStopDistanceError(
                f"Capital.com pre-check: stop distance {distance:.6f} for "
                f"{rules.epic} is below current minimum "
                f"{rules.min_stop_or_limit_distance:.6f} "
                f"(reference={reference_price}, sl={sl_price}).",
                min_distance=rules.min_stop_or_limit_distance,
            )

    @staticmethod
    def _validate_tp_distance(
            rules: _InstrumentRules,
            reference_price: float | None,
            tp_price: float,
    ) -> None:
        """Raise :class:`InvalidTakeProfitDistanceError` if ``tp_price`` is
        closer than the cached minimum distance to ``reference_price``.

        Capital.com applies ``minNormalStopOrLimitDistance`` symmetrically
        to both legs — the same threshold gates ``stopLevel`` and
        ``profitLevel``. See :meth:`_validate_sl_distance` for the
        anchor-price discussion.
        """
        if reference_price is None or reference_price <= 0.0:
            return
        if rules.min_stop_or_limit_distance <= 0.0:
            return
        distance = abs(reference_price - tp_price)
        if distance < rules.min_stop_or_limit_distance:
            raise InvalidTakeProfitDistanceError(
                f"Capital.com pre-check: take-profit distance {distance:.6f} "
                f"for {rules.epic} is below current minimum "
                f"{rules.min_stop_or_limit_distance:.6f} "
                f"(reference={reference_price}, tp={tp_price}).",
                min_distance=rules.min_stop_or_limit_distance,
            )

    @staticmethod
    def _quantize_size(qty: float, rules: _InstrumentRules) -> float:
        """Snap ``qty`` to the nearest multiple of ``rules.lot_step``.

        Uses :func:`round` (banker-free here — lot steps are decimal) on
        the step count, then multiplies back out. Below-min-size is the
        caller's concern: silently inflating to ``rules.min_size`` would
        make the exchange's executed size diverge from the strategy's
        ``intent.qty`` and corrupt every downstream sizing assumption
        (bracket full-row check, position accounting, P&L). The entry
        path raises :class:`OrderSkippedByPlugin` *before* calling this
        helper, so a runtime sizing model that yields a too-small qty
        becomes a logged skip rather than silent up-inflation.
        """
        if rules.lot_step <= 0.0:
            return qty
        units = round(qty / rules.lot_step)
        return units * rules.lot_step
    async def get_position(self, symbol: str) -> ExchangePosition | None:
        """Return the aggregate position across all rows for ``symbol``.

        Capital.com netting opens a fresh row per same-direction entry
        (confirmed empirically after §9 #5 is closed) — aggregation is
        therefore mandatory for Pine's one-way model. Returns ``None``
        when no row exists for the symbol.
        """
        res = await self._call('positions', method='get')
        rows = res.get('positions') or []
        long_size = 0.0
        short_size = 0.0
        long_notional = 0.0
        short_notional = 0.0
        unrealized_pnl = 0.0
        leverage = 0.0
        margin_mode = 'cross'
        matched_any = False

        for row in rows:
            market = row.get('market') or {}
            if market.get('epic') != symbol:
                continue
            position = row.get('position') or {}
            direction = (position.get('direction') or '').upper()
            size = float(position.get('size', 0.0))
            open_level = float(position.get('level', 0.0))
            upl = float(position.get('upl', 0.0))
            if direction == 'BUY':
                long_size += size
                long_notional += size * open_level
            elif direction == 'SELL':
                short_size += size
                short_notional += size * open_level
            else:
                continue
            unrealized_pnl += upl
            row_leverage = position.get('leverage')
            if isinstance(row_leverage, (int, float)):
                leverage = max(leverage, float(row_leverage))
            matched_any = True

        if not matched_any:
            return None

        net = long_size - short_size
        if abs(net) < 1e-12:
            side = 'flat'
            entry_price = 0.0
        elif net > 0:
            side = 'long'
            entry_price = long_notional / long_size if long_size else 0.0
        else:
            side = 'short'
            entry_price = short_notional / short_size if short_size else 0.0

        return ExchangePosition(
            symbol=symbol,
            side=side,
            size=abs(net),
            entry_price=entry_price,
            unrealized_pnl=unrealized_pnl,
            liquidation_price=None,
            leverage=leverage,
            margin_mode=margin_mode,
        )

    async def get_open_orders(
            self, symbol: str | None = None,
    ) -> list[ExchangeOrder]:
        """Return all pending working orders, optionally filtered by symbol.

        Capital.com's ``GET /workingorders`` returns server-generated
        ``dealId`` values and no user-side reference. To restore
        ``client_order_id`` echo semantics (required by the sync engine
        for dispatch recovery), the plugin queries the unified broker
        storage's generic alias index (``order_refs`` table) by
        ``ref_type='deal_id'`` — a single indexed SELECT.  When no matching
        row exists (or when the plugin runs without a ``store_ctx``, e.g.
        tests), ``client_order_id`` is left ``None`` and the sync engine
        treats the order as externally owned.
        """
        res = await self._call('workingorders', method='get')
        orders = res.get('workingOrders') or []
        result: list[ExchangeOrder] = []
        for wo in orders:
            data = wo.get('workingOrderData') or {}
            market = wo.get('market') or {}
            epic = market.get('epic') or data.get('epic')
            if symbol is not None and epic != symbol:
                continue
            deal_id = str(data.get('dealId') or '')
            direction = (data.get('direction') or '').lower()
            side = 'buy' if direction == 'buy' else 'sell'
            order_type_raw = (data.get('orderType') or '').upper()
            if order_type_raw == 'LIMIT':
                order_type = OrderType.LIMIT
                price = float(data.get('orderLevel', 0.0))
                stop_price = None
            elif order_type_raw == 'STOP':
                order_type = OrderType.STOP
                price = None
                stop_price = float(data.get('orderLevel', 0.0))
            else:
                continue
            qty = float(data.get('orderSize', 0.0))

            coid: str | None = None
            if deal_id and self.store_ctx is not None:
                if order_row := self.store_ctx.find_by_ref('deal_id', deal_id):
                    coid = order_row.client_order_id

            created_raw = str(data.get('createdDateUTC')
                              or data.get('createdDate') or '')
            timestamp = _parse_iso_timestamp(created_raw)

            result.append(ExchangeOrder(
                id=deal_id,
                symbol=epic or '',
                side=side,
                order_type=order_type,
                qty=qty,
                filled_qty=0.0,
                remaining_qty=qty,
                price=price,
                stop_price=stop_price,
                average_fill_price=None,
                status=OrderStatus.OPEN,
                timestamp=timestamp,
                fee=0.0,
                fee_currency='',
                reduce_only=False,
                client_order_id=coid,
            ))
        return result

    # --- BrokerPlugin: execute path ---------------------------------------

    async def execute_entry(
            self, envelope: DispatchEnvelope,
    ) -> list[ExchangeOrder]:
        """Open a position (MARKET) or place a working order (LIMIT/STOP).

        Routed through the Core
        :class:`~pynecore.core.broker.journal.DispatchJournal` — the
        journal owns every ``upsert_order`` / ``add_ref`` /
        ``log_event`` / state transition along the lifecycle. The plugin
        contributes the pre-dispatch prep (capability gate, instrument
        rules, ``OrderSkippedByPlugin`` floor, qty quantize, endpoint /
        body construction) and a typed :class:`_CapitalComEntryHooks`
        instance that the journal calls back into for the REST round
        trips and reject classification.

        Every state transition is **PERSIST-FIRST** so a crash between
        the REST round trips leaves enough audit trail for
        :meth:`_recover_in_flight_submissions` to resolve the ambiguity
        on restart. The six-point crash matrix from the research dossier
        §5.1 is the truth table the journal implements.
        """
        intent = envelope.intent
        assert isinstance(intent, EntryIntent)
        coid = envelope.client_order_id(KIND_ENTRY)

        if intent.order_type == OrderType.STOP_LIMIT:
            raise ExchangeCapabilityError(
                "Capital.com does not support STOP_LIMIT orders — core "
                "validation should have caught this at startup.",
            )

        rules = await self._get_instrument_rules(intent.symbol)
        if rules.min_size > 0 and intent.qty < rules.min_size:
            raise OrderSkippedByPlugin(
                f"Skipping {intent.symbol} {intent.side.upper()} entry "
                f"id={intent.pine_id!r}: qty={intent.qty} below Capital.com "
                f"minimum size {rules.min_size}. No order sent.",
                intent_key=intent.intent_key,
                reason="below_min_size",
                context={
                    'symbol': intent.symbol,
                    'side': intent.side,
                    'qty': intent.qty,
                    'min_size': rules.min_size,
                },
            )
        quantized_qty = self._quantize_size(intent.qty, rules)
        direction = "BUY" if intent.side == 'buy' else "SELL"

        if intent.order_type == OrderType.MARKET:
            endpoint = 'positions'
            body: dict = {
                'epic': intent.symbol,
                'direction': direction,
                'size': quantized_qty,
            }
            kind = ENTRY_KIND_POSITION
        else:
            endpoint = 'workingorders'
            if intent.order_type == OrderType.LIMIT:
                level = intent.limit
                wo_type = 'LIMIT'
            else:  # STOP
                level = intent.stop
                wo_type = 'STOP'
            if level is None:
                raise ExchangeOrderRejectedError(
                    f"Capital execute_entry: {wo_type} order requires a "
                    f"level, got None (pine_id={intent.pine_id!r})",
                )
            body = {
                'epic': intent.symbol,
                'direction': direction,
                'size': quantized_qty,
                'level': float(level),
                'type': wo_type,
            }
            kind = ENTRY_KIND_WORKING

        if self.store_ctx is None:
            raise RuntimeError(
                "execute_entry requires an active store_ctx; "
                "the journal owns persistence and cannot operate without it.",
            )

        hooks = _CapitalComEntryHooks(
            plugin=self,
            endpoint=endpoint,
            body=body,
            quantized_qty=quantized_qty,
        )
        journal = DispatchJournal(self.store_ctx)
        return await journal.run_entry(
            coid=coid,
            intent=intent,
            qty=quantized_qty,
            kind=kind,
            hooks=hooks,
            audit_payload={'endpoint': endpoint, 'body': body},
        )

    async def execute_exit(
            self, envelope: DispatchEnvelope,
    ) -> list[ExchangeOrder]:
        """Attach a TP/SL/trailing bracket to an open position.

        Capital.com's bracket is a *full-row* attribute on the position
        (``PUT /positions/{dealId}`` with ``profitLevel`` / ``stopLevel``
        / ``trailingStop``) — there is no separate exit order. The plugin
        synthesises two :class:`ExchangeOrder` objects with composite
        ``dealId:tp`` / ``dealId:sl`` ids so the sync engine's leg
        accounting still works.

        ``partial_qty_bracket_exit`` is declared ``False`` in
        :meth:`get_capabilities`, so core validation rejects
        ``strategy.exit(qty=N, from_entry='L')`` with ``N < row qty`` at
        startup. A runtime-safety-net assertion here stays as a
        belt-and-braces check — the unit count comparison uses the
        instrument's ``lot_step`` so rounding noise does not falsely trip
        it.

        Trailing with an activation threshold (Pine ``trail_price``) is
        *deferred*: the plugin sets an ``activating`` state and lets the
        activation monitor flip the exchange to native ``trailingStop``
        when the mid-price crosses the threshold. Immediate trailing
        (``trail_offset`` only, no ``trail_price``) goes out on the PUT
        directly.
        """
        intent = envelope.intent
        assert isinstance(intent, ExitIntent)
        # Fresh market data: ``_get_current_mid_price`` also refreshes the
        # rules cache as a side effect, so the subsequent rules lookup is
        # a single in-process dict read rather than a second REST call.
        mid_price = await self._get_current_mid_price(intent.symbol)
        rules = await self._get_instrument_rules(intent.symbol)

        # --- Resolve target row ---
        # The parent entry row carries ``pine_entry_id == intent.from_entry``
        # and ``from_entry IS NULL`` (entries have no from_entry). Filtering
        # the SQL by ``from_entry`` would wrongly exclude it — filter by
        # ``symbol`` only and disambiguate in Python.
        #
        # Skip rows flagged with ``natural_close_at``: after a TP/SL
        # closes the position, the entry row stays live in BrokerStore
        # so subsequent ``modify_exit`` lookups still work for the
        # *same* entry. Once Pine opens a *new* entry under the same
        # ``pine_entry_id`` (e.g. ``'Long'`` again), both rows match
        # the search criteria — without this skip the iteration would
        # return the old (already-closed-on-the-broker) row and the
        # PUT would 404 with ``error.not-found.dealId``.
        target_row: 'OrderRow | None' = self._find_active_entry_row(
            intent.symbol, intent.from_entry,
        )
        if target_row is None or not target_row.exchange_order_id:
            raise ExchangeOrderRejectedError(
                f"Capital execute_exit: no confirmed entry row for "
                f"from_entry={intent.from_entry!r} symbol={intent.symbol!r}",
            )
        deal_id = target_row.exchange_order_id

        # Partial-qty runtime safety net (core validation is authoritative).
        row_units = round(target_row.qty / rules.lot_step) if rules.lot_step > 0 else 0
        intent_units = round(intent.qty / rules.lot_step) if rules.lot_step > 0 else 0
        if row_units and intent_units and row_units != intent_units:
            raise ExchangeCapabilityError(
                f"Capital.com bracket is full-row only (row_qty={target_row.qty}, "
                f"intent_qty={intent.qty}). Core validation should have "
                f"rejected this script at startup.",
            )

        # --- Pre-validate bracket distances before assembling the body ---
        # Capital.com checks distance against the live mid quote, not the
        # entry's fill price — using ``confirm_level`` would falsely reject
        # a valid bracket whenever price has drifted away from the entry.
        if intent.sl_price is not None:
            self._validate_sl_distance(
                rules, mid_price, float(intent.sl_price),
            )
        if intent.tp_price is not None:
            self._validate_tp_distance(
                rules, mid_price, float(intent.tp_price),
            )

        # --- Build body + decide trailing deferral ---
        trail_pending = (
            intent.trail_offset is not None and intent.trail_price is not None
        )
        body: dict = {}
        if intent.tp_price is not None:
            body['profitLevel'] = float(intent.tp_price)
        if intent.sl_price is not None:
            body['stopLevel'] = float(intent.sl_price)
        if intent.trail_offset is not None and not trail_pending:
            body['trailingStop'] = True
            body['stopDistance'] = float(intent.trail_offset)

        tp_coid = envelope.client_order_id(KIND_EXIT_TP)
        sl_coid = envelope.client_order_id(KIND_EXIT_SL)

        # --- PERSIST-FIRST the TP leg row ---
        # ``parent_coid`` lets ``_resolve_bracket_leg_disposition`` route
        # ``record_resolution`` to the same id the engine parked under
        # (``OrderDispositionUnknownError(client_order_id=target_row.client_order_id)``
        # below); without it the leg row would only know the parent's
        # *deal_id*, which is not the engine's park key.
        #
        # **Reopen on retry**: when an earlier dispatch on this same
        # ``DispatchEnvelope`` (run_tag, pine_id, bar_ts_ms, retry_seq)
        # placed a leg row that was later flipped ``rejected`` and
        # closed by :meth:`_record_bracket_resolution`, the engine drops
        # the active intent and re-dispatches on the next sync. If that
        # retry lands within the same Pine bar the COIDs are identical
        # (``DispatchEnvelope.client_order_id`` is a pure function of
        # the envelope), so this upsert hits the existing row but does
        # not touch ``closed_ts_ms`` — leaving the row invisible to
        # ``iter_live_orders``, reconcile, and the bracket-deal-id ref
        # lookup. Mirror the :meth:`modify_exit` reopen pattern: clear
        # ``closed_ts_ms`` if a prior cycle closed this COID before
        # writing the live-leg fields.
        if intent.tp_price is not None and self.store_ctx is not None:
            tp_existing = self.store_ctx.get_order(tp_coid)
            if tp_existing is not None and tp_existing.closed_ts_ms is not None:
                self.store_ctx.reopen_order(tp_coid)
            self.store_ctx.upsert_order(
                tp_coid,
                symbol=intent.symbol,
                side=intent.side,
                qty=target_row.qty,
                state='submitted',
                intent_key=intent.intent_key + '\0TP',
                from_entry=intent.from_entry,
                pine_entry_id=intent.from_entry,
                tp_level=intent.tp_price,
                extras={
                    'leg_kind': 'tp',
                    'parent_deal_id': deal_id,
                    'parent_coid': target_row.client_order_id,
                },
            )
            self.store_ctx.add_ref(tp_coid, 'bracket_deal_id', deal_id)

        has_sl_leg = (intent.sl_price is not None or intent.trail_offset is not None)
        if has_sl_leg and self.store_ctx is not None:
            sl_existing = self.store_ctx.get_order(sl_coid)
            if sl_existing is not None and sl_existing.closed_ts_ms is not None:
                self.store_ctx.reopen_order(sl_coid)
            sl_extras: dict = {
                'leg_kind': 'sl',
                'parent_deal_id': deal_id,
                'parent_coid': target_row.client_order_id,
            }
            if trail_pending:
                sl_extras['trail_activation_price'] = intent.trail_price
                sl_extras['trail_offset'] = intent.trail_offset
                sl_extras['trail_state'] = 'pending'
            self.store_ctx.upsert_order(
                sl_coid,
                symbol=intent.symbol,
                side=intent.side,
                qty=target_row.qty,
                state='submitted',
                intent_key=intent.intent_key + '\0SL',
                from_entry=intent.from_entry,
                pine_entry_id=intent.from_entry,
                sl_level=intent.sl_price,
                trailing_distance=intent.trail_offset,
                trailing_stop=(intent.trail_offset is not None and not trail_pending),
                extras=sl_extras,
            )
            self.store_ctx.add_ref(sl_coid, 'bracket_deal_id', deal_id)

        # --- PUT the bracket (skipped entirely when body is empty, e.g.
        # trailing-with-activation only — the activation monitor issues
        # the PUT on the crossing tick) ---
        #
        # Failure handling:
        #   * Network timeout / connection drop: bracket may or may not
        #     have landed; flip leg rows to ``disposition_unknown`` so the
        #     reconcile snapshot can promote them, and raise
        #     :class:`OrderDispositionUnknownError`.  Engine parks the
        #     intent and re-evaluates next sync.
        #   * Synchronous broker error / REJECTED confirm: bracket is
        #     definitely NOT attached.  Roll back the persisted TP/SL
        #     leg rows (close + audit event) so a restart cannot see a
        #     stale ``submitted`` protective leg, then raise.
        # Only legs actually present in the PUT body (TP, fixed SL, or
        # immediate native trailing) are subject to ambiguous-PUT
        # recovery. A pending-trail SL leg lives in BrokerStore but the
        # broker has no knowledge of it yet — flagging it as
        # ``disposition_unknown`` would route it through
        # :meth:`_resolve_bracket_leg_disposition`, which can only see
        # ``trailingStop=True`` on the parent (it isn't yet) and would
        # therefore wrongly mark the row ``rejected`` and close it.
        sl_in_body = (
            intent.sl_price is not None
            or (intent.trail_offset is not None and not trail_pending)
        )
        if body:
            try:
                resp = await self._call(
                    f'positions/{deal_id}', data=body, method='put',
                )
            except (httpx.TimeoutException, httpx.RequestError,
                    ConnectionError, ExchangeConnectionError) as net:
                self._mark_bracket_legs_disposition_unknown(
                    intent=intent, parent_coid=target_row.client_order_id,
                    deal_id=deal_id,
                    tp_coid=tp_coid if intent.tp_price is not None else None,
                    sl_coid=sl_coid if sl_in_body else None,
                    reason=str(net), stage='put',
                )
                # Pending-trail SL was deliberately NOT in the PUT body,
                # so its disposition is unambiguous: the broker never
                # heard about it. The post-PUT block that normally
                # promotes ``submitted`` → ``confirmed`` does not run
                # because we're about to raise — without this explicit
                # promote, the row stays as ``submitted`` and (a)
                # ``_recover_in_flight_submissions`` treats it as an
                # in-flight broker submission on the next restart, and
                # (b) ``_trailing_activation_monitor`` may eventually
                # PUT the activation while the row's ``state`` is still
                # the pre-PUT placeholder. Confirm it locally now —
                # the activation monitor owns the row's lifecycle from
                # here on.
                if (has_sl_leg and not sl_in_body
                        and self.store_ctx is not None):
                    self.store_ctx.set_order_state(sl_coid, 'confirmed')
                raise OrderDispositionUnknownError(
                    f"Capital bracket PUT positions/{deal_id} ambiguous: {net}",
                    client_order_id=target_row.client_order_id,
                    cause=net if isinstance(net, Exception) else None,
                ) from net
            except (CapitalComError, BrokerError) as exc:
                # `_call` runs `_map_exception` and re-raises 4xx as
                # mapped subclasses (`ExchangeOrderRejectedError`,
                # `InvalidStopDistanceError`, `InvalidStopMaxValueError`,
                # `InvalidTakeProfitDistanceError`,
                # `InvalidTakeProfitMaxValueError`, `OrderNotFoundError`,
                # `InsufficientMarginError`, `ExchangeRateLimitError`,
                # `AuthenticationError`). All of these mean the PUT did
                # not attach the bracket — roll back the persisted leg
                # rows. ``ExchangeConnectionError`` cannot reach this
                # branch because the network ``except`` above already
                # absorbs it.
                self._rollback_bracket_legs(
                    intent=intent, parent_coid=target_row.client_order_id,
                    deal_id=deal_id,
                    tp_coid=tp_coid if intent.tp_price is not None else None,
                    sl_coid=sl_coid if has_sl_leg else None,
                    reason='put_failed',
                )
                # The parent ENTRY is filled and committed; only the
                # bracket attach failed, so the position is OPEN AND
                # UNPROTECTED. Surface this distinctly so the sync
                # engine can dispatch a defensive market close instead
                # of halting (which would leave the unprotected fill
                # exposed indefinitely). The original mapped exception
                # is preserved on ``__cause__`` for log correlation.
                raise BracketAttachAfterFillRejectedError(
                    f"Capital bracket attach rejected after entry fill "
                    f"(deal_id={deal_id}): {exc}",
                    position_deal_id=deal_id,
                    position_coid=target_row.client_order_id,
                    symbol=intent.symbol,
                    position_side=target_row.side,
                    qty=target_row.qty,
                    from_entry=intent.from_entry,
                ) from exc

            new_ref = resp.get('dealReference')
            if new_ref:
                try:
                    attach_confirm = await self._call(
                        f'confirms/{new_ref}', method='get',
                    )
                except (httpx.TimeoutException, httpx.RequestError,
                        ConnectionError, ExchangeConnectionError,
                        CapitalComError, BrokerError) as net:
                    # PUT landed, confirm read failed: state on the
                    # exchange is unknown until the next /positions
                    # snapshot reveals the real SL/TP. We catch BOTH
                    # network/timeout AND mapped broker errors here:
                    # the PUT already returned a ``dealReference``, so
                    # the bracket may already be attached — a confirm
                    # GET that 404s (``OrderNotFoundError``), 429s
                    # (``ExchangeRateLimitError``) or otherwise lands
                    # on the mapped-broker-error path is just as
                    # ambiguous as a network timeout. Without this
                    # branch the persisted TP/SL rows would stay in
                    # ``submitted`` with no ``OrderDispositionUnknownError``
                    # park record, so :meth:`_resolve_bracket_leg_disposition`
                    # would never get a chance to promote or reject
                    # them — leaving an orphan that can only be cleaned
                    # up manually.
                    self._mark_bracket_legs_disposition_unknown(
                        intent=intent, parent_coid=target_row.client_order_id,
                        deal_id=deal_id,
                        tp_coid=tp_coid if intent.tp_price is not None else None,
                        sl_coid=sl_coid if sl_in_body else None,
                        reason=str(net), stage='confirm',
                        deal_reference=new_ref,
                    )
                    # Same reasoning as the PUT-timeout path above —
                    # pending-trail SL is purely local; promote it to
                    # ``confirmed`` so the activation monitor + recovery
                    # see a coherent state.
                    if (has_sl_leg and not sl_in_body
                            and self.store_ctx is not None):
                        self.store_ctx.set_order_state(sl_coid, 'confirmed')
                    raise OrderDispositionUnknownError(
                        f"Capital bracket confirm {new_ref} ambiguous: {net}",
                        client_order_id=target_row.client_order_id,
                        cause=net if isinstance(net, Exception) else None,
                    ) from net
                if (attach_confirm.get('dealStatus') or '').upper() == 'REJECTED':
                    reason = attach_confirm.get('reason') or 'unknown'
                    self._rollback_bracket_legs(
                        intent=intent, parent_coid=target_row.client_order_id,
                        deal_id=deal_id,
                        tp_coid=tp_coid if intent.tp_price is not None else None,
                        sl_coid=sl_coid if has_sl_leg else None,
                        reason=reason,
                    )
                    # See the put_failed branch above — same unprotected
                    # position semantics, raised distinctly so the sync
                    # engine triggers a defensive close.
                    raise BracketAttachAfterFillRejectedError(
                        f"Capital bracket attach REJECTED after entry fill "
                        f"(deal_id={deal_id}): {reason}",
                        position_deal_id=deal_id,
                        position_coid=target_row.client_order_id,
                        symbol=intent.symbol,
                        position_side=target_row.side,
                        qty=target_row.qty,
                        from_entry=intent.from_entry,
                    )

        # --- Mark legs confirmed + persist risk on entry row ---
        if self.store_ctx is not None:
            if intent.tp_price is not None:
                self.store_ctx.set_order_state(tp_coid, 'confirmed')
            if has_sl_leg:
                # ``confirmed`` even when trailing is pending — the row
                # exists; the activation monitor promotes it to active.
                self.store_ctx.set_order_state(sl_coid, 'confirmed')
            self.store_ctx.set_risk(
                target_row.client_order_id,
                sl=intent.sl_price, tp=intent.tp_price,
                trailing_distance=intent.trail_offset,
                trailing_stop=(intent.trail_offset is not None and not trail_pending),
            )
            self.store_ctx.log_event(
                'bracket_attached',
                client_order_id=target_row.client_order_id,
                exchange_order_id=deal_id,
                intent_key=intent.intent_key,
                payload={
                    'tp': intent.tp_price, 'sl': intent.sl_price,
                    'trail_offset': intent.trail_offset,
                    'trail_activation': intent.trail_price,
                    'trail_pending': trail_pending,
                },
            )

        # --- Build canonical ExchangeOrder legs ---
        legs: list[ExchangeOrder] = []
        now_ts = epoch_time()
        if intent.tp_price is not None:
            legs.append(ExchangeOrder(
                id=_bracket_leg_id(deal_id, 'tp'),
                symbol=intent.symbol,
                side=intent.side,
                order_type=OrderType.LIMIT,
                qty=target_row.qty,
                filled_qty=0.0,
                remaining_qty=target_row.qty,
                price=intent.tp_price,
                stop_price=None,
                average_fill_price=None,
                status=OrderStatus.OPEN,
                timestamp=now_ts,
                fee=0.0,
                fee_currency='',
                reduce_only=True,
                client_order_id=tp_coid,
            ))
        if has_sl_leg:
            legs.append(ExchangeOrder(
                id=_bracket_leg_id(deal_id, 'sl'),
                symbol=intent.symbol,
                side=intent.side,
                order_type=(
                    OrderType.TRAILING_STOP
                    if intent.trail_offset is not None else OrderType.STOP
                ),
                qty=target_row.qty,
                filled_qty=0.0,
                remaining_qty=target_row.qty,
                price=None,
                stop_price=intent.sl_price,
                average_fill_price=None,
                status=OrderStatus.OPEN,
                timestamp=now_ts,
                fee=0.0,
                fee_currency='',
                reduce_only=True,
                client_order_id=sl_coid,
            ))
        return legs

    async def execute_close(
            self, envelope: DispatchEnvelope,
    ) -> ExchangeOrder:
        """Close (full) or reduce (partial) a position.

        *Full close* uses ``DELETE /positions/{dealId}`` — preferred, no
        race window. *Partial close* is emulated via an opposite-direction
        ``POST /positions`` because Capital.com has no partial-close
        endpoint. The opposite POST is inherently racy against another
        REST session opening a fresh opposite row in the same instant;
        the plugin protects with a pre- + post-snapshot comparison and a
        corrective ``DELETE`` *only* when the fresh row's
        ``createdDateUTC`` falls within a ±3 s window of our POST. If the
        race cannot be confidently resolved, the plugin raises
        :class:`BrokerManualInterventionError` — the sync engine halts.

        Both branches run through the Core
        :class:`~pynecore.core.broker.journal.DispatchJournal` so a
        process crash between the wire calls and the persisted result is
        recoverable: the close command row is written *before* the wire
        calls, and recovery on next start verifies each target ``dealId``
        against the positions snapshot (full close) or the
        ``deal_reference`` confirm GET (partial close).
        """
        from pynecore.core.broker.journal import DispatchJournal
        from pynecore.core.broker.store_helpers import (
            KIND_FULL_CLOSE,
            KIND_PARTIAL_CLOSE,
        )

        from .dispatch_hooks import _CapitalComCloseHooks

        intent = envelope.intent
        assert isinstance(intent, CloseIntent)
        coid = envelope.client_order_id(KIND_CLOSE)
        rules = await self._get_instrument_rules(intent.symbol)

        targets: list['OrderRow'] = []
        if self.store_ctx is not None:
            for row in self.store_ctx.iter_live_orders(symbol=intent.symbol):
                if (row.state == 'confirmed'
                        and row.exchange_order_id
                        and (row.extras or {}).get('kind') == 'position'):
                    targets.append(row)
        if not targets:
            raise ExchangeOrderRejectedError(
                f"Capital execute_close: no confirmed position rows for "
                f"symbol={intent.symbol!r}",
            )

        if self.store_ctx is None:
            raise RuntimeError(
                "execute_close requires an active store_ctx; the journal "
                "owns persistence and cannot operate without it.",
            )

        total_live_units = sum(
            round(max(0.0, row.qty - row.filled_qty) / rules.lot_step)
            if rules.lot_step > 0 else 0
            for row in targets
        )
        intent_units = (
            round(intent.qty / rules.lot_step) if rules.lot_step > 0 else 0
        )

        kind = (
            KIND_FULL_CLOSE if intent_units == total_live_units
            else KIND_PARTIAL_CLOSE
        )

        hooks = _CapitalComCloseHooks(plugin=self, rules=rules)
        journal = DispatchJournal(self.store_ctx)
        return await journal.run_close(
            coid=coid,
            intent=intent,
            kind=kind,
            targets=targets,
            hooks=hooks,
            audit_payload={'pine_id': intent.pine_id},
        )

    async def execute_cancel(
            self, envelope: DispatchEnvelope,
    ) -> bool:
        """Cancel pending working orders and/or bracket legs matching the intent.

        Idempotent: if no live row matches the intent, returns ``True``
        with a ``cancel_noop`` audit event. If the exchange reports
        ``error.not-found.dealId`` / ``.dealReference`` on the DELETE,
        the plugin treats it as already-gone (benign race), logs a
        ``cancel_already_gone`` event, and continues.

        The per-target sweep runs through the Core
        :class:`~pynecore.core.broker.journal.DispatchJournal` so a
        mid-loop crash is recoverable: the cancel command row is
        persisted *before* the wire calls, and recovery on next start
        compares the target rows against the broker's authoritative
        snapshots to decide whether the cancel landed.
        """
        from pynecore.core.broker.journal import DispatchJournal

        from .dispatch_hooks import _CapitalComCancelHooks

        intent = envelope.intent
        assert isinstance(intent, CancelIntent)
        coid = envelope.client_order_id(KIND_CANCEL)

        targets: list['OrderRow'] = []
        if self.store_ctx is not None:
            for row in self.store_ctx.iter_live_orders(
                    symbol=intent.symbol,
                    from_entry=intent.from_entry,
            ):
                # Match by entry id always; only match by ``from_entry``
                # when the intent explicitly carries one. A None-from_entry
                # cancel (entry cancel) would otherwise sweep every row
                # whose ``from_entry`` is also None — i.e. every other live
                # entry on the same symbol.
                matches_pine = row.pine_entry_id == intent.pine_id or (
                    intent.from_entry is not None
                    and row.from_entry == intent.from_entry
                )
                if matches_pine:
                    targets.append(row)

        if not targets:
            if self.store_ctx is not None:
                self.store_ctx.log_event(
                    'cancel_noop',
                    client_order_id=coid,
                    intent_key=intent.intent_key,
                    payload={'reason': 'no_matching_live_row',
                             'pine_id': intent.pine_id,
                             'from_entry': intent.from_entry},
                )
            return True

        if self.store_ctx is None:
            # Journal needs a store; without one we cannot orchestrate
            # the persist-first state machine. No live target was found
            # above either when ``store_ctx is None``, so this branch
            # is effectively unreachable in production — the engine
            # never dispatches against a plugin with no run context.
            return True

        hooks = _CapitalComCancelHooks(plugin=self)
        journal = DispatchJournal(self.store_ctx)
        await journal.run_cancel(
            coid=coid,
            intent=intent,
            targets=targets,
            hooks=hooks,
            audit_payload={
                'pine_id': intent.pine_id,
                'from_entry': intent.from_entry,
            },
        )
        return True

    # --- BrokerPlugin: modify overrides -----------------------------------

    @override
    async def modify_entry(
            self, old: DispatchEnvelope, new: DispatchEnvelope,
    ) -> list[ExchangeOrder]:
        """Amend a pending working order's level, or fall back to cancel+create.

        Capital.com lets us ``PUT /workingorders/{dealId}`` with a new
        ``level`` — cheaper and more atomic than cancel+create. But
        ``size`` is **not** amendable, and switching between LIMIT and
        STOP is not either. Both of those fall through to the base
        class's cancel+execute path so the canonical CO-ID formula (same
        bar, same pine_id → same id) keeps idempotency intact.

        The PUT/confirm/persist lifecycle runs through the Core
        :class:`~pynecore.core.broker.journal.DispatchJournal` so a
        crash between the wire call and the persisted result is
        recoverable: the amend command row is written *before* the
        REST call, and recovery on next start verifies the broker's
        view via the same ``deal_reference`` confirm GET path.
        """
        from pynecore.core.broker.journal import DispatchJournal

        from .dispatch_hooks import _CapitalComModifyEntryHooks

        old_i = old.intent
        new_i = new.intent
        assert isinstance(old_i, EntryIntent)
        assert isinstance(new_i, EntryIntent)

        rules = await self._get_instrument_rules(new_i.symbol)
        old_units = (
            round(old_i.qty / rules.lot_step) if rules.lot_step > 0 else 0
        )
        new_units = (
            round(new_i.qty / rules.lot_step) if rules.lot_step > 0 else 0
        )
        if old_units != new_units or old_i.order_type != new_i.order_type:
            return await super().modify_entry(old, new)

        target_coid = old.client_order_id(KIND_ENTRY)
        target_row: 'OrderRow | None' = (
            self.store_ctx.get_order(target_coid)
            if self.store_ctx is not None else None
        )
        if target_row is None or not target_row.exchange_order_id:
            return await super().modify_entry(old, new)

        new_level: float | None = None
        if new_i.order_type == OrderType.LIMIT and new_i.limit is not None:
            new_level = float(new_i.limit)
        elif new_i.order_type == OrderType.STOP and new_i.stop is not None:
            new_level = float(new_i.stop)
        if new_level is None:
            # Nothing to amend (both levels None) — tell the sync engine
            # nothing changed by returning the existing row as-is.
            return [self._row_to_exchange_order(target_row, new_i)]

        if self.store_ctx is None:
            raise RuntimeError(
                "modify_entry requires an active store_ctx; the journal "
                "owns persistence and cannot operate without it.",
            )

        coid = new.client_order_id(KIND_MODIFY_ENTRY)
        hooks = _CapitalComModifyEntryHooks(
            plugin=self,
            target_row=target_row,
            new_level=new_level,
            order_type=new_i.order_type,
        )
        journal = DispatchJournal(self.store_ctx)
        return await journal.run_modify_entry(
            coid=coid,
            target_coid=target_coid,
            old_intent=old_i,
            new_intent=new_i,
            qty=new_i.qty,
            hooks=hooks,
            audit_payload={
                'order_type': new_i.order_type.value,
                'target_exchange_id': target_row.exchange_order_id,
            },
        )

    @override
    async def modify_exit(
            self, old: DispatchEnvelope, new: DispatchEnvelope,
    ) -> list[ExchangeOrder]:
        """Amend an open position's TP/SL levels in place.

        ``PUT /positions/{dealId}`` re-writes ``profitLevel`` /
        ``stopLevel`` atomically — no cancel window where the position
        sits unprotected. Size / from_entry changes are semantic, not
        level changes; those fall through to cancel+create so the
        bracket row is rebuilt from scratch.

        Thin orchestrator over :class:`.dispatch_hooks._CapitalComModifyExitHooks`
        + the Core :class:`~pynecore.core.broker.journal.DispatchJournal`.
        The pre-flight (target lookup, distance validation, PUT body
        construction, derivation of effective / ambiguous COIDs) stays
        here because it decides whether to fall back to
        ``super().modify_exit`` (cancel+create) or whether the amend
        even has a wire-payload. The PUT + confirm + ambiguous-path
        seeding live in the hook; the journal owns the command-row
        state machine.
        """
        from pynecore.core.broker.journal import DispatchJournal, ModifyExitOutcome
        from .dispatch_hooks import _CapitalComModifyExitHooks

        old_i = old.intent
        new_i = new.intent
        assert isinstance(old_i, ExitIntent)
        assert isinstance(new_i, ExitIntent)

        if (old_i.from_entry != new_i.from_entry
                or abs(old_i.qty - new_i.qty) > 1e-9):
            return await super().modify_exit(old, new)

        # Locate the parent position row via its confirmed entry. Same
        # NULL-vs-from_entry + natural_close_at skip as :meth:`execute_exit`
        # — see :meth:`_find_active_entry_row`.
        target_row: 'OrderRow | None' = self._find_active_entry_row(
            new_i.symbol, new_i.from_entry,
        )
        if target_row is None or not target_row.exchange_order_id:
            return await super().modify_exit(old, new)

        # Live mid drives the pre-check: Capital.com validates against the
        # current quote, not the entry's confirmed level.
        mid_price = await self._get_current_mid_price(new_i.symbol)
        rules = await self._get_instrument_rules(new_i.symbol)
        if new_i.sl_price is not None:
            self._validate_sl_distance(
                rules, mid_price, float(new_i.sl_price),
            )
        if new_i.tp_price is not None:
            self._validate_tp_distance(
                rules, mid_price, float(new_i.tp_price),
            )

        # Pine ``trail_price`` + ``trail_offset`` defers native trailing
        # until the activation monitor sees the threshold crossed. The
        # PUT must NOT include ``trailingStop`` in that case — the leg
        # row is persisted in ``pending`` state below and the activation
        # monitor issues its own PUT later.
        trail_pending = (
            new_i.trail_offset is not None and new_i.trail_price is not None
        )
        body: dict = {}
        if new_i.tp_price is not None:
            body['profitLevel'] = float(new_i.tp_price)
        elif old_i.tp_price is not None:
            # TP removal: Capital.com preserves any field NOT mentioned
            # in the PUT, so without this explicit null the old
            # ``profitLevel`` stays live on the position even though
            # BrokerStore / ``_active_intents`` no longer carry the
            # TP — the position could later close at a stale level the
            # engine believes was removed. Mirrors the cancel-bracket-leg
            # flow's ``profitLevel: null`` clear (see ``execute_cancel``).
            body['profitLevel'] = None
        if new_i.sl_price is not None:
            body['stopLevel'] = float(new_i.sl_price)
        elif (old_i.sl_price is not None
                and new_i.trail_offset is None):
            # Fixed SL removal: Capital.com preserves any field NOT
            # mentioned in the PUT, so without this explicit null the
            # old ``stopLevel`` stays live on the position even though
            # BrokerStore / ``_active_intents`` no longer carry the
            # SL — the position could later close at a stale level the
            # engine believes was removed. Mirrors the TP-removal
            # explicit clear above and ``execute_cancel``'s
            # ``stopLevel: null`` flow. The ``trail_offset is None``
            # guard avoids a redundant clear on the fixed-SL → trailing
            # transition: ``trailingStop=True`` + ``stopDistance``
            # below replace the prior fixed stop atomically.
            body['stopLevel'] = None
        if new_i.trail_offset is not None and not trail_pending:
            body['trailingStop'] = True
            body['stopDistance'] = float(new_i.trail_offset)
        elif (old_i.trail_offset is not None
                and old_i.trail_price is None
                and new_i.trail_offset is None):
            # Native-trailing removal: Capital.com keeps the prior
            # ``trailingStop`` flag active unless we explicitly clear it
            # in the PUT body. Covers both transitions:
            #   * trail → fixed SL: without this branch the broker keeps
            #     trailing while the leg row records a fixed stop —
            #     :meth:`_activity_to_event` would classify the fill as
            #     ``LegType.STOP_LOSS`` even though the exchange ran a
            #     trailing stop, mispricing the close.
            #   * trail → no stop at all (e.g. ``TP+trail`` → ``TP``):
            #     the broker would silently keep trailing while local
            #     state shows the position unprotected, leaving live
            #     risk active that the engine no longer tracks.
            body['trailingStop'] = False
        if trail_pending and (
                old_i.sl_price is not None
                or (old_i.trail_offset is not None
                    and old_i.trail_price is None)):
            # Transitioning to pending trailing from a broker-side
            # active stop (fixed SL or immediate native trailing).
            # Capital.com preserves any field NOT mentioned in the PUT,
            # so the prior protective leg would stay live while
            # BrokerStore is rewritten as ``trail_state='pending'`` —
            # the position would close at the OLD stop before Pine's
            # activation price is reached. Send an explicit clear:
            # ``stopLevel: null`` removes the fixed stop (matches the
            # cancel-bracket-leg flow), and ``trailingStop: false``
            # disables the native trailing flag if it was set. We only
            # null out ``stopLevel`` when the new intent did not also
            # specify a fixed SL (defensive — Pine's pending trail and
            # a fixed SL are mutually exclusive in practice).
            if new_i.sl_price is None:
                body['stopLevel'] = None
            body['trailingStop'] = False
        # Persist pending-trail SL legs even when the PUT body is empty
        # (e.g. amend that adds only ``trail_price`` + ``trail_offset``):
        # the activation monitor needs the row, and skipping it here
        # would leave the position protected only by whatever the broker
        # already had.
        needs_persist = (
            new_i.tp_price is not None
            or new_i.sl_price is not None
            or new_i.trail_offset is not None
        )
        # Pending-trail-only old exit + empty new exit: the body is
        # empty (the prior leg was purely local — no broker stop to
        # actively clear in the PUT) and ``needs_persist`` is False
        # (the new intent has no leg to seed), but the previously
        # ``confirmed`` SL row still carries ``trail_state='pending'``
        # in extras. ``_trailing_activation_monitor`` would later PUT
        # ``trailingStop=true`` against that stale row when the
        # threshold crossed — actively attaching a stop Pine has
        # already removed. The leg retirement branch below handles
        # all three SL shapes (fixed SL, immediate native trail,
        # pending trail), but only fires once we get past this early
        # return. The other two shapes naturally populate ``body``
        # (``stopLevel: None`` / ``trailingStop: false``), so the
        # gap is specific to pending-trail-only.
        needs_pending_trail_retire = (
            old_i.trail_offset is not None
            and old_i.trail_price is not None
            and new_i.tp_price is None
            and new_i.sl_price is None
            and new_i.trail_offset is None
        )
        if not body and not needs_persist and not needs_pending_trail_retire:
            return []

        deal_id = target_row.exchange_order_id
        tp_coid = new.client_order_id(KIND_EXIT_TP)
        sl_coid = new.client_order_id(KIND_EXIT_SL)
        # Existing leg rows for the parent — marked ``disposition_unknown``
        # below if the PUT or confirm GET goes ambiguous, so the snapshot
        # reconcile path (:meth:`_resolve_bracket_leg_disposition`) can
        # promote them to attached/closed against the next /positions
        # poll. ``_find_bracket_leg_row`` returns the *current* live row
        # under the parent dealId and leg_kind, which is what we have
        # to flip — the ``new``-envelope coid may not match the live
        # row's coid (different bar / different envelope identity).
        existing_tp = self._find_bracket_leg_row(target_row, 'tp')
        existing_sl = self._find_bracket_leg_row(target_row, 'sl')
        # When an existing leg is found, it owns the live row — keep
        # working under THAT row's COID so the post-PUT mirror UPDATEs
        # the same row instead of inserting a duplicate keyed by the
        # new envelope's COID. ``DispatchEnvelope.client_order_id`` is a
        # pure function of the envelope, so a later-bar amend produces
        # a different ``tp_coid``/``sl_coid`` than the original attach
        # used. Without this redirection ``get_order(tp_coid)`` would
        # miss the live leg, the INSERT branch would create a second
        # live TP row, and ``_find_bracket_leg_row`` could later return
        # the stale one — mispricing fills.
        effective_tp_coid = (
            existing_tp.client_order_id if existing_tp is not None else tp_coid
        )
        effective_sl_coid = (
            existing_sl.client_order_id if existing_sl is not None else sl_coid
        )
        # TP-removal symmetry: when the PUT actively clears the broker-
        # side TP (``profitLevel: None`` set above), the same ambiguous-
        # outcome handling applies as for an attach. Without flipping
        # the existing TP row to ``disposition_unknown`` on PUT/confirm
        # timeout, ``_resolve_bracket_leg_disposition`` would have no
        # row to reconcile against the parent's actual ``profitLevel``
        # — the engine raises ``OrderDispositionUnknownError`` and parks
        # the dispatch, but the leg row stays ``confirmed`` with the
        # OLD ``tp_level`` forever, leaving a phantom TP visible to
        # ``_find_bracket_leg_row`` and natural-close bookkeeping. If
        # the broker did apply the clear, the phantom is plain wrong;
        # if it did not, the leg is still attached and the row should
        # be promoted back to ``confirmed``. The reconciler decides by
        # comparing ``row.tp_level`` against the parent's live
        # ``profitLevel`` — but only for ``disposition_unknown`` rows.
        clears_broker_tp = (
            old_i.tp_price is not None and new_i.tp_price is None
        )
        ambiguous_tp_coid = (
            effective_tp_coid
            if (new_i.tp_price is not None
                or (clears_broker_tp and existing_tp is not None))
            else None
        )
        # Pending-trail SL is purely local until the activation monitor
        # PUTs it, so the broker has no leg to flip — exclude it from
        # ambiguous recovery (otherwise the snapshot reconcile would
        # see ``trailingStop=False`` on the parent and wrongly mark the
        # row rejected).
        sl_in_body = (
            new_i.sl_price is not None
            or (new_i.trail_offset is not None and not trail_pending)
        )
        # Exception: a fixed/native-active → pending-trail transition has
        # ``sl_in_body=False`` (the new pending-trail leg is purely local)
        # but the PUT body still ACTIVELY CLEARS the broker-side stop
        # (``stopLevel=None`` / ``trailingStop=False`` set above). If that
        # PUT or its confirm times out after Capital.com applied the clear,
        # the existing SL leg row would stay ``confirmed`` in BrokerStore
        # forever — the snapshot reconcile only visits ``disposition_unknown``
        # rows, so the local state would silently keep showing a protective
        # leg the broker has dropped, leaving the position unprotected.
        # Flip the existing live SL row into ambiguous recovery for that
        # transition: ``_resolve_bracket_leg_disposition`` then compares
        # the row's prior level against the broker's actual ``stopLevel`` /
        # ``trailingStop`` — broker cleared → mismatch → ``rejected`` →
        # engine re-dispatches; broker did not get the PUT → match →
        # ``attached`` → row preserved.
        sl_clears_broker_active = trail_pending and (
            old_i.sl_price is not None
            or (old_i.trail_offset is not None and old_i.trail_price is None)
        )
        # Fixed-SL removal symmetry with the TP-clear case above. The
        # PUT body now carries ``stopLevel: None`` (added above) when
        # an existing fixed SL is removed without a trailing
        # replacement — same ambiguous-outcome reasoning. Pending-trail
        # is excluded from the SL-clear set: an old pending-trail leg
        # was purely local (``_trailing_activation_monitor`` had not
        # PUT it yet), so the broker has no leg to ambiguously flip.
        # An old immediate native trailing ALSO carries
        # ``trailingStop: false`` in the body (set in the
        # native-trailing removal branch above), so it counts as a
        # broker-side clear here.
        sl_clears_broker_no_replacement = (
            new_i.sl_price is None
            and new_i.trail_offset is None
            and (
                old_i.sl_price is not None
                or (old_i.trail_offset is not None
                    and old_i.trail_price is None)
            )
        )
        ambiguous_sl_coid = (
            effective_sl_coid
            if sl_in_body
            or (sl_clears_broker_active and existing_sl is not None)
            or (sl_clears_broker_no_replacement and existing_sl is not None)
            else None
        )

        coid = new.client_order_id(KIND_MODIFY_EXIT)
        hooks = _CapitalComModifyExitHooks(
            plugin=self,
            target_row=target_row,
            body=body,
            existing_tp=existing_tp,
            existing_sl=existing_sl,
            tp_coid=tp_coid,
            sl_coid=sl_coid,
            effective_tp_coid=effective_tp_coid,
            effective_sl_coid=effective_sl_coid,
            trail_pending=trail_pending,
            ambiguous_tp_coid=ambiguous_tp_coid,
            ambiguous_sl_coid=ambiguous_sl_coid,
            sl_in_body=sl_in_body,
            sl_clears_broker_active=sl_clears_broker_active,
            clears_broker_tp=clears_broker_tp,
        )

        if body:
            # Wire round-trip required — run through the Core journal so
            # the entry-side command row is persisted and state-machined.
            # The hook owns the PUT + confirm + ambiguous-path seeding.
            if self.store_ctx is None:
                raise RuntimeError(
                    "modify_exit requires a live BrokerStore context"
                )
            journal = DispatchJournal(self.store_ctx)
            return await journal.run_modify_exit(
                coid=coid,
                target_coid=target_row.client_order_id,
                target_row=target_row,
                old_intent=old_i,
                new_intent=new_i,
                qty=target_row.qty,
                hooks=hooks,
                audit_payload={'pine_id': new_i.pine_id,
                               'from_entry': new_i.from_entry,
                               'tp_coid': effective_tp_coid,
                               'sl_coid': effective_sl_coid,
                               'envelope_tp_coid': tp_coid,
                               'envelope_sl_coid': sl_coid},
            )

        # Pure local seed amend (e.g. pending-trail bookkeeping with an
        # empty PUT body): no broker round-trip means there is nothing
        # for the journal to state-machine. Apply the post-success mirror
        # directly so the leg rows and entry-row risk snapshot are
        # consistent with the new intent.
        hooks._apply_mirror(target_row=target_row, new_intent=new_i)
        return hooks.exchange_order_from_state(
            row=target_row, new_intent=new_i,
            outcome=ModifyExitOutcome(
                server_ref='', deal_status='ACCEPTED',
                rejected_reason=None, post_put_state={}, raw=None,
            ),
        )

    @staticmethod
    def _row_to_exchange_order(
            row: 'OrderRow', intent: EntryIntent,
    ) -> ExchangeOrder:
        """Synthesize an :class:`ExchangeOrder` from a stored ``OrderRow``.

        Used by :meth:`modify_entry` to return the post-amend representation
        to the sync engine without making another REST call — the stored
        row already carries the authoritative state after the PUT + confirm.
        """
        return ExchangeOrder(
            id=row.exchange_order_id or '',
            symbol=row.symbol,
            side=row.side,
            order_type=intent.order_type,
            qty=row.qty,
            filled_qty=row.filled_qty,
            remaining_qty=max(0.0, row.qty - row.filled_qty),
            price=intent.limit,
            stop_price=intent.stop,
            average_fill_price=None,
            status=OrderStatus.OPEN,
            timestamp=row.updated_ts_ms / 1000.0,
            fee=0.0,
            fee_currency='',
            reduce_only=False,
            client_order_id=row.client_order_id,
        )


class _CapitalComEntryHooks:
    """Capital.com-specific hook set for the Core :class:`DispatchJournal`.

    Constructed per dispatch by :meth:`_ExecutionMixin.execute_entry`.
    Holds the request shape decided by the plugin (endpoint, body,
    quantized qty) and a back-reference to the plugin instance for the
    REST call helper.

    Conforms structurally to
    :class:`~pynecore.core.broker.journal.EntryDispatchHooks` — no
    ``isinstance`` check is needed because the journal calls hooks by
    method name (Protocol semantics).
    """

    def __init__(
            self,
            *,
            plugin: '_ExecutionMixin',
            endpoint: str,
            body: dict,
            quantized_qty: float,
    ) -> None:
        self._plugin = plugin
        self._endpoint = endpoint
        self._body = body
        self._quantized_qty = quantized_qty

    async def submit(
            self, *, coid: str, intent: EntryIntent, qty: float,
    ) -> 'SubmitOutcome':
        """POST the entry to ``positions`` / ``workingorders``.

        A network timeout is converted to
        :class:`OrderDispositionUnknownError`, and a successful POST
        without ``dealReference`` is treated the same way — there is no
        exchange-side anchor to confirm against.
        """
        from pynecore.core.broker.journal import SubmitOutcome

        del qty  # captured at construction time as quantized_qty
        try:
            resp = await self._plugin._call(  # type: ignore[attr-defined]
                self._endpoint, data=self._body, method='post',
            )
        except (httpx.TimeoutException, httpx.RequestError,
                ConnectionError, ExchangeConnectionError) as net:
            raise OrderDispositionUnknownError(
                f"Capital POST {self._endpoint} ambiguous: {net}",
                client_order_id=coid,
                cause=net if isinstance(net, Exception) else None,
            ) from net

        deal_ref = resp.get('dealReference')
        if not deal_ref:
            raise OrderDispositionUnknownError(
                f"Capital POST {self._endpoint}: no dealReference in response",
                client_order_id=coid,
            )
        return SubmitOutcome(server_ref=str(deal_ref), raw=resp)

    async def confirm_submission(
            self, *, coid: str, intent: EntryIntent, server_ref: str,
    ) -> 'ConfirmOutcome':
        """GET ``/confirms/{server_ref}`` and classify the outcome.

        ``REJECTED`` maps to :class:`InsufficientMarginError` /
        :class:`ExchangeOrderRejectedError`, otherwise extract
        ``dealId`` and the confirm-time fill data. Transport-level
        failures on the confirms GET are converted to
        :class:`OrderDispositionUnknownError` so the journal parks the
        already-submitted order for recovery instead of treating it as
        a generic dispatch failure.
        """
        from pynecore.core.broker.journal import ConfirmOutcome

        try:
            confirm = await self._plugin._call(  # type: ignore[attr-defined]
                f'confirms/{server_ref}', method='get',
            )
        except (httpx.TimeoutException, httpx.RequestError,
                ConnectionError, ExchangeConnectionError) as net:
            raise OrderDispositionUnknownError(
                f"Capital GET confirms/{server_ref} ambiguous: {net}",
                client_order_id=coid,
                cause=net if isinstance(net, Exception) else None,
            ) from net

        deal_status = (confirm.get('dealStatus') or '').upper()
        if deal_status == 'REJECTED':
            reason = confirm.get('reason') or 'unknown'
            reason_lc = reason.lower()
            if 'margin' in reason_lc or 'leverage' in reason_lc:
                raise InsufficientMarginError(f"Capital reject: {reason}")
            raise ExchangeOrderRejectedError(
                f"Capital confirm REJECTED: {reason}"
            )

        deal_id: str | None = None
        affected = confirm.get('affectedDeals') or []
        if affected:
            deal_id = affected[0].get('dealId')
        if not deal_id:
            deal_id = confirm.get('dealId')
        if deal_id is not None:
            deal_id = str(deal_id)

        level_confirmed = float(confirm.get('level') or 0.0)
        filled_size = float(confirm.get('size') or self._quantized_qty)
        confirm_status = (confirm.get('status') or '').upper()
        is_filled = (
            intent.order_type == OrderType.MARKET
            and confirm_status == 'OPEN'
        )

        return ConfirmOutcome(
            exchange_id=deal_id,
            is_filled=is_filled,
            filled_qty=filled_size if is_filled else 0.0,
            fill_price=level_confirmed if is_filled else None,
            raw=confirm,
        )

    def exchange_order_from_state(
            self, *, row: 'OrderRow', intent: EntryIntent,
    ) -> ExchangeOrder:
        """Build the public :class:`ExchangeOrder` from the persisted row."""
        is_filled = row.filled_qty > 0.0
        extras = row.extras or {}
        confirm_level_raw = extras.get('confirm_level')
        average_fill_price: float | None = None
        if is_filled and confirm_level_raw is not None:
            try:
                average_fill_price = float(confirm_level_raw)
            except (TypeError, ValueError):
                average_fill_price = None
        return ExchangeOrder(
            id=row.exchange_order_id or '',
            symbol=intent.symbol,
            side=intent.side,
            order_type=intent.order_type,
            qty=row.qty,
            filled_qty=row.filled_qty,
            remaining_qty=max(0.0, row.qty - row.filled_qty),
            price=intent.limit,
            stop_price=intent.stop,
            average_fill_price=average_fill_price,
            status=OrderStatus.FILLED if is_filled else OrderStatus.OPEN,
            timestamp=epoch_time(),
            fee=0.0,
            fee_currency='',
            reduce_only=False,
            client_order_id=row.client_order_id,
        )

    async def resume_pending_dispatch(
            self, *, row: 'OrderRow', refs,
    ) -> 'ResumeOutcome':
        """Defensive raise — Capital.com recovery uses ``_CapitalComResumeHooks``.

        The journal's :meth:`DispatchJournal.recover_pending` is called
        from :meth:`_recover_in_flight_submissions` with a separate
        ``hooks_for()`` provider that returns
        :class:`_CapitalComResumeHooks` instances. This method exists
        only to satisfy the :class:`EntryDispatchHooks` Protocol; it is
        never invoked on the production code path. A reachable call
        here means the recovery wiring was bypassed and must be
        debugged before live trading resumes.
        """
        del row, refs
        raise RuntimeError(
            "_CapitalComEntryHooks.resume_pending_dispatch should not be "
            "called: Capital.com recovery is routed through "
            "_CapitalComResumeHooks. Check the hooks_for() provider in "
            "_recover_in_flight_submissions."
        )
