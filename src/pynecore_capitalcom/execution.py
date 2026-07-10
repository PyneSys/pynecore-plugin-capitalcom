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
from time import time as epoch_time
from typing import TYPE_CHECKING, Callable

import httpx

from pynecore.core.broker.exceptions import (
    BracketAttachAfterFillRejectedError,
    BrokerError,
    ExchangeCapabilityError,
    ExchangeConnectionError,
    ExchangeOrderRejectedError,
    InsufficientMarginError,
    OrderDispositionUnknownError,
    OrderSkippedByPlugin,
)
from pynecore.core.broker.idempotency import (
    KIND_CANCEL,
    KIND_CLOSE,
    KIND_ENTRY,
    KIND_ENTRY_STOP,
    KIND_EXIT_SL,
    KIND_EXIT_TP,
    KIND_MODIFY_ENTRY,
    KIND_MODIFY_EXIT,
)
from pynecore.core.broker.emulator import aggregate_positions
from pynecore.core.broker.journal import DispatchJournal
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
)
from pynecore.core.broker.models import (
    CancelDispositionOutcome,
    CancelIntent,
    CloseIntent,
    DispatchEnvelope,
    EntryIntent,
    ExchangeOrder,
    ExchangePosition,
    ExitIntent,
    OrderStatus,
    OrderType,
    PositionLeg,
)
from pynecore.core.plugin import override

from ._base import _CapitalComBase
from .exceptions import (
    CapitalComError,
    InvalidStopDistanceError,
    InvalidTakeProfitDistanceError,
    OrderNotFoundError,
)
from .helpers import (
    _extract_reject_reason,
    _is_funds_reject,
    _parse_iso_timestamp,
    _size_from_units,
)
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
        return _size_from_units(units, rules.lot_step)
    async def get_position(self, symbol: str) -> ExchangePosition | None:
        """Return the aggregate position across all rows for ``symbol``.

        Capital.com netting opens a fresh row per same-direction entry
        (confirmed empirically after §9 #5 is closed) — aggregation is
        therefore mandatory for Pine's one-way model. Returns ``None``
        when no row exists for the symbol.

        On a hedging-mode account the rows are netted through the core
        :func:`~pynecore.core.broker.emulator.aggregate_positions` instead:
        that virtual-FIFO survivor view is what the
        :class:`~pynecore.core.broker.one_way_emulator.OneWayEmulator`'s
        close/reversal plans assume, whereas the netting branch below
        computes a gross same-side average (fine when opposite rows cannot
        coexist).
        """
        if self._hedging_enabled:
            return aggregate_positions(
                symbol, await self.fetch_raw_positions(symbol),
            )
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

    async def fetch_raw_positions(self, symbol: str) -> list[PositionLeg]:
        """Return every open position row ("leg") for ``symbol``, oldest first.

        :class:`~pynecore.core.plugin.broker.PositionPort` transport
        primitive: one :class:`PositionLeg` per Capital.com position row
        with ZERO aggregation — the core emulator nets them. Sorted by
        ``(open_time, leg_id)`` so the FIFO close order is deterministic
        and replay-stable across polls (``createdDateUTC`` has millisecond
        resolution; the dealId tiebreak covers same-millisecond fills).
        """
        res = await self._call('positions', method='get')
        rows = res.get('positions') or []
        legs: list[PositionLeg] = []
        for row in rows:
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
                symbol=symbol,
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

    async def get_volume_quantizer(self, symbol: str) -> 'Callable[[float], int]':
        """Return a sync Pine-units -> lot-step-grid quantizer for ``symbol``.

        :class:`~pynecore.core.plugin.broker.PositionPort` transport
        primitive: a closure capturing the cached ``lot_step`` so the core
        emulator can snap per-leg volumes in a tight loop without an await
        per call. The grid unit is the lot-step count;
        :func:`~pynecore_capitalcom.helpers._size_from_units` converts it
        back to a JSON-safe ``size`` on the wire.
        """
        rules = await self._get_instrument_rules(symbol)
        # ``_fetch_market`` clamps ``lot_step`` positive (0.01 floor), so
        # the closure can divide unconditionally.
        step = rules.lot_step
        return lambda qty: round(qty / step)

    async def reject_out_of_range(
            self, envelope: DispatchEnvelope, qty: float,
    ) -> None:
        """Raise the non-halting volume-bounds skip when ``qty`` is out of range.

        :class:`~pynecore.core.plugin.broker.PositionPort` transport
        primitive: core's reversal pre-flights the residual size through
        this before any leg close lands, so an out-of-range reversal skips
        while that is still true (never leaving the book half-reduced).
        """
        intent = envelope.intent
        assert isinstance(intent, EntryIntent)
        rules = await self._get_instrument_rules(intent.symbol)
        self._reject_out_of_range_entry(intent, rules, qty)

    async def place_leg(
            self, envelope: DispatchEnvelope, qty: float,
    ) -> list[ExchangeOrder]:
        """Open ONE order of ``qty`` (Pine units) for the envelope's entry.

        :class:`~pynecore.core.plugin.broker.PositionPort` transport
        primitive — the residual leg of a reversal, or a plain add. Same
        persist-first journaled dispatch as :meth:`execute_entry`; only the
        size differs from ``intent.qty``.
        """
        return await self._place_entry_order(envelope, qty)

    #: ``DELETE /positions/{dealId}`` is full-row only — it silently ignores
    #: any ``size`` parameter, body or query (measured on demo, 2026-07-10) —
    #: so the core emulator must never plan a partial leg slice. A fractional
    #: ``strategy.close(qty=...)`` on a hedging account becomes a loud
    #: non-halting skip; partial closes need a one-way (netting) account.
    supports_partial_leg_close = False

    async def close_leg(
            self, symbol: str, leg_id: str, volume: int, coid: str,
    ) -> None:
        """Close ONE broker position row (``leg_id`` is its dealId).

        :class:`~pynecore.core.plugin.broker.PositionPort` transport
        primitive. ``volume`` always covers the whole leg (see
        :attr:`supports_partial_leg_close`); ``coid`` never reaches the
        wire — the DELETE carries no client reference — idempotency is
        owned by the core persist-first close-leg ledger. The resulting
        fill surfaces through the activity stream as an ordinary natural
        close (``LegType.CLOSE`` on the tracked dealId).

        The error taxonomy deliberately differs from the netting
        full-close hook: an ambiguous transport fault maps to
        :class:`OrderDispositionUnknownError`, NOT
        :class:`BrokerManualInterventionError` — the emulator's close-leg
        ledger + restart replay reconcile the leg against the live book
        and re-dispatch only the residual, so the in-session
        unverifiability that forces the netting path to halt does not
        apply. A vanished leg is a benign no-op (the close already landed
        or the row was swept broker-side; the replay finalises the row).
        """
        # ``volume`` is audit-only here: full-leg by contract, and a size is
        # not expressible on the DELETE wire anyway.
        store_ctx = self.store_ctx
        entry_row = (
            store_ctx.find_by_ref('deal_id', leg_id)
            if store_ctx is not None else None
        )
        try:
            await self._call(f'positions/{leg_id}', method='delete')
        except OrderNotFoundError:
            if store_ctx is not None:
                store_ctx.log_event(
                    'close_already_gone',
                    client_order_id=(
                        entry_row.client_order_id if entry_row else coid
                    ),
                    exchange_order_id=leg_id,
                )
            return
        except ExchangeConnectionError as net:
            # ``_call`` maps httpx transport faults to
            # ``ExchangeConnectionError`` centrally; at THIS dispatch site a
            # DELETE was in flight, so the leg's fate is genuinely unknown.
            raise OrderDispositionUnknownError(
                f"Capital DELETE positions/{leg_id} ({symbol}) ambiguous "
                f"during one-way leg close: {net}",
                client_order_id=coid,
                cause=net,
            ) from net
        # Bookkeeping AFTER the wire call, mirroring the netting full-close
        # hook: the core close-leg ledger row (written persist-first by the
        # emulator before this call) owns the crash window, so the entry
        # row's ``closing`` mirror only needs to reflect a dispatch that
        # actually went out.
        if store_ctx is not None and entry_row is not None:
            store_ctx.set_order_state(entry_row.client_order_id, 'closing')
            store_ctx.log_event(
                'close_dispatched',
                client_order_id=entry_row.client_order_id,
                exchange_order_id=leg_id,
                payload={'leg_volume': volume, 'leg_coid': coid},
            )

    async def amend_bracket(
            self, symbol: str, leg_id: str, *,
            side: str,
            tp_price: float | None,
            sl_price: float | None,
            trail_offset: float | None,
            coid: str,
    ) -> None:
        """Replicate (or clear) a protective bracket on ONE position row.

        :class:`~pynecore.core.plugin.broker.PositionPort` transport
        primitive. Capital.com ``PUT /positions/{dealId}`` is *full
        replacement* (verified on demo 2026-05-29): every bracket field is
        sent explicitly — a float to arm, JSON ``null`` / ``false`` to
        clear — so the all-``None`` call clears the whole bracket instead
        of no-oping on an empty body. A ``trail_offset`` maps to the
        native ``trailingStop`` + ``stopDistance`` pair and takes the stop
        slot: Capital.com carries ONE stop per row, so a coexisting fixed
        ``sl_price`` is superseded, and the netting path's deferred
        ``trail_price`` activation is not expressible per-leg — the
        trailing is live immediately.

        Idempotent on ``coid``: an unchanged re-amend is a broker-level
        no-op (full replacement with identical values). A vanished leg
        (``error.not-found.dealId``) is a benign return; a definitive
        reject raises :class:`ExchangeOrderRejectedError` (on the attach
        path the core emulator wraps it into
        :class:`BracketAttachAfterFillRejectedError` and flattens
        defensively); an ambiguous PUT or confirm round-trip raises
        :class:`OrderDispositionUnknownError` so the ownership row stays
        replayable.

        ``side`` is audit-only — Capital.com levels are absolute prices on
        the row; no side-dependent anchor is needed (cTrader seeds its
        trailing anchor from it).
        """
        rules = await self._get_instrument_rules(symbol)
        # Same live-mid pre-check as the netting bracket paths: turns an
        # obviously-rejectable level into the typed distance error before
        # the round-trip. Distance violations subclass
        # ``ExchangeOrderRejectedError``, so the attach path still ends in
        # the defensive flatten rather than a halt.
        mid_price = await self._get_current_mid_price(symbol)
        if sl_price is not None:
            self._validate_sl_distance(rules, mid_price, float(sl_price))
        if tp_price is not None:
            self._validate_tp_distance(rules, mid_price, float(tp_price))

        body: dict = {
            'profitLevel': float(tp_price) if tp_price is not None else None,
        }
        if trail_offset is not None:
            body['trailingStop'] = True
            body['stopDistance'] = float(trail_offset)
        else:
            body['trailingStop'] = False
            body['stopLevel'] = (
                float(sl_price) if sl_price is not None else None
            )

        try:
            resp = await self._call(
                f'positions/{leg_id}', data=body, method='put',
            )
        except OrderNotFoundError:
            # Leg vanished between the emulator's fetch and this amend —
            # the bracket it would carry is moot.
            return
        except ExchangeConnectionError as net:
            raise OrderDispositionUnknownError(
                f"Capital PUT positions/{leg_id} ({symbol}) ambiguous during "
                f"one-way bracket amend: {net}",
                client_order_id=coid,
                cause=net,
            ) from net
        except CapitalComError as exc:
            # An unmapped Capital.com API error on the PUT is a definitive
            # refusal of THIS amend — classify it so the attach path runs
            # the defensive flatten instead of escaping as an untyped
            # provider error (which the engine's write-side net would
            # escalate to a halt).
            raise ExchangeOrderRejectedError(
                f"Capital PUT positions/{leg_id} bracket amend refused: {exc}",
            ) from exc

        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'one_way_bracket_amend',
                client_order_id=coid,
                exchange_order_id=leg_id,
                payload={
                    'side': side,
                    'tp_price': tp_price,
                    'sl_price': sl_price,
                    'trail_offset': trail_offset,
                },
            )
        deal_ref = resp.get('dealReference') if isinstance(resp, dict) else None
        if not deal_ref:
            # No dealReference to confirm — treat the round-trip as success;
            # the reconcile pass verifies the observed levels later. Mirrors
            # ``publish_native_failsafe_sl``.
            return
        try:
            confirm = await self._call(f'confirms/{deal_ref}', method='get')
        except (OrderNotFoundError, ExchangeConnectionError) as net:
            # The PUT went out but its outcome cannot be verified — leave
            # the ownership row replayable rather than guessing.
            raise OrderDispositionUnknownError(
                f"Capital confirms/{deal_ref} ambiguous after one-way "
                f"bracket amend on {leg_id} ({symbol}): {net}",
                client_order_id=coid,
                cause=net,
            ) from net
        if (confirm.get('dealStatus') or '').upper() == 'REJECTED':
            reason = _extract_reject_reason(confirm)
            raise ExchangeOrderRejectedError(
                f"Capital PUT positions/{leg_id} bracket amend REJECTED "
                f"(coid={coid}): {reason}",
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
        return await self._place_entry_order(envelope, intent.qty)

    def _reject_out_of_range_entry(
            self, intent: EntryIntent, rules: _InstrumentRules, qty: float,
    ) -> None:
        """Raise the non-halting :class:`OrderSkippedByPlugin` when ``qty``
        is outside the instrument's tradable size bounds.

        Preserves the historical asymmetry: the minimum gates the RAW
        requested qty (rounding an undersized request up to the grid would
        be silent up-inflation), the maximum gates the QUANTIZED qty —
        i.e. what actually reaches the broker. Capital.com rejects sizes
        above ``maxDealSize`` with ``error.invalid.size.maxvalue``, which
        surfaces as a fatal :class:`ExchangeOrderRejectedError` and would
        halt the live run — pre-empting both bounds here turns them into
        logged skips (no silent clamp; the size is the caller's concern).
        """
        if rules.min_size > 0 and qty < rules.min_size:
            raise OrderSkippedByPlugin(
                f"Skipping {intent.symbol} {intent.side.upper()} entry "
                f"id={intent.pine_id!r}: qty={qty} below Capital.com "
                f"minimum size {rules.min_size}. No order sent.",
                intent_key=intent.intent_key,
                reason="below_min_size",
                context={
                    'symbol': intent.symbol,
                    'side': intent.side,
                    'qty': qty,
                    'min_size': rules.min_size,
                },
            )
        quantized_qty = self._quantize_size(qty, rules)
        if 0 < rules.max_size < quantized_qty:
            raise OrderSkippedByPlugin(
                f"Skipping {intent.symbol} {intent.side.upper()} entry "
                f"id={intent.pine_id!r}: qty={quantized_qty} above Capital.com "
                f"maximum size {rules.max_size}. No order sent.",
                intent_key=intent.intent_key,
                reason="above_max_size",
                context={
                    'symbol': intent.symbol,
                    'side': intent.side,
                    'qty': quantized_qty,
                    'max_size': rules.max_size,
                },
            )

    async def _place_entry_order(
            self, envelope: DispatchEnvelope, qty: float,
    ) -> list[ExchangeOrder]:
        """Shared journaled entry dispatch for ``execute_entry`` / ``place_leg``.

        ``qty`` is the size to place — ``intent.qty`` on the plain entry
        path, the emulator-supplied residual on the ``place_leg`` path;
        everything else (coid derivation, bounds skip, quantize, endpoint /
        body construction, persist-first :class:`DispatchJournal` flow) is
        identical.
        """
        intent = envelope.intent
        assert isinstance(intent, EntryIntent)
        # The stop-fired MARKET of a both-set entry uses a distinct
        # client-order-id from the native LIMIT leg (which already holds the
        # KIND_ENTRY id for the same pine_id) — otherwise the broker's local
        # idempotency dedup would treat the market POST as a duplicate of the
        # just-cancelled limit and skip it.
        coid = envelope.client_order_id(
            KIND_ENTRY_STOP if intent.stop_fired_market else KIND_ENTRY,
        )

        rules = await self._get_instrument_rules(intent.symbol)
        self._reject_out_of_range_entry(intent, rules, qty)
        quantized_qty = self._quantize_size(qty, rules)
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
            # Defense-in-depth: an open exchange-side position with no
            # findable BrokerStore entry row is structurally identical to
            # "bracket attach rejected after parent fill" — the fill is on
            # the exchange and unprotected. Raising a plain
            # ``ExchangeOrderRejectedError`` here halts the bot and leaves
            # the position exposed; instead, surface the rich
            # ``BracketAttachAfterFillRejectedError`` so the sync engine's
            # ``_handle_bracket_attach_after_fill_reject`` flattens the
            # position with a defensive market close and the runner
            # continues. The mismatch typically follows a startup-orphan
            # race (engine in-memory anchor cache stale-pop after the
            # retire pass) or a manual BrokerStore wipe with a live
            # position still on the exchange.
            #
            # CRITICAL: ``get_position`` returns the *symbol-level
            # aggregate*, not proof that the open position belongs to
            # this ``from_entry``. The defensive ``CloseIntent`` routes
            # through :meth:`execute_close`, which sweeps *every*
            # confirmed position row for the symbol. If a sibling entry
            # under the same symbol has a live persisted row, the
            # recovery would flatten the sibling too — silently closing
            # a position that has nothing to do with the orphan exit.
            # Only attempt the defensive close when no other live
            # confirmed position row exists for the symbol; otherwise
            # the symbol-aggregate is ambiguous and we must fall back
            # to the plain reject (operator intervention).
            has_sibling_position = False
            if self.store_ctx is not None:
                for row in self.store_ctx.iter_live_orders(
                        symbol=intent.symbol,
                ):
                    if row.state != 'confirmed':
                        continue
                    if not row.exchange_order_id:
                        continue
                    extras = row.extras or {}
                    if extras.get('kind') != 'position':
                        continue
                    if extras.get('natural_close_at') is not None:
                        continue
                    has_sibling_position = True
                    break
            if not has_sibling_position:
                open_pos = await self.get_position(intent.symbol)
                # ``get_position`` returns the symbol-level aggregate, with no
                # link to ``from_entry``. Only raise the defensive close when
                # the aggregate side actually matches the side the exit intent
                # would flatten — ``intent.side='sell'`` closes a long,
                # ``='buy'`` closes a short. Otherwise the sync engine's
                # follow-up :meth:`execute_close` would buy/sell against an
                # unrelated opposite-direction position on the same symbol.
                expected_pos_side = 'long' if intent.side == 'sell' else 'short'
                if (open_pos is not None and open_pos.size > 0
                        and open_pos.side == expected_pos_side):
                    pos_side = (
                        'buy' if open_pos.side == 'long' else 'sell'
                    )
                    # No real ``client_order_id`` exists for the orphan
                    # position; synthesise one that is stable across retries
                    # (symbol + from_entry) so the defensive ``CloseIntent``'s
                    # ``pine_id`` does not collide with a real Pine id and
                    # repeated cycles converge on the same envelope.
                    surrogate_coid = (
                        f"__pyne_orphan__{intent.symbol}__{intent.from_entry}"
                    )
                    # The exchange-side deal id is only used for log
                    # correlation in this branch; the defensive close routes
                    # through ``execute_close`` which re-derives the dealId(s)
                    # from a fresh ``positions`` snapshot.
                    raise BracketAttachAfterFillRejectedError(
                        f"Capital execute_exit: no confirmed entry row in "
                        f"BrokerStore for from_entry={intent.from_entry!r} "
                        f"symbol={intent.symbol!r} but exchange reports an "
                        f"open {open_pos.side} position of size "
                        f"{open_pos.size} — defensive close required",
                        position_deal_id=surrogate_coid,
                        position_coid=surrogate_coid,
                        symbol=intent.symbol,
                        position_side=pos_side,
                        qty=float(open_pos.size),
                        from_entry=intent.from_entry,
                    )
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
                    reason = _extract_reject_reason(attach_confirm)
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
                extras = row.extras or {}
                # Skip rows already stamped as naturally closed (TP/SL hit
                # or bracket-attach defensive close). The row stays in the
                # live range (``closed_ts_ms IS NULL``) so ``find_by_ref``
                # can still match the close-leg activity, but its
                # ``exchange_order_id`` already refers to a deal the broker
                # has dropped from ``/positions``. Treating it as a target
                # would route the DELETE at a stale ``dealId`` (a 404 is
                # then read as "already gone" by the close path) while the
                # *actual* open orphan position the defensive close was
                # meant to flatten never gets reconstructed below and stays
                # open and unprotected. Same skip convention as
                # :meth:`execute_exit`'s sibling-position scan and the
                # reconciler's missing-pending grace-window logic.
                if (row.state == 'confirmed'
                        and row.exchange_order_id
                        and extras.get('kind') == 'position'
                        and extras.get('natural_close_at') is None):
                    targets.append(row)

        # Orphan-close fallback: when the sync engine issues a defensive
        # ``CloseIntent`` (``pine_id`` starts with ``__pyne_defensive_close__``)
        # for a position that has no persisted store row — typically a
        # ``BracketAttachAfterFillRejectedError`` raised from
        # :meth:`execute_exit` after a missing-entry-row detection on a
        # live exchange position — the store-driven target derivation
        # above yields nothing and a plain
        # :class:`ExchangeOrderRejectedError` would halt the engine while
        # the exchange position stays open AND unprotected. Reconstruct
        # the missing target(s) from a fresh broker-side ``positions``
        # snapshot, materialise a minimal ``position``-kind ``OrderRow``
        # per matching exchange row so the journal's bookkeeping
        # (``set_order_state('closing')``, ``mark_closing``, activity
        # stream promotion to ``closed``) stays intact, and proceed with
        # the normal DELETE chain. Filter by the close side
        # (``CloseIntent.side='sell'`` flattens a long, ``='buy'``
        # flattens a short) so a defensive close cannot accidentally
        # sweep an opposite-direction unrelated position on the same
        # symbol.
        if (not targets
                and self.store_ctx is not None
                and intent.pine_id.startswith('__pyne_defensive_close__')):
            raw_snap = await self._call('positions', method='get')
            target_broker_dir = 'BUY' if intent.side == 'sell' else 'SELL'
            # Recover the original Pine entry id from the defensive
            # ``pine_id``. The sync engine builds the defensive close as
            # ``__pyne_defensive_close__{position_coid}`` and in the
            # missing-entry orphan path this plugin supplies
            # ``position_coid = __pyne_orphan__{symbol}__{from_entry}``
            # (see :meth:`execute_exit`'s ``BracketAttachAfterFillRejectedError``
            # raise site). Carry that ``from_entry`` onto the synthetic
            # row so that when the close-leg activity later builds
            # ``OrderEvent.pine_id`` from ``row.pine_entry_id``, the
            # engine's :meth:`_cleanup_closed_position` can drop the
            # stale Pine entry/exit order book entries — without it the
            # next sync would re-emit the same ``strategy.exit`` and
            # spin through another orphan defensive-close cycle.
            parent_from_entry: str | None = None
            orphan_surrogate_prefix = (
                f"__pyne_defensive_close____pyne_orphan__"
                f"{intent.symbol}__"
            )
            if intent.pine_id.startswith(orphan_surrogate_prefix):
                candidate = intent.pine_id[len(orphan_surrogate_prefix):]
                if candidate:
                    parent_from_entry = candidate
            # Require an unambiguous snapshot before the defensive close can
            # touch anything on the exchange. ``intent.qty`` was seeded from
            # :meth:`get_position`'s symbol-level aggregate (see the
            # ``BracketAttachAfterFillRejectedError`` raise site in
            # :meth:`execute_exit`), so it is the *sum* of every same-side
            # row, not the size of any one row. With two same-side rows of
            # equal size (orphan + manual deal, or two bots on the same
            # symbol), a unit-cap that only forbids overshoot would still
            # accept the full snapshot — both rows together equal
            # ``intent.qty`` exactly — and DELETE the unrelated row alongside
            # ours. The only safe condition is exactly one matching
            # same-side exchange row whose size also matches the aggregate;
            # any other shape is inherently ambiguous and must halt so the
            # operator can investigate, rather than guessing which deal we
            # own.
            intent_units = (
                round(intent.qty / rules.lot_step) if rules.lot_step > 0 else 0
            )
            # First pass: discover matching same-side rows without
            # persisting anything. Persisting synthetic rows and only later
            # clearing ``targets`` on an ambiguity breach would leave the
            # earlier ``upsert_order`` writes in BrokerStore; on the next
            # retry/restart the normal target scan above would pick them up,
            # skip this orphan path entirely, and DELETE a subset of an
            # ambiguous snapshot. Collect candidate rows here, reject on
            # ambiguity, and only persist after the snapshot is proven safe.
            candidates: list[tuple[str, str, float, int]] = []
            for raw in (raw_snap.get('positions') or []):
                market = raw.get('market') or {}
                if market.get('epic') != intent.symbol:
                    continue
                position = raw.get('position') or {}
                direction = (position.get('direction') or '').upper()
                if direction != target_broker_dir:
                    continue
                deal_id = position.get('dealId')
                size = float(position.get('size', 0.0))
                if not deal_id or size <= 0.0:
                    continue
                row_units = (
                    round(size / rules.lot_step) if rules.lot_step > 0 else 0
                )
                candidates.append((direction, str(deal_id), size, row_units))

            # Ambiguity guard: only proceed when exactly one matching
            # exchange row exists. Multiple same-side rows mean we cannot
            # tell which deal is the bot's orphan vs. an unrelated position
            # (manual deal, second bot, or a same-side row opened between
            # the ``BracketAttachAfterFillRejectedError`` raise site and
            # this snapshot). Log and abort; the post-loop empty-targets
            # check escalates to ``ExchangeOrderRejectedError`` so the
            # engine halts with no BrokerStore writes.
            planned: list[tuple[str, str, float]] = []  # (direction, deal_id, size)
            ambiguous = len(candidates) > 1
            if ambiguous:
                self.store_ctx.log_event(
                    'orphan_close_ambiguous_snapshot',
                    intent_key=intent.intent_key,
                    payload={
                        'symbol': intent.symbol,
                        'broker_direction': target_broker_dir,
                        'intent_qty': intent.qty,
                        'candidate_count': len(candidates),
                        'candidate_deal_ids': [c[1] for c in candidates],
                        'candidate_sizes': [c[2] for c in candidates],
                    },
                )
            elif candidates:
                direction, deal_id, size, row_units = candidates[0]
                # Even with a single row, require an *exact* size match to
                # ``intent.qty``. Any mismatch is ambiguous:
                #   * ``row_units > intent_units`` — the row grew between
                #     the raise site and this snapshot (unrelated deal
                #     bundled into the same dealId, or a manual top-up);
                #     DELETing it would over-close past the orphan.
                #   * ``row_units < intent_units`` — the aggregate
                #     captured at the raise site summed *multiple*
                #     same-side rows, but only one survives by this
                #     snapshot. The missing row(s) may have been the
                #     bot's orphan (closed manually in the meantime),
                #     leaving this lone survivor as an unrelated
                #     position; DELETing it would close someone else's
                #     deal.
                # Only the exact-equality case proves the surviving row
                # is the orphan we set out to flatten. Any other shape
                # falls through to the empty-``planned`` path and the
                # post-loop ``ExchangeOrderRejectedError`` so the
                # operator can investigate.
                if (intent_units and rules.lot_step > 0
                        and row_units != intent_units):
                    self.store_ctx.log_event(
                        'orphan_close_qty_mismatch',
                        intent_key=intent.intent_key,
                        payload={
                            'symbol': intent.symbol,
                            'broker_direction': direction,
                            'intent_qty': intent.qty,
                            'intent_units': intent_units,
                            'row_units': row_units,
                            'deal_id': deal_id,
                        },
                    )
                else:
                    planned.append((direction, deal_id, size))

            # Second pass: persist only after the snapshot proved safe.
            # When the snapshot was ambiguous or capped, ``planned`` is
            # empty and the post-loop ``ExchangeOrderRejectedError`` fires
            # with no BrokerStore writes left behind.
            if planned:
                for direction, deal_id, size in planned:
                    synthetic_coid = (
                        f"__pyne_orphan_close__{intent.symbol}__{deal_id}"
                    )
                    # Seed the synthetic row as a *fully-filled* position so
                    # later activity classification works correctly. The row
                    # represents an already-open exchange position the
                    # defensive close is about to flatten; without
                    # ``filled_qty == qty`` and ``entry_filled_at``,
                    # :meth:`_activity_to_event` would lack the
                    # ``entry_already_filled`` breadcrumb and route the
                    # resulting ``USER`` / ``DEALER`` close activity as a
                    # fresh :class:`LegType.ENTRY` — ``BrokerPosition.record_fill``
                    # would then ADD exposure instead of reducing it.
                    # Same invariants the reconcile loop's working-to-position
                    # promotion path enforces (see :meth:`reconcile.run` →
                    # ``working_to_position`` branch).
                    self.store_ctx.upsert_order(
                        synthetic_coid,
                        symbol=intent.symbol,
                        side='buy' if direction == 'BUY' else 'sell',
                        qty=size,
                        filled_qty=size,
                        state='confirmed',
                        exchange_order_id=deal_id,
                        pine_entry_id=parent_from_entry,
                        from_entry=parent_from_entry,
                        extras={
                            'kind': 'position',
                            'entry_filled_at': epoch_time(),
                            'orphan_close_synthetic': True,
                        },
                    )
                    # Mirror the normal entry path's ``deal_id`` ref so the
                    # activity loop's ``find_by_ref('deal_id', deal_id)`` lookup
                    # in :meth:`CapitalcomActivityLoop._process_activity_batch`
                    # resolves the close-leg activity back to this synthetic
                    # row. Without the ref, the close activity is classified as
                    # external; the row never advances to ``closed``, picks up
                    # ``missing_pending_since`` after the next ``/positions``
                    # poll drops the deal, and the grace window can fire a
                    # false ``UnexpectedCancelError`` even though the defensive
                    # DELETE succeeded.
                    self.store_ctx.add_ref(
                        synthetic_coid, 'deal_id', deal_id,
                    )
                    rebuilt = self.store_ctx.get_order(synthetic_coid)
                    if rebuilt is not None:
                        targets.append(rebuilt)
                    self.store_ctx.log_event(
                        'orphan_close_target_synthesised',
                        client_order_id=synthetic_coid,
                        exchange_order_id=deal_id,
                        intent_key=intent.intent_key,
                        payload={
                            'symbol': intent.symbol,
                            'broker_direction': direction,
                            'size': size,
                        },
                    )

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

        # Synthetic orphan-close rows are seeded with ``filled_qty == qty``
        # so the activity classification breadcrumbs land correctly (see
        # the orphan-fallback branch above). That makes their residual live
        # quantity ``row.qty - row.filled_qty == 0``, which would push the
        # full-vs-partial decision below into :data:`KIND_PARTIAL_CLOSE`
        # and ignore the reconstructed ``dealId``. An orphan defensive
        # close always flattens the entire reconstructed exchange position,
        # so pin the kind to :data:`KIND_FULL_CLOSE` whenever any synthetic
        # orphan row is in play.
        has_orphan_synthetic = any(
            (row.extras or {}).get('orphan_close_synthetic') is True
            for row in targets
        )
        if has_orphan_synthetic:
            kind = KIND_FULL_CLOSE
        else:
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

    @override
    async def execute_cancel_with_outcome(
            self, envelope: DispatchEnvelope,
    ) -> CancelDispositionOutcome:
        """Cancel a pending working order and classify the broker disposition.

        Capital.com working-order cancel is ``DELETE /workingorders/{dealId}``.
        The sync engine's cancel-tentative reconcile loop needs a precise
        disposition rather than the bool-only :meth:`execute_cancel`
        contract, so this override normalizes the wire result into the five
        :class:`CancelDispositionOutcome` categories:

        * DELETE succeeds → :attr:`CancelDispositionOutcome.CANCEL_CONFIRMED`.
        * DELETE 404 (:class:`OrderNotFoundError`): the working order is
          gone, but that alone does not say why. ``GET /history/activity``
          is queried for the same ``dealId`` and the most recent terminal
          row decides — a fill →
          :attr:`CancelDispositionOutcome.ALREADY_FILLED` (the engine must
          restore the bracket legs onto the freshly opened position), a
          prior cancel → :attr:`CancelDispositionOutcome.CANCEL_CONFIRMED`
          (idempotent), an expiry / rejection without fill →
          :attr:`CancelDispositionOutcome.TOO_LATE_TO_CANCEL` (terminal
          non-fill). No terminal row →
          :attr:`CancelDispositionOutcome.UNKNOWN` so the loop retries.
        * Transient network failure →
          :attr:`CancelDispositionOutcome.UNKNOWN` (keep cancel-tentative
          armed).

        Only pending entry working orders enter the cancel-tentative state,
        so bracket-leg and filled-position rows are not classified here; an
        empty working-order target set returns
        :attr:`CancelDispositionOutcome.UNKNOWN` rather than fabricating a
        resolution.

        Aggregation across multiple working-order targets is conservative:
        any ``ALREADY_FILLED`` wins (the engine must restore legs onto a
        position that now exists), then any ``UNKNOWN`` keeps the loop
        armed, then ``TOO_LATE_TO_CANCEL`` / ``CANCEL_CONFIRMED`` (both
        resolve as a confirmed cancel engine-side).
        """
        intent = envelope.intent
        assert isinstance(intent, CancelIntent)

        if self.store_ctx is None:
            return CancelDispositionOutcome.UNKNOWN

        targets: list[tuple[str, str]] = []
        for row in self.store_ctx.iter_live_orders(
                symbol=intent.symbol,
                from_entry=intent.from_entry,
        ):
            matches_pine = row.pine_entry_id == intent.pine_id or (
                intent.from_entry is not None
                and row.from_entry == intent.from_entry
            )
            if not matches_pine:
                continue
            extras = row.extras or {}
            if extras.get('leg_kind') in ('tp', 'sl'):
                continue
            if extras.get('kind', ENTRY_KIND_POSITION) != ENTRY_KIND_WORKING:
                continue
            if row.exchange_order_id:
                targets.append((row.client_order_id, row.exchange_order_id))

        if not targets:
            return CancelDispositionOutcome.UNKNOWN

        outcomes: list[CancelDispositionOutcome] = []
        for coid, deal_id in targets:
            try:
                await self._call(
                    f'workingorders/{deal_id}', method='delete',
                )
            except (httpx.TimeoutException, httpx.RequestError,
                    ConnectionError, ExchangeConnectionError):
                # Transient — keep the row live so the retry loop re-runs.
                outcomes.append(CancelDispositionOutcome.UNKNOWN)
                continue
            except OrderNotFoundError:
                resolved = await self._classify_cancel_via_activity(deal_id)
                self._finalize_cancel_outcome_row(
                    coid, deal_id, resolved, intent=intent,
                    already_gone=True,
                )
                outcomes.append(resolved)
                continue
            # Clean DELETE: the broker order is gone. Close and audit the
            # live working row here — the sync engine's cancel-tentative
            # resolution only drops in-memory mappings / leg ledger state,
            # never the persisted ``OrderRow``. Leaving it live would make
            # the next ``_reconcile_snapshot`` see a row whose deal is
            # absent from both ``/positions`` and ``/workingorders``, stamp
            # ``missing_pending_since``, and eventually raise a false
            # :class:`UnexpectedCancelError`.
            self._finalize_cancel_outcome_row(
                coid, deal_id, CancelDispositionOutcome.CANCEL_CONFIRMED,
                intent=intent, already_gone=False,
            )
            outcomes.append(CancelDispositionOutcome.CANCEL_CONFIRMED)

        if CancelDispositionOutcome.ALREADY_FILLED in outcomes:
            return CancelDispositionOutcome.ALREADY_FILLED
        if CancelDispositionOutcome.UNKNOWN in outcomes:
            return CancelDispositionOutcome.UNKNOWN
        if CancelDispositionOutcome.TOO_LATE_TO_CANCEL in outcomes:
            return CancelDispositionOutcome.TOO_LATE_TO_CANCEL
        return CancelDispositionOutcome.CANCEL_CONFIRMED

    def _finalize_cancel_outcome_row(
            self, coid: str, deal_id: str,
            outcome: CancelDispositionOutcome, *,
            intent: CancelIntent, already_gone: bool,
    ) -> None:
        """Close and audit a working row whose cancel resolved terminally.

        Mirrors the ``_CapitalComCancelHooks`` working-order sweep so the
        :meth:`execute_cancel_with_outcome` retry path leaves the same
        audit trail and never strands a live :class:`OrderRow` whose deal
        is gone broker-side.

        Only terminal-cancel outcomes close the row:

        * :attr:`CancelDispositionOutcome.CANCEL_CONFIRMED` /
          :attr:`CancelDispositionOutcome.TOO_LATE_TO_CANCEL`: the working
          order is terminally gone without a fill — close the row and log
          ``cancel_already_gone`` (404 path) then ``cancelled``.
        * :attr:`CancelDispositionOutcome.ALREADY_FILLED`: the order became
          a position; the activity reconcile needs the live row to record
          the fill, so leave it untouched (same rule as the hook never
          closing a filled position from a cancel).
        * :attr:`CancelDispositionOutcome.UNKNOWN`: keep the row live so
          the cancel-tentative loop retries.

        :param coid: The working row's ``client_order_id``.
        :param deal_id: The working order's exchange ``dealId``.
        :param outcome: The resolved cancel disposition for this row.
        :param intent: The originating cancel intent (for ``intent_key``).
        :param already_gone: ``True`` when the DELETE returned 404 (the
            broker had already removed the order), driving the extra
            ``cancel_already_gone`` audit event.
        """
        if self.store_ctx is None:
            return
        if outcome not in (
                CancelDispositionOutcome.CANCEL_CONFIRMED,
                CancelDispositionOutcome.TOO_LATE_TO_CANCEL,
        ):
            return
        if already_gone:
            self.store_ctx.log_event(
                'cancel_already_gone',
                client_order_id=coid,
                exchange_order_id=deal_id,
                intent_key=intent.intent_key,
            )
        self.store_ctx.close_order(coid)
        self.store_ctx.log_event(
            'cancelled',
            client_order_id=coid,
            exchange_order_id=deal_id,
            intent_key=intent.intent_key,
            payload={'via': 'cancel_with_outcome',
                     'outcome': outcome.value},
        )

    async def _classify_cancel_via_activity(
            self, deal_id: str,
    ) -> CancelDispositionOutcome:
        """Disambiguate a vanished working order via ``GET /history/activity``.

        Called when ``DELETE /workingorders/{dealId}`` returns 404: the
        order is gone, and the most recent terminal activity row for
        ``dealId`` decides whether it filled, was already cancelled, or
        expired / rejected without filling. Returns
        :attr:`CancelDispositionOutcome.UNKNOWN` when no terminal row is
        found (the reconcile loop retries) or the history fetch fails
        transiently.

        :param deal_id: The working order's exchange ``dealId``.
        :return: Normalized cancel disposition outcome.
        """
        try:
            resp = await self._call(
                'history/activity',
                data={'lastPeriod': 60, 'detailed': 'true'},
                method='get',
            )
        except (httpx.TimeoutException, httpx.RequestError,
                ConnectionError, ExchangeConnectionError):
            return CancelDispositionOutcome.UNKNOWN

        matching = [
            a for a in (resp.get('activities') or [])
            if str(a.get('dealId') or '') == str(deal_id)
        ]
        matching.sort(
            key=lambda a: _parse_iso_timestamp(a.get('dateUTC') or ''),
            reverse=True,
        )
        for activity in matching:
            status = (activity.get('status') or '').upper()
            activity_type = (activity.get('type') or '').upper()
            # A fill requires positive evidence of an opened position or an
            # executed working order. A bare ``ACCEPTED`` on a
            # ``WORKING_ORDER`` row is the broker acknowledging the order's
            # creation/amend, NOT a fill (mirrors ``activity.py``'s
            # ``WORKING_ORDER`` + ``ACCEPTED``/``CREATED`` → ``created``
            # routing). Treating it as ``ALREADY_FILLED`` would make the
            # sync engine restore bracket legs onto a parent that never
            # opened. Only ``POSITION`` activity (a position came into
            # existence) or a ``WORKING_ORDER`` ``EXECUTED`` row counts.
            position_fill = activity_type == 'POSITION' and status in (
                'EXECUTED', 'ACCEPTED', 'FILLED',
            )
            working_fill = (
                activity_type == 'WORKING_ORDER'
                and status in ('EXECUTED', 'FILLED')
            )
            if position_fill or working_fill:
                return CancelDispositionOutcome.ALREADY_FILLED
            if status in ('CANCELLED', 'DELETED'):
                return CancelDispositionOutcome.CANCEL_CONFIRMED
            if status in ('EXPIRED', 'REJECTED'):
                return CancelDispositionOutcome.TOO_LATE_TO_CANCEL
        return CancelDispositionOutcome.UNKNOWN

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
            # TP removal: Capital.com ``PUT /positions/{dealId}`` is
            # full-replacement for bracket attributes — unmentioned
            # ``stopLevel`` / ``profitLevel`` / ``trailingStop`` fields
            # are CLEARED, not preserved (verified 2026-05-25, partial-
            # qty bracket exit plan §9 #19 Exp D2). The explicit ``null``
            # here makes the clearing intent visible in the wire payload
            # so this branch is robust to body-construction reordering
            # and mirrors the cancel-bracket-leg flow.
            body['profitLevel'] = None
        if new_i.sl_price is not None:
            body['stopLevel'] = float(new_i.sl_price)
        elif (old_i.sl_price is not None
                and new_i.trail_offset is None):
            # Fixed SL removal: Capital.com ``PUT /positions/{dealId}``
            # is full-replacement for bracket attributes — unmentioned
            # ``stopLevel`` / ``profitLevel`` / ``trailingStop`` fields
            # are CLEARED, not preserved (verified 2026-05-25, partial-
            # qty bracket exit plan §9 #19 Exp D2). The explicit ``null``
            # here makes the clearing intent visible in the wire payload
            # and mirrors the TP-removal branch + the cancel-bracket-leg
            # flow. The ``trail_offset is None`` guard avoids overwriting
            # a fixed-SL → trailing transition: ``trailingStop=True`` +
            # ``stopDistance`` below replace the prior fixed stop
            # atomically.
            body['stopLevel'] = None
        if new_i.trail_offset is not None and not trail_pending:
            body['trailingStop'] = True
            body['stopDistance'] = float(new_i.trail_offset)
        elif (old_i.trail_offset is not None
                and old_i.trail_price is None
                and new_i.trail_offset is None):
            # Native-trailing removal: Capital.com ``PUT /positions/{dealId}``
            # is full-replacement (§9 #19 Exp D2) — an absent
            # ``trailingStop`` field would be cleared anyway, but we
            # send the explicit ``False`` so the wire payload reflects
            # intent and stays stable across body-construction
            # reordering. Covers both transitions:
            #   * trail → fixed SL: without this explicit clear the
            #     leg row records a fixed stop while the broker fill
            #     would still be classified by :meth:`_activity_to_event`
            #     based on the actual trailing flag at fill time —
            #     redundant ``trailingStop: False`` here keeps the wire
            #     state aligned with the leg row.
            #   * trail → no stop at all (e.g. ``TP+trail`` → ``TP``):
            #     local state shows the position unprotected, the wire
            #     payload makes the trailing removal explicit.
            body['trailingStop'] = False
        if trail_pending and (
                old_i.sl_price is not None
                or (old_i.trail_offset is not None
                    and old_i.trail_price is None)):
            # Transitioning to pending trailing from a broker-side
            # active stop (fixed SL or immediate native trailing).
            # Capital.com ``PUT /positions/{dealId}`` is full-replacement
            # (§9 #19 Exp D2): unmentioned bracket fields are cleared, so
            # leaving the body untouched here happens to remove the prior
            # protective leg already — but the wire payload would not
            # reflect the engine's clearing intent. Send an explicit
            # clear: ``stopLevel: null`` removes the fixed stop (matches
            # the cancel-bracket-leg flow), and ``trailingStop: false``
            # disables the native trailing flag if it was set. We only
            # null out ``stopLevel`` when the new intent did not also
            # specify a fixed SL (Pine's pending trail and a fixed SL
            # are mutually exclusive in practice).
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
            reason = _extract_reject_reason(confirm)
            if _is_funds_reject(reason):
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
