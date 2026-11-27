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
import sqlite3
from abc import ABC
from collections.abc import Sequence
from dataclasses import replace
from time import time as epoch_time
from typing import TYPE_CHECKING, Callable, cast

import httpx

from pynecore.core.broker.exceptions import (
    BracketAttachAfterFillRejectedError,
    BrokerError,
    ExchangeConnectionError,
    ExchangeOrderRejectedError,
    MarketClosedError,
    InsufficientMarginError,
    OrderDispositionUnknownError,
    OrderSkippedByPlugin,
)
from pynecore.core.broker.idempotency import (
    build_client_order_id,
    encode_wire_client_order_id,
    KIND_CANCEL,
    KIND_CLOSE,
    KIND_ENTRY,
    KIND_ENTRY_TRIGGERED,
    KIND_EXIT_SL,
    KIND_EXIT_TP,
    KIND_MODIFY_ENTRY,
    KIND_MODIFY_EXIT,
)
from pynecore.core.broker.emulator import aggregate_positions
from pynecore.core.broker.journal import DispatchJournal
from pynecore.core.broker.one_way_emulator import close_leg_row_client_order_id
from pynecore.core.broker.store_helpers import (
    CLOSE_LEG_STATE_PENDING,
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    EXTRAS_KEY_CLOSE_LEG_ID,
    EXTRAS_KEY_LEG_KIND,
    KIND_FULL_CLOSE,
    KIND_PARTIAL_CLOSE,
    PENDING_DISPATCH_STATES,
    STATE_DISPOSITION_UNKNOWN,
    mark_disposition_unknown,
    update_close_leg_state,
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
    INTENT_KEY_SEP,
    OrderStatus,
    OrderType,
    PositionLeg,
)
from pynecore.core.plugin import override
from pynecore.lib.log import broker_error, broker_warning
from pynecore.types.strategy import ADOPTED_STARTUP_EXTRA_KEY

from ._base import _CapitalComBase, CapitalComHookCollaborator
from .delivery import entry_executed
from .dispatch_hooks import _CapitalComCloseHooks
from .exceptions import (
    CapitalComApiError,
    CapitalComError,
    ForeignDealBlocksWriteError,
    InvalidStopDistanceError,
    InvalidTakeProfitDistanceError,
    OrderNotFoundError,
    PositionSnapshotUnavailableError,
)
from .helpers import (
    EXTRAS_KEY_CLOSE_LEG_DISPATCHED,
    EXTRAS_KEY_HELD_CLOSE,
    EXTRAS_KEY_HELD_CLOSE_SEEDED,
    EXTRAS_KEY_ORDER_LEVEL,
    EXTRAS_KEY_TAKE_OVER_UNPROVEN,
    _extract_reject_reason,
    _is_crossed_working_order_reject,
    _is_funds_reject,
    _is_market_closed_reject,
    _modify_exit_dispatch,
    _parse_iso_timestamp,
    _position_legs_from_payload,
    _size_from_units,
    _venue_rendered_level,
    _wire_float,
    _wire_id,
)
from .models import _DealVerdict, _InstrumentRules, _bracket_leg_id

if TYPE_CHECKING:
    from pynecore.core.broker.journal import (
        ConfirmOutcome,
        SubmitOutcome,
    )
    from pynecore.core.broker.storage import OrderRow

    from .dispatch_hooks import _CapitalComModifyExitHooks


class _PositionDealPending(Exception):
    """A bracket ``PUT`` found the entry's deal not addressable yet.

    The row now holds another deal (:attr:`~pynecore_capitalcom.models._DealVerdict.REKEYED`)
    or the deal is still materialising
    (:attr:`~pynecore_capitalcom.models._DealVerdict.MATERIALISING`): the
    exit is retried on the next sync, never answered with a defensive close.

    :ivar verdict: The deal verdict.
    :ivar deal_id: The deal the entry row holds now.
    """

    def __init__(self, verdict: _DealVerdict, deal_id: str) -> None:
        super().__init__(f"position deal {verdict.value}, entry holds {deal_id}")
        self.verdict = verdict
        self.deal_id = deal_id


class _ExecutionMixin(_CapitalComBase, ABC):
    """Order execution mix-in: every ``execute_*`` and ``modify_*`` path."""

    @staticmethod
    def _level_on_triggered_side(
            position_side: str | None,
            reference_price: float,
            level: float,
            *,
            stop: bool,
    ) -> bool:
        """Whether a protective level sits at or beyond ``reference_price`` on
        the side where it is already triggered.

        A long's stop-loss at or above the quote, or its take-profit at or
        below it, fires immediately; a short mirrors that. Without a
        ``position_side`` nothing can be said, so the answer is ``False``.

        :param position_side: ``'buy'`` for a long, ``'sell'`` for a short.
        :param reference_price: The quote the level is measured against.
        :param level: The requested stop-loss or take-profit price.
        :param stop: ``True`` for the stop-loss leg, ``False`` for take-profit.
        """
        if position_side not in ('buy', 'sell'):
            return False
        above = level >= reference_price
        below = level <= reference_price
        if position_side == 'buy':
            return above if stop else below
        return below if stop else above

    @staticmethod
    def _same_level(held: float | None, level: float, tick: float) -> bool:
        """Whether a level the row holds is ``level``, within half a price tick.

        :param held: The level the row carries, ``None`` when it carries none.
        :param level: The requested level.
        :param tick: The instrument's price tick; ``0.0`` (not quoted) compares
            exactly.
        """
        return held is not None and abs(held - level) <= tick / 2.0

    @staticmethod
    def _validate_sl_distance(
            rules: _InstrumentRules,
            reference_price: float | None,
            sl_price: float,
            position_side: str | None = None,
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

        With ``position_side`` (``'buy'`` long / ``'sell'`` short) the raised
        error is ``level_crossed`` when the stop already sits on its
        triggered side of ``reference_price``.
        """
        if reference_price is None or reference_price <= 0.0:
            return
        min_distance = rules.min_bracket_distance_at(reference_price)
        if min_distance <= 0.0:
            return
        distance = abs(reference_price - sl_price)
        if distance < min_distance:
            raise InvalidStopDistanceError(
                f"Capital.com pre-check: stop distance {distance:.6f} for "
                f"{rules.epic} is below current minimum {min_distance:.6f} "
                f"(reference={reference_price}, sl={sl_price}).",
                min_distance=min_distance,
                level_crossed=_ExecutionMixin._level_on_triggered_side(
                    position_side, reference_price, sl_price, stop=True,
                ),
            )

    @staticmethod
    def _validate_tp_distance(
            rules: _InstrumentRules,
            reference_price: float | None,
            tp_price: float,
            position_side: str | None = None,
    ) -> None:
        """Raise :class:`InvalidTakeProfitDistanceError` if ``tp_price`` is
        closer than the cached minimum distance to ``reference_price``.

        Capital.com applies the bracket-distance minimum symmetrically to
        both legs — the same threshold gates ``stopLevel`` and
        ``profitLevel`` (the venue field is even named
        ``minStopOrProfitDistance``). See :meth:`_validate_sl_distance`
        for the anchor-price discussion and ``position_side``.
        """
        if reference_price is None or reference_price <= 0.0:
            return
        min_distance = rules.min_bracket_distance_at(reference_price)
        if min_distance <= 0.0:
            return
        distance = abs(reference_price - tp_price)
        if distance < min_distance:
            raise InvalidTakeProfitDistanceError(
                f"Capital.com pre-check: take-profit distance {distance:.6f} "
                f"for {rules.epic} is below current minimum "
                f"{min_distance:.6f} "
                f"(reference={reference_price}, tp={tp_price}).",
                min_distance=min_distance,
                level_crossed=_ExecutionMixin._level_on_triggered_side(
                    position_side, reference_price, tp_price, stop=False,
                ),
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
        """Return the aggregate position across the run's own rows for ``symbol``.

        Capital.com netting opens a fresh row per same-direction entry
        (confirmed empirically after §9 #5 is closed) — aggregation is
        therefore mandatory for Pine's one-way model. Returns ``None``
        when no row exists for the symbol.

        Only the deals this run's journal produced count
        (:meth:`_owned_deal_ids`): a deal someone else opened on the epic is
        not the bot's exposure, and counting it would hide an own deal's
        disappearance behind it. Without a journal (a raw account read)
        every row counts. The deal of an order of an entry the engine holds
        for its execution evidence counts too, also when no live row tracks
        it (:meth:`~pynecore_capitalcom.foreign._ForeignExposureMixin._held_execution_order_ids`):
        the engine sizes its ``held_execution_close`` from this read, and that
        close must reach the execution its book does not hold
        (:meth:`_execute_held_execution_close`). An own deal whose fill the
        venue confirmed but that ``/positions`` does not list yet counts from
        the journal
        (:meth:`~pynecore_capitalcom.identity._DealIdentityMixin._materialising_position_rows`):
        the read lagging the venue is no close, and a position that left it
        out would let the engine book one (measured live: capitalcom cycle
        442, a defensive close settled against a read that omitted the deal
        it never reached).

        On a hedging-mode account the rows are netted through the core
        :func:`~pynecore.core.broker.emulator.aggregate_positions` instead:
        that virtual-FIFO survivor view is what the
        :class:`~pynecore.core.broker.one_way_emulator.OneWayEmulator`'s
        close/reversal plans assume, whereas the netting branch below
        computes a gross same-side average (fine when opposite rows cannot
        coexist).
        """
        if self._hedging_enabled:
            legs = await self.fetch_raw_positions(symbol)
            unlisted = self._materialising_position_rows(
                [{'position': {'dealId': leg.leg_id}} for leg in legs], symbol,
            )
            return aggregate_positions(
                symbol, legs + _position_legs_from_payload({'positions': unlisted}, symbol),
            )
        res = await self._call('positions', method='get')
        rows = res.get('positions') or []
        self._note_foreign_deals(rows, symbol)
        owned = self._owned_deal_ids(rows)
        if owned is not None:
            rows = [row for row in rows
                    if str((row.get('position') or {}).get('dealId') or '') in owned]
        rows = rows + self._materialising_position_rows(res.get('positions') or [], symbol)
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

    @staticmethod
    def _bracket_put_body(
            tp_price: float | None, sl_price: float | None, trail_offset: float | None,
    ) -> dict:
        """The ``PUT /positions/{dealId}`` body of one whole-bracket amend.

        Every bracket field is set explicitly (full replacement). A
        ``trail_offset`` takes the stop slot as the ``trailingStop`` +
        ``stopDistance`` pair; otherwise the stop is the fixed ``stopLevel``.
        """
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
        return body

    async def bracket_matches(
            self, symbol: str, leg_id: str, *,
            side: str,
            tp_price: float | None,
            sl_price: float | None,
            trail_offset: float | None,
            qty: float,
    ) -> bool | None:
        """Whether position row ``leg_id`` holds exactly the bracket this amend writes.

        :class:`~pynecore.core.plugin.broker.PositionPort` transport
        primitive. Reads ``GET /positions`` and compares the row's bracket
        with the one :meth:`amend_bracket` would leave for the same levels
        (:meth:`_bracket_put_body`): the trailing flag, the profit level, and
        the stop level of a fixed stop. A trailing stop is identified by its
        flag and its distance, never by its stop level, which the venue moves
        itself. A trailing row whose payload leaves the distance out does not
        show which distance it trails at: when its flag and profit level agree
        with a trailing write, the answer is ``None``, "cannot tell" (the core
        then re-asserts the write once after its settle window), where a
        guessed match could leave an older distance in force. A row that is no
        longer open matches no write.
        """
        del side, qty
        rules = await self._get_instrument_rules(symbol)
        res = await self._call('positions', method='get')
        row = next(
            (pos for pos in res.get('positions') or []
             if str((pos.get('position') or {}).get('dealId') or '') == leg_id),
            None,
        )
        if row is None:
            return False
        held = self._levels_from_position(row)
        wanted = self._levels_from_put_body(
            self._bracket_put_body(tp_price, sl_price, trail_offset))
        tolerance = rules.price_tick / 2.0

        def same(have: float | None, want: float | None) -> bool:
            if have is None or want is None:
                return have is want
            return abs(have - want) <= tolerance

        if held.trailing != wanted.trailing or not same(held.limit, wanted.limit):
            return False
        if wanted.trailing:
            if held.trail_distance is None:
                return None
            return same(held.trail_distance, wanted.trail_distance)
        return same(held.stop, wanted.stop)

    async def fetch_raw_positions(self, symbol: str) -> list[PositionLeg]:
        """Return the run's own open position rows ("legs") for ``symbol``, oldest first.

        :class:`~pynecore.core.plugin.broker.PositionPort` transport
        primitive: one :class:`PositionLeg` per Capital.com position row
        with ZERO aggregation — the core emulator nets them. Parsing and
        the deterministic ``(open_time, leg_id)`` FIFO ordering live in
        :func:`~pynecore_capitalcom.helpers._position_legs_from_payload`.

        Only legs this run's journal produced are returned
        (:meth:`_owned_deal_ids`): the emulator plans closes, reversals and
        bracket replication over exactly these, so a leg someone else opened
        is never closed, amended or netted against the bot's legs. Without a
        journal every leg is returned. A leg an order of an entry the engine
        holds for its execution evidence opened is the run's too, also when no
        live row tracks it: the one-way emulator's ``held_execution_close``
        closes it (:meth:`close_leg`).

        On a hedging account each leg carries
        :attr:`~pynecore.core.broker.models.PositionLeg.entry_id`, the Pine
        entry whose own dispatch opened it
        (:meth:`~pynecore_capitalcom.foreign._ForeignExposureMixin._owned_deal_entries`),
        so the core emulator replicates a ``from_entry`` exit onto that
        entry's legs only. It stays ``None`` when the journal ties the deal
        to no single entry, without a journal, and on a netting account: the
        emulator, its only reader, runs on hedging accounts alone, and
        ``None`` is the contract's always-safe value.
        """
        res = await self._call('positions', method='get')
        rows = res.get('positions') or []
        self._note_foreign_deals(rows, symbol)
        legs = _position_legs_from_payload(res, symbol)
        owned = self._owned_deal_entries(rows)
        if owned is None:
            return legs
        if not self._hedging_enabled:
            return [leg for leg in legs if leg.leg_id in owned]
        return [replace(leg, entry_id=owned[leg.leg_id]) for leg in legs if leg.leg_id in owned]

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
        """Raise the volume-bounds :class:`OrderSkippedByPlugin` skip when ``qty`` is out of range.

        The skip drops this order only; the run continues.

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
    #: so the core emulator never sends a partial leg slice. A fractional
    #: ``strategy.close(qty=...)`` on a hedging account closes the whole leg
    #: it would slice, with a warning; exact partial closes need a one-way
    #: (netting) account.
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
        close (``LegType.CLOSE`` on the tracked dealId), or from the
        position's disappearance when no activity is published. That fill
        carries ``coid`` as its client order id, also after a restart: the
        activity's from the own-close record written before the DELETE leaves
        (:meth:`~pynecore_capitalcom.intervention._InterventionMixin._own_close`,
        :meth:`~pynecore_capitalcom.intervention._InterventionMixin._own_close_leg_coid`),
        so a lost answer keeps it too, and only on an activity of the whole
        leg's ``volume``: a smaller one inside the record's window may be a
        manual partial close, is not the bot's own and keeps the position
        row's id; the
        disappearance's from the
        :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_CLOSE_LEG_DISPATCHED`
        stamp written with the ``closing`` state once the venue accepted the
        DELETE, so an earlier attempt's record never masks it.

        An ambiguous transport fault maps to
        :class:`OrderDispositionUnknownError`: the emulator's close-leg
        ledger + restart replay reconcile the leg against the live book
        and re-dispatch only the residual.

        A not-found DELETE is a benign no-op only when the leg is gone (the
        close already landed or the row was swept broker-side), judged by
        :meth:`~pynecore_capitalcom.identity._DealIdentityMixin._resolve_own_deal`
        as on the netting full close: the venue has answered not-found on a
        deal ``/positions`` still listed (measured, cycle 65), and reporting
        that as landed would drop a close the venue never made. A leg the
        venue lists is DELETEd once more, a journal row that moved onto
        another deal is closed there (the core's close-leg row is retargeted
        to that deal first, so its replay follows the open position), and a
        leg still materialising, or still
        not addressable on the second try, raises
        :class:`OrderDispositionUnknownError`: nothing closed, the core's
        close-leg row stays pending for the replay. Without a journal row for
        the leg there is nothing to judge it by, and the not-found stays
        benign.

        A leg no journal row tracks that an order of an entry the engine holds
        for its execution evidence opened (a ``held_execution_close`` closes
        it) gets a seeded row before the DELETE (:meth:`_held_close_leg_row`),
        read from a fresh ``GET /positions``; a failed read raises
        :class:`ExchangeConnectionError` with nothing sent. The deal of a
        take-over held for its unproven fill
        (:meth:`~pynecore_capitalcom.foreign._ForeignExposureMixin._held_take_over_refs`)
        raises :class:`OrderDispositionUnknownError` with nothing sent: it is
        closed once the bot has booked that fill from the venue's history
        (:mod:`~pynecore_capitalcom.order_history`).
        """
        # ``volume`` is audit-only here: full-leg by contract, and a size is
        # not expressible on the DELETE wire anyway.
        if leg_id in self._held_take_over_refs():
            # The deal of a take-over held for its unproven fill: owned, but
            # closed only once the bot has booked that fill from the venue's
            # history (``order_history.py``); a DELETE before would leave a
            # close activity the bot could not attribute to itself.
            raise OrderDispositionUnknownError(
                f"Capital DELETE positions/{leg_id} ({symbol}) not sent: the deal is a "
                f"take-over whose fill the bot is proving from the venue's history; the "
                f"leg close did not go out",
                client_order_id=coid,
            )
        store_ctx = self.store_ctx
        entry_row = (
            store_ctx.find_by_ref('deal_id', leg_id)
            if store_ctx is not None else None
        )
        if entry_row is None and store_ctx is not None:
            # The deal of a held entry no journal row tracks: seeded before the
            # DELETE, so the close's activity routes to a row and its fill is
            # reported under ``coid``.
            entry_row = await self._held_close_leg_row(symbol, leg_id)
        # The venue size of the whole leg the core closes: only a close
        # activity of exactly this size is identified as this DELETE's. The
        # lot step is the one the core quantized ``volume`` with
        # (:meth:`get_volume_quantizer`); nothing is read before the DELETE,
        # which needs only the dealId. Without it no activity is identified.
        rules = self._instrument_rules_cache.get(symbol)
        leg_size = _size_from_units(volume, rules.lot_step) if rules is not None else None
        deal_id = leg_id
        for attempt in range(2):
            try:
                with self._own_close([deal_id], full=True, size=None,
                                     coid=coid, leg_size=leg_size):
                    await self._call(f'positions/{deal_id}', method='delete')
                break
            except OrderNotFoundError as missing:
                if entry_row is None or store_ctx is None:
                    if store_ctx is not None:
                        store_ctx.log_event(
                            'close_already_gone',
                            client_order_id=coid,
                            exchange_order_id=deal_id,
                        )
                    return
                try:
                    verdict, current = await self._resolve_own_deal(entry_row, deal_id)
                except ExchangeConnectionError as net:
                    raise OrderDispositionUnknownError(
                        f"Capital DELETE positions/{deal_id} ({symbol}) not found "
                        f"and the deal could not be judged: {net}",
                        client_order_id=coid,
                        cause=net,
                    ) from net
                if verdict is _DealVerdict.GONE:
                    store_ctx.log_event(
                        'close_already_gone',
                        client_order_id=entry_row.client_order_id,
                        exchange_order_id=deal_id,
                    )
                    return
                if verdict is _DealVerdict.MATERIALISING or attempt == 1:
                    # The deal is the run's and is not gone, but the venue
                    # cannot route a close to it yet: nothing closed. The
                    # close-leg row stays pending for the replay.
                    store_ctx.log_event(
                        'close_target_not_addressable',
                        client_order_id=entry_row.client_order_id,
                        exchange_order_id=deal_id,
                        payload={'verdict': verdict.value, 'deal_id': current},
                    )
                    raise OrderDispositionUnknownError(
                        f"Capital DELETE positions/{deal_id} ({symbol}) not found "
                        f"while the deal is {verdict.value} (the entry holds "
                        f"{current}): the leg close did not land",
                        client_order_id=coid,
                        cause=missing,
                    ) from missing
                if current != deal_id:
                    # The position lives on under another dealId. The core's
                    # close-leg row is retargeted BEFORE that DELETE goes out,
                    # so a replay of an unresolved close asks about, and
                    # re-sends to, the deal that is still open instead of
                    # settling on the old id's absence.
                    row_coid = close_leg_row_client_order_id(coid)
                    if store_ctx.get_order(row_coid) is not None:
                        update_close_leg_state(
                            store_ctx, coid=row_coid,
                            new_state=CLOSE_LEG_STATE_PENDING,
                            extras_patch={EXTRAS_KEY_CLOSE_LEG_ID: current},
                        )
                deal_id = current
            except ExchangeConnectionError as net:
                # ``_call`` maps httpx transport faults to
                # ``ExchangeConnectionError`` centrally; at THIS dispatch site a
                # DELETE was in flight, so the leg's fate is genuinely unknown.
                raise OrderDispositionUnknownError(
                    f"Capital DELETE positions/{deal_id} ({symbol}) ambiguous "
                    f"during one-way leg close: {net}",
                    client_order_id=coid,
                    cause=net,
                ) from net
        # Bookkeeping AFTER the wire call, mirroring the netting full-close
        # hook: the core close-leg ledger row (written persist-first by the
        # emulator before this call) owns the crash window, so the entry
        # row's ``closing`` mirror only needs to reflect a dispatch that
        # actually went out.
        # The state and its audit are one write; a failed write after the
        # DELETE landed parks the leg close (the core's close-leg row keeps its
        # reservation and is settled from the venue's evidence) instead of
        # escaping as a raw storage error.
        if store_ctx is not None and entry_row is not None:
            try:
                with store_ctx.transaction():
                    store_ctx.set_order_state(entry_row.client_order_id, 'closing')
                    self._rewrite_extras(
                        entry_row.client_order_id,
                        lambda extras: extras.__setitem__(EXTRAS_KEY_CLOSE_LEG_DISPATCHED, coid),
                    )
                    store_ctx.log_event(
                        'close_dispatched',
                        client_order_id=entry_row.client_order_id,
                        exchange_order_id=deal_id,
                        payload={'leg_volume': volume, 'leg_coid': coid},
                    )
            except sqlite3.Error as failed:
                broker_error(
                    "close of leg %s (%s) landed, but its closing state could not be "
                    "journaled (%s); the leg close is settled from venue data",
                    deal_id, symbol, failed,
                )
                raise OrderDispositionUnknownError(
                    f"Capital DELETE positions/{deal_id} ({symbol}) landed, but the "
                    f"entry's closing state could not be journaled: {failed}",
                    client_order_id=coid,
                    cause=failed,
                ) from failed

    async def close_leg_executed(
            self, symbol: str, leg_id: str, coids: 'Sequence[str]', since_ms: int,
    ) -> int | None:
        """What the closes sent for a still-open position row executed: nothing.

        :class:`~pynecore.core.plugin.broker.PositionPort` primitive. A close
        here is ``DELETE /positions/{dealId}``, which removes the whole row or
        nothing (:attr:`supports_partial_leg_close` is ``False``), so a row the
        core still sees open was not closed by any of them. A DELETE that is
        still in flight cannot close more either: it targets the same dealId a
        re-send does, and the row can be deleted only once (the later DELETE
        answers not-found, a benign no-op in :meth:`close_leg`).

        The answer cannot authorize a duplicate close. The core asks only
        while ``leg_id`` is listed open (a leg missing from
        :meth:`fetch_raw_positions` settles the close without a re-send), and
        the only re-send it can make is :meth:`close_leg` on that same
        ``leg_id``. When an earlier DELETE executed but the ``/positions`` read
        still lists the row (a lagging read), the re-send's DELETE meets a
        removed row and answers not-found; it never reaches another position,
        because the DELETE names the row by its own dealId and a position a
        later entry opens has a dealId of its own. That Capital.com never hands a closed position's dealId to a new
        position is not measured; no reuse has been observed.
        """
        del symbol, leg_id, coids, since_ms
        return 0

    async def amend_bracket(
            self, symbol: str, leg_id: str, *,
            side: str,
            tp_price: float | None,
            sl_price: float | None,
            trail_offset: float | None,
            qty: float,
            coid: str,
    ) -> None:
        """Replicate (or clear) a protective bracket on ONE position row.

        :class:`~pynecore.core.plugin.broker.PositionPort` transport
        primitive. Capital.com ``PUT /positions/{dealId}`` is *full
        replacement* (verified on demo 2026-05-29): every bracket field is
        sent explicitly — a float to arm, JSON ``null`` / ``false`` to
        clear — so the all-``None`` call clears the whole bracket instead
        of no-oping on an empty body. Every call is the leg's WHOLE
        protection (the core merges the levels of every exit owning the
        leg), so the body never merges with what the row carried before,
        and nothing here tells an attach from a clear: an exit's retirement
        may re-assert the remaining owners' levels under its cancel coid,
        and is written, confirmed and classified like any other amend.
        A ``trail_offset`` maps to the
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

        ``side`` (the exit side) only tells the pre-check which side of the
        quote a level is already triggered on — Capital.com levels are
        absolute prices on the row; no side-dependent anchor is needed
        (cTrader seeds its trailing anchor from it).

        The distance pre-check covers only a level the body sets: the
        ``stopLevel`` it writes (never an ``sl_price`` a ``trail_offset``
        supersedes) and the ``profitLevel``, and neither when the row already
        carries that exact level (:meth:`~_InterventionMixin._held_protection`):
        restating a level the leg holds sets nothing new, so the price having
        moved inside the minimum distance since it was set is no reason to
        refuse the amend. Whether the venue accepts such a restated level is
        its own call; its reject propagates like any other.

        ``qty`` is unused: a row's bracket covers the whole row, which is all
        the core ever asks of this port (``sized_leg_bracket`` is declared
        ``SOFTWARE``).
        """
        del qty
        # Same live-mid pre-check as the netting bracket paths: turns an
        # obviously-rejectable level into the typed distance error before
        # the round-trip. Distance violations subclass
        # ``ExchangeOrderRejectedError``, so the attach path still ends in
        # the defensive flatten rather than an untranslated write fault. The quote read refreshes
        # the rules cache, so the rules are read after it: the minimum
        # distance checked is the one the same market response carried.
        mid_price = await self._get_current_mid_price(symbol)
        rules = await self._get_instrument_rules(symbol)
        position_side = 'buy' if side == 'sell' else 'sell'
        held = self._held_protection(leg_id)
        tick = rules.price_tick
        if (sl_price is not None and trail_offset is None
                and not (held is not None and not held.trailing
                         and self._same_level(held.stop, float(sl_price), tick))):
            self._validate_sl_distance(
                rules, mid_price, float(sl_price), position_side,
            )
        if (tp_price is not None
                and not (held is not None
                         and self._same_level(held.limit, float(tp_price), tick))):
            self._validate_tp_distance(
                rules, mid_price, float(tp_price), position_side,
            )

        body = self._bracket_put_body(tp_price, sl_price, trail_offset)

        # Full replacement: the body is the whole protection the row
        # carries afterwards.
        with self._protection_write(leg_id, body) as protection:
            try:
                resp = await protection.send(self._call(
                    f'positions/{leg_id}', data=body, method='put',
                ))
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
                # provider error (which the engine would only take as an
                # untranslated write fault: an entry block, no flatten).
                raise ExchangeOrderRejectedError(
                    f"Capital PUT positions/{leg_id} bracket amend refused: {exc}",
                ) from exc

            if self.store_ctx is not None:
                try:
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
                except sqlite3.Error as failed:
                    # Only an audit: the amend's outcome is still settled from
                    # its confirm below, which a raw storage error would skip.
                    broker_error(
                        "bracket amend of leg %s (%s) went out, but its audit could "
                        "not be journaled (%s)", leg_id, symbol, failed,
                    )
            deal_ref = resp.get('dealReference') if isinstance(resp, dict) else None
            if not deal_ref:
                # No dealReference to confirm — treat the round-trip as success;
                # the reconcile pass verifies the observed levels later. Mirrors
                # ``publish_native_failsafe_sl``.
                protection.accepted()
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
                protection.refused()
                reason = _extract_reject_reason(confirm)
                raise ExchangeOrderRejectedError(
                    f"Capital PUT positions/{leg_id} bracket amend REJECTED "
                    f"(coid={coid}): {reason}",
                )
            protection.accepted()

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
        """Raise :class:`OrderSkippedByPlugin` when ``qty`` is outside the instrument's size bounds.

        The skip drops this entry only; the run continues.

        Preserves the historical asymmetry: the minimum gates the RAW
        requested qty (rounding an undersized request up to the grid would
        be silent up-inflation), the maximum gates the QUANTIZED qty —
        i.e. what actually reaches the broker. Capital.com rejects sizes
        above ``maxDealSize`` with ``error.invalid.size.maxvalue``, which
        surfaces as a venue reject of the dispatch — pre-empting both bounds here turns them into
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
        # The order the engine sends for a triggered entry (a stop-limit's
        # LIMIT, a crossed entry's re-send) uses its own client-order-id
        # kind, distinct from the entry's KIND_ENTRY id for the same pine_id,
        # so the broker's local idempotency dedup never takes it for that one.
        coid = envelope.client_order_id(
            KIND_ENTRY_TRIGGERED if intent.triggered_entry else KIND_ENTRY,
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

        # A netting entry nets the oldest opposite deal FIFO when it executes.
        # Opposite foreign exposure the last poll saw refuses it before
        # anything is journaled. A market entry is then decided by the final
        # snapshot under the account write lock (``submit_market_entry``); a
        # resting entry executes later, so the poll's snapshot is all there
        # is at placement (an execution that nets a foreign deal anyway is
        # never booked as the run's, see ``_retire_order_netted_into_foreign_deal``).
        # A refused entry only drops this signal: the foreign deal is someone
        # else's separate activity.
        netted_direction = 'SELL' if intent.side == 'buy' else 'BUY'
        write = {'entry': intent.pine_id, 'side': intent.side, 'qty': quantized_qty}
        if not self._hedging_enabled:
            foreign = self._cached_foreign_deals(intent.symbol, netted_direction)
            if foreign:
                raise self._foreign_block_skip(
                    reason=('foreign_deal_blocks_opposite_entry' if endpoint == 'positions'
                            else 'foreign_deal_blocks_resting_entry'),
                    intent_key=intent.intent_key, symbol=intent.symbol,
                    foreign=foreign, write=write, latch=False,
                )

        hooks = _CapitalComEntryHooks(
            plugin=self,
            endpoint=endpoint,
            body=body,
            quantized_qty=quantized_qty,
        )
        journal = DispatchJournal(self.store_ctx)
        from pynecore.core.broker.journal import EntryDispatchHooks

        try:
            orders = await journal.run_entry(
                coid=coid,
                intent=intent,
                qty=quantized_qty,
                kind=kind,
                hooks=cast(EntryDispatchHooks, hooks),
                audit_payload={'endpoint': endpoint, 'body': body},
            )
        except ForeignDealBlocksWriteError as refused:
            raise self._foreign_block_skip(
                reason=refused.reason, intent_key=intent.intent_key,
                symbol=intent.symbol, foreign=refused.foreign_deals,
                write=write, latch=False,
            ) from refused
        except OrderDispositionUnknownError as unknown:
            if isinstance(unknown.__cause__, sqlite3.Error):
                self._park_entry_after_journal_failure(coid, hooks.server_ref, unknown.__cause__)
            raise
        except PositionSnapshotUnavailableError as unavailable:
            # Nothing left the client: the signal is retried on the next sync.
            # A skip, not an exchange reject, so a read outage never counts
            # toward the engine's bounded entry-reject budget.
            raise OrderSkippedByPlugin(
                f"Skipping {intent.symbol} {intent.side.upper()} entry "
                f"id={intent.pine_id!r}: {unavailable}; retried next sync.",
                intent_key=intent.intent_key,
                reason='entry_preflight_unavailable',
                context={'symbol': intent.symbol, 'side': intent.side, 'write': write},
            ) from unavailable
        if any(order.filled_qty > 0.0 for order in orders):
            # The confirm reported the fill: the deal is the run's from here
            # on, listed by ``/positions`` or not (see ``identity.py``).
            self._note_entry_fill_unlisted(coid)
        return orders

    def _park_entry_after_journal_failure(
            self, coid: str, server_ref: str | None, failed: sqlite3.Error,
    ) -> None:
        """Park an entry whose journal write failed after the venue call.

        The journal raised the storage failure as an unknown disposition, but
        the row is left in its pending state, which only a restart's recovery
        reads. Parked ``disposition_unknown`` with the POST's
        ``dealReference``, the row is the poll's to settle from venue data
        (:meth:`~pynecore_capitalcom.reconcile._ReconcileMixin._resolve_parked_submission_dispositions`):
        the confirm decides whether the entry landed, and a landed one is
        taken over and its fill booked. The park is one transaction, retried
        on every poll until it commits.

        :param coid: The entry's client order id.
        :param server_ref: The ``dealReference`` the POST returned, when it did.
        :param failed: The storage failure.
        """
        store_ctx = self.store_ctx
        assert store_ctx is not None

        def park() -> None:
            with store_ctx.transaction():
                row = store_ctx.get_order(coid)
                if (row is None or row.closed_ts_ms is not None or row.exchange_order_id
                        or row.state not in PENDING_DISPATCH_STATES):
                    return
                extras = row.extras or {}
                if server_ref is not None and extras.get('deal_reference') is None:
                    store_ctx.add_ref(coid, 'deal_reference', server_ref)
                    store_ctx.upsert_order(coid, extras={**extras, 'deal_reference': server_ref})
                mark_disposition_unknown(store_ctx, coid=coid)
                store_ctx.log_event(
                    'disposition_unknown',
                    client_order_id=coid,
                    intent_key=row.intent_key,
                    payload={'phase': 'journal', 'deal_reference': server_ref,
                             'reason': str(failed)},
                )

        self._park_after_venue_write(coid, park)

    async def _put_bracket_with_notfound_retry(
            self, target_row: 'OrderRow', deal_id: str, body: dict,
    ) -> dict:
        """PUT the bracket, judging a not-found by the deal's identity first.

        Measured (cycle 65, pyramid, 3 events): the ``positions/{deal_id}``
        PUT issued immediately after the entry fill 404'd with
        ``error.not-found.dealId`` while the deal demonstrably existed — the
        defensive close's DELETE on the very same id landed seconds later.
        And the confirm's ``dealId`` may not be the position's at all
        (cycle 442: the confirm named the executed working order, the
        position opened under a fresh ``dealId``). The not-found is judged
        by :meth:`~pynecore_capitalcom.identity._DealIdentityMixin._resolve_own_deal`:

        * the deal is listed: the 404 was stale routing, the PUT is retried
          once;
        * the row now holds another deal, or the deal is still
          materialising: :class:`_PositionDealPending` — the exit waits for
          the next sync, which protects the row's current deal; flattening
          a just-filled entry for a venue view that has not caught up would
          close the position for nothing, or not at all;
        * the deal is gone: the error propagates to the defensive-close
          path unchanged.

        A network failure during a verify read escapes as a connection
        error and lands in the caller's ambiguous-PUT branch
        (disposition-unknown park), which the reconcile machinery resolves.
        """
        try:
            return await self._call(
                f'positions/{deal_id}', data=body, method='put',
            )
        except OrderNotFoundError:
            verdict, current = await self._resolve_own_deal(target_row, deal_id)
            if verdict in (_DealVerdict.REKEYED, _DealVerdict.MATERIALISING):
                raise _PositionDealPending(verdict, current)
            if verdict is not _DealVerdict.PRESENT:
                raise
            if self.store_ctx is not None:
                self.store_ctx.log_event(
                    'bracket_put_notfound_retried',
                    exchange_order_id=deal_id,
                    payload={'body': body},
                )
            return await self._call(
                f'positions/{deal_id}', data=body, method='put',
            )

    async def execute_exit(
            self, envelope: DispatchEnvelope,
    ) -> list[ExchangeOrder]:
        """Attach a TP/SL/trailing bracket to every open deal of the exit's entry.

        Capital.com's bracket is a *full-row* attribute on the position
        (``PUT /positions/{dealId}`` with ``profitLevel`` / ``stopLevel``
        / ``trailingStop``) — there is no separate exit order. The plugin
        synthesises two :class:`ExchangeOrder` objects per deal with
        composite ``dealId:tp`` / ``dealId:sl`` ids so the sync engine's leg
        accounting still works.

        Every market entry opens its own deal, so a Pine entry id the script
        pyramids under (``strategy.entry('L1')`` re-issued while ``L1`` is
        open) holds several deals. A whole-row ``strategy.exit(from_entry=
        'L1')`` covers every open trade of the id, so the same levels go onto
        each deal, oldest first, each with its own TP/SL leg rows
        (:meth:`_attach_bracket_to_deal`). Each deal then carries its own
        native protection: the venue closes it at the exit's levels while the
        engine is down, and one deal closing leaves the others protected.

        The deals covered are sized against the exit's quantity by
        :meth:`_bracket_targets`; a partial-quantity exit never reaches this
        path (the engine drives it in software).

        A deal that already carries this exit's legs is rewritten in place:
        the PUT replaces its whole protection atomically and the legs keep
        their rows, so :meth:`modify_exit` re-runs this method to re-attach
        an exit whose set of deals changed without clearing any protection
        first.

        Trailing with an activation threshold (Pine ``trail_price``) is
        *deferred*: the plugin sets an ``activating`` state and lets the
        activation monitor flip the exchange to native ``trailingStop``
        when the mid-price crosses the threshold. Immediate trailing
        (``trail_offset`` only, no ``trail_price``) goes out on the PUT
        directly.
        """
        intent = envelope.intent
        assert isinstance(intent, ExitIntent)
        # The parent entry row carries ``pine_entry_id == intent.from_entry``
        # and ``from_entry IS NULL`` (entries have no from_entry), so the
        # lookup filters by ``symbol`` and disambiguates in Python. Rows
        # flagged ``natural_close_at`` are skipped: after a TP/SL closes a
        # deal its entry row stays live, and a later deal opened under the
        # same ``pine_entry_id`` must not be confused with it (the PUT would
        # 404 with ``error.not-found.dealId``).
        targets = self._find_active_entry_rows(intent.from_entry)
        # The position's protection changed on the venue and who changed it is
        # still being read from the activity feed: the bracket waits, it would
        # replace levels someone else may have set.
        for protected in targets:
            self._defer_protection_write(
                protected.exchange_order_id, intent_key=intent.intent_key,
                symbol=intent.symbol,
            )
        # Fresh market data: ``_get_current_mid_price`` also refreshes the
        # rules cache as a side effect, so the subsequent rules lookup is
        # a single in-process dict read rather than a second REST call.
        mid_price = await self._get_current_mid_price(intent.symbol)
        rules = await self._get_instrument_rules(intent.symbol)

        if not targets:
            # Close-in-flight: the entry row exists but is ``closing`` — a
            # defensive close or a ``strategy.close`` is on its way to the
            # venue and its fill is not booked yet, so the Pine book still
            # shows the position and the script legitimately re-sends the
            # exit. Nothing to protect once the close lands; a raw reject
            # here is fatal on the engine's exit contract (measured live
            # 2026-08-15: the re-dispatched exit killed the run one bar
            # after the defensive close). Skip without a venue call — the
            # next sync books the close and re-evaluates freely.
            if self.store_ctx is not None:
                for row in self.store_ctx.iter_live_orders():
                    extras = row.extras or {}
                    if row.pine_entry_id != intent.from_entry:
                        continue
                    if (extras.get('kind') == ENTRY_KIND_WORKING
                            and row.state == 'confirmed'):
                        # Working->position promotion window: the STOP /
                        # LIMIT working order executed (the engine booked
                        # its fill, which is why an exit is being asked
                        # for), but the activity poll has not yet rewritten
                        # the row to a position deal. The venue-side
                        # position exists — a defensive close here would
                        # flatten a healthy just-filled entry. Wait; the
                        # promotion lands within the next polls and the
                        # re-sent exit finds the position row.
                        raise OrderSkippedByPlugin(
                            f"Exit {intent.intent_key} skipped: entry "
                            f"{intent.from_entry!r} is mid promotion from "
                            f"working order to position; re-evaluating "
                            f"next bar.",
                            intent_key=intent.intent_key,
                            reason="close_in_flight",
                            context={'symbol': intent.symbol,
                                     'from_entry': intent.from_entry,
                                     'row_state': row.state},
                        )
                    if extras.get('kind') != 'position':
                        continue
                    if row.state != 'confirmed':
                        raise OrderSkippedByPlugin(
                            f"Exit {intent.intent_key} skipped: entry "
                            f"{intent.from_entry!r} is being closed "
                            f"(row state {row.state!r}); re-evaluating "
                            f"next bar.",
                            intent_key=intent.intent_key,
                            reason="close_in_flight",
                            context={'symbol': intent.symbol,
                                     'from_entry': intent.from_entry,
                                     'row_state': row.state},
                        )
            # Defense-in-depth: an open exchange-side position with no
            # findable BrokerStore entry row is structurally identical to
            # "bracket attach rejected after parent fill" — the fill is on
            # the exchange and unprotected. Raising a plain
            # ``ExchangeOrderRejectedError`` here leaves the position
            # exposed; instead, surface the rich
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
                for row in self.store_ctx.iter_live_orders():
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
                raw_positions = await self._call('positions', method='get')
                symbol_positions = [
                    raw.get('position') or {}
                    for raw in (raw_positions.get('positions') or [])
                    if (raw.get('market') or {}).get('epic') == intent.symbol
                    and (raw.get('position') or {}).get('dealId')
                    and float((raw.get('position') or {}).get('size', 0.0)) > 0.0
                ]
                if not symbol_positions:
                    # The venue holds NOTHING on this symbol: no entry row,
                    # no sibling, no exposure — there is nothing an exit
                    # could protect. A raw reject would count as a refused
                    # protective exit (exit contract); skip instead, the next bar
                    # re-evaluates against fresh book state.
                    raise OrderSkippedByPlugin(
                        f"Exit {intent.intent_key} skipped: no entry row "
                        f"and the venue holds no open position on "
                        f"{intent.symbol!r}; re-evaluating next bar.",
                        intent_key=intent.intent_key,
                        reason="no_position_to_protect",
                        context={'symbol': intent.symbol,
                                 'from_entry': intent.from_entry},
                    )
                candidates = list(symbol_positions)
                expected_direction = 'BUY' if intent.side == 'sell' else 'SELL'
                if len(candidates) == 1:
                    orphan = candidates[0]
                    direction = (orphan.get('direction') or '').upper()
                    if direction != expected_direction:
                        candidates = []
                if len(candidates) == 1:
                    orphan = candidates[0]
                    orphan_size = float(orphan.get('size', 0.0))
                    orphan_side = 'long' if expected_direction == 'BUY' else 'short'
                    pos_side = 'buy' if expected_direction == 'BUY' else 'sell'
                    # No real ``client_order_id`` exists for the orphan
                    # position; synthesise one that is stable across retries
                    # (symbol + from_entry) so repeated defensive cycles converge
                    # on the same envelope. The exact venue deal comes from this
                    # same authoritative snapshot, avoiding a second-read race.
                    surrogate_coid = (
                        f"__pyne_orphan__{intent.symbol}__{intent.from_entry}"
                    )
                    orphan_deal_id = str(orphan['dealId'])
                    raise BracketAttachAfterFillRejectedError(
                        f"Capital execute_exit: no confirmed entry row in "
                        f"BrokerStore for from_entry={intent.from_entry!r} "
                        f"symbol={intent.symbol!r} but exchange reports one "
                        f"exact open {orphan_side} deal of size {orphan_size} — "
                        f"defensive close required",
                        position_deal_id=orphan_deal_id,
                        position_coid=surrogate_coid,
                        symbol=intent.symbol,
                        position_side=pos_side,
                        qty=orphan_size,
                        from_entry=intent.from_entry,
                    )
            raise ExchangeOrderRejectedError(
                f"Capital execute_exit: no confirmed entry row for "
                f"from_entry={intent.from_entry!r} symbol={intent.symbol!r}",
            )

        targets = await self._bracket_targets(intent, targets, rules)

        # --- Pre-validate bracket distances before any PUT ---
        # Capital.com checks distance against the live mid quote, not the
        # entry's fill price — using ``confirm_level`` would falsely reject
        # a valid bracket whenever price has drifted away from the entry.
        # The parent entry is already FILLED here, so a level the venue
        # would refuse leaves the position open and unprotected — exactly
        # the venue-side attach-reject case below. Classify it identically
        # (the sync engine dispatches a defensive close;
        # Pine-wise a level inside the minimum band is at-market anyway).
        # The levels are the same for every deal, so the check runs once and
        # names the oldest deal; the re-dispatch next sync reaches the rest.
        first = targets[0]
        try:
            if intent.sl_price is not None:
                self._validate_sl_distance(
                    rules, mid_price, float(intent.sl_price), first.side,
                )
            if intent.tp_price is not None:
                self._validate_tp_distance(
                    rules, mid_price, float(intent.tp_price), first.side,
                )
        except ExchangeOrderRejectedError as exc:
            raise BracketAttachAfterFillRejectedError(
                f"Capital bracket attach rejected after entry fill "
                f"(deal_id={first.exchange_order_id}): {exc}",
                position_deal_id=first.exchange_order_id or '',
                position_coid=first.client_order_id,
                symbol=intent.symbol,
                position_side=first.side,
                qty=first.qty,
                from_entry=intent.from_entry,
                level_crossed=exc.level_crossed,
            ) from exc

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

        legs: list[ExchangeOrder] = []
        shared = len(targets) > 1
        for target_row in targets:
            legs.extend(await self._attach_bracket_to_deal(
                envelope, target_row, body=body, trail_pending=trail_pending,
                shared=shared,
            ))
        return legs

    async def _bracket_targets(
            self, intent: ExitIntent, rows: list['OrderRow'],
            rules: _InstrumentRules,
    ) -> list['OrderRow']:
        """Pick the deals of ``intent.from_entry`` a whole-row bracket protects.

        The engine sizes a whole-row exit at the total the Pine book holds
        under the entry id, so the deals it covers normally add up to the
        exit's quantity and every one of them is protected. The comparison
        runs in ``lot_step`` units so rounding noise never splits a deal.

        ``OrderRow.qty`` is the size that was SUBMITTED, which is not always
        the size the venue holds: a one-way reversal posts one order covering
        the old exposure plus the new one (long 200 becomes ``SELL 400``) and
        the netting account keeps a 200-unit deal. When the stored sizes
        disagree with the exit, the live sizes are read from ``/positions``;
        a deal the venue no longer lists keeps its stored size and the PUT
        surfaces the not-found.

        * Live deals add up to the exit: all of them.
        * The venue holds LESS than the exit asks for: an own resting entry
          order under the same id is still being promoted to a position, so
          the exit waits (re-evaluated next sync) — protecting only the
          visible deals now would leave the promoted one bare, as no later
          exit change would follow to cover it. Without such an order the
          venue simply holds less (a deal closed under the book's feet), and
          every live deal is protected.
        * The venue holds MORE than the exit covers: the Pine book has not
          booked the newest deals yet. The oldest deals that make up the
          exit's quantity are protected, the order the Pine book closes the
          entry's trades in; the next exit change, which follows once the
          book catches up, covers the rest. An exit that ends inside a deal
          cannot be expressed on a whole-deal bracket, so that deal is
          protected whole rather than left bare, and the shortfall is
          journaled as ``bracket_covers_more_than_intent``.

        :param intent: The whole-row exit.
        :param rows: The entry's live position rows, oldest first.
        :param rules: The instrument's dealing rules.
        :return: The rows to protect, oldest first; never empty.
        :raises OrderSkippedByPlugin: ``reason='close_in_flight'`` while an
            own entry order under the id awaits its promotion.
        """
        step = rules.lot_step
        if step <= 0:
            return rows
        intent_units = round(intent.qty / step)
        if intent_units <= 0:
            return rows
        if sum(round(row.qty / step) for row in rows) == intent_units:
            return rows
        raw_positions = await self._call('positions', method='get')
        live_units: dict[str, int] = {}
        for raw in (raw_positions.get('positions') or []):
            position = raw.get('position') or {}
            deal_id = str(position.get('dealId') or '')
            if deal_id:
                live_units[deal_id] = round(float(position.get('size', 0.0)) / step)
        sized = [
            (row, live_units.get(str(row.exchange_order_id), round(row.qty / step)))
            for row in rows
        ]
        total = sum(units for _, units in sized)
        if total == intent_units:
            return rows
        if total < intent_units:
            if self._entry_promotion_pending(intent.from_entry):
                raise OrderSkippedByPlugin(
                    f"Exit {intent.intent_key} skipped: entry "
                    f"{intent.from_entry!r} holds {total * step} on the venue "
                    f"against the exit's {intent.qty} while an entry order under "
                    f"the same id is mid promotion to a position; re-evaluating "
                    f"next bar.",
                    intent_key=intent.intent_key,
                    reason="close_in_flight",
                    context={'symbol': intent.symbol,
                             'from_entry': intent.from_entry},
                )
            return rows
        selected: list['OrderRow'] = []
        covered = 0
        for row, units in sized:
            if covered >= intent_units:
                break
            selected.append(row)
            covered += units
        if covered != intent_units and self.store_ctx is not None:
            self.store_ctx.log_event(
                'bracket_covers_more_than_intent',
                intent_key=intent.intent_key,
                payload={'intent_qty': intent.qty,
                         'covered_qty': covered * step,
                         'deal_ids': [row.exchange_order_id for row in selected]},
            )
        return selected

    def _entry_promotion_pending(self, pine_entry_id: str | None) -> bool:
        """Whether an own resting entry order under ``pine_entry_id`` is live.

        A STOP / LIMIT working order that executed stays a ``confirmed``
        working row until the activity poll rewrites it to its position deal.

        :param pine_entry_id: The Pine entry id.
        """
        if self.store_ctx is None:
            return False
        return any(
            row.pine_entry_id == pine_entry_id
            and row.state == 'confirmed'
            and (row.extras or {}).get('kind') == ENTRY_KIND_WORKING
            for row in self.store_ctx.iter_live_orders()
        )

    def _exit_leg_rows(self, intent: ExitIntent, deal_id: str) -> list['OrderRow']:
        """The live TP/SL leg rows ``intent``'s exit holds on deal ``deal_id``.

        :param intent: The exit; its legs are keyed ``intent_key + '\\0TP'`` /
            ``'\\0SL'``.
        :param deal_id: The deal the legs sit on.
        """
        if self.store_ctx is None:
            return []
        leg_keys = (intent.intent_key + '\0TP', intent.intent_key + '\0SL')
        return [
            row for row in self.store_ctx.iter_live_orders()
            if row.intent_key in leg_keys
            and str((row.extras or {}).get('parent_deal_id') or '') == deal_id
            and (row.extras or {}).get('natural_close_at') is None
        ]

    @staticmethod
    def _bracket_leg_coid(
            envelope: DispatchEnvelope, kind: str, deal_id: str,
            existing: 'OrderRow | None', *, shared: bool,
    ) -> str:
        """The client order id of one bracket leg on one deal.

        A leg this exit already holds on the deal keeps its row, so a
        re-attach rewrites it instead of stacking a second live leg on the
        same deal. An exit on a single deal takes the envelope's own id. When
        the exit spans several deals, each deal's legs take an id derived
        from the envelope and the deal id — same run tag, bar and kind, so
        the journal still reads the dispatch anchor from it.

        :param envelope: The exit dispatch.
        :param kind: :data:`KIND_EXIT_TP` or :data:`KIND_EXIT_SL`.
        :param deal_id: The deal the leg sits on.
        :param existing: This exit's live leg row of the same kind on the deal.
        :param shared: Whether the exit spans several deals.
        """
        if existing is not None:
            return existing.client_order_id
        if not shared:
            return envelope.client_order_id(kind)
        intent = envelope.intent
        assert isinstance(intent, ExitIntent)
        return encode_wire_client_order_id(
            build_client_order_id(
                run_tag=envelope.run_tag,
                pine_id=(f"{intent.pine_id}{INTENT_KEY_SEP}{intent.from_entry}"
                         f"{INTENT_KEY_SEP}{deal_id}"),
                bar_ts_ms=envelope.bar_ts_ms,
                kind=kind,
                retry_seq=envelope.retry_seq,
            ),
            envelope.coid_max_len,
        )

    async def _attach_bracket_to_deal(
            self, envelope: DispatchEnvelope, target_row: 'OrderRow', *,
            body: dict, trail_pending: bool, shared: bool,
    ) -> list[ExchangeOrder]:
        """PUT the exit's bracket onto one deal and journal its legs.

        ``PUT /positions/{dealId}`` is full replacement: the body is the whole
        protection the deal carries afterwards. Legs this exit already holds
        on the deal are rewritten under their own rows; a leg the new body no
        longer carries is closed once the PUT landed (``bracket_leg_superseded``).
        When the exit has nothing for the venue yet (a trailing stop waiting
        for its activation) but the deal still carries this exit's earlier
        levels, the PUT clears them.

        :param envelope: The exit dispatch.
        :param target_row: The deal's position row.
        :param body: The exit's PUT fields.
        :param trail_pending: Whether the trailing stop waits for activation.
        :param shared: Whether the exit spans several deals.
        :return: The deal's synthetic TP / SL legs.
        """
        intent = envelope.intent
        assert isinstance(intent, ExitIntent)
        deal_id = str(target_row.exchange_order_id)
        previous = self._exit_leg_rows(intent, deal_id)
        tp_coid = self._bracket_leg_coid(
            envelope, KIND_EXIT_TP, deal_id,
            next((row for row in previous
                  if (row.extras or {}).get('leg_kind') == 'tp'), None),
            shared=shared,
        )
        sl_coid = self._bracket_leg_coid(
            envelope, KIND_EXIT_SL, deal_id,
            next((row for row in previous
                  if (row.extras or {}).get('leg_kind') == 'sl'), None),
            shared=shared,
        )
        written: set[str] = set()
        if intent.tp_price is not None:
            written.add(tp_coid)
        if intent.sl_price is not None or intent.trail_offset is not None:
            written.add(sl_coid)
        superseded = [row for row in previous if row.client_order_id not in written]
        body = dict(body)
        if not body and any(
                row.tp_level is not None or row.sl_level is not None
                or row.trailing_stop
                or (row.extras or {}).get('trail_state') == 'activating'
                for row in previous):
            body = {'profitLevel': None, 'stopLevel': None}
            if any(row.trailing_stop
                   or (row.extras or {}).get('trail_state') == 'activating'
                   for row in previous):
                body['trailingStop'] = False

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
            # Full replacement: the body is the whole protection the
            # position carries afterwards.
            with self._protection_write(deal_id, body) as protection:
                try:
                    resp = await protection.send(
                        self._put_bracket_with_notfound_retry(target_row, deal_id, body),
                    )
                except _PositionDealPending as pending:
                    # Nothing attached: the deal the PUT named cannot be
                    # reached yet. The legs go, and the exit is re-sent next
                    # sync against the row's current deal.
                    protection.refused()
                    self._rollback_bracket_legs(
                        intent=intent, parent_coid=target_row.client_order_id,
                        deal_id=deal_id,
                        tp_coid=tp_coid if intent.tp_price is not None else None,
                        sl_coid=sl_coid if has_sl_leg else None,
                        reason=f'position_deal_{pending.verdict.value}',
                    )
                    raise OrderSkippedByPlugin(
                        f"Exit {intent.intent_key} skipped: the deal of entry "
                        f"{intent.from_entry!r} is not addressable yet "
                        f"({deal_id} {pending.verdict.value}, the entry holds "
                        f"{pending.deal_id}); re-evaluating next sync.",
                        intent_key=intent.intent_key,
                        reason='position_deal_pending',
                        context={'symbol': intent.symbol,
                                 'from_entry': intent.from_entry,
                                 'deal_id': deal_id,
                                 'verdict': pending.verdict.value,
                                 'current_deal_id': pending.deal_id},
                    ) from pending
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
                    # engine can dispatch a defensive market close (anything
                    # else would leave the unprotected fill exposed
                    # indefinitely). The original mapped exception
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
                        level_crossed=(
                            isinstance(exc, ExchangeOrderRejectedError)
                            and exc.level_crossed
                        ),
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
                        protection.refused()
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
                protection.accepted()

        # --- Mark legs confirmed + persist risk on entry row ---
        # One transaction: a leg left ``submitted`` beside a confirmed one is
        # settled by no pass while the process runs. When it fails, the legs
        # the PUT carried go to the snapshot as after an unreadable confirm,
        # and the exit parks until the venue's levels resolve them.
        if self.store_ctx is not None:
            try:
                with self.store_ctx.transaction():
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
                    for row in superseded:
                        self.store_ctx.close_order(row.client_order_id)
                        self.store_ctx.log_event(
                            'bracket_leg_superseded',
                            client_order_id=row.client_order_id,
                            exchange_order_id=deal_id,
                            intent_key=intent.intent_key,
                            payload={'leg_kind': (row.extras or {}).get('leg_kind')},
                        )
            except sqlite3.Error as failed:
                if not body or (intent.tp_price is None and not sl_in_body):
                    # Nothing went to the venue, or no leg the snapshot could
                    # resolve: the rolled-back journal is the whole state.
                    raise
                with self.store_ctx.transaction():
                    self._mark_bracket_legs_disposition_unknown(
                        intent=intent, parent_coid=target_row.client_order_id,
                        deal_id=deal_id,
                        tp_coid=tp_coid if intent.tp_price is not None else None,
                        sl_coid=sl_coid if sl_in_body else None,
                        reason=f"attach bookkeeping failed: {failed}", stage='journal',
                    )
                    if has_sl_leg and not sl_in_body:
                        self.store_ctx.set_order_state(sl_coid, 'confirmed')
                raise OrderDispositionUnknownError(
                    f"Capital bracket on {deal_id} was accepted, but its legs could "
                    f"not be journaled: {failed}",
                    client_order_id=target_row.client_order_id,
                    cause=failed,
                ) from failed

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
        writer opening a fresh opposite row. The plugin reports a fill only
        when the post-snapshot proves the exact target-deal residual, and it
        deletes a reverse leg only when the venue confirm attributes that
        exact ``dealId`` to this POST. Unproven ownership fails closed.

        Both branches run through the Core
        :class:`~pynecore.core.broker.journal.DispatchJournal` so a
        process crash between the wire calls and the persisted result is
        recoverable: the close command row is written *before* the wire
        calls, and recovery on next start verifies each target ``dealId``
        against the positions snapshot (full close) or the
        ``deal_reference`` confirm GET (partial close).

        A ``held_execution_close`` is sized against the venue, not the
        journal: :meth:`_execute_held_execution_close`.
        """
        intent = envelope.intent
        assert isinstance(intent, CloseIntent)
        coid = envelope.client_order_id(KIND_CLOSE)
        rules = await self._get_instrument_rules(intent.symbol)
        if intent.synthetic_kind == 'held_execution_close':
            return await self._execute_held_execution_close(envelope, rules)

        # A keyed ``strategy.close(id)`` owns only the position rows opened by
        # that Pine entry id. Capital.com keeps same-direction entries as
        # separate ``dealId`` rows, so symbol-wide targeting would turn a
        # keyed close into a partial-close POST against the aggregate and could
        # reduce another entry's exposure. Synthetic close producers carry
        # explicit target provenance on the intent; the Pine-controlled
        # ``pine_id`` is never parsed for authority. Defensive closes target
        # one exact position COID. Only ``close_all`` (empty pine id) is
        # intentionally symbol-wide — over the run's own rows: a deal the
        # run's journal did not produce is never a close target.
        live_position_rows: list['OrderRow'] = []
        if self.store_ctx is not None:
            for row in self.store_ctx.iter_live_orders():
                extras = row.extras or {}
                if (row.state == 'confirmed'
                        and row.exchange_order_id
                        and extras.get('kind') == 'position'
                        and extras.get('natural_close_at') is None
                        and row.side != intent.side):
                    # A close order REDUCES opposite-side exposure: a buy
                    # close flattens sell rows and vice versa. A same-side
                    # row can never be this close's target — without the
                    # filter a symbol-wide close_all whose sell book was
                    # just flattened by in-flight DELETEs would grab a
                    # freshly filled BUY entry as its target and route a
                    # mis-directed reduction through the opposite-POST
                    # partial path (measured 2026-08-23, cycle 60).
                    live_position_rows.append(row)

        target_entry_id: str | None = None
        target_position_coid: str | None = None
        target_exchange_id: str | None = None
        defensive_close = intent.synthetic_kind == 'defensive_close'
        reversal_close = intent.synthetic_kind == 'reversal_close'
        if intent.synthetic_kind in {'partial_trigger', 'marketable_exit'}:
            target_entry_id = intent.target_entry_id
        elif defensive_close:
            target_position_coid = intent.target_position_coid
            target_exchange_id = intent.target_exchange_id
        elif reversal_close:
            # Symbol-wide by contract: the engine flattens the WHOLE book
            # before dispatching the raw reversing entry, and its synthetic
            # ``__pyne_reversal_close__*`` pine id matches no position row —
            # keyed targeting would reject a close the engine sized against
            # the full position. Leaving every target var None selects all
            # live rows below, and the kind is pinned to the native
            # per-``dealId`` DELETE so this close can never take the
            # opposite-POST partial path (which could open opposite
            # exposure against a racing protective fill).
            pass
        elif intent.pine_id:
            target_entry_id = intent.pine_id

        targets: list['OrderRow'] = []
        for row in live_position_rows:
            if target_entry_id is not None:
                if row.pine_entry_id == target_entry_id:
                    targets.append(row)
            elif target_position_coid is not None:
                # The deal the defensive close was cut for may have moved
                # onto the position's own ``dealId`` since (a market entry
                # executed through the venue's working-order machinery, see
                # ``identity.py``): the row then names the old deal as the
                # one it was promoted from.
                if (row.client_order_id == target_position_coid
                        and target_exchange_id is not None
                        and target_exchange_id in (
                            row.exchange_order_id,
                            (row.extras or {}).get('promoted_from_working_order_id'))):
                    targets.append(row)
            else:
                targets.append(row)

        # An orphan defensive close has no persisted position row, but the
        # engine carries the exact venue deal id captured at the bracket-reject
        # seam. Reconstruct only that deal from a fresh positions snapshot;
        # symbol/direction/quantity similarity is never accepted as ownership.
        if (not targets and self.store_ctx is not None and defensive_close
                and target_exchange_id is not None):
            raw_snap = await self._call('positions', method='get')
            target_broker_dir = 'BUY' if intent.side == 'sell' else 'SELL'
            # Recover the original Pine entry id from the orphan position COID.
            # Carrying it onto the synthetic row lets the close activity clean
            # the stale parent entry/exit state after the exact deal closes.
            parent_from_entry: str | None = None
            orphan_surrogate_prefix = f"__pyne_orphan__{intent.symbol}__"
            if (target_position_coid is not None
                    and target_position_coid.startswith(orphan_surrogate_prefix)):
                candidate = target_position_coid[len(orphan_surrogate_prefix):]
                if candidate:
                    parent_from_entry = candidate
            planned: list[tuple[str, str, float]] = []
            for raw in (raw_snap.get('positions') or []):
                market = raw.get('market') or {}
                if market.get('epic') != intent.symbol:
                    continue
                position = raw.get('position') or {}
                deal_id = position.get('dealId')
                if str(deal_id) != target_exchange_id:
                    continue
                direction = (position.get('direction') or '').upper()
                size = float(position.get('size', 0.0))
                if direction != target_broker_dir or size <= 0.0:
                    break
                planned.append((direction, str(deal_id), size))
                break

            if not planned:
                self.store_ctx.log_event(
                    'orphan_close_exact_target_missing',
                    intent_key=intent.intent_key,
                    payload={
                        'symbol': intent.symbol,
                        'broker_direction': target_broker_dir,
                        'target_deal_id': target_exchange_id,
                        'intent_qty': intent.qty,
                    },
                )

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
                    # The row, its ``deal_id`` ref and the audit commit
                    # together: a row without the ref would be a live
                    # position row the close activity cannot find.
                    with self.store_ctx.transaction():
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
            # A DELETE mirrors its target row to ``closing`` until the venue's
            # activity confirm lands (~a minute on Capital.com), so a re-issued
            # close inside that window finds no ``confirmed`` row even though
            # the whole owned exposure is already being flattened. That is not
            # an anomaly to reject — the close intent is satisfied by the
            # in-flight DELETE; declining lets the engine re-evaluate against
            # the settled book next sync without a counting error.
            closing_rows: list['OrderRow'] = []
            if self.store_ctx is not None:
                for row in self.store_ctx.iter_live_orders():
                    extras = row.extras or {}
                    if (row.state == 'closing'
                            and extras.get('kind') == 'position'
                            and row.side != intent.side
                            and (target_entry_id is None
                                 or row.pine_entry_id == target_entry_id)):
                        closing_rows.append(row)
            if closing_rows:
                raise OrderSkippedByPlugin(
                    f"Capital execute_close: every owned position row for "
                    f"symbol={intent.symbol!r} is already closing "
                    f"({len(closing_rows)} in-flight DELETE(s)); nothing to "
                    f"dispatch",
                    intent_key=intent.intent_key,
                    reason='close_already_in_flight',
                    context={
                        'symbol': intent.symbol,
                        'closing_deal_ids': [
                            row.exchange_order_id for row in closing_rows],
                    },
                )
            if intent.synthetic_kind is None:
                # A script-emitted close (keyed or close_all) that finds no
                # owned exposure on its reducing side is already satisfied —
                # the book is flat there (typically the prior close's DELETEs
                # settled between the script's re-emission and this dispatch).
                # Declining lets the engine re-evaluate against the settled
                # book next bar; raising here escapes the dispatch site as a
                # fatal run-crash (measured 2026-08-23, cycle 60). Synthetic
                # closes (defensive / reversal) keep the loud reject — their
                # producers own dedicated recovery contracts.
                raise OrderSkippedByPlugin(
                    f"Capital execute_close: no owned position rows on the "
                    f"reducing side for symbol={intent.symbol!r}; nothing to "
                    f"close",
                    intent_key=intent.intent_key,
                    reason='nothing_to_close',
                    context={'symbol': intent.symbol, 'side': intent.side},
                )
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
        close_write = {'close': intent.pine_id or '<close_all>',
                       'side': intent.side, 'qty': intent.qty,
                       'targets': sorted(row.exchange_order_id or '' for row in targets)}
        if has_orphan_synthetic or reversal_close:
            kind = KIND_FULL_CLOSE
        else:
            # ``kind='position'`` rows represent live venue exposure. Their
            # ``filled_qty`` is the entry fill already incorporated into that
            # exposure, not a quantity to subtract from it. Using
            # ``qty - filled_qty`` here turns every normally filled position
            # into zero live units and misroutes a whole-row close through the
            # opposite-POST partial-close path.
            stored_live_units = sum(
                round(max(0.0, row.qty) / rules.lot_step)
                if rules.lot_step > 0 else 0
                for row in targets
            )
            intent_units = (
                round(intent.qty / rules.lot_step) if rules.lot_step > 0 else 0
            )

            # A successful emulated partial close reduces the venue row but
            # deliberately leaves ``OrderRow.qty`` as the original entry-fill
            # quantity. A later close of the residual therefore cannot decide
            # full-vs-partial from the stored quantity alone: 100 remaining
            # units still sit behind a 200-unit entry row. When the intent is
            # smaller than that original quantity, resolve the current size at
            # the authoritative positions seam and compare only the dealIds
            # owned by these targets. This keeps the first 100-unit reduction
            # on the opposite-POST path and routes the subsequent residual 100
            # through native DELETE.
            #
            # The reduction POST cannot name a ``dealId``: the netting account
            # applies it FIFO to the oldest deal of the target's direction. A
            # deal this run did not open as old as the target or older could
            # be consumed, so a partial close is refused while one exists
            # (a younger one is never reached: the close never takes the
            # whole target) — from this fresh read, finally under the account
            # write lock right before the POST. The last poll's snapshot only
            # repeats a refusal a fresh read already reported, and only while
            # that refusal's block clock runs (no extra read for a refusal
            # that recurs every bar); it never latches a quarantine on its
            # own, so once the block is old enough to latch, or a block the
            # poll ended recurs, a fresh read decides again.
            total_live_units = stored_live_units
            other_same_side_deal_ids: set[str] = set()
            target_broker_direction = 'BUY' if intent.side == 'sell' else 'SELL'
            target_deal_ids = {
                row.exchange_order_id for row in targets
                if row.exchange_order_id
            }
            if intent_units != stored_live_units:
                cached_units = self._cached_deal_units(target_deal_ids, rules.lot_step)
                foreign = self._cached_exit_blockers(
                    intent.symbol, target_broker_direction, target_deal_ids,
                )
                if foreign and cached_units is not None and cached_units != intent_units:
                    reason = self._partial_close_block(foreign)
                    if self._foreign_exit_cached_refusal(reason, intent.intent_key, foreign):
                        raise self._foreign_exit_skip(
                            reason=reason, intent_key=intent.intent_key,
                            symbol=intent.symbol, foreign=foreign, write=close_write,
                            fresh=False, direction=target_broker_direction,
                            targets=target_deal_ids,
                        )
                raw_positions = await self._call('positions', method='get')
                symbol_rows = [
                    raw for raw in (raw_positions.get('positions') or [])
                    if (raw.get('market') or {}).get('epic') == intent.symbol
                ]
                symbol_positions = [raw.get('position') or {} for raw in symbol_rows]
                total_live_units = sum(
                    round(float(position.get('size', 0.0)) / rules.lot_step)
                    if rules.lot_step > 0 else 0
                    for position in symbol_positions
                    if position.get('dealId') in target_deal_ids
                )
                if intent_units != total_live_units:
                    foreign = self._exit_blockers(
                        symbol_rows, intent.symbol, target_broker_direction,
                        target_deal_ids,
                    )
                    if foreign:
                        raise self._foreign_exit_skip(
                            reason=self._partial_close_block(foreign),
                            intent_key=intent.intent_key, symbol=intent.symbol,
                            foreign=foreign, write=close_write, fresh=True,
                            direction=target_broker_direction, targets=target_deal_ids,
                        )
                # Only the run's own other deals on the side make the close
                # ambiguous; a younger foreign deal is never reached.
                owned = self._owned_deal_ids(symbol_rows)
                other_same_side_deal_ids = {
                    str(position['dealId'])
                    for position in symbol_positions
                    if position.get('dealId')
                    and position.get('dealId') not in target_deal_ids
                    and (owned is None or str(position['dealId']) in owned)
                    and (position.get('direction') or '').upper()
                    == target_broker_direction
                }

            kind = (
                KIND_FULL_CLOSE if intent_units == total_live_units
                else KIND_PARTIAL_CLOSE
            )
            if (kind == KIND_PARTIAL_CLOSE
                    and (len(targets) != 1 or other_same_side_deal_ids)):
                raise ExchangeOrderRejectedError(
                    "Capital execute_close: a partial close requires one "
                    "exclusive owned position row because the venue reduction "
                    "API cannot target a dealId"
                )

        # Nothing foreign blocks the exit now: a later block starts its
        # clock afresh.
        self._foreign_exit_unblocked(intent.intent_key)
        close_target_ids = frozenset(
            row.exchange_order_id for row in targets
            if row.exchange_order_id
        )
        hooks = _CapitalComCloseHooks(
            plugin=self,
            rules=rules,
            target_deal_ids=close_target_ids,
            target_coid=(targets[0].client_order_id
                         if kind == KIND_PARTIAL_CLOSE and targets else None),
        )
        journal = DispatchJournal(self.store_ctx)
        try:
            exch_order = await journal.run_close(
                coid=coid,
                intent=intent,
                kind=kind,
                targets=targets,
                hooks=hooks,
                audit_payload={'pine_id': intent.pine_id},
            )
        except ForeignDealBlocksWriteError as refused:
            # Refused under the account write lock: the command row stays as
            # the ``rejected`` audit trail; the engine re-evaluates the close
            # next bar and it goes through once the foreign deal is gone.
            raise self._foreign_exit_skip(
                reason=self._partial_close_block(refused.foreign_deals),
                intent_key=intent.intent_key, symbol=intent.symbol,
                foreign=refused.foreign_deals, write=close_write, fresh=True,
                direction='BUY' if intent.side == 'sell' else 'SELL',
                targets=close_target_ids,
            ) from refused
        except PositionSnapshotUnavailableError as unavailable:
            # The read under the account write lock failed: nothing was sent
            # and the command row stays as the ``rejected`` audit trail. A
            # skip, not an exchange reject, so a read outage never counts
            # toward the engine's close-reject quarantine.
            raise OrderSkippedByPlugin(
                f"Capital execute_close {intent.intent_key!r}: {unavailable}; "
                f"re-evaluated next bar",
                intent_key=intent.intent_key,
                reason='close_preflight_unavailable',
                context={'symbol': intent.symbol, 'write': close_write},
            ) from unavailable
        return exch_order

    #: A ``held_execution_close`` (the engine's close of the venue position over
    #: an entry whose execution evidence never arrived) is honoured on both
    #: account modes. On a netting account :meth:`execute_close` DELETEs whole
    #: own deals, the held entry's included, summing to at most ``intent.qty``
    #: (:meth:`_execute_held_execution_close`); on a hedging account the core
    #: one-way emulator closes the legs :meth:`fetch_raw_positions` lists, the
    #: held entry's included (:meth:`close_leg`).
    supports_held_execution_close = True

    async def _execute_held_execution_close(
            self, envelope: DispatchEnvelope, rules: _InstrumentRules,
    ) -> ExchangeOrder:
        """Close the run's venue position over an entry whose execution evidence never came.

        The engine read the venue position (:meth:`get_position`), which counts
        the deal of the held entry's order although no booked fill (and
        possibly no live journal row) stands behind it
        (:meth:`~pynecore_capitalcom.foreign._ForeignExposureMixin._held_execution_order_ids`),
        and sends that quantity as a symbol-wide ``held_execution_close``. It
        is closed here against a fresh ``GET /positions`` read, never against
        the journal's rows:

        * **Only own deals.** The targets are the deals on the reducing side
          the run owns (:meth:`~pynecore_capitalcom.foreign._ForeignExposureMixin._owned_deal_ids`),
          the held entry's included; a deal someone else opened is never
          touched. The deal of a take-over held for its unproven fill
          (:meth:`~pynecore_capitalcom.foreign._ForeignExposureMixin._held_take_over_refs`)
          waits until the bot has proven and booked that fill from the venue's
          history (:mod:`~pynecore_capitalcom.order_history`, the next poll
          that reads it): a close sent before would leave a close activity
          the bot could not attribute to itself. It is left open meanwhile,
          like a deal that does not fit; the engine reads the venue again
          before it sends another close.
        * **Never more than** ``intent.qty`` **and never the other side.** Each
          target is closed whole by ``DELETE /positions/{dealId}``, which
          closes exactly that deal and cannot open a position (research
          dossier §4 and §8; the DELETE ignores any ``size``, measured on demo
          2026-07-10). A partial close would need the opposite-direction
          ``POST /positions``, which nets FIFO into the oldest deal of the
          side, someone else's included, and opens the other side when the
          deal is gone meanwhile (§8, §9 #13): it is never used here. A deal
          whose own close is already in flight (``closing``) counts against
          the quantity and is not sent again; the rest go oldest first
          (``createdDateUTC``, then ``dealId``) while their whole size fits.
          A deal that does not fit is left open with a warning: the engine
          reads the venue again before it sends another close.
        * **Rows for the activity.** A target no live journal row tracks gets
          a seeded row (:meth:`_held_close_row`), so the close's activity
          routes to it like any own close. Every target row is stamped with
          the close's client order id, Pine id and the deal's venue size
          (:data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_HELD_CLOSE`) before
          the DELETE leaves; the fills are then reported under the close's
          client order id (the own-close record and the ``closing`` stamp
          carry it) and labelled with its Pine id
          (:func:`~pynecore_capitalcom.helpers._held_close_label`), and they
          are delivered through the deal's fill queue
          (:mod:`~pynecore_capitalcom.delivery`) behind any fill of the deal
          still owed, the held entry's included.

        The close is journaled under ``envelope.client_order_id(KIND_CLOSE)``
        as a full close of the targets
        (:meth:`~pynecore.core.broker.journal.DispatchJournal.run_close`), so
        a restart recovers it like any full close.

        :param envelope: The dispatch envelope of the ``held_execution_close``.
        :param rules: The instrument's dealing rules.
        :return: The close, reported at the sum of the targets' venue sizes.
        :raises OrderSkippedByPlugin: Nothing is sent: no own deal on the
            reducing side, every one already closing, or none that fits
            ``intent.qty`` whole.
        """
        intent = envelope.intent
        assert isinstance(intent, CloseIntent)
        store_ctx = self.store_ctx
        if store_ctx is None:
            raise RuntimeError(
                "execute_close requires an active store_ctx; the journal "
                "owns persistence and cannot operate without it.",
            )
        coid = envelope.client_order_id(KIND_CLOSE)
        snapshot = await self._call('positions', method='get')
        symbol_rows = [raw for raw in snapshot.get('positions') or []
                       if (raw.get('market') or {}).get('epic') == intent.symbol]
        # A held take-over's deal is owned, but its close waits for the fill
        # the bot proves from the venue's history: its close activity could
        # not be attributed before (see ``order_history.py``).
        owned = (self._owned_deal_ids(symbol_rows) or set()) - self._held_take_over_refs()
        direction = 'BUY' if intent.side == 'sell' else 'SELL'
        step = rules.lot_step if rules.lot_step > 0.0 else 0.0

        def units(qty: float) -> float:
            return float(round(qty / step)) if step > 0.0 else qty

        candidates: list[tuple[float, str, float, dict]] = []
        for raw in symbol_rows:
            position = raw.get('position') or {}
            deal_id = _wire_id(position.get('dealId'))
            size = _wire_float(position.get('size'), finite=True)
            if (deal_id is None or deal_id not in owned or size is None or size <= 0.0
                    or str(position.get('direction') or '').upper() != direction):
                continue
            created = _parse_iso_timestamp(str(position.get('createdDateUTC') or ''))
            candidates.append((created, deal_id, size, position))
        candidates.sort(key=lambda candidate: (candidate[0], candidate[1]))

        budget = units(intent.qty)
        tolerance = 1e-9 * max(1.0, abs(budget))
        in_flight: list[str] = []
        open_targets: list[tuple[str, float, dict, 'OrderRow | None']] = []
        for _, deal_id, size, position in candidates:
            row = self._held_close_target_row(position)
            if row is not None and row.state == 'closing':
                in_flight.append(deal_id)
                budget -= units(size)
                continue
            open_targets.append((deal_id, size, position, row))
        selected: list[tuple[str, float, dict, 'OrderRow | None']] = []
        left: list[str] = []
        for target in open_targets:
            if units(target[1]) <= budget + tolerance:
                selected.append(target)
                budget -= units(target[1])
            else:
                left.append(target[0])
        context = {'symbol': intent.symbol, 'side': intent.side, 'qty': intent.qty,
                   'owned_deals': [candidate[1] for candidate in candidates],
                   'in_flight': in_flight, 'left_open': left}
        if not selected:
            if left:
                reason = 'held_close_exceeds_qty'
                message = (f"every own deal on the reducing side is larger than the "
                           f"{intent.qty} left to close; a whole-deal DELETE would "
                           f"close more than the venue position the engine read")
            elif in_flight:
                reason = 'close_already_in_flight'
                message = "every own deal on the reducing side is already closing"
            else:
                reason = 'nothing_to_close'
                message = "no own deal on the reducing side"
            store_ctx.log_event('held_execution_close_skipped', client_order_id=coid,
                                intent_key=intent.intent_key,
                                payload={**context, 'reason': reason})
            raise OrderSkippedByPlugin(
                f"Capital execute_close {intent.intent_key!r} (held_execution_close, "
                f"{intent.symbol}): {message}; nothing sent",
                intent_key=intent.intent_key, reason=reason, context=context,
            )
        if left:
            broker_warning(
                "held-execution close %s on %s: own deal(s) %s do not fit the %s left to "
                "close whole and stay open; the engine reads the venue again before it "
                "sends another close",
                intent.pine_id, intent.symbol, ', '.join(left), intent.qty,
            )

        targets: list['OrderRow'] = []
        held_sizes: dict[str, float] = {}
        with store_ctx.transaction():
            for deal_id, size, position, row in selected:
                if row is None:
                    row = self._held_close_row(intent.symbol, position)
                current = store_ctx.get_order(row.client_order_id) or row
                store_ctx.upsert_order(row.client_order_id, extras={
                    **(current.extras or {}),
                    EXTRAS_KEY_HELD_CLOSE: {'coid': coid, 'pine_id': intent.pine_id,
                                            'size': size},
                })
                targets.append(store_ctx.get_order(row.client_order_id) or current)
                held_sizes[row.client_order_id] = size
            store_ctx.log_event(
                'held_execution_close_targets', client_order_id=coid,
                intent_key=intent.intent_key,
                payload={**context, 'targets': [target[0] for target in selected]},
            )
        hooks = _CapitalComCloseHooks(
            plugin=self,
            rules=rules,
            target_deal_ids=frozenset(
                row.exchange_order_id for row in targets if row.exchange_order_id),
            held_sizes=held_sizes,
        )
        return await DispatchJournal(store_ctx).run_close(
            coid=coid,
            intent=intent,
            kind=KIND_FULL_CLOSE,
            targets=targets,
            hooks=hooks,
            audit_payload={'pine_id': intent.pine_id, 'held_execution_close': True},
        )

    def _held_close_target_row(self, position: dict) -> 'OrderRow | None':
        """The live own journal row that tracks the deal of ``position``, if any.

        The row whose ``exchange_order_id`` is the deal's ``dealId`` first;
        else an own entry row the deal's ``workingOrderId`` back-links (its
        working order executed into the deal, or it was promoted from that
        order). A full-close DELETE on a working row's order id answers
        not-found and moves the row onto the deal it back-links before the
        DELETE is sent there (:meth:`~pynecore_capitalcom.identity._DealIdentityMixin._resolve_own_deal`).
        Bracket-leg rows never qualify, and a back-link to a row a previous
        design seeded at startup or a held take-over does not either.

        :param position: The ``position`` object of a ``GET /positions`` row.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return None
        deal_id = _wire_id(position.get('dealId'))
        working_order_id = _wire_id(position.get('workingOrderId'))
        linked: 'OrderRow | None' = None
        for row in store_ctx.iter_live_orders():
            extras = row.extras or {}
            if (row.state not in ('confirmed', 'closing')
                    or extras.get(EXTRAS_KEY_LEG_KIND) is not None
                    or extras.get('kind') not in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING)):
                continue
            if deal_id is not None and row.exchange_order_id == deal_id:
                return row
            if (linked is None and working_order_id is not None
                    and not extras.get(ADOPTED_STARTUP_EXTRA_KEY)
                    and extras.get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is None
                    and working_order_id in (
                        row.exchange_order_id,
                        _wire_id(extras.get('promoted_from_working_order_id')))):
                linked = row
        return linked

    def _held_close_row(self, symbol: str, position: dict) -> 'OrderRow':
        """Seed the position row of a held entry's deal no live journal row tracks.

        The deal is the run's own (its ``dealId`` or ``workingOrderId`` is an
        order of an entry the engine holds,
        :meth:`~pynecore_capitalcom.foreign._ForeignExposureMixin._held_execution_order_ids`),
        but no row would route its close activity: the activity of a deal
        without a row is someone else's. The seeded row is a ``confirmed``
        position on the deal, sized from the venue, with the ``deal_id``
        reference the activity loop resolves deals by. It books no exposure of
        the run (``filled_qty`` ``0.0``,
        :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_HELD_CLOSE_SEEDED`):
        the engine's book does not hold the execution. Its entry counts as
        accounted (``entry_filled_at``), so the deal's opening activity read
        later is not booked as a fresh entry of no Pine id; a close
        activity of the deal is a close.

        :param symbol: The epic.
        :param position: The deal's ``position`` object from ``GET /positions``.
        :return: The seeded row.
        """
        store_ctx = self.store_ctx
        assert store_ctx is not None
        deal_id = str(_wire_id(position.get('dealId')))
        size = _wire_float(position.get('size'), finite=True) or 0.0
        direction = str(position.get('direction') or '').upper()
        seeded_coid = f"__pyne_held_close__{symbol}__{deal_id}"
        now = epoch_time()
        with store_ctx.transaction():
            store_ctx.upsert_order(
                seeded_coid,
                symbol=symbol,
                side='buy' if direction == 'BUY' else 'sell',
                qty=size,
                filled_qty=0.0,
                state='confirmed',
                exchange_order_id=deal_id,
                extras={
                    'kind': ENTRY_KIND_POSITION,
                    'entry_filled_at': now,
                    EXTRAS_KEY_HELD_CLOSE_SEEDED: now,
                },
            )
            store_ctx.add_ref(seeded_coid, 'deal_id', deal_id)
            store_ctx.log_event(
                'held_execution_close_target_seeded',
                client_order_id=seeded_coid,
                exchange_order_id=deal_id,
                payload={'symbol': symbol, 'direction': direction, 'size': size,
                         'working_order_id': _wire_id(position.get('workingOrderId'))},
            )
            seeded = store_ctx.get_order(seeded_coid)
        assert seeded is not None
        return seeded

    async def _held_close_leg_row(self, symbol: str, leg_id: str) -> 'OrderRow | None':
        """The row a core leg close of a held entry's untracked deal routes its fill through.

        :meth:`close_leg` found no journal row for ``leg_id``. The leg came from
        :meth:`fetch_raw_positions`, which lists a deal an order of a held entry
        opened (:meth:`~pynecore_capitalcom.foreign._ForeignExposureMixin._held_execution_order_ids`)
        even without one. Such a deal gets a seeded row (:meth:`_held_close_row`)
        before the DELETE leaves, so its close activity routes to a row and is
        reported under the leg close's ``coid``; any other deal stays as it is.

        :param symbol: The epic.
        :param leg_id: The leg's ``dealId``.
        :return: The seeded row, or ``None`` when the deal is not listed or is
            not a held entry's.
        :raises ExchangeConnectionError: The ``GET /positions`` read failed;
            nothing was sent.
        """
        held = self._held_execution_order_ids() - self._held_take_over_refs()
        if not held:
            return None
        snapshot = await self._call('positions', method='get')
        for raw in snapshot.get('positions') or []:
            if (raw.get('market') or {}).get('epic') != symbol:
                continue
            position = raw.get('position') or {}
            if _wire_id(position.get('dealId')) != leg_id:
                continue
            working_order_id = _wire_id(position.get('workingOrderId'))
            if leg_id in held or (working_order_id is not None and working_order_id in held):
                return self._held_close_row(symbol, position)
            return None
        return None

    async def execute_cancel(
            self, envelope: DispatchEnvelope,
    ) -> bool:
        """Cancel pending working orders and/or bracket legs matching the intent.

        Idempotent: if no live row matches the intent, returns ``True``
        with a ``cancel_noop`` audit event. If the exchange reports
        ``error.not-found.dealId`` / ``.dealReference`` on the DELETE,
        the plugin treats it as already-gone (benign race), logs a
        ``cancel_already_gone`` event, and continues.

        A take-over held because its fill could not be proven
        (:data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_TAKE_OVER_UNPROVEN`)
        is never swept: its working order already executed into a live
        position, so a DELETE answers 404 and closing the row would release
        the entry for a second send. The row stays as it is until the bot has
        proven its fill from the venue's history (:mod:`~pynecore_capitalcom.order_history`),
        a warning says so (:meth:`_hold_take_over_dispatch`), and the call
        returns ``False``: the cancel did not land, so the engine keeps the
        entry, sends no replacement and retries; once the fill is booked the
        cancel is a no-op on a filled position. The same holds for a row the poll stamps while
        its DELETE is in flight: the sweep reads it again before closing it
        and leaves it live. A resting entry with no execution read whose
        DELETE answers 404 is not closed either unless the venue's activity
        shows the order ended without a fill: the order may have executed (a
        taken-over one before its id was known, an own one before its
        activity was read), and the execution is booked onto the live row.
        Until then the call returns ``False`` too, so no cancel+create sends
        the entry a second time.

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
        held = False
        if self.store_ctx is not None:
            for row in self.store_ctx.iter_live_orders(
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
                if not matches_pine:
                    continue
                if (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None:
                    self._hold_take_over_dispatch(row, 'cancel')
                    held = True
                    continue
                targets.append(row)

        if held and not targets:
            return False

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
        return not held and not hooks.held_coids and not hooks.unsettled_coids

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
          :attr:`CancelDispositionOutcome.UNKNOWN` so the loop retries. A row
          an earlier DELETE of which the venue accepted
          (:meth:`~pynecore_capitalcom.intervention._InterventionMixin._own_cancel_accepted`)
          is :attr:`CancelDispositionOutcome.CANCEL_CONFIRMED` without the
          history: that DELETE cancelled the resting order, also when its
          booking could not be journaled.
        * Transient network failure → :class:`OrderDispositionUnknownError`
          (the cancel-tentative stays armed, but the probe does not count as
          an answer from the venue).

        Only pending entry working orders enter the cancel-tentative state,
        so bracket-leg and filled-position rows are not classified here; an
        empty working-order target set returns
        :attr:`CancelDispositionOutcome.UNKNOWN` rather than fabricating a
        resolution, unless the entry has no live row other than the ones its
        working order's fill moved onto the position and its last order's
        journaled ending answers the probe
        (:meth:`_ended_working_order_outcome`).

        A take-over held because its fill could not be proven
        (:data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_TAKE_OVER_UNPROVEN`)
        is not DELETEd and not classified: it stays as it is until the bot has
        proven its fill from the venue's history, a warning says so, and it
        counts as
        :attr:`CancelDispositionOutcome.UNKNOWN`, so the engine neither
        settles the cancel nor books a fill the venue never proved. A row the
        poll stamps while its DELETE is in flight counts the same way, and a
        row whose fill the poll books meanwhile is never closed and counts as
        :attr:`CancelDispositionOutcome.ALREADY_FILLED`
        (:meth:`_finalize_cancel_outcome_row` reads it again first). A
        working row whose execution was read before the cancel
        (:func:`~pynecore_capitalcom.delivery.entry_executed`) is not
        DELETEd: it counts as
        :attr:`CancelDispositionOutcome.ALREADY_FILLED` at once.

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

        outcomes: list[CancelDispositionOutcome] = []
        targets: list[tuple[str, str]] = []
        entry_live = False
        promoted: list['OrderRow'] = []
        for row in self.store_ctx.iter_live_orders(
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
            if extras.get('promoted_from_working_order_id') is not None:
                promoted.append(row)
                continue
            entry_live = True
            if extras.get('kind', ENTRY_KIND_POSITION) != ENTRY_KIND_WORKING:
                continue
            if extras.get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None:
                self._hold_take_over_dispatch(row, 'cancel')
                outcomes.append(CancelDispositionOutcome.UNKNOWN)
                continue
            if entry_executed(extras):
                # Its execution was read before its promotion: the order is
                # gone because it executed, so the row is a live position
                # and no DELETE is sent.
                outcomes.append(self._finalize_cancel_outcome_row(
                    row.client_order_id, row.exchange_order_id or '',
                    CancelDispositionOutcome.ALREADY_FILLED,
                    intent=intent, already_gone=False,
                ))
                continue
            if row.exchange_order_id:
                targets.append((row.client_order_id, row.exchange_order_id))

        if not targets:
            if not entry_live and not outcomes:
                ended = self._ended_working_order_outcome(intent, promoted)
                if ended is not None:
                    return ended
            if CancelDispositionOutcome.ALREADY_FILLED in outcomes:
                return CancelDispositionOutcome.ALREADY_FILLED
            return CancelDispositionOutcome.UNKNOWN

        for coid, deal_id in targets:
            try:
                with self._own_cancel(coid) as sent_at:
                    await self._call(
                        f'workingorders/{deal_id}', method='delete',
                    )
            except (httpx.TimeoutException, httpx.RequestError,
                    ConnectionError, ExchangeConnectionError) as exc:
                # Transient — the row stays live so the retry loop re-runs.
                raise OrderDispositionUnknownError(
                    f"Capital.com cancel transport failure for working order "
                    f"{deal_id}; disposition unknown",
                    client_order_id=coid, cause=exc,
                ) from exc
            except OrderNotFoundError:
                # An earlier DELETE of this order the venue accepted while it
                # was still resting cancelled it: the 404 now is that cancel,
                # whether or not its booking was journaled and whether or not
                # the activity history still lists it.
                if self._own_cancel_accepted(coid):
                    resolved = CancelDispositionOutcome.CANCEL_CONFIRMED
                else:
                    resolved = await self._classify_cancel_via_activity(deal_id)
                outcomes.append(self._finalize_cancel_outcome_row(
                    coid, deal_id, resolved, intent=intent,
                    already_gone=True,
                ))
                continue
            # Clean DELETE: the broker order is gone. Close and audit the
            # live working row here — the sync engine's cancel-tentative
            # resolution only drops in-memory mappings / leg ledger state,
            # never the persisted ``OrderRow``. Leaving it live would make
            # the next ``_reconcile_snapshot`` see a row whose deal is
            # absent from both ``/positions`` and ``/workingorders``, stamp
            # ``missing_pending_since``, and eventually raise a false
            # :class:`UnexpectedCancelError`.
            self._stamp_own_cancel_accepted(coid, sent_at)
            outcomes.append(self._finalize_cancel_outcome_row(
                coid, deal_id, CancelDispositionOutcome.CANCEL_CONFIRMED,
                intent=intent, already_gone=False,
            ))

        if CancelDispositionOutcome.ALREADY_FILLED in outcomes:
            return CancelDispositionOutcome.ALREADY_FILLED
        if CancelDispositionOutcome.UNKNOWN in outcomes:
            return CancelDispositionOutcome.UNKNOWN
        if CancelDispositionOutcome.TOO_LATE_TO_CANCEL in outcomes:
            return CancelDispositionOutcome.TOO_LATE_TO_CANCEL
        return CancelDispositionOutcome.CANCEL_CONFIRMED

    def _ended_working_order_outcome(
            self, intent: CancelIntent, promoted: 'list[OrderRow]',
    ) -> CancelDispositionOutcome | None:
        """The cancel outcome the journal holds for an entry order that already ended.

        The probe of a cancel can find no working order of its entry left:
        the order ended meanwhile, by a booking of this process that the
        probe's caller never saw (its result raised), or by a restart that
        judged the order gone from the venue (``_retire_startup_orphans``) or
        booked its fill, where the engine re-arms the cancel from its
        journal. The entry's most recently placed working order is the one
        the cancel aims at: a closed working-order row, or a row the order's
        fill moved onto its position (``promoted_from_working_order_id``),
        live or closed. Closed without a fill, the order is gone without one,
        which the probe reports as a confirmed cancel; still live on its
        open position, the order filled, which the probe reports as filled.
        A row whose fill is booked (``filled_qty``) or whose execution was read
        (:func:`~pynecore_capitalcom.delivery.entry_executed`) and
        whose position closed (a closed row, or a live one its bracket's fill
        stamped ``natural_close_at``) is reported as filled and closed: the
        order filled, but its position is no longer open to restore
        protection onto. A live row stamped ``natural_close_at`` whose fill
        the journal does not show decides nothing.

        :param intent: The cancel's intent.
        :param promoted: The entry's live rows its working order's fill moved
            onto the position.
        :return: :attr:`CancelDispositionOutcome.CANCEL_CONFIRMED`,
            :attr:`CancelDispositionOutcome.ALREADY_FILLED`,
            :attr:`CancelDispositionOutcome.FILLED_AND_CLOSED`, or ``None``
            when no ending of the entry's last order decides the cancel.
        """
        assert self.store_ctx is not None
        ended: 'OrderRow | None' = None
        for row in self.store_ctx.iter_orders_closed_since_for_run_id(0):
            extras = row.extras or {}
            if (extras.get('leg_kind') is not None
                    or (extras.get('kind') != ENTRY_KIND_WORKING
                        and extras.get('promoted_from_working_order_id') is None)
                    or not (row.pine_entry_id == intent.pine_id
                            or (intent.from_entry is not None
                                and row.from_entry == intent.from_entry))):
                continue
            if ended is None or row.created_ts_ms >= ended.created_ts_ms:
                ended = row
        for row in promoted:
            if ended is None or row.created_ts_ms >= ended.created_ts_ms:
                ended = row
        if ended is None:
            return None
        extras = ended.extras or {}
        fill_booked = ended.filled_qty > 0.0 or entry_executed(extras)
        if ended.closed_ts_ms is None:
            if extras.get('natural_close_at') is None:
                return CancelDispositionOutcome.ALREADY_FILLED
            return CancelDispositionOutcome.FILLED_AND_CLOSED if fill_booked else None
        if fill_booked:
            return CancelDispositionOutcome.FILLED_AND_CLOSED
        return CancelDispositionOutcome.CANCEL_CONFIRMED

    def _hold_take_over_dispatch(self, row: 'OrderRow', action: str) -> None:
        """Report a cancel or amend of a held take-over not sent, or not settled.

        The row is stamped
        :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_TAKE_OVER_UNPROVEN`:
        its working order executed into a position whose fill the snapshot
        could not prove. Until the bot has proven that fill from the venue's
        history (:mod:`~pynecore_capitalcom.order_history`), the engine's
        cancel or amend leaves the row as it is, also when the stamp came
        while the cancel's DELETE was in flight. One warning and a
        ``take_over_unproven_dispatch_held`` audit event per row and action
        for the process say so (the engine retries an unlanded cancel every
        sync, and once the fill is booked the row is a filled position).

        :param row: The stamped entry row.
        :param action: ``'cancel'`` or ``'modify'``.
        """
        coid = row.client_order_id
        if (coid, action) in self._take_over_dispatch_held_reported:
            return
        self._take_over_dispatch_held_reported.add((coid, action))
        unproven = (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) or {}
        broker_warning(
            "%s of entry %s (submission %s) not sent or not settled: its deal %s showed "
            "size %s instead of the submitted %s, so the entry stays held until the "
            "bot has proven its fill from the venue's history",
            action, row.intent_key, coid, unproven.get('deal_id'), unproven.get('size'),
            row.qty,
        )
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'take_over_unproven_dispatch_held',
                client_order_id=coid,
                exchange_order_id=row.exchange_order_id,
                intent_key=row.intent_key,
                payload={'action': action, 'submitted': row.qty, 'unproven': unproven},
            )

    def _finalize_cancel_outcome_row(
            self, coid: str, deal_id: str,
            outcome: CancelDispositionOutcome, *,
            intent: CancelIntent, already_gone: bool,
    ) -> CancelDispositionOutcome:
        """Close and audit a working row whose cancel resolved terminally.

        Mirrors the ``_CapitalComCancelHooks`` working-order sweep so the
        :meth:`execute_cancel_with_outcome` retry path leaves the same
        audit trail and never strands a live :class:`OrderRow` whose deal
        is gone broker-side.

        The row is read again under the store's write lock first, because
        the poll may have changed it while the DELETE was in flight (its
        order had executed):

        * Stamped
          :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_TAKE_OVER_UNPROVEN`:
          the row is left as it is whatever the outcome, a warning says so
          (:meth:`_hold_take_over_dispatch`), and the cancel counts as
          :attr:`CancelDispositionOutcome.UNKNOWN`.
        * Its fill booked (``kind`` is a position, or its execution was read
          before its promotion,
          :func:`~pynecore_capitalcom.delivery.entry_executed`): the row is a live
          position and is never closed from a cancel; a ``cancel_noop`` event
          is logged and the cancel counts as
          :attr:`CancelDispositionOutcome.ALREADY_FILLED`, whatever the
          DELETE or the activity said.

        Otherwise only terminal-cancel outcomes close the row:

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
        :return: The disposition the cancel reports for this row.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return outcome
        with store_ctx.transaction():
            current = store_ctx.get_order(coid)
            current_extras = (current.extras or {}) if current is not None else {}
            booked = (current is not None
                      and (current_extras.get('kind') == ENTRY_KIND_POSITION
                           or entry_executed(current_extras)))
            if current_extras.get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None:
                held = current
            elif booked:
                held = None
                store_ctx.log_event(
                    'cancel_noop',
                    client_order_id=coid,
                    exchange_order_id=current.exchange_order_id if current is not None else None,
                    intent_key=intent.intent_key,
                    payload={'reason': 'already_filled',
                             'pine_id': intent.pine_id,
                             'from_entry': intent.from_entry},
                )
            else:
                held = None
                if outcome in (CancelDispositionOutcome.CANCEL_CONFIRMED,
                               CancelDispositionOutcome.TOO_LATE_TO_CANCEL):
                    if already_gone:
                        store_ctx.log_event(
                            'cancel_already_gone',
                            client_order_id=coid,
                            exchange_order_id=deal_id,
                            intent_key=intent.intent_key,
                        )
                    store_ctx.close_order(coid)
                    store_ctx.log_event(
                        'cancelled',
                        client_order_id=coid,
                        exchange_order_id=deal_id,
                        intent_key=intent.intent_key,
                        payload={'via': 'cancel_with_outcome',
                                 'outcome': outcome.value},
                    )
        if held is not None:
            self._hold_take_over_dispatch(held, 'cancel')
            return CancelDispositionOutcome.UNKNOWN
        if booked:
            return CancelDispositionOutcome.ALREADY_FILLED
        return outcome

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

        An entry held because its take-over's fill could not be proven
        (:data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_TAKE_OVER_UNPROVEN`)
        is neither amended nor cancelled and re-sent: its working order
        already executed into a live position, so a cancel+create would open
        the entry a second time. The held row is returned unchanged, with a
        warning (:meth:`_hold_take_over_dispatch`), until the bot has proven
        its fill from the venue's history. The same holds
        when the poll holds the row while the cancel+create runs
        (:meth:`_cancel_and_replace_entry`).
        """
        from pynecore.core.broker.journal import DispatchJournal

        from .dispatch_hooks import _CapitalComModifyEntryHooks

        old_i = old.intent
        new_i = new.intent
        assert isinstance(old_i, EntryIntent)
        assert isinstance(new_i, EntryIntent)

        if self.store_ctx is not None:
            for row in self.store_ctx.iter_live_orders():
                if (row.pine_entry_id == old_i.pine_id
                        and (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None):
                    self._hold_take_over_dispatch(row, 'modify')
                    return [self._row_to_exchange_order(row, old_i)]

        rules = await self._get_instrument_rules(new_i.symbol)
        old_units = (
            round(old_i.qty / rules.lot_step) if rules.lot_step > 0 else 0
        )
        new_units = (
            round(new_i.qty / rules.lot_step) if rules.lot_step > 0 else 0
        )
        if old_units != new_units or old_i.order_type != new_i.order_type:
            return await self._cancel_and_replace_entry(old, new)

        target_coid = old.client_order_id(KIND_ENTRY)
        target_row: 'OrderRow | None' = (
            self.store_ctx.get_order(target_coid)
            if self.store_ctx is not None else None
        )
        if target_row is None or not target_row.exchange_order_id:
            return await self._cancel_and_replace_entry(old, new)

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

    async def _cancel_and_replace_entry(
            self, old: DispatchEnvelope, new: DispatchEnvelope,
    ) -> list[ExchangeOrder]:
        """Replace an entry the venue cannot amend: cancel it, then send ``new``.

        The replacement goes out only when the cancel landed. The cancel does
        not land when it meets a take-over held because its fill could not be
        proven (:data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_TAKE_OVER_UNPROVEN`),
        also one the poll held while the cancel's DELETE was in flight
        (:meth:`execute_cancel`): its working order already executed into a
        live position, so the replacement would open the entry a second time.
        Nor does it land on an unbooked take-over whose order is gone without
        the venue's word that it ended unfilled (:meth:`execute_cancel`).
        Then nothing is sent and the entry's rows are returned unchanged; for
        a held take-over a warning says so (:meth:`_hold_take_over_dispatch`).

        :param old: The entry as the venue holds it.
        :param new: The entry Pine wants.
        :return: The replacement's orders, or the entry's unchanged rows as orders.
        """
        old_i = old.intent
        assert isinstance(old_i, EntryIntent)
        cancel = DispatchEnvelope(
            intent=CancelIntent(pine_id=old_i.pine_id, symbol=old_i.symbol),
            run_tag=new.run_tag,
            bar_ts_ms=new.bar_ts_ms,
            retry_seq=new.retry_seq,
            coid_max_len=new.coid_max_len,
        )
        if await self.execute_cancel(cancel):
            return await self.execute_entry(new)
        kept: list[ExchangeOrder] = []
        if self.store_ctx is not None:
            for row in self.store_ctx.iter_live_orders():
                extras = row.extras or {}
                if (row.pine_entry_id != old_i.pine_id
                        or extras.get('kind') not in (ENTRY_KIND_WORKING, ENTRY_KIND_POSITION)):
                    continue
                if extras.get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None:
                    self._hold_take_over_dispatch(row, 'modify')
                kept.append(self._row_to_exchange_order(row, old_i))
        return kept

    @override
    async def modify_exit(
            self, old: DispatchEnvelope, new: DispatchEnvelope,
    ) -> list[ExchangeOrder]:
        """Amend an open position's TP/SL levels in place.

        ``PUT /positions/{dealId}`` re-writes ``profitLevel`` /
        ``stopLevel`` atomically — no cancel window where the position
        sits unprotected. A ``from_entry`` change is a different bracket
        and falls through to cancel+create.

        The bracket is a per-deal attribute, not a sized order, so a size
        change only changes which deals of the entry the exit covers: a
        same-id pyramid add filled (the exit grows over a second deal) or
        one of the entry's deals closed. That, and any amend of an exit
        that spans several deals, re-runs :meth:`execute_exit`: every live
        deal of the entry gets the new levels in one full-replacement PUT,
        its existing legs are rewritten under their own rows, and no deal
        is cleared first.

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

        old_i = old.intent
        new_i = new.intent
        assert isinstance(old_i, ExitIntent)
        assert isinstance(new_i, ExitIntent)

        if old_i.from_entry != new_i.from_entry:
            return await super().modify_exit(old, new)

        # Locate the parent position rows via their confirmed entry. Same
        # NULL-vs-from_entry + natural_close_at skip as :meth:`execute_exit`
        # — see :meth:`_find_active_entry_rows`.
        targets = self._find_active_entry_rows(new_i.from_entry)
        if not targets:
            return await super().modify_exit(old, new)
        if len(targets) > 1 or abs(old_i.qty - new_i.qty) > 1e-9:
            return await self.execute_exit(new)
        target_row = targets[0]
        # Same wait as :meth:`execute_exit`: the amend would replace levels
        # someone else may have set while who changed them is undecided.
        self._defer_protection_write(
            target_row.exchange_order_id, intent_key=new_i.intent_key,
            symbol=new_i.symbol,
        )

        # Live mid drives the pre-check: Capital.com validates against the
        # current quote, not the entry's confirmed level.
        mid_price = await self._get_current_mid_price(new_i.symbol)
        rules = await self._get_instrument_rules(new_i.symbol)
        # The parent position is already open — a min-distance reject here is
        # equivalent to the venue rejecting the amend, so it must surface as a
        # bracket-attach reject (defensive close path), never as a raw
        # ``ExchangeOrderRejectedError`` which the engine treats as a fatal
        # contract violation on the exit path.
        try:
            if new_i.sl_price is not None:
                self._validate_sl_distance(
                    rules, mid_price, float(new_i.sl_price), target_row.side,
                )
            if new_i.tp_price is not None:
                self._validate_tp_distance(
                    rules, mid_price, float(new_i.tp_price), target_row.side,
                )
        except ExchangeOrderRejectedError as exc:
            raise BracketAttachAfterFillRejectedError(
                f"Capital bracket amend rejected on an open position "
                f"(deal_id={target_row.exchange_order_id}): {exc}",
                position_deal_id=target_row.exchange_order_id,
                position_coid=target_row.client_order_id,
                symbol=new_i.symbol,
                position_side=target_row.side,
                qty=target_row.qty,
                from_entry=new_i.from_entry,
                level_crossed=exc.level_crossed,
            ) from exc

        tp_coid = new.client_order_id(KIND_EXIT_TP)
        sl_coid = new.client_order_id(KIND_EXIT_SL)
        planned = self._plan_modify_exit(
            target_row, old_i, new_i, tp_coid=tp_coid, sl_coid=sl_coid,
        )
        if planned is None:
            return []
        body, hooks = planned
        effective_tp_coid, effective_sl_coid = hooks.effective_leg_coids
        coid = new.client_order_id(KIND_MODIFY_EXIT)

        if body:
            # Wire round-trip required — run through the Core journal so
            # the entry-side command row is persisted and state-machined.
            # The hook owns the PUT + confirm + ambiguous-path seeding.
            if self.store_ctx is None:
                raise RuntimeError(
                    "modify_exit requires a live BrokerStore context"
                )
            journal = DispatchJournal(self.store_ctx)
            try:
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
            except OrderDispositionUnknownError as unknown:
                # A journal write the Core makes after the PUT failed (the
                # ``dealReference``, or the park of an ambiguous PUT): the
                # Core parks the amend under the command's id with the legs
                # untouched, where nothing the poll resolves would release it.
                # Parked like a failed mirror, the snapshot settles it.
                failed = unknown.__cause__
                if unknown.client_order_id != coid or not isinstance(failed, sqlite3.Error):
                    raise
                park_hooks = hooks
                if hooks.mirror_committed:
                    # The legs carry the amend already: parked from the bracket
                    # they hold, as a restart parks it
                    # (:meth:`_park_unsettled_modify_exit`), never from the
                    # bracket before the amend, whose transition (a stop
                    # replaced by a local pending trail) would refuse the legs
                    # the venue holds.
                    replanned = self._plan_modify_exit(
                        target_row, self._bracket_held_by_legs(target_row, new_i), new_i,
                        tp_coid=tp_coid, sl_coid=sl_coid,
                    )
                    if replanned is not None:
                        park_hooks = replanned[1]
                raise park_hooks.park_after_journal_failure(
                    new_i, failed, stage='journal', deal_reference=hooks.accepted_ref or None,
                    command_coid=coid,
                ) from failed

        # Pure local seed amend (e.g. pending-trail bookkeeping with an
        # empty PUT body): no broker round-trip means there is nothing
        # for the journal to state-machine. Apply the post-success mirror
        # directly so the leg rows and entry-row risk snapshot are
        # consistent with the new intent. All of it or none: the venue holds
        # nothing of this amend, so a failed write leaves the old bracket.
        if self.store_ctx is not None:
            with self.store_ctx.transaction():
                hooks.apply_mirror(target_row=target_row, new_intent=new_i)
        else:
            hooks.apply_mirror(target_row=target_row, new_intent=new_i)
        return hooks.exchange_order_from_state(
            row=target_row, new_intent=new_i,
            outcome=ModifyExitOutcome(
                server_ref='', deal_status='ACCEPTED',
                rejected_reason=None, post_put_state={}, raw=None,
            ),
        )

    def _plan_modify_exit(
            self, target_row: 'OrderRow', old_i: ExitIntent, new_i: ExitIntent, *,
            tp_coid: str, sl_coid: str,
    ) -> 'tuple[dict, _CapitalComModifyExitHooks] | None':
        """Build the ``PUT /positions/{dealId}`` body of a bracket amend and its hooks.

        One derivation for the amend (:meth:`modify_exit`) and for the restart
        that parks an amend whose park never committed
        (:meth:`_park_unsettled_modify_exit`): the body, the legs the amend
        touches and how an unknown outcome leaves them for the snapshot.

        :param target_row: The position's entry row.
        :param old_i: The bracket before the amend.
        :param new_i: The bracket the amend sets.
        :param tp_coid: The COID a take-profit leg the amend adds is written under.
        :param sl_coid: The COID a stop leg the amend adds is written under.
        :return: The PUT body and the amend's hooks, or ``None`` when the amend
            changes nothing.
        """
        from .dispatch_hooks import _CapitalComModifyExitHooks

        # Pine ``trail_price`` + ``trail_offset`` defers native trailing
        # until the activation monitor sees the threshold crossed. The
        # PUT must NOT include ``trailingStop`` in that case — the leg
        # row is persisted in ``pending`` state below and the activation
        # monitor issues its own PUT later.
        trail_pending = (
            new_i.trail_offset is not None and new_i.trail_price is not None
        )
        old_trailing_active = False
        if self.store_ctx is not None:
            old_trailing_active = any(
                row.from_entry == old_i.from_entry
                and (row.extras or {}).get('leg_kind') == 'sl'
                and row.trailing_stop
                for row in self.store_ctx.iter_live_orders()
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
                and (old_i.trail_price is None or old_trailing_active)
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
            return None

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

        return body, _CapitalComModifyExitHooks(
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
        )

    def _park_unsettled_modify_exit(
            self, command: 'OrderRow', deal_reference: str | None,
    ) -> bool:
        """Hand a restart's unsettled bracket amend to the positions snapshot.

        A pending ``modify_exit`` command a restart finds while no leg of its
        position waits for the snapshot, and no leg of it was refused by one
        (:meth:`~pynecore_capitalcom.reconcile._ReconcileMixin._modify_exit_legs_refused`),
        was sent, but neither its completion nor the park of its legs
        committed (the process ended first): its legs may still hold the
        bracket from before the amend while the venue holds the new one, and
        the engine's park on the position's entry, which only a settled leg
        releases, would stay. The amend is parked as the running process
        parks it (:meth:`_plan_modify_exit`, ``park_for_snapshot``), with the
        bracket before the amend read from the legs, so the poll settles the
        legs against the venue's levels and completes the command from the
        same verdict the runtime uses
        (:func:`~pynecore_capitalcom.helpers._modify_exit_snapshot_verdict`).

        :param command: The ``modify_exit`` command row.
        :param deal_reference: The amend's ``dealReference`` when it is journaled.
        :return: Whether the amend was handed over; ``False`` leaves the
            command to the restart's own verdict (its legs are with the
            snapshot already, its target is gone, or its dispatch is not
            journaled).
        """
        store_ctx = self.store_ctx
        assert store_ctx is not None
        extras = command.extras or {}
        target_coid = extras.get('target_coid')
        target_row = store_ctx.get_order(str(target_coid)) if target_coid else None
        if (target_row is None or target_row.closed_ts_ms is not None
                or not target_row.exchange_order_id
                or command.pine_entry_id is None or command.from_entry is None):
            return False
        if (any((row.extras or {}).get('leg_kind') in ('tp', 'sl')
                and (row.extras or {}).get('parent_coid') == target_row.client_order_id
                and row.state == STATE_DISPOSITION_UNKNOWN
                for row in store_ctx.iter_live_orders())
                or self._modify_exit_legs_refused(command)):
            return False
        submitted = _modify_exit_dispatch(store_ctx, command.client_order_id)
        tp_coid = submitted.get('envelope_tp_coid') if submitted is not None else None
        sl_coid = submitted.get('envelope_sl_coid') if submitted is not None else None
        if not isinstance(tp_coid, str) or not isinstance(sl_coid, str):
            return False
        new_i = ExitIntent(
            pine_id=command.pine_entry_id, from_entry=command.from_entry,
            symbol=command.symbol, side=command.side, qty=command.qty,
            tp_price=_wire_float(extras.get('new_tp'), finite=True),
            sl_price=_wire_float(extras.get('new_sl'), finite=True),
            trail_offset=_wire_float(extras.get('new_trail'), finite=True),
            trail_price=_wire_float(extras.get('new_trail_price'), finite=True),
        )
        if new_i.intent_key != command.intent_key:
            return False
        planned = self._plan_modify_exit(
            target_row, self._bracket_held_by_legs(target_row, new_i), new_i,
            tp_coid=tp_coid, sl_coid=sl_coid,
        )
        if planned is None:
            return False
        _, hooks = planned
        hooks.park_for_snapshot(
            new_i, command_coid=command.client_order_id, stage='restart',
            reason='the amend was sent, but neither its completion nor its park was journaled',
            deal_reference=deal_reference,
        )
        return True

    def _bracket_held_by_legs(self, target_row: 'OrderRow', new_i: ExitIntent) -> ExitIntent:
        """The bracket the live legs of a position hold, as an exit intent.

        The bracket an unsettled amend is parked from once its legs may already
        carry it (:meth:`_park_unsettled_modify_exit`, and :meth:`modify_exit`
        after a committed mirror): a pending trailing stop keeps its
        activation price, so the amend's own pending trail is no transition.

        :param target_row: The position's entry row.
        :param new_i: The amend's target intent, whose identity the result keeps.
        :return: ``new_i`` with the levels of the position's live legs.
        """
        existing_tp = self._find_bracket_leg_row(target_row, 'tp')
        existing_sl = self._find_bracket_leg_row(target_row, 'sl')
        sl_extras = (existing_sl.extras or {}) if existing_sl is not None else {}
        return replace(
            new_i,
            tp_price=existing_tp.tp_level if existing_tp is not None else None,
            sl_price=existing_sl.sl_level if existing_sl is not None else None,
            trail_offset=existing_sl.trailing_distance if existing_sl is not None else None,
            trail_price=(_wire_float(sl_extras.get('trail_activation_price'), finite=True)
                         if sl_extras.get('trail_state') == 'pending' else None),
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
            plugin: CapitalComHookCollaborator,
            endpoint: str,
            body: dict,
            quantized_qty: float,
    ) -> None:
        self._plugin = plugin
        self._endpoint = endpoint
        self._body = body
        self._quantized_qty = quantized_qty
        #: The ``dealReference`` the venue answered the POST with, once it did.
        self.server_ref: str | None = None

    def _record_order_level(self, coid: str, level: float | None = None) -> None:
        """Keep the level a working order is placed at on its row.

        The level is the bot's own state the working-order guard judges the
        venue against. Merged into the row's current extras, so it survives
        every other extras key.

        :param level: The level to keep: the venue's echo of the placed
            level once the confirm returned it, else the requested level.
        """
        store_ctx = self._plugin.store_ctx
        if self._endpoint != 'workingorders' or store_ctx is None:
            return
        with store_ctx.transaction():
            existing = store_ctx.get_order(coid)
            if existing is None:
                return
            extras = dict(existing.extras or {})
            extras[EXTRAS_KEY_ORDER_LEVEL] = (
                level if level is not None else self._body.get('level'))
            store_ctx.upsert_order(coid, extras=extras)

    async def _confirmed_order_level(self, symbol: str, confirm: dict) -> float | None:
        """The level the venue kept a placed working order at, when its confirm says.

        A level between two price ticks is kept at one of its grid
        neighbours; the confirm's ``level`` is that rendering when it is
        within a tick of the requested level.
        """
        requested = _wire_float(self._body.get('level'), finite=True)
        if self._endpoint != 'workingorders' or requested is None:
            return None
        try:
            rules = await self._plugin.instrument_rules(symbol)
        except (BrokerError, CapitalComError, httpx.HTTPError):
            return None
        return _venue_rendered_level(confirm.get('level'), requested, rules.price_tick)

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

        if intent.symbol != self._body.get('epic'):
            raise ValueError(
                f"Entry hook symbol mismatch: body has {self._body.get('epic')!r}, "
                f"intent has {intent.symbol!r}"
            )
        if abs(qty - self._quantized_qty) > 1e-9:
            raise ValueError(
                f"Entry hook quantity mismatch: captured "
                f"{self._quantized_qty!r}, journal supplied {qty!r}"
            )
        self._record_order_level(coid)
        try:
            if self._endpoint == 'positions':
                resp = await self._plugin.submit_market_entry(
                    coid=coid, symbol=intent.symbol, side=intent.side, body=self._body,
                )
            else:
                resp = await self._plugin.call_api(
                    self._endpoint, data=self._body, method='post',
                )
        except (httpx.TimeoutException, httpx.RequestError,
                ConnectionError, ExchangeConnectionError) as net:
            raise OrderDispositionUnknownError(
                f"Capital POST {self._endpoint} ambiguous: {net}",
                client_order_id=coid,
                cause=net if isinstance(net, Exception) else None,
            ) from net
        except CapitalComApiError as exc:
            # A parsed error response: the venue evaluated and synchronously
            # refused the POST — nothing landed, so this is a definitive
            # reject. The engine converts an ENTRY reject into a skip (signal
            # dropped, next bar re-evaluates); the raw provider error would
            # only reach the generic dispatch net as an untranslated write
            # fault (live incident, which then ended the run:
            # ``error.validation.stop.price`` on a stop entry).
            if _is_market_closed_reject(str(exc)):
                raise MarketClosedError(
                    f"Capital POST {self._endpoint} declined, market closed "
                    f"(coid={coid}): {exc}",
                ) from exc
            # A STOP / LIMIT entry whose level the quote already crossed is
            # Pine's immediate fill, not a refused signal: ``level_crossed``
            # lets the engine send it at market instead of dropping it.
            raise ExchangeOrderRejectedError(
                f"Capital POST {self._endpoint} rejected (coid={coid}): {exc}",
                level_crossed=(self._endpoint == 'workingorders'
                               and _is_crossed_working_order_reject(
                                   exc.error_code, self._body.get('type'))),
            ) from exc
        except CapitalComError as exc:
            # A response arrived but could not be parsed (JSON error /
            # schema drift) — the order may have landed, so park the
            # dispatch as unknown for recovery instead of guessing.
            raise OrderDispositionUnknownError(
                f"Capital POST {self._endpoint} ambiguous (unparseable "
                f"response): {exc}",
                client_order_id=coid,
                cause=exc,
            ) from exc

        deal_ref = resp.get('dealReference')
        if not deal_ref:
            raise OrderDispositionUnknownError(
                f"Capital POST {self._endpoint}: no dealReference in response",
                client_order_id=coid,
            )
        self.server_ref = str(deal_ref)
        return SubmitOutcome(server_ref=self.server_ref, raw=resp)

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

        A working order's placed level is recorded again first: the journal
        replaces the row's whole extras when it records ``server_ref``.
        """
        from pynecore.core.broker.journal import ConfirmOutcome

        self._record_order_level(coid)
        try:
            confirm = await self._plugin.call_api(
f'confirms/{server_ref}', method='get',
            )
        except (httpx.TimeoutException, httpx.RequestError,
                ConnectionError, ExchangeConnectionError) as net:
            raise OrderDispositionUnknownError(
                f"Capital GET confirms/{server_ref} ambiguous: {net}",
                client_order_id=coid,
                cause=net if isinstance(net, Exception) else None,
            ) from net
        except OrderNotFoundError as exc:
            # ``error.not-found.dealReference`` on the confirms READ: the
            # POST already went out and the venue may well have executed
            # it — the confirm ledger just has not indexed the reference
            # yet (observed live: the deal filled while the GET 404ed, and
            # treating the miss as a terminal reject duplicated the entry).
            # Park the dispatch as unknown so pending-verification recovery
            # judges it from activity/positions instead of retrying blind.
            raise OrderDispositionUnknownError(
                f"Capital GET confirms/{server_ref} ambiguous: {exc}",
                client_order_id=coid,
                cause=exc,
            ) from exc
        except CapitalComError as exc:
            # API error / unparseable body on the confirms READ: the POST
            # already went out, so its outcome stays unverified — park the
            # dispatch for recovery rather than crashing the run on a raw
            # provider error.
            raise OrderDispositionUnknownError(
                f"Capital GET confirms/{server_ref} ambiguous: {exc}",
                client_order_id=coid,
                cause=exc,
            ) from exc

        deal_status = (confirm.get('dealStatus') or '').upper()
        if deal_status == 'REJECTED':
            reason = _extract_reject_reason(confirm)
            if _is_market_closed_reject(reason):
                raise MarketClosedError(f"Capital confirm REJECTED, market closed: {reason}")
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
        confirmed_level = await self._confirmed_order_level(intent.symbol, confirm)
        if confirmed_level is not None:
            self._record_order_level(coid, confirmed_level)

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

    @staticmethod
    def exchange_order_from_state(
            *, row: 'OrderRow', intent: EntryIntent,
    ) -> ExchangeOrder:
        """Build the public :class:`ExchangeOrder` from the persisted row."""
        is_filled = row.filled_qty > 0.0
        extras = row.extras or {}
        confirm_level_raw = extras.get('confirm_level')
        average_fill_price = (
            _wire_float(confirm_level_raw, finite=True) if is_filled else None
        )
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
