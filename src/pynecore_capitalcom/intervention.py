"""External-intervention detection: moved or removed SL/TP, manual closes.

Only the plugin can tell who changed a bot-owned position on Capital.com, so
it reports two kinds of evidence through
:meth:`~pynecore.core.plugin.broker.BrokerPlugin.report_external_intervention`:

* **Protection levels.** A Capital.com bracket is a set of attributes on the
  position (``stopLevel`` / ``profitLevel`` / ``trailingStop`` +
  ``stopDistance``), and every own ``PUT /positions/{dealId}`` replaces all
  of them. Each such PUT is recorded on a core
  :class:`~pynecore.core.broker.protection_guard.ProtectionLevelGuard` before
  it leaves (:meth:`_InterventionMixin._protection_write`), and the
  ``/positions`` poll judges every guarded position against the levels the
  bot put there (:meth:`_InterventionMixin._observe_protection`). The guard
  key is the position ``dealId``; the accepted levels persist on the parent
  entry row, which lives exactly as long as the position does — bracket leg
  rows can be retired while the position stays open, and hedging-mode rows
  have none.
* **Closes.** The venue stamps the bot's own ``DELETE /positions/{dealId}``
  and opposite-direction reduce ``POST`` with ``source=USER``, exactly like a
  close from the web platform. Every own close therefore leaves a persisted
  "expected own close" record on the target entry row before the request
  leaves (:meth:`_InterventionMixin._own_close`). A ``USER`` close activity
  that no record and no own netting order explains is someone else's: it is
  booked as before, then reported. A ``DEALER`` close is booked with a loud
  warning and never reported; TP / SL / stop-out / system closes never reach
  this path.

Parents the core native fail-safe manager owns are left out of the guard:
the manager writes their stop itself and detects edits on its own.
"""
import logging
import math
from abc import ABC
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from time import time as epoch_time
from typing import TYPE_CHECKING, Any

import httpx

from pynecore.core.broker.exceptions import BrokerError, UnexpectedCancelError
from pynecore.core.broker.protection_guard import (
    ProtectionLevelGuard,
    ProtectionLevels,
)
from pynecore.core.broker.store_helpers import (
    ENGINE_TRIGGER_LEG_KINDS,
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    EXTRAS_KEY_LEG_KIND,
    EXTRAS_KEY_PARENT_ENTRY_DISPATCH_REF,
    KIND_CANCEL,
    KIND_MODIFY_EXIT,
    KIND_PARTIAL_CLOSE,
    STATE_REJECTED,
)

from ._base import _CapitalComBase
from .exceptions import CapitalComError
from .helpers import (
    EXTRAS_KEY_FAILSAFE_OWNED,
    EXTRAS_KEY_OWN_CLOSES,
    EXTRAS_KEY_PROTECTION_ADOPT_AFTER,
    _parse_iso_timestamp,
    _wire_float,
    _wire_id,
)
from .models import (
    _VENUE_REFUSALS,
    _OwnCloseHandle,
    _ProtectionWrite,
    _activity_fingerprint,
)

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow

logger = logging.getLogger(__name__)

_PROTECTION_KEY_PREFIX = 'position:'

#: Tolerated difference between the venue's activity clock (``dateUTC``) and
#: the local wall clock the own-close records are stamped with. Generous on
#: purpose: an own close missed because of a drifting local clock would be a
#: false quarantine, while the window only has to keep an unmatched record
#: from explaining an unrelated close much later. ``dateUTC`` is the deal
#: time, so the venue's publication lag does not count against it.
_CLOCK_SKEW_S = 300.0

#: Upper bound on how long an own close request whose outcome never came
#: back can still land on the venue.
_UNSETTLED_WRITE_S = 120.0

#: Own-close records older than this are dropped (the activity feed's
#: longest lookback).
_OWN_CLOSE_RETAIN_S = 86400.0

#: Row states of a bracket leg whose venue outcome is not settled yet.
_UNSETTLED_LEG_STATES = frozenset({'submitted', 'disposition_unknown', 'cancel_pending'})

#: Pending-trail states: the trailing activation monitor owns the stop.
_TRAIL_MONITOR_STATES = frozenset({'pending', 'activating'})


def _protection_key(deal_id: str) -> str:
    return _PROTECTION_KEY_PREFIX + deal_id


class _InterventionMixin(_CapitalComBase, ABC):
    """Protection-level guard wiring and manual-close attribution."""

    # --- Protection levels -------------------------------------------------

    def _protection_guard(self) -> ProtectionLevelGuard:
        """The lazily-built guard; ``store_ctx`` is injected after ``__init__``."""
        guard = self._protection
        if guard is None:
            guard = ProtectionLevelGuard(
                self._report_protection_edit, store_ctx=self.store_ctx,
            )
            self._protection = guard
        return guard

    def _report_protection_edit(self, reason: str, context: dict) -> None:
        """Guard ``report`` hook: route a confirmed edit into the policy."""
        key = str(context.get('protection_key') or '')
        deal_id = (key[len(_PROTECTION_KEY_PREFIX):]
                   if key.startswith(_PROTECTION_KEY_PREFIX) else None)
        logger.error("%s", reason)
        coid = context.get('client_order_id')
        self.report_external_intervention(
            reason, context,
            client_order_id=coid if isinstance(coid, str) else None,
            exchange_order_id=deal_id,
        )

    @staticmethod
    def _levels_from_put_body(body: dict) -> ProtectionLevels:
        """The protection a ``PUT /positions/{dealId}`` body leaves on the position.

        The PUT replaces every bracket field: a field the body omits is
        cleared, so an omitted level is an absent level here.
        """
        trailing = body.get('trailingStop') is True
        return ProtectionLevels(
            stop=None if trailing else _wire_float(body.get('stopLevel'), finite=True),
            limit=_wire_float(body.get('profitLevel'), finite=True),
            trailing=trailing,
            trail_distance=(_wire_float(body.get('stopDistance'), finite=True)
                            if trailing else None),
        )

    @staticmethod
    def _levels_from_position(pos: dict) -> ProtectionLevels:
        """The protection a ``GET /positions`` row shows.

        A level the payload leaves out is read as absent, the same reading
        the leg-disposition resolver and the native fail-safe feed use: the
        venue carries no bracket field it was not given. A trailing stop's
        distance missing from the payload stays unknown (``None``), which the
        guard never counts as a mismatch.
        """
        data = pos.get('position') or {}
        trailing = bool(data.get('trailingStop'))
        return ProtectionLevels(
            stop=_wire_float(data.get('stopLevel'), finite=True),
            limit=_wire_float(data.get('profitLevel'), finite=True),
            trailing=trailing,
            trail_distance=(_wire_float(data.get('stopDistance'), finite=True)
                            if trailing else None),
        )

    def _protected_entry_row(self, deal_id: str) -> 'OrderRow | None':
        """The live bot position row a protection write on ``deal_id`` targets.

        ``None`` when the write is not guarded: no journal, no live position
        row for this exact deal, or a parent the native fail-safe owns.
        """
        store_ctx = self.store_ctx
        if store_ctx is None or not deal_id:
            return None
        row = store_ctx.find_by_ref('deal_id', deal_id)
        if (row is None or row.closed_ts_ms is not None
                or row.exchange_order_id != deal_id):
            return None
        if (row.extras or {}).get('kind') != ENTRY_KIND_POSITION:
            return None
        if self._failsafe_owned(row, self._failsafe_parent_refs()):
            return None
        return row

    @contextmanager
    def _protection_write(self, deal_id: str, body: dict) -> Iterator[_ProtectionWrite]:
        """Record one own ``PUT /positions/{deal_id}`` on the guard.

        ``expect`` runs before the caller sends the PUT; on exit the write is
        settled with its levels when the caller marked it
        :meth:`~._ProtectionWrite.accepted`, as refused (its levels stop being
        acceptable) when it was marked :meth:`~._ProtectionWrite.refused`,
        and as unknown otherwise (the guard resolves it from later
        snapshots).

        :param deal_id: The position the PUT targets.
        :param body: The PUT body, exactly as sent.
        """
        write = _ProtectionWrite(self._levels_from_put_body(body))
        row = self._protected_entry_row(deal_id)
        if row is None:
            yield write
            return
        guard = self._protection_guard()
        key = _protection_key(deal_id)
        write_id = guard.expect(key, write.levels, row_coid=row.client_order_id)
        self._protection_in_flight[key] = self._protection_in_flight.get(key, 0) + 1
        self._protection_written_at[key] = epoch_time()
        try:
            yield write
        finally:
            remaining = self._protection_in_flight.get(key, 1) - 1
            if remaining > 0:
                self._protection_in_flight[key] = remaining
            else:
                self._protection_in_flight.pop(key, None)
            self._protection_written_at[key] = epoch_time()
            if write.ok:
                guard.settle(key, write.levels, write=write_id)
            elif write.rejected:
                guard.settle(key, None, rejected=write.levels, write=write_id)
            else:
                guard.settle(key, None, write=write_id)

    def _forget_protection(self, deal_id: str) -> None:
        """Stop guarding ``deal_id``: the position is gone or no longer ours to judge."""
        if self.store_ctx is None or not deal_id:
            return
        key = _protection_key(deal_id)
        self._protection_sizes.pop(key, None)
        self._protection_written_at.pop(key, None)
        self._protection_guard().forget(key)

    def _failsafe_parent_refs(self) -> set[str]:
        """Entry COIDs with a live software partial-bracket leg.

        The engine registers exactly these parents with its native fail-safe
        manager, which then owns their venue stop.
        """
        refs: set[str] = set()
        if self.store_ctx is None:
            return refs
        for row in self.store_ctx.iter_live_orders():
            extras = row.extras or {}
            if extras.get(EXTRAS_KEY_LEG_KIND) in ENGINE_TRIGGER_LEG_KINDS:
                parent = extras.get(EXTRAS_KEY_PARENT_ENTRY_DISPATCH_REF)
                if isinstance(parent, str):
                    refs.add(parent)
        return refs

    def _failsafe_owned(self, row: 'OrderRow', failsafe_parents: set[str]) -> bool:
        """Whether the native fail-safe manager owns ``row``'s protection."""
        if row.exchange_order_id in self._failsafe_deal_ids:
            return True
        if (row.extras or {}).get(EXTRAS_KEY_FAILSAFE_OWNED) is not None:
            return True
        return row.client_order_id in failsafe_parents

    def _claim_failsafe_protection(self, deal_ids: Iterable[str]) -> None:
        """Hand ``deal_ids`` over to the native fail-safe manager, before its PUT.

        The manager compares the venue against its own desired levels and
        raises its own external-edit event, so the guard stops judging these
        positions for good — judging them too would double-report.
        """
        store_ctx = self.store_ctx
        for deal_id in deal_ids:
            self._failsafe_deal_ids.add(deal_id)
            self._forget_protection(deal_id)
            if store_ctx is None:
                continue
            row = store_ctx.find_by_ref('deal_id', deal_id)
            if row is None or row.closed_ts_ms is not None:
                continue
            if (row.extras or {}).get(EXTRAS_KEY_FAILSAFE_OWNED) is None:
                stamped_at = epoch_time()
                self._rewrite_extras(
                    row.client_order_id,
                    lambda extras: extras.__setitem__(EXTRAS_KEY_FAILSAFE_OWNED, stamped_at),
                )

    async def _price_tick(self, symbol: str) -> float:
        """The instrument's price tick, ``0.0`` when unknown."""
        rules = self._instrument_rules_cache.get(symbol)
        if rules is None:
            try:
                rules = await self._get_instrument_rules(symbol)
            except (BrokerError, CapitalComError, httpx.HTTPError, ConnectionError):
                rules = None
        tick = rules.price_tick if rules is not None else 0.0
        sym_info = self._sym_info
        if tick <= 0.0 and sym_info is not None and sym_info.ticker == symbol:
            tick = float(sym_info.mintick)
        return tick if math.isfinite(tick) and tick > 0.0 else 0.0

    async def _observe_protection(
            self,
            positions_by_deal: dict[str, dict],
            read_started_at: float | None,
    ) -> None:
        """Judge every guarded bot position in this ``/positions`` snapshot.

        A position is skipped — nothing is judged, no mismatch is counted —
        while the bot itself may be changing it: a bracket leg row whose
        venue outcome is unsettled, a pending trailing activation, an amend
        or cancel command for it that the venue has not refused, an own
        close in flight, a size
        change (a partial close, whose effect on the bracket the venue does
        not document). After an own or venue partial close the next snapshot
        that started later becomes the accepted protection.

        :param positions_by_deal: The poll's ``GET /positions`` rows by ``dealId``.
        :param read_started_at: Wall-clock time the snapshot read started.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return
        guard = self._protection_guard()
        live = list(store_ctx.iter_live_orders())
        busy_deals: set[str] = set()
        busy_coids: set[str] = set()
        failsafe_parents: set[str] = set()
        for row in live:
            extras = row.extras or {}
            leg_kind = extras.get(EXTRAS_KEY_LEG_KIND)
            kind = extras.get('kind')
            if leg_kind in ENGINE_TRIGGER_LEG_KINDS:
                parent = extras.get(EXTRAS_KEY_PARENT_ENTRY_DISPATCH_REF)
                if isinstance(parent, str):
                    failsafe_parents.add(parent)
            elif leg_kind in ('tp', 'sl'):
                if (row.state in _UNSETTLED_LEG_STATES
                        or extras.get('trail_state') in _TRAIL_MONITOR_STATES):
                    parent_deal = _wire_id(extras.get('parent_deal_id'))
                    if parent_deal is not None:
                        busy_deals.add(parent_deal)
            elif row.state == STATE_REJECTED:
                # A refused command stays live for audit only; nothing of it
                # can still land on the venue.
                continue
            elif kind == KIND_MODIFY_EXIT:
                target = extras.get('target_coid')
                if isinstance(target, str):
                    busy_coids.add(target)
            elif kind == KIND_CANCEL:
                for target_coid in extras.get('target_coids') or ():
                    target = store_ctx.get_order(str(target_coid))
                    parent_deal = (_wire_id((target.extras or {}).get('parent_deal_id'))
                                   if target is not None else None)
                    if parent_deal is not None:
                        busy_deals.add(parent_deal)
            elif kind == KIND_PARTIAL_CLOSE:
                target_deal = _wire_id(extras.get('target_deal_id'))
                if target_deal is not None:
                    busy_deals.add(target_deal)

        now = epoch_time()
        for row in live:
            extras = row.extras or {}
            deal_id = row.exchange_order_id
            if (not deal_id or extras.get('kind') != ENTRY_KIND_POSITION
                    or extras.get(EXTRAS_KEY_LEG_KIND) is not None):
                continue
            key = _protection_key(deal_id)
            if (extras.get('natural_close_at') is not None
                    or self._failsafe_owned(row, failsafe_parents)):
                self._forget_protection(deal_id)
                continue
            if not guard.known(key):
                continue
            pos = positions_by_deal.get(deal_id)
            if pos is None:
                # The disappearance tracker owns a vanished deal.
                continue
            if (row.state != 'confirmed'
                    or deal_id in busy_deals
                    or row.client_order_id in busy_coids
                    or self._own_close_in_flight(extras, now)):
                continue
            venue = self._levels_from_position(pos)
            size = _wire_float((pos.get('position') or {}).get('size'), finite=True)
            previous_size = self._protection_sizes.get(key)
            if size is not None:
                self._protection_sizes[key] = size
            if (previous_size is not None and size is not None
                    and abs(size - previous_size) > 1e-9 * max(1.0, abs(previous_size))):
                self._stamp_protection_adopt(
                    row.client_order_id,
                    read_started_at if read_started_at is not None else now,
                )
                continue
            adopt_after = _wire_float(
                extras.get(EXTRAS_KEY_PROTECTION_ADOPT_AFTER), finite=True,
            )
            if adopt_after is not None:
                # Adopt only a snapshot that started after the size change
                # AND after the last own protection write: an older read
                # would replace the levels the bot just put on the venue.
                if (read_started_at is None or read_started_at <= adopt_after
                        or self._protection_in_flight.get(key, 0) > 0
                        or read_started_at < self._protection_written_at.get(key, 0.0)):
                    continue
                guard.adopt(key, venue, row_coid=row.client_order_id)
                self._rewrite_extras(
                    row.client_order_id,
                    lambda e: e.pop(EXTRAS_KEY_PROTECTION_ADOPT_AFTER, None),
                )
                store_ctx.log_event(
                    'protection_adopted_after_size_change',
                    client_order_id=row.client_order_id,
                    exchange_order_id=deal_id,
                    payload={'levels': venue.as_list(), 'size': size},
                )
                continue
            tick = await self._price_tick(row.symbol)
            if tick <= 0.0:
                continue
            guard.observe(key, venue, tick=tick, read_started_at=read_started_at)

    def _stamp_protection_adopt(self, coid: str, after: float) -> None:
        """Make the next snapshot read started after ``after`` the accepted protection."""
        self._rewrite_extras(
            coid,
            lambda extras: extras.__setitem__(EXTRAS_KEY_PROTECTION_ADOPT_AFTER, after),
        )

    def _rewrite_extras(self, coid: str, mutate: Callable[[dict], Any]) -> None:
        """Apply ``mutate`` to the CURRENT extras of a live row and store them."""
        store_ctx = self.store_ctx
        if store_ctx is None:
            return
        with store_ctx.transaction():
            row = store_ctx.get_order(coid)
            if row is None or row.closed_ts_ms is not None:
                return
            extras = dict(row.extras or {})
            mutate(extras)
            store_ctx.upsert_order(coid, extras=extras)

    # --- Own closes ----------------------------------------------------------

    @contextmanager
    def _own_close(
            self, deal_ids: Iterable[str], *, full: bool, size: float | None,
    ) -> Iterator[_OwnCloseHandle]:
        """Record an own close of ``deal_ids`` around the request that sends it.

        The record is persisted before the request leaves, so the close
        activity — which the venue stamps ``source=USER`` like a manual
        close — is attributed to the bot even if it is read while the request
        is still in flight, after a crash, or long after the command finished.
        A request the venue refused (or that never left) withdraws the
        record; an ambiguous outcome keeps it.

        :param deal_ids: The positions the request closes or reduces.
        :param full: The whole position is closed (any activity size matches).
        :param size: The reduced size of a partial close.
        :return: A handle whose :meth:`~._OwnCloseHandle.failed` withdraws the
            record when the refusal only shows after the request returned.
        """
        store_ctx = self.store_ctx
        coids: list[str] = []
        if store_ctx is not None:
            for deal_id in deal_ids:
                row = store_ctx.find_by_ref('deal_id', deal_id) if deal_id else None
                if row is not None and row.closed_ts_ms is None:
                    coids.append(row.client_order_id)
        sent_at = epoch_time()
        record = {'full': full, 'size': size, 'sent_at': sent_at,
                  'done_at': None, 'matched': []}

        def add(extras: dict) -> None:
            kept = [r for r in extras.get(EXTRAS_KEY_OWN_CLOSES) or ()
                    if isinstance(r, dict)
                    and (_wire_float(r.get('sent_at'), finite=True) or 0.0)
                    > sent_at - _OWN_CLOSE_RETAIN_S]
            extras[EXTRAS_KEY_OWN_CLOSES] = [*kept, dict(record)]

        def withdraw() -> None:
            def drop(extras: dict) -> None:
                extras[EXTRAS_KEY_OWN_CLOSES] = [
                    r for r in extras.get(EXTRAS_KEY_OWN_CLOSES) or ()
                    if isinstance(r, dict) and r.get('sent_at') != sent_at
                ]

            for target in coids:
                self._rewrite_extras(target, drop)

        for coid in coids:
            self._rewrite_extras(coid, add)
        handle = _OwnCloseHandle(withdraw=withdraw)
        refused = False
        try:
            yield handle
        except _VENUE_REFUSALS:
            # Refused by the venue, or never sent: no deal can follow.
            refused = True
            raise
        finally:
            if refused:
                withdraw()
            else:
                done_at = epoch_time()

                def finish(extras: dict) -> None:
                    for r in extras.get(EXTRAS_KEY_OWN_CLOSES) or ():
                        if isinstance(r, dict) and r.get('sent_at') == sent_at:
                            r['done_at'] = done_at
                    if not full:
                        extras[EXTRAS_KEY_PROTECTION_ADOPT_AFTER] = done_at

                for coid in coids:
                    self._rewrite_extras(coid, finish)

    @staticmethod
    def _own_close_in_flight(extras: dict, now: float) -> bool:
        """Whether an own close of this position may still be landing."""
        for record in extras.get(EXTRAS_KEY_OWN_CLOSES) or ():
            if not isinstance(record, dict) or record.get('done_at') is not None:
                continue
            sent_at = _wire_float(record.get('sent_at'), finite=True)
            if sent_at is not None and now - sent_at < _UNSETTLED_WRITE_S:
                return True
        return False

    def _claim_own_close(self, row: 'OrderRow', activity: dict) -> bool:
        """Attribute a close activity to a recorded own close, if one fits.

        A record fits when the activity's venue time falls between the
        request's send and its answer (within the clock skew) and, for a
        partial close, the activity size matches within half a lot step. A
        partial record is consumed by the first activity it explains; a
        replay of that same activity (same ``dateUTC``) still matches it. A
        full close may produce more than one activity row, so its record
        keeps matching inside the window.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return False
        fresh = store_ctx.get_order(row.client_order_id) or row
        records = [r for r in (fresh.extras or {}).get(EXTRAS_KEY_OWN_CLOSES) or ()
                   if isinstance(r, dict)]
        if not records:
            return False
        date_utc = str(activity.get('dateUTC') or '')
        activity_ts = _parse_iso_timestamp(date_utc)
        details = activity.get('details') or {}
        size = _wire_float(activity.get('size') or details.get('size'), finite=True)
        rules = self._instrument_rules_cache.get(row.symbol)
        size_tolerance = max(
            0.5 * rules.lot_step if rules is not None else 0.0,
            1e-9 * max(1.0, abs(size or 0.0)),
        )
        for record in records:
            sent_at = _wire_float(record.get('sent_at'), finite=True)
            if sent_at is None:
                continue
            done_at = _wire_float(record.get('done_at'), finite=True)
            if activity_ts > 0.0:
                latest = done_at if done_at is not None else sent_at + _UNSETTLED_WRITE_S
                if not (sent_at - _CLOCK_SKEW_S <= activity_ts <= latest + _CLOCK_SKEW_S):
                    continue
            matched = [str(d) for d in record.get('matched') or ()]
            if not record.get('full'):
                if matched and date_utc not in matched:
                    continue
                expected = _wire_float(record.get('size'), finite=True)
                if (size is not None and expected is not None
                        and abs(size - expected) > size_tolerance):
                    continue
            if date_utc not in matched:
                record_sent_at = sent_at

                def mark(extras: dict) -> None:
                    for r in extras.get(EXTRAS_KEY_OWN_CLOSES) or ():
                        if (isinstance(r, dict)
                                and _wire_float(r.get('sent_at'), finite=True) == record_sent_at):
                            r['matched'] = [*(r.get('matched') or ()), date_utc]

                self._rewrite_extras(row.client_order_id, mark)
            return True
        return False

    def _own_netting_candidates(
            self, row: 'OrderRow', activity_ts: float, size: float | None,
    ) -> tuple[list[str], list[str]]:
        """The own opposite-direction orders that may have netted ``row`` down.

        On a netting account an opposite entry — a resting working order
        that executed, a ``strategy.order``, a stop-fired market — reduces
        the existing position under the existing ``dealId``, and the venue
        logs that reduction like a close. Such an entry row exists before the
        reduction, is filled no earlier than it, and reduces the position by
        at most its own size.

        A position entry dispatched before the reduction is own execution
        evidence. A resting working order is not: nothing shows that it
        executed, so it only makes the reduction ambiguous.

        :param row: The bot position the close activity reduced.
        :param activity_ts: The activity's venue time (epoch seconds).
        :param size: The reduced size, ``None`` when the activity omits it.
        :return: The client order ids of the executed candidates and of the
            resting working-order candidates.
        """
        executed: list[str] = []
        resting: list[str] = []
        store_ctx = self.store_ctx
        if store_ctx is None or self._hedging_enabled:
            return executed, resting
        rules = self._instrument_rules_cache.get(row.symbol)
        size_tolerance = max(
            0.5 * rules.lot_step if rules is not None else 0.0,
            1e-9 * max(1.0, abs(size or 0.0)),
        )
        for other in store_ctx.iter_live_orders(symbol=row.symbol):
            if other.client_order_id == row.client_order_id or other.side == row.side:
                continue
            extras = other.extras or {}
            if extras.get(EXTRAS_KEY_LEG_KIND) is not None:
                continue
            if (size is not None
                    and max(other.qty, other.filled_qty) < size - size_tolerance):
                continue
            kind = extras.get('kind')
            if kind == ENTRY_KIND_WORKING:
                resting.append(other.client_order_id)
                continue
            if kind != ENTRY_KIND_POSITION:
                continue
            if other.created_ts_ms / 1000.0 > activity_ts + _CLOCK_SKEW_S:
                continue
            filled_at = _wire_float(extras.get('entry_filled_at'), finite=True)
            if filled_at is None or filled_at >= activity_ts - _CLOCK_SKEW_S:
                executed.append(other.client_order_id)
        return executed, resting

    def _attribute_close_activity(self, row: 'OrderRow', activity: dict) -> bool:
        """Attribute a ``USER`` / ``DEALER`` close of a bot position before it is booked.

        Own closes and closes an own netting order explains are logged. A
        ``DEALER`` close is a dealing-desk action: booked with a loud warning,
        never reported.

        :return: ``True`` when the close is someone else's and must be
            reported once it was booked.
        """
        source = str(activity.get('source') or '').upper()
        if source not in ('USER', 'DEALER'):
            return False
        store_ctx = self.store_ctx
        if store_ctx is None:
            return False
        deal_id = row.exchange_order_id or _wire_id(activity.get('dealId'))
        payload = {
            'dateUTC': activity.get('dateUTC'),
            'source': source,
            'size': (activity.get('details') or {}).get('size', activity.get('size')),
        }
        if self._claim_own_close(row, activity):
            store_ctx.log_event(
                'own_close_activity_attributed',
                client_order_id=row.client_order_id,
                exchange_order_id=deal_id,
                payload=payload,
            )
            return False
        if source == 'DEALER':
            logger.warning(
                "Capital.com dealing desk closed bot position %s (%s) at %s; "
                "booked, not treated as an operator intervention",
                deal_id, row.symbol, activity.get('dateUTC'),
            )
            store_ctx.log_event(
                'dealer_close_booked',
                client_order_id=row.client_order_id,
                exchange_order_id=deal_id,
                payload=payload,
            )
            return False
        activity_ts = _parse_iso_timestamp(str(activity.get('dateUTC') or ''))
        size = _wire_float(
            activity.get('size') or (activity.get('details') or {}).get('size'),
            finite=True,
        )
        executed, resting = self._own_netting_candidates(
            row, activity_ts or epoch_time(), size,
        )
        if executed:
            logger.warning(
                "close of bot position %s (%s) at %s matches no own close, but "
                "own opposite order(s) %s netted it; not reported",
                deal_id, row.symbol, activity.get('dateUTC'), executed,
            )
            store_ctx.log_event(
                'user_close_attributed_to_own_netting',
                client_order_id=row.client_order_id,
                exchange_order_id=deal_id,
                payload={**payload, 'candidates': executed},
            )
            return False
        if resting:
            # Only a resting own working order could explain the reduction:
            # it may have executed, or someone else closed the position while
            # it still rests. Without execution evidence the close is neither
            # attributed nor reported — a false quarantine is worse than a
            # missed report — but it stays loud and auditable.
            logger.error(
                "close of bot position %s (%s) at %s matches no own close and "
                "no own fill; only resting own working order(s) %s could have "
                "netted it — possibly a manual close, not reported, check the "
                "venue history",
                deal_id, row.symbol, activity.get('dateUTC'), resting,
            )
            store_ctx.log_event(
                'user_close_ambiguous_netting',
                client_order_id=row.client_order_id,
                exchange_order_id=deal_id,
                payload={**payload, 'candidates': resting},
            )
            return False
        # The close becomes durable before it is booked, so the report it
        # owes is persisted with it: a process that dies before reporting
        # sends it on the next start (:meth:`_send_pending_manual_close_reports`).
        store_ctx.log_event(
            'manual_close_report_pending',
            client_order_id=row.client_order_id,
            exchange_order_id=deal_id,
            payload=self._manual_close_report(row, activity),
        )
        return True

    @staticmethod
    def _manual_close_report(row: 'OrderRow', activity: dict) -> dict:
        """The report of a booked ``USER`` close no own write explains."""
        deal_id = row.exchange_order_id or _wire_id(activity.get('dealId'))
        details = activity.get('details') or {}
        size = details.get('size', activity.get('size'))
        return {
            'report_id': _activity_fingerprint(activity),
            'client_order_id': row.client_order_id,
            'deal_id': deal_id,
            'reason': (
                f"bot position {deal_id} ({row.symbol}) was closed on the venue by "
                f"someone else: USER close of {size} at {activity.get('dateUTC')} "
                f"matches no own close"
            ),
            'context': {
                'deal_id': deal_id,
                'client_order_id': row.client_order_id,
                'activity': {k: activity.get(k) for k in (
                    'dateUTC', 'dealId', 'type', 'status', 'source')},
                'size': size,
                'direction': details.get('direction'),
            },
        }

    def _report_manual_close(self, row: 'OrderRow', activity: dict) -> None:
        """Report a booked ``USER`` close no own write explains."""
        self._send_manual_close_report(self._manual_close_report(row, activity))

    def _send_manual_close_report(self, report: dict) -> None:
        """Send one persisted manual-close report and mark it sent.

        The mark follows the policy's action, so a process that dies in
        between sends it again on restart (the quarantine latch is
        idempotent). A halting policy's exception counts as sent: the halt
        is the action, and repeating it would stop every restart.
        """
        reason = str(report.get('reason') or 'bot position was closed on the venue by someone else')
        logger.error("%s", reason)
        try:
            self.report_external_intervention(
                reason,
                report.get('context') or {},
                client_order_id=report.get('client_order_id'),
                exchange_order_id=report.get('deal_id'),
            )
        except UnexpectedCancelError:
            self._mark_manual_close_report_sent(report)
            raise
        self._mark_manual_close_report_sent(report)

    def _mark_manual_close_report_sent(self, report: dict) -> None:
        """Persist that ``report`` reached the policy."""
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'manual_close_report_sent',
                client_order_id=report.get('client_order_id'),
                exchange_order_id=report.get('deal_id'),
                payload={'report_id': report.get('report_id')},
            )

    def _send_pending_manual_close_reports(self) -> None:
        """Send the manual-close reports an earlier process booked but never sent.

        Reads every instance of the logical run: the reports were persisted
        by the process that died.
        """
        store_ctx = self.store_ctx
        if store_ctx is None or self._connection_generation > 0:
            return
        sent = {
            payload.get('report_id')
            for _, _, _, payload in store_ctx.iter_events_by_kind_for_run_id(
                'manual_close_report_sent')
        }
        for _, _, _, report in store_ctx.iter_events_by_kind_for_run_id(
                'manual_close_report_pending'):
            report_id = report.get('report_id')
            if report_id in sent:
                continue
            sent.add(report_id)
            self._send_manual_close_report(report)
