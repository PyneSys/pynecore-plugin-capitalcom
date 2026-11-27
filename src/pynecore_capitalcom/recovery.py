"""Restart recovery — runs once at ``connect()`` before the WS loop opens.

Capital.com's idempotency capability is SOFTWARE: the server generates
``dealReference`` from each POST, so the plugin must keep its own crash-
safe ledger of which dispatches reached the exchange and which rolled
back. This mix-in walks the BrokerStore at startup and resolves every
non-terminal row:

* ``_load_activity_cursor_from_events`` — replays the
  ``activity_processed`` events of the venue's activity window to rebuild
  ``_activity_cursor.seen_fingerprints`` + watermark.
* ``_recover_in_flight_submissions`` — orchestration: batch-fetches the
  three REST snapshots, then hands ``submitted`` /
  ``disposition_unknown`` / ``server_ref_seen`` rows to the journal's
  :meth:`pynecore.core.broker.journal.DispatchJournal.recover_pending`
  via :class:`_CapitalComResumeHooks`. The journal owns the persist-
  first ``confirmed`` / ``rejected`` / ``still_unknown`` state writes
  and the canonical ``recovered_*`` audit events.
* ``_CapitalComResumeHooks`` — recovery-time hook implementation.
  Routes by ``row.state`` to two verdict-builders that read the
  snapshots and return a :class:`ResumeOutcome` carrying the resolution
  route in ``recovery_path``.
* ``_inherited_submission_verdict`` — the in-run lookup of an entry
  submission an earlier process left pending and the startup pass could not
  resolve yet: the same activity matching against a fresh activity read (or
  the open book for a row older than the activity history), settling it with
  every pending submission its deals match. Each deal goes to its own
  submission in a fixed order; more deals than the submissions explain are
  someone else's trading on the epic, reported as an external intervention.
  None of these stops the process or waits without an end.

Bracket leg ``disposition_unknown`` rows are NOT handled here — they
are owned by ``_reconcile_snapshot``'s ``_resolve_bracket_leg_disposition``
since the resolution depends on the parent position's live state.

State touched: BrokerStore through ``self.store_ctx``,
``_activity_cursor`` (rebuilt).
"""
import logging
import math
from abc import ABC
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from time import time as epoch_time
from typing import TYPE_CHECKING, cast

import httpx

from pynecore.core.broker.exceptions import (
    ExchangeConnectionError,
    ExchangeRateLimitError,
)
from pynecore.core.broker.models import LegType
from pynecore.core.broker.journal import (
    DispatchJournal,
    EntryDispatchHooks,
    PendingHooksProvider,
    ResumeOutcome,
)
from pynecore.core.broker.parked_dispatch import ParkedOutcome, ParkedVerdict
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    KIND_CANCEL,
    KIND_FULL_CLOSE,
    KIND_MODIFY_ENTRY,
    KIND_MODIFY_EXIT,
    KIND_PARTIAL_CLOSE,
    STATE_DISPOSITION_UNKNOWN,
    find_pending_dispatch,
)
from pynecore.lib.log import broker_info, broker_warning
from pynecore.types.strategy import ADOPTED_STARTUP_EXTRA_KEY

from ._base import _CapitalComBase, CapitalComHookCollaborator
from .delivery import book_fill, entry_executed, entry_pending_since, is_delivered
from .exceptions import CapitalComError, OrderNotFoundError
from .foreign import _UNCONFIRMED_ENTRY_SKEW_S
from .helpers import (
    CLOSE_VERDICT_FOREIGN,
    CLOSE_VERDICT_VENUE,
    EXTERNAL_REDUCTION_VERDICTS,
    EXTRAS_KEY_CANCEL_REQUESTED,
    EXTRAS_KEY_EXECUTION_HELD,
    EXTRAS_KEY_NETTED_RESIDUAL_PENDING,
    EXTRAS_KEY_ORDER_LEVEL,
    EXTRAS_KEY_TAKE_OVER_UNPROVEN,
    _ACTIVITY_LAG_S,
    _ACTIVITY_MAX_PERIOD_S,
    _extract_reject_reason,
    _parse_iso_timestamp,
    _mirror_amended_level,
    _modify_entry_snapshot_verdict,
    _modify_exit_snapshot_verdict,
    _retire_parked_partial_close_exposure,
    _settle_vanished_full_close_targets,
    _wire_float,
    _wire_id,
    _wire_int,
)
from .models import _activity_fingerprint, _close_identity

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow, RunContext

logger = logging.getLogger(__name__)

#: Largest distance between a pending entry row's creation (taken just before
#: its POST) and the activity of the deal the POST opened, for the activity to
#: count as that POST's trace.
_ACTIVITY_MATCH_WINDOW_S = 3.0

@dataclass(frozen=True)
class _DealEvidence:
    """What the venue shows of one deal that may be a pending submission's.

    :ivar when: The deal's venue creation time (epoch seconds), ``0.0`` when
        unknown.
    :ivar pinned: Created within :data:`_ACTIVITY_MATCH_WINDOW_S` of the
        submission (clock offset applied): the clock agreement every landing
        verdict rests on.
    :ivar refutable: Its creation time is the submission's send time, so a
        measured venue clock refutes it when it is not pinned. An executed
        working order's position carries its execution time instead.
    """
    when: float
    pinned: bool
    refutable: bool = True


@dataclass(frozen=True)
class _GroupCase:
    """One inherited submission judged together with every submission its deals match.

    :ivar outcome: ``'landed'`` (on :attr:`deal_id`), ``'absent'``,
        ``'surplus'`` (more deals than the group's submissions explain) or
        ``'inconclusive'`` (a matching activity carries no deal id).
    :ivar members: The group's submissions, by creation time then client
        order id.
    :ivar deals: The group's deals, by venue creation time then id.
    :ivar compat: Per member client order id, the deals it may have opened.
    :ivar when: Per deal, its venue creation time (``0.0`` unknown).
    """
    outcome: str
    deal_id: str | None = None
    members: tuple['OrderRow', ...] = ()
    deals: tuple[str, ...] = ()
    compat: Mapping[str, frozenset[str]] | None = None
    when: Mapping[str, float] | None = None


def _deal_out(members: Sequence['OrderRow'], compat: Mapping[str, frozenset[str]],
              deals: Sequence[str]) -> dict[str, str] | None:
    """The first assignment, in order, that gives every deal its own compatible submission.

    Deals are placed in ``deals`` order, each on the earliest free member it
    is compatible with, backtracking when a later deal is left without one.
    Every own POST opens one deal at most, so ``None`` (no such assignment)
    proves a deal no pending submission explains.

    :return: Client order id -> deal id, or ``None``.
    """
    if len(deals) > len(members):
        return None
    taken: dict[str, str] = {}

    def place(index: int) -> bool:
        if index == len(deals):
            return True
        deal = deals[index]
        for member in members:
            coid = member.client_order_id
            if coid not in taken and deal in compat.get(coid, frozenset()):
                taken[coid] = deal
                if place(index + 1):
                    return True
                del taken[coid]
        return False

    return dict(taken) if place(0) else None


def _assign_members(members: Sequence['OrderRow'], compat: Mapping[str, frozenset[str]],
                    deals: Sequence[str]) -> dict[str, str]:
    """The first largest assignment of submissions to distinct compatible deals, in order.

    Members in ``members`` order, each tried on its compatible deals in
    ``deals`` order before being left without one.

    :return: Client order id -> deal id.
    """
    best: dict[str, str] = {}
    current: dict[str, str] = {}

    def search(index: int) -> None:
        nonlocal best
        if len(current) + len(members) - index <= len(best):
            return
        if index == len(members):
            best = dict(current)
            return
        coid = members[index].client_order_id
        for deal in deals:
            if deal in compat.get(coid, frozenset()) and deal not in current.values():
                current[coid] = deal
                search(index + 1)
                del current[coid]
        search(index + 1)

    search(0)
    return best


def _open_ids(positions: list[dict], working: list[dict]) -> set[str]:
    """The ids the open book holds: positions by ``dealId`` and ``workingOrderId``, resting orders by ``dealId``.

    ``workingOrderId`` is the back-link from a position to the order that
    opened it, the one
    :meth:`~pynecore_capitalcom.foreign._ForeignExposureMixin._owned_deal_ids`
    reads.
    """
    ids: set[str] = set()
    for raw in positions:
        position = raw.get('position') or {}
        for key in ('dealId', 'workingOrderId'):
            if (ref := _wire_id(position.get(key))) is not None:
                ids.add(ref)
    for raw in working:
        order_id = _wire_id((raw.get('workingOrderData') or {}).get('dealId'))
        if order_id is not None:
            ids.add(order_id)
    return ids


def _settle_group(
        row: 'OrderRow', evidence: Callable[['OrderRow'], Mapping[str, _DealEvidence]],
        pending: Sequence['OrderRow'], *, clock_measured: bool,
        fits: Callable[['OrderRow', str], bool] | None = None,
) -> _GroupCase:
    """Judge ``row`` with every pending submission its deals match.

    The pinned tier is used when ``row`` has a pinned deal; otherwise its
    other deals of the candidate band, without the ones a measured clock
    refutes (``clock_measured``). A submission with a pinned deal is judged
    in the pinned tier only, so it never takes part in a band group. The group
    grows to a fixed point: a pending submission joins when one of its
    deals of the tier is already a group deal, and brings its other ones.
    ``fits`` drops a deal a member's venue-observable attributes contradict.

    * Every deal gets its own compatible submission (:func:`_deal_out`): the
      first such assignment decides; ``row`` lands on its deal, or opened
      nothing when it is left without one. Submissions a deal matches alike
      are the same order for the account (Capital.com shows no client order
      id and the entry POST carries nothing else), so which one holds it
      changes nothing the venue or the book shows.
    * No such assignment: a deal that no pending submission explains, so
      someone else trades the epic on this account (``'surplus'``).
    """
    own = evidence(row)
    pinned_tier = any(item.pinned for item in own.values())

    def tier_deals(member: 'OrderRow') -> dict[str, _DealEvidence]:
        seen = evidence(member)
        if pinned_tier:
            return {ref: item for ref, item in seen.items() if item.pinned}
        if any(item.pinned for item in seen.values()):
            return {}
        return {ref: item for ref, item in seen.items()
                if not (clock_measured and item.refutable)}

    when: dict[str, float] = {}
    members: dict[str, 'OrderRow'] = {row.client_order_id: row}
    tiers: dict[str, dict[str, _DealEvidence]] = {row.client_order_id: tier_deals(row)}
    if not tiers[row.client_order_id]:
        return _GroupCase('absent')
    deals = set(tiers[row.client_order_id])
    grown = True
    while grown:
        grown = False
        for other in pending:
            coid = other.client_order_id
            if coid in members:
                continue
            theirs = tier_deals(other)
            if deals.intersection(theirs):
                members[coid] = other
                tiers[coid] = theirs
                deals.update(theirs)
                grown = True
    for found in tiers.values():
        for deal, item in found.items():
            known = when.get(deal, 0.0)
            when[deal] = item.when if known <= 0.0 else min(known, item.when or known)
    ordered_members = tuple(sorted(members.values(),
                                   key=lambda member: (member.created_ts_ms,
                                                       member.client_order_id)))
    ordered_deals = tuple(sorted(deals, key=lambda ref: (when.get(ref, 0.0), ref)))
    compat = {coid: frozenset(deal for deal in found
                              if fits is None or fits(members[coid], deal))
              for coid, found in tiers.items()}
    dealt = _deal_out(ordered_members, compat, ordered_deals)
    if dealt is None:
        return _GroupCase('surplus', members=ordered_members, deals=ordered_deals,
                          compat=compat, when=when)
    deal_id = dealt.get(row.client_order_id)
    return _GroupCase('landed' if deal_id else 'absent', deal_id=deal_id,
                      members=ordered_members, deals=ordered_deals, compat=compat, when=when)


def _clock_offset(opened: Mapping[tuple[str, str], float],
                  rows: Iterable['OrderRow']) -> float | None:
    """The venue clock's lead over the local one, the median over the bot's own entries.

    A live entry row that carries its deal id and whose creation (taken just
    before its POST) is the send time is a sample: a market entry's
    ``('POSITION', deal id)`` or a resting entry's ``('WORKING_ORDER', deal
    id)`` venue creation time in ``opened``, less the row's creation. An
    adopted row's creation is the adoption's, and a market entry executed
    through a resting order carries its execution's time, so neither is a
    sample. Samples beyond
    :data:`~pynecore_capitalcom.foreign._UNCONFIRMED_ENTRY_SKEW_S` are dropped.

    :return: The offset in seconds, or ``None`` without a sample.
    """
    samples: list[float] = []
    for live in rows:
        extras = live.extras or {}
        if (not live.exchange_order_id or not live.created_ts_ms
                or extras.get(ADOPTED_STARTUP_EXTRA_KEY)
                or extras.get('leg_kind') is not None):
            continue
        if extras.get('kind') == ENTRY_KIND_POSITION and extras.get('order_type') == 'market':
            key = ('POSITION', live.exchange_order_id)
        elif extras.get('kind') == ENTRY_KIND_WORKING:
            key = ('WORKING_ORDER', live.exchange_order_id)
        else:
            continue
        if key in opened:
            offset = opened[key] - live.created_ts_ms / 1000.0
            if abs(offset) <= _UNCONFIRMED_ENTRY_SKEW_S:
                samples.append(offset)
    if not samples:
        return None
    ordered = sorted(samples)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _resting_order_fits(working: list[dict]) -> Callable[['OrderRow', str], bool]:
    """A resting order the open book lists fits a submission only at its level and type.

    ``workingOrderData.orderLevel`` and ``orderType`` of a listed order
    against the submission's placed level
    (:data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_ORDER_LEVEL`) and order
    type, where both are known. A deal not listed as a resting order fits.
    """
    listed: dict[str, tuple[float | None, str]] = {}
    for raw in working:
        data = raw.get('workingOrderData') or {}
        if (deal_id := _wire_id(data.get('dealId'))) is not None:
            listed[deal_id] = (_wire_float(data.get('orderLevel'), finite=True),
                               str(data.get('orderType') or '').upper())

    def fits(member: 'OrderRow', ref: str) -> bool:
        if ref not in listed:
            return True
        level, listed_type = listed[ref]
        extras = member.extras or {}
        placed = _wire_float(extras.get(EXTRAS_KEY_ORDER_LEVEL), finite=True)
        order_type = str(extras.get('order_type') or '').upper()
        if order_type and listed_type and order_type != listed_type:
            return False
        return (placed is None or level is None
                or abs(level - placed) <= 1e-9 * max(1.0, abs(placed)))

    return fits


#: Fill legs that reduce a position: what a close activity books.
_CLOSE_LEG_TYPES = frozenset({
    LegType.CLOSE, LegType.TAKE_PROFIT, LegType.STOP_LOSS, LegType.TRAILING_STOP,
})

#: ``WORKING_ORDER`` activity statuses that end a resting order.
_WORKING_ORDER_ENDS = frozenset({'CANCELLED', 'DELETED', 'EXPIRED', 'REJECTED', 'EXECUTED'})


class _RecoveryMixin(_CapitalComBase, ABC):
    """Restart recovery mix-in — replays cursor + in-flight rows at connect."""

    async def _load_activity_cursor_from_events(self) -> None:
        """Rebuild :attr:`_activity_cursor` from persisted ``activity_processed``
        events written during previous process incarnations.

        Capital.com's ``/history/activity`` returns a rolling window;
        without cross-restart fingerprint awareness, a restart within
        the window would re-emit every activity row as a duplicate
        event. The plugin side-steps the issue by writing the
        fingerprint to the BrokerStore on every processed row and
        reloading, on connect, the rows the venue can still return: those
        whose ``dateUTC`` lies within the largest ``lastPeriod`` window.
        The events are read across every instance of the logical run —
        they were written by the process that ended, not by this one.

        Deferred-close clamp: the in-batch clamp in
        :meth:`_process_activity` only protects the in-memory cursor.
        Across restarts the rebuilt cursor must also stay below the
        oldest *unresolved* deferred close, otherwise a later activity
        in the same batch (logged as ``activity_processed``) would
        rebuild ``last_date_utc`` past the deferred row's ``dateUTC``
        and the next poll's ``date_utc < cursor.last_date_utc`` guard
        would permanently drop the deferred close. Resolution is
        detected by ``(dateUTC, deal_id)`` identity: a deferred close
        is considered resolved only when an ``activity_processed`` row
        for the **same** ``dealId`` and ``dateUTC`` exists. Matching on
        ``dateUTC`` alone is unsafe — a sibling deal's activity at the
        same timestamp would mask an unrelated deferred close, letting
        the cursor advance past it and silently desync the strategy.
        Capital re-issues the deferred row with a populated ``level``
        (fresh fingerprint, identical ``dealId`` and ``dateUTC``), so
        the pair is a stable identity for the resolution probe.

        Only a fresh cursor is rebuilt. On a reconnect inside the running
        process the in-memory cursor already holds every fingerprint this
        process logged, and its watermark is pinned below rows the events
        do not record as unresolved — an activity no bot-owned order
        matched yet stays retryable without a persisted constraint. A
        rebuild would advance the watermark past such a row to a newer
        processed one, and the date guard would drop its fill once the
        reference lands. A fresh process needs no such pin: the engine
        adopts the venue position at startup.
        """
        store_ctx = self.store_ctx
        if store_ctx is None or self._connection_generation > 0:
            return
        cutoff_s = epoch_time() - _ACTIVITY_MAX_PERIOD_S - _ACTIVITY_LAG_S
        cursor = self._activity_cursor

        def recent(kind: str) -> list[dict]:
            return [
                payload
                for _, _, _, payload in store_ctx.iter_events_by_kind_for_run_id(kind)
                if _parse_iso_timestamp(str(payload.get('dateUTC') or '')) >= cutoff_s
            ]

        processed_payloads = recent('activity_processed')
        deferred_payloads = recent('activity_close_deferred_no_price')

        processed_keys: set[tuple[str, str]] = set()
        for payload in processed_payloads:
            d: str | None = payload.get('dateUTC')
            if d:
                processed_keys.add((d, str(payload.get('deal_id') or '')))

        deferred_min_unresolved: str | None = None
        for payload in deferred_payloads:
            d = payload.get('dateUTC')
            if not isinstance(d, str):
                continue
            key = (d, str(payload.get('deal_id') or ''))
            if key in processed_keys:
                continue
            if deferred_min_unresolved is None or d < deferred_min_unresolved:
                deferred_min_unresolved = d

        for payload in processed_payloads:
            fp: str | None = payload.get('fingerprint')
            date_utc = str(payload['dateUTC'])
            if fp:
                cursor.seen_fingerprints[fp] = date_utc
            if (deferred_min_unresolved is not None
                    and date_utc >= deferred_min_unresolved):
                continue
            if cursor.last_date_utc is None or date_utc > cursor.last_date_utc:
                cursor.last_date_utc = date_utc

    async def _recover_in_flight_submissions(self) -> None:
        """Replay §5.1 crash-matrix recovery on connect.

        Batch-fetches the three REST snapshots once, then routes every
        ``submitted`` / ``disposition_unknown`` / ``server_ref_seen``
        entry row through
        :meth:`DispatchJournal.recover_pending`. The journal calls the
        plugin's :class:`_CapitalComResumeHooks` once per row; the hook
        returns a :class:`ResumeOutcome` and the journal does the
        persist-first state advance and writes the canonical
        ``recovered_*`` / ``recovery_pending`` audit event with the
        plugin's ``recovery_path`` annotation.

        Bracket leg rows (``extras['kind']`` not in
        ``{'position', 'working'}``) skip the journal — their disposition
        is handled by ``_resolve_bracket_leg_disposition`` later.
        """
        if self.store_ctx is None:
            return
        positions_read_started_at = epoch_time()
        positions_resp = await self._call('positions', method='get')
        working_resp = await self._call('workingorders', method='get')
        activity_resp = await self._call(
            'history/activity',
            data={'lastPeriod': 86400, 'detailed': 'true'},
            method='get',
        )
        activity_read_at = epoch_time()

        pos_by_ref: dict[str, dict] = {}
        pos_by_deal_id: dict[str, dict] = {}
        for row in positions_resp.get('positions') or []:
            pos_data = row.get('position') or {}
            ref = pos_data.get('dealReference')
            if ref:
                pos_by_ref[str(ref)] = row
            did = pos_data.get('dealId')
            if did:
                pos_by_deal_id[str(did)] = row
        wo_by_ref: dict[str, dict] = {}
        wo_by_deal_id: dict[str, dict] = {}
        for wo in working_resp.get('workingOrders') or []:
            data = wo.get('workingOrderData') or {}
            ref = data.get('dealReference')
            if ref:
                wo_by_ref[str(ref)] = wo
            did = data.get('dealId')
            if did:
                wo_by_deal_id[str(did)] = wo
        activities = activity_resp.get('activities') or []
        # A deal the startup snapshot still lists long after a leg close of it
        # could have landed proves that close did not execute, so its later
        # disappearance is not taken for that close's unknown outcome.
        self._note_outlived_leg_closes(pos_by_deal_id, positions_read_started_at)

        resume_hooks = _CapitalComResumeHooks(
            plugin=self,
            activities=activities,
            pos_by_ref=pos_by_ref,
            wo_by_ref=wo_by_ref,
            wo_by_deal_id=wo_by_deal_id,
            pos_by_deal_id=pos_by_deal_id,
        )

        def hooks_for(pending_row: 'OrderRow') -> 'EntryDispatchHooks | None':
            # Bracket legs (``leg_kind in {'tp','sl'}``) are owned by
            # ``_resolve_bracket_leg_disposition``; everything else is
            # an entry row and goes through the journal. Filtering on
            # ``leg_kind`` (negative) rather than ``kind`` (positive)
            # matches the pre-M3 outer-loop behaviour: every non-bracket
            # row is recovered regardless of whether ``extras['kind']``
            # was explicitly stamped (older rows / test fixtures may
            # omit it).
            leg_kind = (pending_row.extras or {}).get('leg_kind')
            if leg_kind in ('tp', 'sl'):
                return None  # bracket legs — own resolution path
            if (pending_row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None:
                # An earlier take-over could not prove this entry's fill: a
                # trace or snapshot match here would book the very fill it
                # held back, so the row is left as it is.
                self._hold_unproven_take_over(pending_row)
                return None
            if (pending_row.extras or {}).get(EXTRAS_KEY_CANCEL_REQUESTED) is not None:
                # Pine cancelled the entry while its submission was pending.
                # A confirm here would make the landed order a live one the
                # cancel never reaches; left pending, the in-run resolver
                # DELETEs it once its deal id is known, or rejects it.
                return None
            return cast(EntryDispatchHooks, resume_hooks)

        journal = DispatchJournal(self.store_ctx)
        resolutions = await journal.recover_pending(
            cast(PendingHooksProvider, hooks_for),
        )
        # Outside every transaction, before the engine's first sync: a landed
        # submission whose execution is closed since goes to the engine.
        landed_unlisted = []
        for landed_coid, (landed_deal, opening, closes) in resume_hooks.landed_unlisted.items():
            landed_row = self.store_ctx.get_order(landed_coid)
            if landed_row is not None and landed_row.state == 'confirmed':
                landed_unlisted.append((landed_row, landed_deal, opening, closes))
        if landed_unlisted:
            self._hand_over_offline_executions(
                landed_unlisted, proof='landed_on_unlisted_deal',
                engine_adopts_closes=self._connection_generation == 0)

        # The ``pos_by_deal_id`` / ``wo_by_deal_id`` maps are snapshots
        # from *before* promotions; rows resolved via activity-heuristic
        # or the direct ``/confirms/{ref}`` call land a ``deal_id`` that
        # is not yet in those maps, so the subsequent orphan retirement
        # would otherwise close a freshly recovered live order.
        promoted_coids: set[str] = {
            r.coid for r in resolutions if r.status == 'confirmed'
        }

        # A fresh process: closes of own positions the previous process never
        # processed (it was stopped, or they came while it was down) are
        # already part of the venue size the engine adopts. They are judged
        # here — someone else's close is reported — and never replayed as
        # fills.
        closed_deals: set[str] = set()
        reports: list[tuple['OrderRow', dict]] = []
        if self._connection_generation == 0:
            self._note_venue_clock(activities, activity_read_at)
            # Who edited a bot position's or resting order's levels while no
            # process ran decides whether the guard, which restores the bot's
            # own levels from the journal, reports the edit.
            for activity in activities:
                self._note_level_edit(activity)
            # An own entry the venue executed under a fresh ``dealId`` while
            # no process watched it is moved onto that deal first, so its
            # downtime closes are judged and it is never retired as residue.
            self._migrate_rekeyed_startup_rows(
                pos_by_deal_id, wo_by_deal_id, promoted_coids,
            )
            closed_deals, reports = self._account_downtime_closes(
                activities, pos_by_deal_id, positions_read_started_at,
            )

        # After the in-flight recovery has had its chance to resolve
        # every ``submitted`` / ``server_ref_seen`` row, retire whatever
        # ``confirmed`` rows the BrokerStore still holds whose exchange
        # counterpart is absent — typical when the operator stopped the
        # bot and then closed the position manually outside it. Without
        # this pass the runtime ``_reconcile_snapshot`` would later
        # stamp ``missing_pending_since`` on those rows and the grace
        # tracker would report an unexpected cancel → an external-
        # intervention quarantine on a clean restart, even though there is
        # nothing to recover.
        self._retire_startup_orphans(
            pos_by_deal_id, wo_by_deal_id, promoted_coids,
            closed_deals=closed_deals, activities=activities,
        )
        for row, activity in reports:
            self._report_manual_close(row, activity)

        if self._connection_generation > 0:
            # A reconnect inside the running process: its engine never
            # re-adopts the venue size, so it still needs every fill the
            # outage held back. Stamping a surviving entry as accounted
            # would suppress its first real fill as a replay. The foreign
            # deals on the epic were already logged by this process.
            return

        self._stamp_surviving_entry_fills(pos_by_deal_id)

        # A live deal on the epic that no row of this run tracks is not the
        # run's exposure: the engine's startup clamp already leaves it out of
        # the book, and the plugin never closes, amends, books or reports it.
        # It is only made visible.
        self._note_untracked_positions_at_startup(positions_resp)

    async def _inherited_submission_verdict(self, row: 'OrderRow') -> ParkedVerdict:
        """What the venue knows about an entry submission an earlier process left pending.

        The lookup of the core
        :class:`~pynecore.core.broker.parked_dispatch.ParkedDispatchResolver`
        for an inherited dispatch
        (:meth:`~pynecore.core.broker.storage.RunContext.is_inherited_dispatch`)
        whose POST outcome the startup pass could not tell: its trace may have
        been published after the startup read, as the venue publishes an
        activity up to a minute after its deal. The engine holds the intent's
        entry behind such a row, because Capital.com carries no client order
        id and a second POST would open a second position.

        The activity history is causal for this venue: a POST that opened a
        deal or a working order leaves an activity, so a complete read that
        covers the row's whole life shows whether it landed. The read reaches
        back from now past the row's creation by :data:`_ACTIVITY_LAG_S`
        (:meth:`_CapitalComResumeHooks.inherited_group_case`). A row older
        than that history (24 hours, research dossier §4) is judged from what
        the venue still lists open (:meth:`_open_book_case`); an absence judged
        that way is logged and audited once
        (``inherited_submission_absent_without_history``) as a verdict taken
        without a history source: a flat venue is the safe state, and the
        strategy decides whether to send the entry again. Either way the
        row is settled together with every pending submission its deals
        match (:func:`_settle_group`), and nothing here waits:

        * Every deal has its own submission: the row lands on its deal, or
          opened nothing (absent: the engine may send the entry again).
        * A deal no pending submission explains: someone else trades the
          epic on this account, and which deal is the bot's cannot be
          proven. That is reported as an external intervention
          (:meth:`_report_inherited_surplus`); while its quarantine forbids
          venue writes the row stays as it is. Once writes are allowed (the
          operator resumed, or the configured policy does not quarantine),
          the earliest open deals go to the submissions in order
          (:func:`_assign_members`) and the rest stay someone else's.

        A row stamped
        :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_TAKE_OVER_UNPROVEN` was
        already traced to its deal: a later read without the trace is never
        an absence for it.

        :param row: The pending entry row.
        :return: :attr:`~ParkedOutcome.LANDED` with the deal id,
            ``ABSENT``, or ``INCONCLUSIVE`` for a row that is not an
            inherited entry submission, a failed read, a matching activity
            without a deal id, or a quarantined surplus.
        """
        store_ctx = self.store_ctx
        if (store_ctx is None
                or not store_ctx.is_inherited_dispatch(row.client_order_id)
                or (row.extras or {}).get('kind') not in (ENTRY_KIND_POSITION,
                                                          ENTRY_KIND_WORKING)):
            return ParkedVerdict(ParkedOutcome.INCONCLUSIVE)
        coid = row.client_order_id
        if coid in self._inherited_surplus_rows and self._venue_writes_held():
            return ParkedVerdict(ParkedOutcome.INCONCLUSIVE)
        age_s = max(0.0, epoch_time() - row.created_ts_ms / 1000.0)
        period = math.ceil(age_s) + _ACTIVITY_LAG_S
        book: tuple[list[dict], list[dict]] | None = None
        try:
            if period > _ACTIVITY_MAX_PERIOD_S:
                book = await self._open_book()
                case = self._open_book_case(row, *book)
            else:
                activity_resp = await self._call(
                    'history/activity',
                    data={'lastPeriod': period, 'detailed': 'true'},
                    method='get',
                )
                activities = activity_resp.get('activities')
                if not isinstance(activities, list):
                    return ParkedVerdict(ParkedOutcome.INCONCLUSIVE)
                hooks = _CapitalComResumeHooks(
                    plugin=self, activities=activities, pos_by_ref={}, wo_by_ref={},
                    wo_by_deal_id={}, pos_by_deal_id={},
                )
                case = hooks.inherited_group_case(row)
                if len(case.members) > 1 or len(case.deals) > 1:
                    # A shared trace: the resting orders still listed show
                    # their level and type, which tell submissions apart.
                    listed = await self._open_book()
                    book = listed
                    case = hooks.inherited_group_case(row, _resting_order_fits(listed[1]))
            if case.outcome == 'surplus':
                self._report_inherited_surplus(row, case)
                if self._venue_writes_held():
                    return ParkedVerdict(ParkedOutcome.INCONCLUSIVE)
                if book is None:
                    book = await self._open_book()
                still_open = _open_ids(*book)
                assert case.compat is not None
                assignment = _assign_members(
                    case.members,
                    {member: frozenset(deal for deal in deals if deal in still_open)
                     for member, deals in case.compat.items()},
                    case.deals,
                )
                deal_id = assignment.get(coid)
                case = _GroupCase('landed' if deal_id else 'absent', deal_id=deal_id)
        except (httpx.HTTPError, ConnectionError, ExchangeConnectionError,
                ExchangeRateLimitError, OrderNotFoundError, CapitalComError):
            return ParkedVerdict(ParkedOutcome.INCONCLUSIVE)
        if case.outcome == 'landed' and case.deal_id is not None:
            return ParkedVerdict(ParkedOutcome.LANDED, order_id=case.deal_id)
        extras = row.extras or {}
        if case.outcome != 'absent' or (
                extras.get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None
                or extras.get(EXTRAS_KEY_EXECUTION_HELD) is not None):
            # An earlier lookup traced this POST to its deal: a read that no
            # longer shows the trace (the deal claimed by another row, a read
            # cut short) does not undo that, and a rejection would send the
            # entry a second time.
            return ParkedVerdict(ParkedOutcome.INCONCLUSIVE)
        if period > _ACTIVITY_MAX_PERIOD_S and coid not in self._absent_without_history_reported:
            # Only what the venue still lists open was read: the activity
            # history no longer covers the submission's life, so nothing shows
            # whether it executed into a deal closed since. The venue is flat
            # for it, and the strategy decides whether to send the entry again.
            self._absent_without_history_reported.add(coid)
            logger.warning(
                "inherited entry submission %s (intent %s, sent %.0f s ago) is judged absent "
                "from what the venue lists open alone: the 24-hour activity history no longer "
                "covers it, so whether it executed into a deal closed since was not checked; "
                "nothing of it is open, and the strategy decides whether to send the entry "
                "again", coid, row.intent_key, age_s,
            )
            store_ctx.log_event(
                'inherited_submission_absent_without_history',
                client_order_id=coid,
                intent_key=row.intent_key,
                payload={'age_s': age_s, 'history_period_s': _ACTIVITY_MAX_PERIOD_S,
                         'evidence': 'open_book_only'},
            )
        return ParkedVerdict(ParkedOutcome.ABSENT)

    def _venue_writes_held(self) -> bool:
        """Whether an external-intervention quarantine forbids venue writes now."""
        hook = self.venue_writes_blocked
        return hook is not None and hook()

    def _report_inherited_surplus(self, row: 'OrderRow', case: _GroupCase) -> None:
        """Report, once per deal set, more deals than the pending submissions explain.

        Each own POST opens one deal at most, deals another Pyne run owns are
        no candidates, and every candidate matches a pending submission's
        epic, kind, direction and size at its send time. A deal left over
        then is someone else's trading on this epic, though which deal it is
        cannot be told: the bot's own state is unprovable, an external
        intervention
        (:meth:`~pynecore.core.plugin.broker.BrokerPlugin.report_external_intervention`).
        The change is dated by the latest deal's venue time, so the
        operator's resume accepts it. Every member of the group is marked, so
        none of them reads the venue while the quarantine holds.

        :raises UnexpectedCancelError: The ``stop`` policy without a working
            quarantine sink (the fail-safe signal the engine latches on).
        """
        for member in case.members:
            self._inherited_surplus_rows.add(member.client_order_id)
        key = frozenset(case.deals)
        if key in self._inherited_surplus_reported:
            return
        self._inherited_surplus_reported.add(key)
        when = case.when or {}
        latest = max((when.get(deal, 0.0) for deal in case.deals), default=0.0)
        if latest <= 0.0:
            latest = max(member.created_ts_ms for member in case.members) / 1000.0
        coids = [member.client_order_id for member in case.members]
        message = (
            f"{len(case.deals)} venue deal(s) {list(case.deals)} on {row.symbol} match the "
            f"{len(coids)} entry submission(s) {coids} an earlier process sent (same epic, "
            f"direction and size, sent at the same time): one POST opens one deal, so at "
            f"least one of them is not the bot's and someone else trades this epic on the "
            f"account. Which deal is the bot's cannot be proven. Close the deals that are not "
            f"the bot's, then resume: the bot takes the earliest open matching deals as its "
            f"own, one per submission, and leaves the rest as someone else's"
        )
        self.report_external_intervention(
            message,
            {'reason': 'inherited_submission_surplus_deals', 'symbol': row.symbol,
             'submissions': coids, 'deals': list(case.deals)},
            client_order_id=row.client_order_id,
            changed_at_ms=int(latest * 1000),
        )

    async def _open_book(self) -> tuple[list[dict], list[dict]]:
        """A fresh read of what the venue holds open: ``/positions`` and ``/workingorders`` rows.

        :raises ExchangeConnectionError: Or another transport error; the
            caller reads the result as inconclusive.
        """
        positions = await self._call('positions', method='get')
        working = await self._call('workingorders', method='get')
        return (list(positions.get('positions') or []),
                list(working.get('workingOrders') or []))

    def _open_book_case(
            self, row: 'OrderRow', positions: list[dict], working: list[dict],
    ) -> _GroupCase:
        """Judge an inherited submission older than the activity history from the open book.

        The POST's trace can no longer be read, but what it opened is either
        still listed, or no longer exposure at all. A candidate is an open
        entity of the row's epic and direction that no journal row tracks and
        no other Pyne run owns
        (:meth:`~pynecore.core.broker.storage.RunContext.foreign_live_exchange_order_ids`),
        created within
        :data:`~pynecore_capitalcom.foreign._UNCONFIRMED_ENTRY_SKEW_S` of the
        row's send (the venue clock's measured offset applied,
        :meth:`_book_clock_offset`), pinned within
        :data:`_ACTIVITY_MATCH_WINDOW_S`:

        * a market submission: a position no larger than the submission (a
          reduced one included), named by its ``dealId``;
        * a working submission: a resting working order of its size, type and
          placed level, named by its ``dealId``; or a position no larger than
          it that back-links an order (``workingOrderId``, the name) and was
          created after the send less the band: its creation is the
          execution's time, so it is never pinned and never refuted.

        The row is then settled with every other pending submission past the
        history (:func:`_settle_group`).
        """
        store_ctx = self.store_ctx
        assert store_ctx is not None
        journal: 'RunContext' = store_ctx
        other_runs = journal.foreign_live_exchange_order_ids()
        offset = self._book_clock_offset(positions, working)
        offset_s = offset if offset is not None else 0.0

        def free(ref: str | None, owner: 'OrderRow') -> bool:
            if ref is None or ref in other_runs:
                return False
            tracked = journal.find_by_ref('deal_id', ref)
            return tracked is None or tracked.client_order_id == owner.client_order_id

        def evidence(member: 'OrderRow') -> dict[str, _DealEvidence]:
            direction = 'BUY' if member.side == 'buy' else 'SELL'
            sent = member.created_ts_ms / 1000.0 + offset_s
            extras = member.extras or {}
            working_row = extras.get('kind') == ENTRY_KIND_WORKING
            tolerance = 1e-9 * max(1.0, abs(member.qty))
            found: dict[str, _DealEvidence] = {}
            for raw in positions:
                position = raw.get('position') or {}
                deal_id = _wire_id(position.get('dealId'))
                name = _wire_id(position.get('workingOrderId')) if working_row else deal_id
                size = _wire_float(position.get('size'), finite=True)
                if ((raw.get('market') or {}).get('epic') != member.symbol
                        or not free(deal_id, member) or name is None or not free(name, member)
                        or str(position.get('direction') or '').upper() != direction
                        or size is None or size <= 0.0 or size > member.qty + tolerance):
                    continue
                opened = _parse_iso_timestamp(str(position.get('createdDateUTC') or ''))
                if working_row:
                    if 0.0 < opened < sent - _UNCONFIRMED_ENTRY_SKEW_S:
                        continue
                    found[name] = _DealEvidence(when=opened, pinned=False, refutable=False)
                    continue
                if opened > 0.0 and abs(opened - sent) > _UNCONFIRMED_ENTRY_SKEW_S:
                    continue
                found[name] = _DealEvidence(
                    when=opened,
                    pinned=opened > 0.0 and abs(opened - sent) <= _ACTIVITY_MATCH_WINDOW_S)
            if working_row:
                placed = _wire_float(extras.get(EXTRAS_KEY_ORDER_LEVEL), finite=True)
                order_type = str(extras.get('order_type') or '').upper()
                for raw in working:
                    data = raw.get('workingOrderData') or {}
                    deal_id = _wire_id(data.get('dealId'))
                    size = _wire_float(data.get('orderSize'), finite=True)
                    level = _wire_float(data.get('orderLevel'), finite=True)
                    listed_type = str(data.get('orderType') or '').upper()
                    opened = _parse_iso_timestamp(
                        str(data.get('createdDateUTC') or data.get('createdDate') or ''))
                    if (((raw.get('market') or {}).get('epic') or data.get('epic'))
                            != member.symbol
                            or not free(deal_id, member)
                            or str(data.get('direction') or '').upper() != direction
                            or size is None or abs(size - member.qty) > tolerance
                            or (order_type and listed_type and order_type != listed_type)
                            or (opened > 0.0 and abs(opened - sent) > _UNCONFIRMED_ENTRY_SKEW_S)
                            or (placed is not None and level is not None
                                and abs(level - placed) > 1e-9 * max(1.0, abs(placed)))):
                        continue
                    assert deal_id is not None
                    found[deal_id] = _DealEvidence(
                        when=opened,
                        pinned=opened > 0.0 and abs(opened - sent) <= _ACTIVITY_MATCH_WINDOW_S)
            return found

        age_floor = epoch_time() - _ACTIVITY_MAX_PERIOD_S + _ACTIVITY_LAG_S
        pending = [other for other in find_pending_dispatch(journal)
                   if other.client_order_id != row.client_order_id
                   and not other.exchange_order_id
                   and journal.is_inherited_dispatch(other.client_order_id)
                   and other.created_ts_ms / 1000.0 < age_floor
                   and (other.extras or {}).get('kind') in (ENTRY_KIND_POSITION,
                                                            ENTRY_KIND_WORKING)]
        return _settle_group(row, evidence, pending, clock_measured=offset is not None)

    def _book_clock_offset(self, positions: list[dict], working: list[dict]) -> float | None:
        """The venue clock's lead over the local one, measured on the open book.

        :func:`_clock_offset` over the positions' ``createdDateUTC`` and the
        resting orders' creation time.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return None
        opened: dict[tuple[str, str], float] = {}
        for raw in positions:
            position = raw.get('position') or {}
            deal_id = _wire_id(position.get('dealId'))
            when = _parse_iso_timestamp(str(position.get('createdDateUTC') or ''))
            if deal_id is not None and when > 0.0:
                opened[('POSITION', deal_id)] = when
        for raw in working:
            data = raw.get('workingOrderData') or {}
            deal_id = _wire_id(data.get('dealId'))
            when = _parse_iso_timestamp(
                str(data.get('createdDateUTC') or data.get('createdDate') or ''))
            if deal_id is not None and when > 0.0:
                opened[('WORKING_ORDER', deal_id)] = when
        return _clock_offset(opened, store_ctx.iter_live_orders())

    def _stamp_surviving_entry_fills(
            self, pos_by_deal_id: dict[str, dict],
    ) -> None:
        """Mark surviving entry rows as accounted so their fill cannot replay.

        The engine's startup reconcile adopts the venue's net size, which
        already contains every entry the previous process filled. The
        activity loop suppresses a replayed entry fill on the
        ``entry_filled_at`` stamp — but that stamp is written when the fill
        is EMITTED, and Capital.com publishes an activity up to a minute
        after the deal. A process that ends inside that window leaves the
        row unstamped, so the next process books the very same exposure a
        second time on top of the position it just adopted (the observed
        200 -> 400 desync after a crash mid-reversal).

        Only rows the venue still backs are stamped: a deal that is gone
        was already retired by :meth:`_retire_startup_orphans`, and a
        working order that has yet to execute must keep its fill.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return
        stamped = 0
        for row in store_ctx.iter_live_orders():
            extras = dict(row.extras or {})
            if extras.get('kind') != 'position' or extras.get('entry_filled_at'):
                continue
            if row.state != 'confirmed' or row.filled_qty <= 0.0:
                continue
            deal_id = str(row.exchange_order_id or '')
            if not deal_id or deal_id not in pos_by_deal_id:
                continue
            extras['entry_filled_at'] = epoch_time()
            store_ctx.upsert_order(row.client_order_id, extras=extras)
            store_ctx.log_event(
                'startup_entry_fill_accounted',
                client_order_id=row.client_order_id,
                exchange_order_id=deal_id,
                payload={'filled_qty': row.filled_qty},
            )
            stamped += 1

        if stamped:
            broker_info(
                "startup: %d entry row(s) whose fill activity never surfaced "
                "before the restart marked as accounted — the adopted "
                "position already carries their exposure",
                stamped,
            )

    def _migrate_rekeyed_startup_rows(
            self,
            pos_by_deal_id: dict[str, dict],
            wo_by_deal_id: dict[str, dict],
            promoted_coids: set[str],
    ) -> None:
        """Move own entry rows onto the live deal the venue re-keyed them into.

        A resting entry that executed while no process watched it opened its
        position under a fresh ``dealId`` whose ``workingOrderId`` back-links
        the order, and the venue can run even a market entry that way
        (measured 2026-08-28, cycle 84). The poll migrates such a row as it
        sees the execution; a fresh process sees only the result, so without
        this pass the row is retired as an orphan and the bot's live deal is
        filed as someone else's: out of the book, never protected, never
        judged. Each live entry row neither snapshot shows under its own
        ``dealId`` is moved onto the live deal the ``/positions`` snapshot
        back-links to it (:meth:`_find_promoted_deal_id`, the deal's
        direction checked). A resting order's fill was never booked: like
        the fill the poll books, its exposure cursor is the order's size (a
        working order executes all-or-nothing), so the closes that fit the
        gap to the deal's present size are the downtime closes
        :meth:`_account_downtime_closes` judges next.

        A row stamped
        :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_TAKE_OVER_UNPROVEN` is
        not moved: its earlier process could not prove the fill this pass
        would book, so it stays held (:meth:`_hold_unproven_take_over`).

        :param pos_by_deal_id: The startup ``/positions`` rows by ``dealId``.
        :param wo_by_deal_id: The startup ``/workingorders`` rows by ``dealId``.
        :param promoted_coids: Rows the in-flight recovery just resolved.
        """
        store_ctx = self.store_ctx
        assert store_ctx is not None
        for row in list(store_ctx.iter_live_orders()):
            extras = row.extras or {}
            did = row.exchange_order_id
            if (not did or row.client_order_id in promoted_coids
                    or row.state not in ('confirmed', 'closing')
                    or extras.get('leg_kind') is not None
                    or extras.get(ADOPTED_STARTUP_EXTRA_KEY)
                    or extras.get('kind') not in ('position', 'working')
                    or did in pos_by_deal_id or did in wo_by_deal_id):
                continue
            if extras.get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None:
                self._hold_unproven_take_over(row)
                continue
            deal_id = self._find_promoted_deal_id(row, [], pos_by_deal_id)
            if deal_id is None:
                continue
            unbooked = row.filled_qty <= 0.0
            venue_size = _wire_float(
                (pos_by_deal_id[deal_id].get('position') or {}).get('size'), finite=True)
            # One transaction: the move onto the new deal stamps the entry
            # fill as accounted, so a move committed without its exposure
            # would be skipped by the next startup (the deal is then listed
            # under the row's own id) and its late activity suppressed.
            with store_ctx.transaction():
                self._promote_working_row_to_position(row, deal_id, stamp_entry_filled=True)
                if unbooked:
                    store_ctx.upsert_order(row.client_order_id, filled_qty=row.qty)
                store_ctx.log_event(
                    'startup_rekeyed_row_migrated',
                    client_order_id=row.client_order_id,
                    exchange_order_id=deal_id,
                    payload={'previous_deal_id': did, 'kind': extras.get('kind'),
                             'exposure': row.qty if unbooked else row.filled_qty,
                             'fill_booked_before': not unbooked,
                             'venue_size': venue_size},
                )
            broker_info(
                "startup: own %s entry %s (%s %s) executed as deal %s while the bot "
                "was down; the row now tracks that deal",
                extras.get('kind'), did, row.side, row.qty, deal_id,
            )

    def _hold_unproven_take_over(self, row: 'OrderRow') -> None:
        """Keep a take-over whose fill an earlier pass could not prove held.

        The row is stamped
        :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_TAKE_OVER_UNPROVEN`:
        its position showed a size other than the submitted quantity. The
        recovery pass neither confirms, moves nor retires it, so no fill is
        booked from the evidence that was too weak to book one, and the entry
        is never released for a second send: the poll proves the fill from the
        venue's history and books it
        (:meth:`~pynecore_capitalcom.order_history._OrderHistoryMixin._resolve_unproven_take_overs`).
        One warning and a ``take_over_unproven_held_at_recovery`` audit event
        per row for the process say so.

        :param row: The stamped entry row.
        """
        store_ctx = self.store_ctx
        assert store_ctx is not None
        coid = row.client_order_id
        if coid in self._take_over_held_reported:
            return
        self._take_over_held_reported.add(coid)
        unproven = (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) or {}
        broker_warning(
            "entry submission %s (intent %s) is still held: its deal %s showed size %s "
            "instead of the submitted %s, so recovery does not book, move or retire "
            "it; the poll proves its fill from the venue's history and books it",
            coid, row.intent_key, unproven.get('deal_id'), unproven.get('size'), row.qty,
        )
        store_ctx.log_event(
            'take_over_unproven_held_at_recovery',
            client_order_id=coid,
            exchange_order_id=row.exchange_order_id,
            intent_key=row.intent_key,
            payload={'state': row.state, 'kind': (row.extras or {}).get('kind'),
                     'submitted': row.qty, 'unproven': unproven},
        )

    def _account_downtime_closes(
            self,
            activities: list[dict],
            pos_by_deal_id: dict[str, dict],
            positions_read_started_at: float,
    ) -> tuple[set[str], list[tuple['OrderRow', dict]]]:
        """Judge the closes of own positions no earlier process processed.

        A fresh process adopts the venue's net size, which already holds every
        close that happened before its ``/positions`` read: a close the
        previous process never processed — it came while the bot was down, or
        just before it stopped — must not be booked again as a fill. Each
        such close of a live own position row is attributed like a close the
        poll sees (:meth:`_attribute_close_activity`), recorded as processed
        and as booked on its row, and someone else's close is returned for
        reporting. A position gone from the venue counts every close. The
        closes of a live deal whose entry has booked no exposure yet (a held
        partly netted entry, or an entry deferred for its price with an empty
        exposure cursor) are left to the poll, which books them behind the
        entry. A
        position still on it counts, oldest first, the closes its snapshot
        size already holds: those that fit the gap between the run's exposure
        cursor and that size, and those dated before the snapshot read. The
        venue clock bound alone never places the account's newest activity
        before the read, so a close made just before the start would
        otherwise be booked again on top of the adopted size. An own partial
        close whose exposure an earlier process already took off the cursor
        (:meth:`_own_close_cursor_retired`) counts whatever the snapshot
        shows: the engine adopts the venue size clamped to that cursor, which
        no longer holds the close, so the close never takes from the gap. A
        close an earlier process already booked (a copy the venue re-issued
        under a new fingerprint) is only recorded as processed.

        :param activities: The startup ``/history/activity`` read.
        :param pos_by_deal_id: The startup ``/positions`` rows by ``dealId``.
        :param positions_read_started_at: Local time the ``/positions`` read started.
        :return: The deals with close evidence, and ``(row, activity)`` per
            close to report.
        """
        store_ctx = self.store_ctx
        assert store_ctx is not None
        cursor = self._activity_cursor
        snapshot_floor = self._venue_time_floor(positions_read_started_at)
        closed_deals: set[str] = set()
        reports: list[tuple['OrderRow', dict]] = []
        snapshot_gaps: dict[str, float] = {}
        for activity in sorted(activities, key=lambda a: str(a.get('dateUTC') or '')):
            deal_id = _wire_id(activity.get('dealId'))
            if deal_id is None:
                continue
            row = store_ctx.find_by_ref('deal_id', deal_id)
            if (row is None or row.closed_ts_ms is not None
                    or row.exchange_order_id != deal_id
                    or (row.extras or {}).get('kind') != 'position'
                    or (row.extras or {}).get('leg_kind') is not None
                    or (row.extras or {}).get(ADOPTED_STARTUP_EXTRA_KEY)):
                # A legacy startup-adopted row tracks a deal this run never
                # opened: its closes are never judged or reported as an
                # intervention (``_retire_startup_orphans`` retires the row).
                continue
            present = deal_id in pos_by_deal_id
            extras = row.extras or {}
            if present and (extras.get(EXTRAS_KEY_NETTED_RESIDUAL_PENDING) is not None
                            or (row.filled_qty <= 0.0
                                and entry_pending_since(extras) is not None)):
                # An entry that has booked no exposure yet — a held partly
                # netted entry, or one deferred for its price with no
                # confirmed fill on its exposure cursor — leaves the size the
                # engine adopts holding none of its closes: the poll defers
                # them until the entry is booked, then books them. An entry
                # deferred for its price whose cursor holds its confirmed fill
                # is the adopted size's own (:meth:`_stamp_surviving_entry_fills`
                # stamps it accounted), so its closes are judged here.
                continue
            event = self._activity_to_event(activity, row, pos_by_deal_id.get(deal_id))
            if (event is None or event.event_type != 'filled'
                    or event.leg_type not in _CLOSE_LEG_TYPES):
                continue
            activity_ts = _parse_iso_timestamp(str(activity.get('dateUTC') or ''))
            size = float(event.fill_qty or 0.0)
            own_retired = (present and event.leg_type == LegType.CLOSE
                           and self._own_close_cursor_retired(row, activity))
            gap = 0.0
            if present and not own_retired:
                gap = snapshot_gaps.get(deal_id, -1.0)
                if gap < 0.0:
                    venue_size = _wire_float(
                        (pos_by_deal_id[deal_id].get('position') or {}).get('size'),
                        finite=True,
                    ) or 0.0
                    gap = max(0.0, row.filled_qty - venue_size)
                    snapshot_gaps[deal_id] = gap
                fits_gap = 0.0 < size <= gap + 1e-9 * max(1.0, gap)
                if not fits_gap and not 0.0 < activity_ts < snapshot_floor:
                    # Possibly after the snapshot: the poll books it as usual.
                    continue
            closed_deals.add(deal_id)
            fingerprint = _activity_fingerprint(activity)
            if fingerprint in cursor.seen_fingerprints:
                continue
            close_id = _close_identity(activity)
            date_utc = str(activity.get('dateUTC') or '')
            fresh = store_ctx.get_order(row.client_order_id) or row
            if is_delivered(fresh.extras or {}, activity):
                # A copy the venue re-issued under a new fingerprint of a close
                # an earlier process booked (and judged, and reported): the
                # snapshot gap and the exposure cursor already hold it.
                cursor.seen_fingerprints[fingerprint] = date_utc
                store_ctx.log_event(
                    'activity_processed',
                    exchange_order_id=deal_id,
                    client_order_id=row.client_order_id,
                    payload={'fingerprint': fingerprint, 'dateUTC': date_utc,
                             'deal_id': deal_id, 'type': activity.get('type'),
                             'status': activity.get('status'),
                             'source': activity.get('source'),
                             'startup_accounted': True, 'already_booked': True},
                )
                continue
            if present and not own_retired:
                snapshot_gaps[deal_id] = max(0.0, gap - size)
            # The verdict, the dedup commit, the booked-close record and the
            # exposure retirement are one transaction, like the poll's
            # booking: a process that dies in between must not find the close
            # consumed while its effect on the journal is missing. The in-memory
            # cursor follows only the committed transaction.
            with store_ctx.transaction():
                verdict = (self._attribute_close_activity(row, activity, pos_by_deal_id)
                           if event.leg_type == LegType.CLOSE else CLOSE_VERDICT_VENUE)
                store_ctx.log_event(
                    'activity_processed',
                    exchange_order_id=deal_id,
                    client_order_id=row.client_order_id,
                    payload={'fingerprint': fingerprint, 'dateUTC': date_utc,
                             'deal_id': deal_id, 'type': activity.get('type'),
                             'status': activity.get('status'),
                             'source': activity.get('source'),
                             'startup_accounted': True},
                )
                fresh = store_ctx.get_order(row.client_order_id) or row
                extras = dict(fresh.extras or {})
                book_fill(extras, activity, close_id)
                remaining: float | None = None
                if present and verdict in EXTERNAL_REDUCTION_VERDICTS:
                    remaining = max(0.0, fresh.filled_qty - size)
                    extras['partial_close_retired_at'] = epoch_time()
                if remaining is None:
                    store_ctx.upsert_order(row.client_order_id, extras=extras)
                else:
                    store_ctx.upsert_order(row.client_order_id, filled_qty=remaining, extras=extras)
                store_ctx.log_event(
                    'startup_close_accounted',
                    client_order_id=row.client_order_id,
                    exchange_order_id=deal_id,
                    payload={'dateUTC': date_utc, 'source': activity.get('source'),
                             'size': size, 'verdict': verdict, 'deal_present': present,
                             'remaining_exposure': remaining},
                )
            cursor.seen_fingerprints[fingerprint] = date_utc
            if verdict == CLOSE_VERDICT_FOREIGN:
                reports.append((fresh, activity))
        return closed_deals, reports

    def _note_untracked_positions_at_startup(self, positions_resp: Mapping) -> None:
        """Log every live deal on the run's epic that no row of this run tracks.

        Such a deal was opened by someone else — an operator, another API
        client, another Pyne run on the account — or by an earlier run under
        a different ``run_id``. Either way this run's journal did not produce
        it, so it is never the bot's: no store row is seeded for it, and the
        close, exit and emulator paths never target it. Each one is logged
        (WARNING, or INFO for a deal another Pyne run on the account owns)
        with a ``foreign_deal_on_epic_at_startup`` audit event. A deal an own
        working order opened with the rest of its size after its execution
        was retired as netting a foreign deal is reported as that instead
        (:meth:`_note_residual_after_netting_retirement`).
        """
        store_ctx = self.store_ctx
        symbol = self.symbol or ''
        if store_ctx is None or not symbol:
            return
        positions = positions_resp.get('positions') or []
        raw_by_deal = {
            str((raw.get('position') or {}).get('dealId')): raw for raw in positions
        }
        other_runs = store_ctx.foreign_live_exchange_order_ids()
        for deal in self._foreign_deals(positions, symbol):
            if deal.get('ownership_pending'):
                # An unconfirmed own entry may have opened it; the poll logs
                # it once that entry's ``dealId`` shows otherwise.
                continue
            deal_id = deal['deal_id']
            raw = raw_by_deal.get(str(deal_id)) or {}
            position = raw.get('position') or {}
            wo_id = _wire_id(position.get('workingOrderId'))
            if (wo_id is not None and wo_id != deal_id
                    and self._note_residual_after_netting_retirement(
                        wo_id, deal_id, 0.0, {deal_id: raw}, position)):
                continue
            self._foreign_deals_logged.add(deal_id)
            other_run = deal_id in other_runs
            message = (
                "startup: deal %s on %s (%s %s, created %s) is not this run's "
                "(%s); it is left alone — never closed, amended or booked"
            )
            args = (deal_id, symbol, deal['direction'], deal['size'], deal['created'],
                    'owned by another Pyne run' if other_run else 'untracked')
            if other_run:
                broker_info(message, *args)
            else:
                broker_warning(message, *args)
            store_ctx.log_event(
                'foreign_deal_on_epic_at_startup',
                exchange_order_id=deal_id,
                payload={'symbol': symbol, 'other_pyne_run': other_run, **deal},
            )

    @staticmethod
    def _startup_vanish_payload(row: 'OrderRow', closed_by_activity: bool) -> dict:
        """The audit payload of a filled own position the startup snapshot no longer shows."""
        return {'state': row.state, 'leg_kind': None, 'exposure': row.filled_qty,
                'closed_by_activity': closed_by_activity}

    def _startup_vanish_report(
            self, row: 'OrderRow', *, closed_by_activity: bool, activities: list[dict],
    ) -> dict | None:
        """The foreign-consumption report of a vanished own position, if one explains it.

        Without a close activity of its own (and without an own DELETE in
        flight), the deal may have been consumed whole by someone else's
        opposite order, which the venue logs on the consuming deal only: such
        an order in the startup read or in the run's audit of activities on
        deals it does not own (:meth:`_foreign_consumption_report`) is
        someone else's close. Reads only.

        :param row: The own position row, still live.
        :param closed_by_activity: A close activity of the deal was found
            (and judged) in the startup activity read.
        :param activities: The startup ``/history/activity`` read.
        """
        store_ctx = self.store_ctx
        assert store_ctx is not None
        if closed_by_activity or row.state == 'closing':
            return None
        audited = [audit.get('activity') or {} for _, _, _, audit
                   in store_ctx.iter_events_by_kind_for_run_id('external_activity_ignored')]
        return self._foreign_consumption_report(
            row, [*activities, *audited],
            earliest=self._venue_time_floor(row.created_ts_ms / 1000.0),
            latest=self._venue_time_ceiling(epoch_time()),
        )

    def _startup_vanish_unresolved(
            self, row: 'OrderRow', *, closed_by_activity: bool, report: dict | None,
    ) -> bool:
        """Whether only a core leg close of unknown outcome can explain a vanished position.

        Neither an own close activity, an own DELETE in flight nor someone
        else's consumption explains the disappearance, and a core leg close
        of the deal was sent with an unknown outcome
        (:meth:`~pynecore_capitalcom.reconcile._ReconcileMixin._unresolved_leg_close`):
        that DELETE or someone else may have closed it.
        """
        return (not closed_by_activity and row.state != 'closing' and report is None
                and self._unresolved_leg_close(row))

    def _request_startup_vanish_quarantine(self, row: 'OrderRow') -> bool:
        """Request the ``EXTERNAL_INTERVENTION`` quarantine of an unresolved leg close's vanish.

        Called outside every store transaction: the core's quarantine write
        joins an enclosing transaction, and a rollback of the row's
        retirement would take the latch with it while the engine holds it as
        persisted. Requested before the row is retired, a failed retirement
        leaves the row live under a durable latch, and the next start
        requests it again. A latch the store does not hold yet
        (:meth:`~pynecore_capitalcom.reconcile._ReconcileMixin._latch_intervention_durably`)
        keeps the row live: the runtime disappearance verdict retires it once
        the engine's retry stored the latch.

        :return: ``True`` when the row may be retired.
        """
        message = (
            f"startup: bot position {row.exchange_order_id} ({row.symbol}, {row.side} "
            f"{row.filled_qty}) is gone from the venue without a close activity "
            f"while a leg close of it had an unknown outcome: whether that close "
            f"or someone else closed it is unknown, the run writes nothing until "
            f"resumed"
        )
        logger.error("%s", message)
        return self._latch_intervention_durably(
            message,
            {**self._startup_vanish_payload(row, False), 'symbol': row.symbol,
             'side': row.side, 'at_startup': True},
        )

    def _retire_vanished_startup_position(
            self, row: 'OrderRow', *, closed_by_activity: bool, report: dict | None,
    ) -> None:
        """Retire a filled own position the startup snapshot no longer shows.

        Its exposure leaves the journal and the row is marked naturally
        closed, so a copy of its close activity the poll reads later is
        suppressed instead of booked into the book the engine adopts from the
        venue. Someone else's consumption of the deal
        (:meth:`_startup_vanish_report`) is reported once the rows are
        retired. A deal whose core leg close was sent with an unknown outcome
        (:meth:`_startup_vanish_unresolved`) gets a
        ``leg_close_unresolved_vanish`` audit event; its quarantine was
        requested before this retirement
        (:meth:`_request_startup_vanish_quarantine`). Otherwise nothing shows
        who closed it: ERROR and a ``position_vanished_unattributed`` audit
        event, never a report.

        :param row: The own position row, still live.
        :param closed_by_activity: A close activity of the deal was found
            (and judged) in the startup activity read.
        :param report: The row's :meth:`_startup_vanish_report`.
        """
        store_ctx = self.store_ctx
        assert store_ctx is not None
        payload = self._startup_vanish_payload(row, closed_by_activity)
        if closed_by_activity or row.state == 'closing':
            store_ctx.log_event(
                'startup_orphan_retired',
                client_order_id=row.client_order_id,
                exchange_order_id=row.exchange_order_id,
                payload=payload,
            )
        elif report is not None:
            store_ctx.log_event(
                'position_netted_away_by_foreign_order',
                client_order_id=row.client_order_id,
                exchange_order_id=row.exchange_order_id,
                payload={**payload, 'symbol': row.symbol, 'side': row.side,
                         'at_startup': True, 'report_id': report['report_id'],
                         'consumer': report['context']['consumer']},
            )
            store_ctx.log_event(
                'manual_close_report_pending',
                client_order_id=row.client_order_id,
                exchange_order_id=row.exchange_order_id,
                payload=report,
            )
        elif self._startup_vanish_unresolved(
                row, closed_by_activity=closed_by_activity, report=report):
            store_ctx.log_event(
                'leg_close_unresolved_vanish',
                client_order_id=row.client_order_id,
                exchange_order_id=row.exchange_order_id,
                payload={**payload, 'symbol': row.symbol, 'side': row.side,
                         'at_startup': True},
            )
        else:
            logger.error(
                "startup: bot position %s (%s, %s %s) is gone from the venue and no "
                "close activity of it is in the last day's history: retired, not "
                "reported — the venue history must show who closed it",
                row.exchange_order_id, row.symbol, row.side, row.filled_qty,
            )
            store_ctx.log_event(
                'position_vanished_unattributed',
                client_order_id=row.client_order_id,
                exchange_order_id=row.exchange_order_id,
                payload={**payload, 'symbol': row.symbol, 'side': row.side,
                         'at_startup': True},
            )
        self._tear_down_natural_close(row)

    def _retire_startup_orphans(
            self,
            pos_by_deal_id: dict[str, dict],
            wo_by_deal_id: dict[str, dict],
            promoted_coids: set[str] | None = None,
            *,
            closed_deals: set[str] | None = None,
            activities: list[dict] | None = None,
    ) -> None:
        """Retire live rows whose exchange counterpart is gone.

        Two passes over :meth:`iter_live_orders`:

        1. Non-bracket rows (``leg_kind`` not in ``{'tp','sl'}``) whose
           state is ``confirmed`` / ``closing`` / ``rejected``: if the
           row's ``exchange_order_id`` is absent from both the positions
           and workingorders snapshots, the order is definitively gone
           from the exchange side. Close the row silently — without
           this, the runtime ``_reconcile_snapshot`` would later flag
           it via ``missing_pending_since`` and the grace tracker would
           report an unexpected cancel, quarantining a bot that has
           nothing left to recover. On a reconnect ``confirmed`` and
           ``closing`` rows are left alone: the engine of this very
           process still books their exposure, and the activity poll,
           the snapshot reconcile and the disappearance tracker turn the
           deal's disappearance into the fill or cancel it is waiting
           for. On a fresh start the engine adopts the venue's net size
           instead, so the row is plain residue and is retired.
        2. Bracket leg rows (``leg_kind in {'tp','sl'}``): Capital.com
           brackets are position attributes with no own ``dealId``,
           so the parent's state is the only handle. Retire the leg
           if its parent ``client_order_id`` was retired in pass 1, OR
           if ``parent_deal_id`` is set and missing from the positions
           snapshot.

        A non-bracket row flagged :data:`ADOPTED_STARTUP_EXTRA_KEY` — seeded by
        an earlier version for a venue deal this run's journal never produced
        — is retired in pass 1 whatever the venue shows; its deal stays on the
        venue untouched.

        A filled own position retired in pass 1 is a close of the bot's own
        exposure, not residue: with a close activity among ``closed_deals``
        (judged and, when someone else's, reported by
        :meth:`_account_downtime_closes`) it is retired as closed; with none,
        someone else's opposite order that netted it away is reported once
        the rows are retired, one with a core leg close of unknown outcome
        requests an ``EXTERNAL_INTERVENTION`` quarantine, committed on its own
        before the row is retired in a transaction of its own after the
        pass (:meth:`_request_startup_vanish_quarantine`; a latch the store
        does not hold yet keeps the row live for the runtime verdict), and
        anything else is logged ERROR with a ``position_vanished_unattributed``
        audit event, never reported (:meth:`_retire_vanished_startup_position`).
        A retired row's exposure leaves the journal and a later copy of its
        close activity is never booked again.

        A row stamped
        :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_TAKE_OVER_UNPROVEN` or
        :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_EXECUTION_HELD` is never
        retired: it stays live and held.

        An own entry with no execution booked (a resting order, or a market
        entry the venue accepted without opening it at once) whose order
        executed while no process ran, into a deal closed since (the startup
        history proves both,
        :meth:`~pynecore_capitalcom.order_history._OrderHistoryMixin._closed_execution_evidence`),
        is not retired either: after the pass it is handed to the engine's
        held-execution recovery, before the engine's first sync
        (:meth:`~pynecore_capitalcom.order_history._OrderHistoryMixin._hand_over_offline_executions`).

        A resting own entry order retired in pass 1 on a fresh start is judged
        against the startup ``activities`` the way a cancel the poll sees is
        (:meth:`_judge_vanished_startup_working_order`); someone else's cancel
        is reported once the rows are retired. One whose execution the journal
        holds (:func:`~pynecore_capitalcom.delivery.entry_executed`) is never
        retired here: the deal it opened carries a fresh ``dealId`` that only a
        back-link names, the rekey migration found none in the snapshot, and
        the runtime disappearance verdict probes for it before it retires the
        row as a closed position. Nor is a position row whose entry is queued
        for its price (:func:`~pynecore_capitalcom.delivery.entry_pending_since`):
        the engine holds none of its exposure, so one bulk read missing its
        deal proves nothing, and the runtime disappearance verdict probes the
        deal itself before it retires the row as a closed position.

        ``promoted_coids`` lists rows resolved by the immediately preceding
        in-flight recovery pass. Their fresh ``deal_id`` came from
        ``activities`` or ``/confirms/{ref}`` and is therefore absent from
        the pre-recovery ``pos_by_deal_id`` / ``wo_by_deal_id`` snapshots,
        so the orphan check must skip them to avoid closing live broker
        orders that were just recovered.

        A single ``[BROKER]`` INFO line is emitted at the end when
        anything was retired, so the operator sees what happened
        without per-row noise.
        """
        if self.store_ctx is None:
            return

        promoted = promoted_coids or set()
        closed = closed_deals or set()
        retired_parent_coids: set[str] = set()
        retired_intent_keys: set[str] = set()
        kept_parent_coids: set[str] = set()
        reconnecting = self._connection_generation > 0
        retired_count = 0
        pending_reports: list[dict] = []
        quarantine_first: list['OrderRow'] = []
        executed_offline: list[tuple['OrderRow', str, dict, list[dict]]] = []
        store_ctx = self.store_ctx

        def retire_parent(retired: 'OrderRow') -> None:
            store_ctx.close_order(retired.client_order_id)
            retired_parent_coids.add(retired.client_order_id)
            # The envelope anchor in the ``envelopes`` table survives
            # ``close_order``; without this delete the next dispatch of
            # the same Pine intent reuses the stale ``bar_ts_ms`` and
            # regenerates the SAME client_order_id, hitting the
            # ``upsert_order`` UPDATE path on the just-closed row — and
            # since UPDATE does not touch ``closed_ts_ms``, the freshly
            # confirmed entry is invisible to ``iter_live_orders``,
            # which makes the immediately-following ``execute_exit``
            # raise ``no confirmed entry row``.
            if retired.intent_key:
                store_ctx.record_complete(retired.intent_key)
                retired_intent_keys.add(retired.intent_key)

        for row in list(self.store_ctx.iter_live_orders()):
            # One row's retirement is one write: a failure part-way leaves
            # the row as it was, and the next start judges it afresh
            # instead of finding a report pending beside a live row.
            with self.store_ctx.transaction():
                extras = row.extras or {}
                if extras.get('leg_kind') in ('tp', 'sl'):
                    continue
                if row.client_order_id in promoted:
                    continue
                if extras.get(ADOPTED_STARTUP_EXTRA_KEY):
                    # A row an earlier version seeded for a venue deal this run's
                    # journal never produced. The deal is not the run's: it is
                    # left on the venue untouched, and only the row is retired so
                    # no close or exit path can target it.
                    self.store_ctx.log_event(
                        'startup_adopted_row_retired',
                        client_order_id=row.client_order_id,
                        exchange_order_id=row.exchange_order_id,
                        payload={'state': row.state, 'filled_qty': row.filled_qty},
                    )
                    self.store_ctx.close_order(row.client_order_id)
                    retired_parent_coids.add(row.client_order_id)
                    if row.intent_key:
                        self.store_ctx.record_complete(row.intent_key)
                        retired_intent_keys.add(row.intent_key)
                    retired_count += 1
                    continue
                if (extras.get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None
                        or extras.get(EXTRAS_KEY_EXECUTION_HELD) is not None):
                    # A held take-over (its order is gone because it executed),
                    # or a row handed to the engine's held-execution recovery:
                    # retiring the row would release the entry for a second
                    # send. It stays live and held, like in the process that
                    # stamped it.
                    kept_parent_coids.add(row.client_order_id)
                    continue
                if reconnecting and row.state in ('confirmed', 'closing'):
                    # The engine of this process still tracks the row and waits
                    # for whatever made its deal disappear: our DELETE landing
                    # right before the link dropped, a native TP/SL, a working
                    # order filling into a new position, a manual close. The
                    # activity poll's lookback covers the outage, and the
                    # snapshot reconcile and disappearance tracker resolve the
                    # rest. Retiring the row drops its deal_id reference, so its
                    # fill reads as external and is never booked — the engine
                    # then keeps the position open, rejects every reversal close
                    # re-dispatch against the retired row and only clears the
                    # book minutes later via its venue-flat detector (measured
                    # 2026-09-30, cycle 225).
                    kept_parent_coids.add(row.client_order_id)
                    continue
                did = row.exchange_order_id
                if not did:
                    # A rejected row with no exchange id exists on no venue by
                    # definition — pure local residue of a failed dispatch.
                    # Left live, it lingers across runs and its state shadows
                    # the exit path's close-in-flight probe (measured live:
                    # rejected rows from five days earlier still matched the
                    # cycle-81 exit lookup). Retire it here; anything else
                    # without an id stays untouched (an in-flight dispatch may
                    # still be waiting for its confirm).
                    if row.state != 'rejected':
                        continue
                    self.store_ctx.log_event(
                        'startup_rejected_residue_retired',
                        client_order_id=row.client_order_id,
                        payload={'state': row.state,
                                 'pine_entry_id': row.pine_entry_id},
                    )
                    self.store_ctx.close_order(row.client_order_id)
                    retired_count += 1
                    continue
                if row.state not in ('confirmed', 'closing', 'rejected'):
                    continue
                if did in pos_by_deal_id or did in wo_by_deal_id:
                    continue
                if (extras.get('kind') == ENTRY_KIND_WORKING
                        and row.state in ('confirmed', 'closing')
                        and entry_executed(extras)):
                    # A resting entry whose execution the journal holds, booked
                    # or queued for its price, opened a deal under a fresh
                    # ``dealId`` that only a back-link names, and no snapshot
                    # read here shows one (the rekey migration ran first). The
                    # bulk ``/positions`` snapshot can omit a freshly opened
                    # deal (measured: cycle 81), and a deal that appears after
                    # the row is retired is never the bot's again: out of the
                    # book, never protected, its fill never booked. The row
                    # stays live for the runtime disappearance verdict, which
                    # probes for the back-linked deal across its grace window
                    # and retires the row as a closed position, never a
                    # cancelled order, once nothing shows it live.
                    self.store_ctx.log_event(
                        'startup_executed_working_row_kept',
                        client_order_id=row.client_order_id,
                        exchange_order_id=did,
                        payload={'state': row.state, 'filled_qty': row.filled_qty,
                                 'entry_booked': extras.get('entry_filled_at') is not None},
                    )
                    kept_parent_coids.add(row.client_order_id)
                    continue
                if (extras.get('kind') == ENTRY_KIND_POSITION
                        and row.state in ('confirmed', 'closing')
                        and entry_pending_since(extras) is not None):
                    # A position whose entry is queued for its price: the
                    # engine holds none of its exposure, so a deal the bulk
                    # snapshot omitted (measured: cycle 81) and a later read
                    # lists would be nobody's, its owed entry never booked.
                    # Absence from one bulk read proves nothing; the row stays
                    # live for the runtime disappearance verdict, which probes
                    # the deal itself and retires the row as a closed
                    # position once nothing shows it live.
                    self.store_ctx.log_event(
                        'startup_queued_entry_position_kept',
                        client_order_id=row.client_order_id,
                        exchange_order_id=did,
                        payload={'state': row.state, 'filled_qty': row.filled_qty},
                    )
                    kept_parent_coids.add(row.client_order_id)
                    continue
                filled_position = (
                    extras.get('kind') == 'position'
                    and row.state in ('confirmed', 'closing')
                    and (row.filled_qty > 0.0
                         or entry_executed(extras)
                         or extras.get('partial_close_retired_at') is not None)
                )
                if filled_position:
                    closed_by_activity = did in closed
                    report = self._startup_vanish_report(
                        row, closed_by_activity=closed_by_activity,
                        activities=activities or [])
                    if self._startup_vanish_unresolved(
                            row, closed_by_activity=closed_by_activity, report=report):
                        # Its quarantine must be committed on its own before
                        # the row is retired (below, after this pass).
                        quarantine_first.append(row)
                        continue
                    self._retire_vanished_startup_position(
                        row, closed_by_activity=closed_by_activity, report=report)
                    if report is not None:
                        pending_reports.append(report)
                elif (row.state == 'confirmed'
                      and extras.get('kind') in (ENTRY_KIND_WORKING, ENTRY_KIND_POSITION)
                      and (evidence := self._closed_execution_evidence(
                          row, activities or [], pos_by_deal_id)) is not None):
                    # It executed while no process ran, into a deal closed
                    # since: the engine books that execution, so the row stays
                    # for the hand-over after this pass.
                    executed_offline.append((row, *evidence))
                    kept_parent_coids.add(row.client_order_id)
                    continue
                elif row.state == 'confirmed' and extras.get('kind') == ENTRY_KIND_WORKING:
                    report = self._judge_vanished_startup_working_order(row, activities or [])
                    if report is not None:
                        pending_reports.append(report)
                else:
                    self.store_ctx.log_event(
                        'startup_orphan_retired',
                        client_order_id=row.client_order_id,
                        exchange_order_id=did,
                        payload={'state': row.state, 'leg_kind': None},
                    )
                retire_parent(row)
                retired_count += 1

        for row in quarantine_first:
            # A deal only a core leg close of unknown outcome can explain: the
            # quarantine is requested outside every transaction, so a failed
            # retirement below cannot roll the latch back, and the row stays
            # live for the next start to judge again. A latch the store does
            # not hold yet keeps the row live, its legs with it, for the
            # runtime verdict.
            if not self._request_startup_vanish_quarantine(row):
                kept_parent_coids.add(row.client_order_id)
                continue
            with self.store_ctx.transaction():
                self._retire_vanished_startup_position(
                    row, closed_by_activity=False, report=None)
                retire_parent(row)
            retired_count += 1

        if executed_offline:
            # Outside every transaction, before the engine's first sync.
            self._hand_over_offline_executions(executed_offline)

        orphaned_exit_keys: set[str] = set()
        for row in list(self.store_ctx.iter_live_orders()):
            # One leg's retirement is one write, like its parent's.
            with self.store_ctx.transaction():
                extras = row.extras or {}
                leg_kind = extras.get('leg_kind')
                if leg_kind not in ('tp', 'sl'):
                    continue
                parent_coid = extras.get('parent_coid')
                parent_deal_id = extras.get('parent_deal_id')
                # If the parent was just promoted by recovery, its new
                # ``deal_id`` (and therefore ``parent_deal_id`` stamped on
                # this leg) may legitimately be absent from the stale
                # pre-recovery ``pos_by_deal_id`` snapshot — don't retire.
                if parent_coid is not None and str(parent_coid) in promoted:
                    continue
                if parent_coid is not None and str(parent_coid) in kept_parent_coids:
                    # The kept parent's own resolution tears its legs down.
                    continue
                orphan = False
                if parent_coid is not None and str(parent_coid) in retired_parent_coids:
                    orphan = True
                elif parent_deal_id and str(parent_deal_id) not in pos_by_deal_id:
                    orphan = True
                if not orphan:
                    continue
                self.store_ctx.log_event(
                    'startup_orphan_retired',
                    client_order_id=row.client_order_id,
                    exchange_order_id=row.exchange_order_id,
                    payload={
                        'state': row.state,
                        'leg_kind': leg_kind,
                        'parent_coid': parent_coid,
                        'parent_deal_id': parent_deal_id,
                    },
                )
                self.store_ctx.close_order(row.client_order_id)
                # Leg rows persist ``intent_key`` as ``<exit_pine>\0<from_entry>\0TP/SL``;
                # strip the leg suffix to get the exit envelope key that
                # ``OrderSyncEngine._build_envelope`` looks up.
                leg_intent_key = row.intent_key
                if leg_intent_key and leg_intent_key.endswith(('\0TP', '\0SL')):
                    orphaned_exit_keys.add(leg_intent_key[:-3])
                retired_count += 1

        # TP and SL legs share one exit envelope, and an exit whose entry
        # pyramided holds legs on several deals. The envelope is retired only
        # once none of the exit's legs is left: a deal that closed while no
        # process ran does not end the exit still protecting its siblings.
        live_exit_keys = {
            row.intent_key[:-3]
            for row in self.store_ctx.iter_live_orders()
            if (row.extras or {}).get('leg_kind') in ('tp', 'sl')
            and row.intent_key and row.intent_key.endswith(('\0TP', '\0SL'))
        }
        for exit_intent_key in sorted(orphaned_exit_keys - live_exit_keys):
            if exit_intent_key not in retired_intent_keys:
                self.store_ctx.record_complete(exit_intent_key)
                retired_intent_keys.add(exit_intent_key)

        if retired_count:
            broker_info(
                "startup: retired %d orphan order row(s) — no matching "
                "position/working order on exchange",
                retired_count,
            )
        for report in pending_reports:
            self._send_manual_close_report(report)

    def _judge_vanished_startup_working_order(
            self, row: 'OrderRow', activities: list[dict],
    ) -> dict | None:
        """Judge who removed a resting own entry order while no process ran.

        The newest terminal activity of the order in the startup read decides,
        as the poll's would while the bot runs:

        * a ``USER`` cancel: the bot's own when an own ``DELETE`` explains it
          (:meth:`_own_cancel_explains`), otherwise someone else's — reported
          like a cancel the poll sees, which the engine's
          ``on_unexpected_cancel`` policy decides. The report is persisted
          before the row is retired (``manual_close_report_pending``), so a
          process that dies in between sends it on the next start;
        * a dealing-desk (``DEALER``) cancel: a WARNING, never reported;
        * any other source, an expiry or a rejection: the venue's own;
        * an execution whose deal no snapshot shows, or no activity at all:
          nothing shows who removed it — ERROR and a
          ``working_order_vanished_unattributed`` audit event, never reported.

        The judged activity is recorded as processed, so the poll does not
        log it as foreign activity later.

        :param row: The own working-order row, still live.
        :param activities: The startup ``/history/activity`` read.
        :return: The report to send once the rows are retired, or ``None``.
        """
        store_ctx = self.store_ctx
        assert store_ctx is not None
        deal_id = row.exchange_order_id or ''
        activity = self._newest_working_order_outcome(deal_id, activities)
        status = str((activity or {}).get('status') or '').upper()
        source = str((activity or {}).get('source') or '').upper()
        date_utc = str((activity or {}).get('dateUTC') or '')
        activity_ts = _parse_iso_timestamp(date_utc)
        if activity is None:
            verdict = 'own_cancel' if self._own_cancel_explains(row, 0.0) else 'unattributed'
        elif (str(activity.get('type') or '').upper() == 'POSITION'
              or status == 'EXECUTED'):
            verdict = 'executed'
        else:
            end = self._venue_end_of_working_order(activity)
            if end == 'venue':
                verdict = 'venue_cancel'
            elif end == 'dealer':
                verdict = 'dealer_cancel'
            elif self._own_cancel_explains(row, activity_ts):
                verdict = 'own_cancel'
            else:
                verdict = 'foreign_cancel'
        payload = {'state': row.state, 'leg_kind': None, 'kind': ENTRY_KIND_WORKING,
                   'verdict': verdict, 'status': status or None, 'source': source or None,
                   'dateUTC': date_utc or None}
        store_ctx.log_event(
            'startup_orphan_retired',
            client_order_id=row.client_order_id,
            exchange_order_id=deal_id,
            payload=payload,
        )
        if activity is not None:
            fingerprint = _activity_fingerprint(activity)
            self._activity_cursor.seen_fingerprints[fingerprint] = date_utc
            store_ctx.log_event(
                'activity_processed',
                exchange_order_id=deal_id,
                client_order_id=row.client_order_id,
                payload={'fingerprint': fingerprint, 'dateUTC': date_utc,
                         'deal_id': deal_id, 'type': activity.get('type'),
                         'status': activity.get('status'),
                         'source': activity.get('source'),
                         'startup_accounted': True},
            )
        described = (deal_id, row.symbol, row.side, row.qty)
        if verdict in ('executed', 'unattributed'):
            logger.error(
                "startup: bot working order %s (%s, %s %s) is gone from the venue and "
                "%s: retired, not reported — the venue history must show what removed it",
                *described,
                'executed into no deal the venue shows' if verdict == 'executed'
                else 'no activity of it is in the last day\'s history',
            )
            store_ctx.log_event(
                'working_order_vanished_unattributed',
                client_order_id=row.client_order_id,
                exchange_order_id=deal_id,
                payload={**payload, 'symbol': row.symbol, 'side': row.side, 'qty': row.qty},
            )
            return None
        if verdict == 'dealer_cancel':
            logger.warning(
                "startup: Capital.com dealing desk cancelled bot working order %s "
                "(%s, %s %s) at %s while the bot was down; not treated as an "
                "operator intervention", *described, date_utc,
            )
            return None
        if verdict == 'venue_cancel':
            broker_info(
                "startup: bot working order %s (%s, %s %s) ended on the venue while "
                "the bot was down (%s %s at %s)", *described, source, status, date_utc,
            )
            return None
        if verdict == 'own_cancel':
            return None
        assert activity is not None
        report = {
            'report_id': _close_identity(activity),
            'client_order_id': row.client_order_id,
            'deal_id': deal_id,
            'changed_at_ms': int(activity_ts * 1000.0) if activity_ts > 0.0 else None,
            'reason': (
                f"bot working order {deal_id} ({row.symbol}, {row.side} {row.qty}) was "
                f"cancelled on the venue by someone else while the bot was down: "
                f"USER {status} at {date_utc} matches no own cancel"
            ),
            'context': {
                'working_order': deal_id,
                'client_order_id': row.client_order_id,
                'symbol': row.symbol,
                'side': row.side,
                'qty': row.qty,
                'activity': {k: activity.get(k) for k in (
                    'dateUTC', 'dealId', 'type', 'status', 'source')},
            },
        }
        store_ctx.log_event(
            'manual_close_report_pending',
            client_order_id=row.client_order_id,
            exchange_order_id=deal_id,
            payload=report,
        )
        return report

    @staticmethod
    def _newest_working_order_outcome(deal_id: str, activities: list[dict]) -> dict | None:
        """The newest activity that ended the working order ``deal_id``, if any.

        A ``WORKING_ORDER`` row of the order that cancelled, deleted, expired,
        rejected or executed it, or a ``POSITION`` row back-linking it (the
        deal its execution opened).
        """
        if not deal_id:
            return None
        outcomes = [
            activity for activity in activities
            if ((str(activity.get('type') or '').upper() == 'WORKING_ORDER'
                 and _wire_id(activity.get('dealId')) == deal_id
                 and str(activity.get('status') or '').upper() in _WORKING_ORDER_ENDS)
                or (str(activity.get('type') or '').upper() == 'POSITION'
                    and _wire_id((activity.get('details') or {}).get('workingOrderId'))
                    == deal_id))
        ]
        return max(outcomes, key=lambda a: str(a.get('dateUTC') or ''), default=None)


# === Recovery hook =========================================================

@dataclass
class _CapitalComResumeHooks:
    """Recovery-time hook implementation for the entry journal.

    Constructed once per ``_recover_in_flight_submissions`` pass; the
    three REST snapshots are captured as attributes so each verdict
    call resolves locally without re-fetching. The journal's
    :meth:`pynecore.core.broker.journal.DispatchJournal.recover_pending`
    calls :meth:`resume_pending_dispatch` once per pending row. Recovery
    uses the journal's resume-only structural contract; fresh
    submission and exchange-order construction methods are intentionally
    not part of this collaborator.

    A submission whose trace the activity history cannot pin to one deal
    (several candidate deals, or a deal another pending submission
    matches too) is never guessed and never stops the process: it stays
    pending (``still_unknown``), and the in-run lookup settles it with every
    submission its deals match (:meth:`inherited_group_case`,
    :meth:`~_RecoveryMixin._inherited_submission_verdict`).
    """
    plugin: CapitalComHookCollaborator
    activities: list[dict]
    pos_by_ref: dict[str, dict]
    wo_by_ref: dict[str, dict]
    wo_by_deal_id: dict[str, dict]
    pos_by_deal_id: dict[str, dict]
    #: The venue order ids another Pyne run on the account owns, read once
    #: per hook instance (:meth:`_other_run_order_ids`).
    other_run_ids: frozenset[str] | None = None
    #: Per client order id, the landed submission whose deal the venue no
    #: longer lists, for the engine's held-execution recovery once the journal
    #: confirmed it: the deal its order executed into, that deal's opening
    #: (``None`` when the history cannot show it) and its closes
    #: (:meth:`_verdict_landed_unlisted`).
    landed_unlisted: dict[str, tuple[str, dict | None, list[dict]]] = field(
        default_factory=dict)

    async def resume_pending_dispatch(
            self, *, row: 'OrderRow', refs: Mapping[str, str],
    ) -> ResumeOutcome:
        """The recovery verdict for one pending row (:meth:`_route_resume`).

        A reconnect's recovery runs while the order poll goes on, and the
        poll may hold the row as a take-over whose fill could not be proven
        (:data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_TAKE_OVER_UNPROVEN`)
        while a verdict read is in flight, or before the journal reaches the
        row it listed earlier. The verdict comes from reads that do not know
        that: the row is read again once every read is done, and a held row
        is left as it is (``still_unknown``, which the journal persists with
        no further await) and reported to the operator, so recovery never
        books the fill the poll held back.

        :param row: The pending row, as the journal listed it.
        :param refs: The row's durable ``order_refs`` alias map.
        :return: The verdict the journal persists.
        """
        outcome = await self._route_resume(row=row, refs=refs)
        store_ctx = self.plugin.store_ctx
        current = store_ctx.get_order(row.client_order_id) if store_ctx is not None else None
        if (current is not None
                and (current.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None):
            self.plugin.hold_unproven_take_over(current)
            return ResumeOutcome(status='still_unknown')
        return outcome

    async def _route_resume(
            self, *, row: 'OrderRow', refs: Mapping[str, str],
    ) -> ResumeOutcome:
        """Route by ``row.state`` to the matching verdict-builder.

        ``server_ref_seen`` means the POST landed and the plugin
        recorded a ``deal_reference``; the verdict-builder tries the
        direct ``/confirms/{ref}`` GET first and falls back to the
        snapshot on TTL expiry.

        ``submitted`` always lacks a ``deal_reference`` — the POST
        either never went out or returned no usable reference — so the
        activity-heuristic verdict is the only option.

        ``disposition_unknown`` is the journal's terminal-pending state
        for both phases:

        - Confirm-phase timeout: ``record_server_ref`` already persisted
          a ``deal_reference`` before ``confirm_submission`` raised, so
          a row in this state can carry a ref. Route those through the
          ``server_ref_seen`` verdict-builder so ``/confirms/{ref}``
          (or, on TTL expiry, the snapshot match on the ref) is tried
          before activity matching — otherwise an accepted POST whose
          snapshot has not yet caught up would be left without a
          ``deal_id`` and the open-order / reconcile paths could not
          map it back. If that verdict comes back ``still_unknown``
          (confirm-GET TTL expired and the snapshot has no ref hit,
          or the GET returned no ``dealId``), fall back to the
          activity heuristic — the order may still have landed and
          a same-bar epic / direction / size / ±3 s activity row is
          enough to recover the ``deal_id``.
        - Submit-phase timeout: no ``deal_reference`` was ever recorded
          and only the activity heuristic can resolve the row.

        ``refs`` carries the durable ``order_refs`` alias map for the
        row. ``deal_reference`` is committed to ``order_refs`` *before*
        it is mirrored into ``orders.extras`` (see
        :func:`pynecore.core.broker.store_helpers.record_server_ref`),
        so during the narrow crash window between those two writes the
        ref is reachable only via ``refs``. The verdict-builders prefer
        ``refs`` over ``extras`` for exactly that reason.

        Modify-entry command rows (``extras['kind'] == KIND_MODIFY_ENTRY``)
        get their own verdict path: the recovery question is "did the
        ``PUT /workingorders/{dealId}`` land?" rather than "did the
        original POST land?". Routed before the state-based switch so a
        ``submitted`` modify row does not fall through to the
        entry-submission heuristic.
        """
        kind = (row.extras or {}).get('kind')
        if kind == KIND_MODIFY_ENTRY:
            return await self._verdict_modify_entry(row, refs)
        if kind == KIND_MODIFY_EXIT:
            return await self._verdict_modify_exit(row, refs)
        if kind == KIND_CANCEL:
            return self._verdict_cancel(row, refs)
        if kind == KIND_FULL_CLOSE:
            return self._verdict_full_close(row, refs)
        if kind == KIND_PARTIAL_CLOSE:
            return await self._verdict_partial_close(row, refs)
        if row.state == 'server_ref_seen':
            return await self._verdict_server_ref_seen(row, refs)
        if row.state == 'disposition_unknown':
            has_ref = bool(
                refs.get('deal_reference')
                or (row.extras or {}).get('deal_reference')
            )
            if has_ref:
                outcome = await self._verdict_server_ref_seen(row, refs)
                if outcome.status != 'still_unknown':
                    return outcome
                # Confirm-GET / snapshot path could not resolve the ref
                # (TTL expired with no snapshot hit, or no dealId on the
                # GET). The POST may still have landed — fall back to
                # activity matching the same way a submit-phase timeout
                # is handled.
                return self._verdict_submitted(row, refs)
            return self._verdict_submitted(row, refs)
        if row.state == 'submitted':
            return self._verdict_submitted(row, refs)
        return ResumeOutcome(status='still_unknown')

    def _live_position_fill(
            self, row: 'OrderRow', deal_id: str | None,
    ) -> tuple[bool, float, float]:
        """Fill fields for a recovered entry, from the live positions snapshot.

        A MARKET entry (``kind='position'``) whose deal is open on the venue
        has filled, and nothing later reports that fill: Capital.com's
        activity window is 60 s, and the snapshot reconcile only reports
        fill progress on a row that is already partly filled. Confirming the
        row unfilled leaves the run without ownership of exposure its own
        dispatch opened — startup adoption then treats the position as
        another run's (measured live: capitalcom cycle 269, 2026-10-11 —
        the next opposite entry netted the run's own long away and its
        bracket attach failed on the vanished deal).

        :return: ``(is_filled, filled_qty, fill_price)``; unfilled for a
            working order row or a deal absent from the snapshot.
        """
        if (row.extras or {}).get('kind') != 'position' or deal_id is None:
            return False, 0.0, 0.0
        hit = self.pos_by_deal_id.get(deal_id)
        if hit is None:
            return False, 0.0, 0.0
        position = hit.get('position') or {}
        size = _wire_float(position.get('size'), finite=True) or 0.0
        level = _wire_float(position.get('level'), finite=True) or 0.0
        filled_qty = min(row.qty, size)
        if filled_qty <= 0.0:
            return False, 0.0, 0.0
        return True, filled_qty, level

    def _activity_matches_row(
            self, activity: Mapping, row: 'OrderRow', *,
            window_s: float | None = _ACTIVITY_MATCH_WINDOW_S, offset_s: float = 0.0,
    ) -> bool:
        """Whether an activity row can be the venue trace of ``row``'s POST.

        The ``detailed=true`` payload carries direction and size inside
        ``details`` only. One market POST leaves two same-size activities
        (the ``POSITION`` open and the ``WORKING_ORDER`` it executed
        through), and a resting entry dispatched the same second leaves a
        third, so the type must match the row's kind: a MARKET entry is
        traced by its ``POSITION`` open, a resting entry by the
        ``WORKING_ORDER`` it created (an ``EXECUTED`` working order is a
        market POST's internal order or a fill, never a creation). Deals
        another journal row already tracks, or another Pyne run on the
        account owns (:meth:`_other_run_order_ids`), are not candidates.

        :param activity: One ``/history/activity`` row.
        :param row: The pending entry row.
        :param window_s: Largest distance between the activity's time and the
            row's creation; ``None`` matches at any time.
        :param offset_s: The venue clock's measured lead over the local one,
            taken off the activity's time first.
        """
        if (activity.get('epic') or '') != row.symbol:
            return False
        activity_type = (activity.get('type') or '').upper()
        status = (activity.get('status') or '').upper()
        if (row.extras or {}).get('kind') == 'working':
            if activity_type != 'WORKING_ORDER' or status in ('EXECUTED', 'REJECTED'):
                return False
        elif activity_type != 'POSITION' or status == 'REJECTED':
            return False
        details = activity.get('details') or {}
        row_side = 'BUY' if row.side == 'buy' else 'SELL'
        if (details.get('direction') or '').upper() != row_side:
            return False
        size = _wire_float(details.get('size'), finite=True)
        if size is None or abs(size - row.qty) > 1e-9:
            return False
        row_created = row.created_ts_ms / 1000.0 if row.created_ts_ms else 0.0
        when = _parse_iso_timestamp(activity.get('dateUTC') or '')
        if (window_s is not None and row_created
                and abs(when - offset_s - row_created) > window_s):
            return False
        deal_id = _wire_id(activity.get('dealId'))
        store_ctx = self.plugin.store_ctx
        if deal_id is not None and store_ctx is not None:
            tracked = store_ctx.find_by_ref('deal_id', deal_id)
            if tracked is not None and tracked.client_order_id != row.client_order_id:
                return False
            if deal_id in self._other_run_order_ids():
                return False
        return True

    def _other_run_order_ids(self) -> frozenset[str]:
        """The venue order ids another Pyne run on the account and epic owns.

        Read once per hook instance
        (:meth:`~pynecore.core.broker.storage.RunContext.foreign_live_exchange_order_ids`):
        a deal another run's live journal row tracks is that run's, never
        this submission's trace.
        """
        if self.other_run_ids is None:
            store_ctx = self.plugin.store_ctx
            self.other_run_ids = frozenset(
                store_ctx.foreign_live_exchange_order_ids() if store_ctx is not None else ())
        return self.other_run_ids

    def _contesting_rows(self, activity: Mapping, row: 'OrderRow') -> list['OrderRow']:
        """The other pending entry submissions without a deal id that ``activity`` matches too."""
        store_ctx = self.plugin.store_ctx
        if store_ctx is None:
            return []
        return [other for other in find_pending_dispatch(store_ctx)
                if other.client_order_id != row.client_order_id
                and not other.exchange_order_id
                and (other.extras or {}).get('kind') in (ENTRY_KIND_POSITION,
                                                         ENTRY_KIND_WORKING)
                and self._activity_matches_row(activity, other)]

    def _trace_contested(self, activity: Mapping, row: 'OrderRow') -> bool:
        """Whether another pending entry submission could have left ``activity`` too.

        The match carries no client order id: two entry submissions of the
        same epic, side, size and kind sent within the match window both
        match the one deal either of them opened. Which one landed is then
        unknown, and handing the deal to one of them is a guess: when it is
        wrong, the deal is booked to a submission that opened nothing, and
        the one that did open it no longer sees its trace, reads as absent
        and is sent again.

        :param activity: An activity that matches ``row``.
        :param row: The pending entry row.
        :return: Whether a live pending entry row without a deal id, other
            than ``row``, matches the activity as well.
        """
        return bool(self._contesting_rows(activity, row))

    def venue_clock_offset(self) -> float | None:
        """The venue clock's lead over the local one, measured on the bot's own deals.

        :func:`_clock_offset` over each deal's first creation activity.

        :return: The offset in seconds, or ``None`` without a sample.
        """
        store_ctx = self.plugin.store_ctx
        if store_ctx is None:
            return None
        opened: dict[tuple[str, str], float] = {}
        for activity in self.activities:
            deal_id = _wire_id(activity.get('dealId'))
            activity_type = (activity.get('type') or '').upper()
            status = (activity.get('status') or '').upper()
            if deal_id is None or status in ('EXECUTED', 'REJECTED'):
                continue
            when = _parse_iso_timestamp(activity.get('dateUTC') or '')
            if when <= 0.0:
                continue
            key = (activity_type, deal_id)
            opened[key] = min(opened.get(key, when), when)
        return _clock_offset(opened, store_ctx.iter_live_orders())

    def activity_evidence(
            self, row: 'OrderRow', offset_s: float,
    ) -> dict[str, _DealEvidence] | None:
        """The deals whose activities may be ``row``'s POST trace, within the candidate band.

        :meth:`_activity_matches_row` within
        :data:`~pynecore_capitalcom.foreign._UNCONFIRMED_ENTRY_SKEW_S` of the
        row (``offset_s`` applied); pinned within
        :data:`_ACTIVITY_MATCH_WINDOW_S`.

        :return: Deal id -> evidence, or ``None`` when a matching activity
            carries no deal id (nothing can be dealt then).
        """
        found: dict[str, _DealEvidence] = {}
        for activity in self.activities:
            if not self._activity_matches_row(activity, row, window_s=_UNCONFIRMED_ENTRY_SKEW_S,
                                              offset_s=offset_s):
                continue
            deal_id = _wire_id(activity.get('dealId'))
            if deal_id is None:
                return None
            when = _parse_iso_timestamp(activity.get('dateUTC') or '')
            pinned = self._activity_matches_row(activity, row, offset_s=offset_s)
            known = found.get(deal_id)
            if known is not None:
                when = min(known.when, when)
                pinned = pinned or known.pinned
            found[deal_id] = _DealEvidence(when=when, pinned=pinned)
        return found

    def inherited_group_case(
            self, row: 'OrderRow', fits: Callable[['OrderRow', str], bool] | None = None,
    ) -> _GroupCase:
        """Judge an inherited entry submission from a complete activity read.

        The venue clock's offset is measured first (:meth:`venue_clock_offset`);
        without a sample it is taken as zero and the candidate band is not
        refuted. The row and the pending submissions without a deal id
        (:func:`~pynecore.core.broker.store_helpers.find_pending_dispatch`)
        are then settled together (:func:`_settle_group`). A group that holds
        a parked submission of this process waits for that one's confirm.

        :param row: The pending entry row; :attr:`activities` must cover its
            whole life.
        :param fits: Drops a deal a member's observable attributes contradict.
        """
        offset = self.venue_clock_offset()
        offset_s = offset if offset is not None else 0.0
        own = self.activity_evidence(row, offset_s)
        if own is None:
            return _GroupCase('inconclusive')
        cache: dict[str, dict[str, _DealEvidence] | None] = {row.client_order_id: own}

        def evidence(member: 'OrderRow') -> Mapping[str, _DealEvidence]:
            coid = member.client_order_id
            if coid not in cache:
                cache[coid] = self.activity_evidence(member, offset_s)
            return cache[coid] or {}

        store_ctx = self.plugin.store_ctx
        pending = [other for other in (find_pending_dispatch(store_ctx) if store_ctx else [])
                   if other.client_order_id != row.client_order_id
                   and not other.exchange_order_id
                   and (other.extras or {}).get('kind') in (ENTRY_KIND_POSITION,
                                                            ENTRY_KIND_WORKING)]
        case = _settle_group(row, evidence, pending, clock_measured=offset is not None,
                             fits=fits)
        if store_ctx is not None and any(
                not store_ctx.is_inherited_dispatch(member.client_order_id)
                for member in case.members):
            # A submission of this process shares the trace: its confirm
            # tells what it opened, which settles the group without a guess.
            return _GroupCase('inconclusive')
        return case

    def _verdict_submitted(
            self, row: 'OrderRow', refs: Mapping[str, str],
    ) -> ResumeOutcome:
        """Verdict for a ``submitted`` / ``disposition_unknown`` row.

        Multi-factor confidence:

        1. Stored ``deal_reference`` (preferred from ``refs``, falling
           back to ``extras``) matches a positions / working snapshot →
           ``recovery_path='stored_ref'``, confirmed.
        2. Activity heuristic (see :meth:`_activity_matches_row`) →
           single match → ``recovery_path='activity_single_match'``;
           a single match another pending submission matches too →
           ``still_unknown`` with ``recovery_path='activity_contested_match'``;
           multiple matches → ``still_unknown`` with
           ``recovery_path='activity_ambiguous_match'`` (settled in-run, see
           the class docstring); zero matches →
           ``still_unknown`` with ``recovery_path='no_match'``.
        """
        stored_ref: str | None = (
            refs.get('deal_reference') or (row.extras or {}).get('deal_reference')
        )
        if stored_ref:
            hit = self.pos_by_ref.get(stored_ref) or self.wo_by_ref.get(stored_ref)
            if hit is not None:
                data = hit.get('position') or hit.get('workingOrderData') or {}
                deal_id = data.get('dealId')
                matched_snapshot = (
                    'position' if hit is self.pos_by_ref.get(stored_ref)
                    else 'working'
                )
                exchange_id = str(deal_id) if deal_id else None
                is_filled, filled_qty, fill_price = self._live_position_fill(
                    row, exchange_id,
                )
                return ResumeOutcome(
                    status='confirmed',
                    exchange_id=exchange_id,
                    is_filled=is_filled,
                    filled_qty=filled_qty,
                    fill_price=fill_price,
                    recovery_path='stored_ref',
                    recovery_context={
                        'deal_reference': stored_ref,
                        'matched_snapshot': matched_snapshot,
                    },
                )

        # Activity heuristic: activity type + epic + direction + size +
        # ±3 s around the row's creation.
        candidates: list[dict] = []
        candidate_deal_ids: set[str] = set()
        for activity in self.activities:
            if not self._activity_matches_row(activity, row):
                continue
            # One deal may leave several activities (a resting entry's
            # creation and its later status rows); it is one candidate.
            deal_id = _wire_id(activity.get('dealId'))
            if deal_id is not None:
                if deal_id in candidate_deal_ids:
                    continue
                candidate_deal_ids.add(deal_id)
            candidates.append(activity)

        if not candidates:
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='no_match',
            )
        if len(candidates) == 1 and self._trace_contested(candidates[0], row):
            # Another pending submission matches the same trace: the group is
            # settled by the in-run lookup, which judges every submission the
            # deal matches together. The row stays pending until then.
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='activity_contested_match',
            )
        if len(candidates) == 1:
            deal_id = _wire_id(candidates[0].get('dealId'))
            is_filled, filled_qty, fill_price = self._live_position_fill(row, deal_id)
            return ResumeOutcome(
                status='confirmed',
                exchange_id=deal_id,
                is_filled=is_filled,
                filled_qty=filled_qty,
                fill_price=fill_price,
                recovery_path='activity_single_match',
                recovery_context={'activity_count': 1},
            )

        # Several deals match and nothing on the venue names the one this
        # POST opened (no client order id, research dossier §5): never a
        # guess, never a stop. The row stays pending, so the engine keeps
        # this entry alone held (no second send), and the in-run lookup
        # settles it on its first pass (``_inherited_submission_verdict``).
        deal_ids = sorted(str(deal_id) for deal_id in candidate_deal_ids)
        return ResumeOutcome(
            status='still_unknown',
            recovery_path='activity_ambiguous_match',
            recovery_context={'candidate_deal_ids': deal_ids},
        )

    async def _verdict_server_ref_seen(
            self, row: 'OrderRow', refs: Mapping[str, str],
    ) -> ResumeOutcome:
        """Verdict for a ``server_ref_seen`` row.

        Direct ``/confirms/{ref}`` GET first: ``dealStatus=REJECTED``
        → rejected; an accepted deal still present in a position or
        working-order snapshot → confirmed unfilled
        (``recovery_path='confirm_get_direct'``); an accepted deal
        absent from both snapshots → rejected
        (``recovery_path='confirm_get_stale_deal'``) — the confirms
        cache outlives the deal itself, so a stale confirm must not
        fabricate a live order. On :class:`OrderNotFoundError` (TTL
        expiry) the verdict falls back to the snapshot map
        (``recovery_path='ttl_fallback_snapshot'``). Without a usable
        ``deal_id`` the verdict is ``still_unknown``.

        The ``deal_reference`` is sourced from ``refs`` first and from
        ``extras`` only as a fallback: the alias is committed to
        ``order_refs`` ahead of the ``extras`` mirror, so ``refs`` is
        the durable view even mid-crash-window.
        """
        ref: str | None = (
            refs.get('deal_reference') or (row.extras or {}).get('deal_reference')
        )
        if not ref:
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='missing_deal_reference',
            )
        try:
            confirm = await self.plugin.call_api(
                f'confirms/{ref}', method='get',
            )
        except OrderNotFoundError:
            # TTL expired — fall back to snapshot match on the ref.
            hit = self.pos_by_ref.get(ref) or self.wo_by_ref.get(ref)
            if hit is None:
                return ResumeOutcome(
                    status='still_unknown',
                    recovery_path='ttl_fallback_no_match',
                )
            data = hit.get('position') or hit.get('workingOrderData') or {}
            deal_id = data.get('dealId')
            if not deal_id:
                return ResumeOutcome(
                    status='still_unknown',
                    recovery_path='ttl_fallback_no_deal_id',
                )
            matched_snapshot = (
                'position' if hit is self.pos_by_ref.get(ref)
                else 'working'
            )
            is_filled, filled_qty, fill_price = self._live_position_fill(
                row, str(deal_id),
            )
            return ResumeOutcome(
                status='confirmed',
                exchange_id=str(deal_id),
                is_filled=is_filled,
                filled_qty=filled_qty,
                fill_price=fill_price,
                recovery_path='ttl_fallback_snapshot',
                recovery_context={
                    'deal_reference': ref,
                    'matched_snapshot': matched_snapshot,
                },
            )

        deal_status = (confirm.get('dealStatus') or '').upper()
        if deal_status == 'REJECTED':
            return ResumeOutcome(
                status='rejected',
                reject_reason=_extract_reject_reason(confirm),
                recovery_path='confirm_get_rejected',
                recovery_context={'deal_reference': ref},
            )
        deal_id = None
        affected = confirm.get('affectedDeals') or []
        if affected:
            deal_id = affected[0].get('dealId')
        if not deal_id:
            deal_id = confirm.get('dealId')
        if not deal_id:
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='confirm_get_no_deal_id',
            )
        deal_id = str(deal_id)
        # An accepted confirm alone does not prove the deal is still
        # alive: the confirms cache outlives the deal (a market entry
        # from a long-dead session confirms "accepted" hours later).
        # The account snapshots were taken at recovery start, after any
        # resubmission POST — a live deal is necessarily present in one
        # of them, so absence from both is conclusive.
        if deal_id in self.pos_by_deal_id or deal_id in self.wo_by_deal_id:
            is_filled, filled_qty, fill_price = self._live_position_fill(row, deal_id)
            return ResumeOutcome(
                status='confirmed',
                exchange_id=deal_id,
                is_filled=is_filled,
                filled_qty=filled_qty,
                fill_price=fill_price,
                recovery_path='confirm_get_direct',
                recovery_context={'deal_reference': ref},
            )
        landed = self._verdict_landed_unlisted(row, deal_id, ref)
        if landed is not None:
            return landed
        return ResumeOutcome(
            status='rejected',
            reject_reason=(
                'accepted deal no longer present in any account snapshot '
                '(stale confirm for a dead deal)'
            ),
            recovery_path='confirm_get_stale_deal',
            recovery_context={'deal_reference': ref, 'deal_id': deal_id},
        )

    def _verdict_landed_unlisted(
            self, row: 'OrderRow', deal_id: str, ref: str,
    ) -> ResumeOutcome | None:
        """Verdict for an accepted submission whose deal no account snapshot lists.

        The confirm proves the POST landed on ``deal_id``. Its absence from the
        snapshots proves the order is no longer live, not that it executed
        nothing: it may have executed into a deal closed since, or (a resting
        order, or a market entry the venue ran through its working-order
        machinery) into a fresh deal that back-links it. A rejection would
        send the entry again next to that execution. So the submission is
        confirmed on its deal, unfilled, when

        * the startup history shows its execution
          (:meth:`~pynecore_capitalcom.order_history._OrderHistoryMixin._execution_in_history`):
          a deal still listed is moved onto by the startup migration, and one
          closed since, with its close in the history, is handed to the
          engine's held-execution recovery after the journal pass
          (:attr:`landed_unlisted`); an execution whose close the history
          does not show yet is left to the runtime disappearance verdict;
        * the submission is older than the history (24 hours, research
          dossier §4): nothing can tell what it executed, so it is handed to
          the engine's held-execution recovery without an opening, whose
          no-evidence rule keeps new entries waiting rather than sending one
          twice.

        Within the history, a submission whose execution the history does not
        show executed nothing (the history is causal for this venue: an
        execution leaves an activity), and ``None`` lets the rejection stand.

        :param row: The pending submission row.
        :param deal_id: The deal its accepted confirm names.
        :param ref: Its ``dealReference``.
        :return: The confirmed verdict, or ``None``.
        """
        executed = self.plugin.execution_in_history(row, deal_id, self.activities)
        age_s = epoch_time() - row.created_ts_ms / 1000.0
        if executed is not None:
            executed_deal, opening, closes = executed
            if closes and executed_deal not in self.pos_by_deal_id:
                self.landed_unlisted[row.client_order_id] = (executed_deal, opening, closes)
            path = 'confirm_get_executed_deal'
        elif age_s > _ACTIVITY_MAX_PERIOD_S - _ACTIVITY_LAG_S:
            self.landed_unlisted[row.client_order_id] = (deal_id, None, [])
            path = 'confirm_get_beyond_history'
        else:
            return None
        return ResumeOutcome(
            status='confirmed',
            exchange_id=deal_id,
            is_filled=False,
            filled_qty=0.0,
            fill_price=None,
            recovery_path=path,
            recovery_context={'deal_reference': ref, 'deal_id': deal_id},
        )

    async def _verdict_modify_entry(
            self, row: 'OrderRow', refs: Mapping[str, str],
    ) -> ResumeOutcome:
        """Verdict for a ``modify_entry`` command row.

        The command row carries ``extras['target_coid']`` (the working
        order being amended) and ``extras['new_level']`` (the requested
        level). Three resolution paths in priority order:

        1. **Stored ``deal_reference``** (recovery extends back to a
           confirm-phase ambiguity): ``/confirms/{ref}`` GET first;
           ``REJECTED`` → rejected. ``ACCEPTED`` with an echoed level
           that matches ``new_level`` → confirmed pointing at the
           target's exchange id. TTL miss falls through to
           snapshot-based verification (path 2).

        2. **Working-order snapshot at target's exchange id**: the
           target row's ``exchange_order_id`` is looked up in
           ``wo_by_deal_id``. If the live order's ``orderLevel``
           matches ``new_level`` → confirmed, the PUT landed even
           though the local persistence was interrupted. If it differs
           → ``still_unknown`` (the engine reconciler will reattempt
           the amend on the next sync rather than declaring rejection
           — the broker's view is authoritative but a delayed PUT
           cannot be ruled out without a confirm).

        3. **No usable signal** (ref TTL expired with no snapshot match,
           target row missing, etc.) → ``still_unknown``. The
           engine-side reconciler retries on next sync.
        """
        extras = row.extras or {}
        new_level_raw = extras.get('new_level')
        target_coid = extras.get('target_coid')
        new_level = _wire_float(new_level_raw, finite=True)

        ref: str | None = (
            refs.get('deal_reference') or extras.get('deal_reference')
        )
        if ref:
            try:
                confirm = await self.plugin.call_api(
                    f'confirms/{ref}', method='get',
                )
            except OrderNotFoundError:
                confirm = None
            if confirm is not None:
                deal_status = (confirm.get('dealStatus') or '').upper()
                if deal_status == 'REJECTED':
                    return ResumeOutcome(
                        status='rejected',
                        reject_reason=_extract_reject_reason(confirm),
                        recovery_path='modify_entry_confirm_rejected',
                        recovery_context={'deal_reference': ref},
                    )
                echoed_level_raw = confirm.get('level')
                echoed_level = _wire_float(echoed_level_raw, finite=True)
                deal_id = confirm.get('dealId')
                if (
                    new_level is not None
                    and echoed_level is not None
                    and abs(echoed_level - new_level) < 1e-9
                ):
                    if self.plugin.store_ctx is not None:
                        _mirror_amended_level(self.plugin.store_ctx, target_coid, echoed_level)
                    return ResumeOutcome(
                        status='confirmed',
                        exchange_id=str(deal_id) if deal_id else None,
                        is_filled=False,
                        filled_qty=0.0,
                        fill_price=0.0,
                        recovery_path='modify_entry_confirm_get',
                        recovery_context={'deal_reference': ref,
                                          'echoed_level': echoed_level},
                    )

        store_ctx = self.plugin.store_ctx
        if store_ctx is None:
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='modify_entry_no_target',
            )
        path, target_deal_id, wo_level = _modify_entry_snapshot_verdict(
            store_ctx, row, self.wo_by_deal_id,
        )
        if path == 'modify_entry_snapshot_match' and wo_level is not None:
            _mirror_amended_level(store_ctx, target_coid, wo_level)
            return ResumeOutcome(
                status='confirmed',
                exchange_id=target_deal_id,
                is_filled=False,
                filled_qty=0.0,
                fill_price=0.0,
                recovery_path=path,
                recovery_context={'echoed_level': wo_level},
            )
        if path == 'modify_entry_snapshot_level_mismatch':
            return ResumeOutcome(
                status='still_unknown',
                recovery_path=path,
                recovery_context={
                    'wo_level': wo_level,
                    'expected_level': new_level,
                },
            )
        return ResumeOutcome(status='still_unknown', recovery_path=path)

    async def _verdict_modify_exit(
            self, row: 'OrderRow', refs: Mapping[str, str],
    ) -> ResumeOutcome:
        """Verdict for a ``modify_exit`` command row.

        The command row carries ``extras['target_coid']`` (the entry
        whose bracket was being amended). Recovery question: did the
        ``PUT /positions/{dealId}`` land? A command not refused by its
        confirm whose legs no snapshot is settling was sent, but neither its
        completion nor the park of its legs committed, so its legs may still
        hold the old bracket: it is parked for the snapshot like the running
        process parks it (``park_unsettled_modify_exit``) and stays
        ``still_unknown`` until the poll settles the legs and the command from
        the venue's levels. Otherwise three resolution paths:

        1. **Stored ``deal_reference`` + confirm GET**: ``REJECTED`` →
           ``rejected``. ``ACCEPTED`` → ``confirmed`` (the leg rows
           seeded by the hook into ``disposition_unknown`` are reconciled
           separately by :meth:`_resolve_bracket_leg_disposition` on the
           next /positions snapshot — recovery does NOT re-run the
           mirror).
        2. **Position snapshot matches attempted target**: if the parent
           position's live ``profitLevel`` / ``stopLevel`` /
           ``trailingStop`` / ``stopDistance`` match the persisted
           ``new_tp`` / ``new_sl`` / ``new_trail`` the PUT landed even
           though local persistence was interrupted → ``confirmed``.
           Pending trailing (``new_trail_price`` + ``new_trail``) is
           treated as a local-only bracket: the snapshot must show
           ``trailingStop=False`` and a cleared ``stopLevel`` to
           match, because the broker carries no native trailing stop
           until the local activation monitor PUTs one.
        3. **No usable signal** (ref TTL expired with no snapshot match,
           target row missing, etc.) → ``still_unknown`` so the engine
           reconciler retries on next sync.

        The journal's command row carries no lifecycle for the leg rows
        themselves — those live outside the entry-row scope for M4 and
        keep their own ``disposition_unknown`` → ``attached`` /
        ``rejected`` path via the snapshot reconciler.
        """
        extras = row.extras or {}
        ref: str | None = (
            refs.get('deal_reference') or extras.get('deal_reference')
        )
        accepted = False
        if ref:
            try:
                confirm = await self.plugin.call_api(
                    f'confirms/{ref}', method='get',
                )
            except OrderNotFoundError:
                confirm = None
            if confirm is not None:
                deal_status = (confirm.get('dealStatus') or '').upper()
                if deal_status == 'REJECTED':
                    return ResumeOutcome(
                        status='rejected',
                        reject_reason=_extract_reject_reason(confirm),
                        recovery_path='modify_exit_confirm_rejected',
                        recovery_context={'deal_reference': ref},
                    )
                accepted = deal_status == 'ACCEPTED'

        if self.plugin.park_unsettled_modify_exit(row, ref):
            # Neither the amend's completion nor the park of its legs
            # committed: the legs may still hold the old bracket. Parked like
            # the running process parks it, the poll settles the legs and the
            # command from the venue's levels.
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='modify_exit_parked_for_snapshot',
                recovery_context={'deal_reference': ref},
            )
        if row.state == STATE_DISPOSITION_UNKNOWN:
            # Parked for the snapshot already: the poll's resolver judges it
            # from the venue's levels, the verdict the running process reaches.
            # An accepted confirm says the venue took the amend once, not that
            # it holds the bracket now.
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='modify_exit_parked_for_snapshot',
                recovery_context={'deal_reference': ref},
            )
        if accepted:
            return ResumeOutcome(
                status='confirmed',
                is_filled=False,
                filled_qty=0.0,
                fill_price=0.0,
                recovery_path='modify_exit_confirm_get',
                recovery_context={'deal_reference': ref},
            )

        store_ctx = self.plugin.store_ctx
        if store_ctx is None:
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='modify_exit_no_target',
            )
        path, target_deal_id, levels = _modify_exit_snapshot_verdict(
            store_ctx, row, self.pos_by_deal_id,
        )
        if path == 'modify_exit_snapshot_match':
            return ResumeOutcome(
                status='confirmed',
                exchange_id=target_deal_id,
                is_filled=False,
                filled_qty=0.0,
                fill_price=0.0,
                recovery_path=path,
                recovery_context=levels,
            )
        if path == 'modify_exit_snapshot_mismatch':
            return ResumeOutcome(
                status='still_unknown',
                recovery_path=path,
                recovery_context=levels,
            )
        return ResumeOutcome(status='still_unknown', recovery_path=path)

    def _verdict_cancel(
            self, row: 'OrderRow', _refs: Mapping[str, str],
    ) -> ResumeOutcome:
        """Verdict for a ``cancel`` command row.

        The cancel command row carries ``extras['target_coids']`` (the
        plugin-side COIDs the per-target loop intended to sweep). For
        each target the verdict checks the broker's snapshots:

        * Target row gone (closed by the hook before crash, or removed
          from the store) → target is considered swept.
        * Target row alive with an ``exchange_order_id`` absent from
          both the working-orders and positions snapshots → broker side
          confirms it vanished; counted as swept.
        * Target row alive with an ``exchange_order_id`` still present
          in either snapshot → still alive; the cancel did NOT land
          for that target → the dispatch verdict is ``still_unknown``
          so the engine reconciler re-issues the cancel on next sync.
        * Bracket-leg targets: lookup is by ``parent_deal_id`` in the
          positions snapshot. Parent gone → bracket gone (swept).

        All targets vanished → ``confirmed`` (the cancel landed even
        though the local persistence was interrupted mid-loop). Any
        target still alive → ``still_unknown``.
        """
        extras = row.extras or {}
        target_coids = list(extras.get('target_coids') or ())
        if self.plugin.store_ctx is None:
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='cancel_no_store',
            )

        if not target_coids:
            # An empty target list was already a ``noop`` at dispatch
            # time (``run_cancel`` would have stamped ``reason_path``
            # right away), so reaching the recovery loop with no
            # targets means the row never finalised — declare it gone.
            return ResumeOutcome(
                status='confirmed',
                recovery_path='cancel_no_targets',
            )

        still_alive: list[str] = []
        swept_targets: list[str] = []
        for target_coid in target_coids:
            target_row = self.plugin.store_ctx.get_order(str(target_coid))
            if target_row is None or target_row.state in (
                    'closed', 'cancelled', 'rejected', 'filled',
            ):
                swept_targets.append(str(target_coid))
                continue

            target_extras = target_row.extras or {}
            leg_kind = target_extras.get('leg_kind')

            if leg_kind in ('tp', 'sl'):
                parent_deal_id = target_extras.get('parent_deal_id')
                if not parent_deal_id:
                    still_alive.append(str(target_coid))
                    continue
                if str(parent_deal_id) in self.pos_by_deal_id:
                    still_alive.append(str(target_coid))
                else:
                    swept_targets.append(str(target_coid))
                continue

            deal_id = target_row.exchange_order_id
            if not deal_id:
                # Target never received an exchange id; the hook would
                # have stamped ``cancel_pending`` rather than DELETE.
                # Without an id we cannot verify against the snapshot —
                # treat it as still alive so the engine retries.
                still_alive.append(str(target_coid))
                continue

            if (str(deal_id) in self.wo_by_deal_id
                    or str(deal_id) in self.pos_by_deal_id):
                still_alive.append(str(target_coid))
            else:
                swept_targets.append(str(target_coid))

        if still_alive:
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='cancel_targets_partial',
                recovery_context={
                    'still_alive_coids': still_alive,
                    'swept_target_coids': swept_targets,
                },
            )
        return ResumeOutcome(
            status='confirmed',
            recovery_path='cancel_targets_vanished',
            recovery_context={'applied_target_coids': swept_targets},
        )

    def _verdict_full_close(
            self, row: 'OrderRow', _refs: Mapping[str, str],
    ) -> ResumeOutcome:
        """Verdict for a ``full_close`` command row.

        The command row carries ``extras['targets']`` (the exchange
        ``dealId`` list the DELETE chain was meant to sweep). For each
        target the verdict consults the live ``positions`` snapshot:

        * Target present in the snapshot → the DELETE never landed for
          that target → ``still_unknown``; the engine reconciler will
          re-issue the close on the next sync.
        * Target absent from the snapshot → swept; the DELETE landed
          even though local persistence was interrupted.

        All targets absent → ``confirmed``; the journal then promotes
        the command row to ``closing`` (matching the live dispatch end
        state) and the activity stream takes over from there. Any target
        still alive → ``still_unknown``. ``extras['targets']`` empty
        → also ``confirmed`` because the dispatch had nothing to do.

        The hook also mirrors the row of each swept target to ``closing``
        (also while another target is still listed) so the activity stream's
        subsequent ``CLOSED`` event matches a row in the same state the live
        dispatch would have left behind. The judgement and the mirror are
        :func:`~pynecore_capitalcom.helpers._settle_vanished_full_close_targets`,
        which the poll's resolver of a parked close runs as well.
        """
        extras = row.extras or {}
        target_deal_ids = list(extras.get('targets') or ())
        if self.plugin.store_ctx is None:
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='full_close_no_store',
            )

        if not target_deal_ids:
            return ResumeOutcome(
                status='confirmed',
                recovery_path='full_close_no_targets',
                recovery_context={'applied_targets': []},
            )

        still_alive, swept, mirrored = _settle_vanished_full_close_targets(
            self.plugin.store_ctx, row, self.pos_by_deal_id,
            path='full_close_targets_vanished',
        )
        if still_alive:
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='full_close_targets_partial',
                recovery_context={
                    'still_alive_targets': still_alive,
                    'swept_targets': swept,
                },
            )
        return ResumeOutcome(
            status='confirmed',
            recovery_path='full_close_targets_vanished',
            recovery_context={
                'applied_targets': swept,
                'mirrored_target_coids': mirrored,
            },
        )

    async def _verdict_partial_close(
            self, row: 'OrderRow', refs: Mapping[str, str],
    ) -> ResumeOutcome:
        """Verdict for a ``partial_close_emulated`` command row.

        Partial close is the emulated opposite-direction POST. A rejected
        confirm remains a normal reject. Every other path is confirmed only
        when the current authoritative snapshot proves the persisted exact
        target ``dealId`` in its original direction at
        ``pre_target_units - intent_units``. Aggregate symbol quantity is not
        ownership evidence: the target can disappear while an unrelated reverse
        or replacement row preserves the same total.

        Missing/expired confirms use the same exact-target verdict. Recovery
        never repeats the opposite POST because a duplicate could open a reverse
        position if the first request already netted.
        """
        extras = row.extras or {}
        ref: str | None = (
            refs.get('deal_reference') or extras.get('deal_reference')
        )
        if not ref:
            return await self._verdict_partial_close_by_target(
                row,
                ref=None,
                confirm=None,
                fallback_path='partial_close_missing_deal_reference',
            )
        try:
            confirm = await self.plugin.call_api(
                f'confirms/{ref}', method='get',
            )
        except OrderNotFoundError:
            return await self._verdict_partial_close_by_target(
                row,
                ref=ref,
                confirm=None,
                fallback_path='partial_close_confirm_ttl_expired',
            )

        deal_status = (confirm.get('dealStatus') or '').upper()
        if deal_status == 'REJECTED':
            return ResumeOutcome(
                status='rejected',
                reject_reason=_extract_reject_reason(confirm),
                recovery_path='partial_close_confirm_rejected',
                recovery_context={'deal_reference': ref},
            )

        return await self._verdict_partial_close_by_target(
            row,
            ref=ref,
            confirm=confirm,
            fallback_path='partial_close_units_mismatch',
        )

    async def _verdict_partial_close_by_target(
            self,
            row: 'OrderRow',
            *,
            ref: str | None,
            confirm: dict | None,
            fallback_path: str,
    ) -> ResumeOutcome:
        """Resolve a partial close from persisted exact-target provenance."""
        extras = row.extras or {}
        target_deal_id = _wire_id(extras.get('target_deal_id'))
        target_direction = str(extras.get('target_direction') or '').upper()
        pre_units = _wire_int(extras.get('pre_target_units'))
        close_units = _wire_int(extras.get('intent_units'))
        recovery_context: dict = {}
        if ref is not None:
            recovery_context['deal_reference'] = ref
        if (target_deal_id is None
                or target_direction not in {'BUY', 'SELL'}
                or pre_units is None
                or close_units is None):
            return ResumeOutcome(
                status='still_unknown',
                recovery_path=(
                    'partial_close_missing_target_context'
                    if confirm is not None else fallback_path
                ),
                recovery_context=recovery_context or None,
            )

        try:
            rules = await self.plugin.instrument_rules(row.symbol)
        except Exception as fetch_err:
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='partial_close_rules_fetch_failed',
                recovery_context={
                    **recovery_context,
                    'error': str(fetch_err),
                },
            )
        if rules.lot_step <= 0:
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='partial_close_no_rules',
                recovery_context=recovery_context or None,
            )

        expected_units = pre_units - close_units
        target_raw = self.pos_by_deal_id.get(target_deal_id)
        target = (target_raw or {}).get('position') or {}
        current_direction = (target.get('direction') or '').upper() or None
        current_units = (
            round(float(target.get('size', 0.0)) / rules.lot_step)
            if target_raw is not None else None
        )
        opposite_direction = 'SELL' if target_direction == 'BUY' else 'BUY'
        opposite_deal_ids = [
            str(position.get('dealId'))
            for raw in self.pos_by_deal_id.values()
            if (raw.get('market') or {}).get('epic') == row.symbol
            for position in [raw.get('position') or {}]
            if (position.get('direction') or '').upper() == opposite_direction
            and position.get('dealId')
        ]
        corrected_reverse_id = _wire_id(
            extras.get('corrected_reverse_deal_id')
        )
        corrected_reverse_present = (
            corrected_reverse_id in self.pos_by_deal_id
            if corrected_reverse_id is not None else False
        )
        match_context = {
            **recovery_context,
            'target_deal_id': target_deal_id,
            'target_direction': current_direction,
            'target_units': current_units,
            'expected_units': expected_units,
            'opposite_deal_ids': opposite_deal_ids,
            'corrected_reverse_deal_id': corrected_reverse_id,
            'corrected_reverse_present': corrected_reverse_present,
        }
        exact_match = (
            current_direction == target_direction
            and current_units == expected_units
            and not opposite_deal_ids
            and corrected_reverse_id is None
        )
        if exact_match:
            if confirm is not None:
                path = 'partial_close_target_match'
            elif ref is not None:
                path = 'partial_close_target_match_ttl_expired'
            else:
                path = 'partial_close_target_match_no_ref'
            # The dispatch never returned, so the target's exposure cursor
            # still holds the closed size; lowered once, before the journal
            # closes the command.
            store_ctx = self.plugin.store_ctx
            if store_ctx is not None:
                _retire_parked_partial_close_exposure(
                    store_ctx, row, close_units, rules.lot_step, path=path,
                )
            confirm_deal_id = _wire_id((confirm or {}).get('dealId'))
            return ResumeOutcome(
                status='confirmed',
                exchange_id=confirm_deal_id,
                recovery_path=path,
                recovery_context=match_context,
            )
        if (corrected_reverse_id is not None
                and not corrected_reverse_present
                and current_direction == target_direction
                and current_units == pre_units
                and not opposite_deal_ids):
            return ResumeOutcome(
                status='rejected',
                reject_reason='corrective reverse leg removed; target unchanged',
                recovery_path='partial_close_correction_target_unchanged',
                recovery_context=match_context,
            )
        return ResumeOutcome(
            status='still_unknown',
            recovery_path=fallback_path,
            recovery_context=match_context,
        )
