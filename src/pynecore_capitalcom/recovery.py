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
  resolve yet: the same activity matching, against a fresh activity read.

Bracket leg ``disposition_unknown`` rows are NOT handled here — they
are owned by ``_reconcile_snapshot``'s ``_resolve_bracket_leg_disposition``
since the resolution depends on the parent position's live state.

State touched: BrokerStore through ``self.store_ctx``,
``_activity_cursor`` (rebuilt).
"""
import logging
import math
from abc import ABC
from collections.abc import Mapping
from dataclasses import dataclass
from time import time as epoch_time
from typing import TYPE_CHECKING, cast

import httpx

from pynecore.core.broker.exceptions import (
    BrokerManualInterventionError,
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
from .helpers import (
    CLOSE_VERDICT_FOREIGN,
    CLOSE_VERDICT_VENUE,
    EXTERNAL_REDUCTION_VERDICTS,
    EXTRAS_KEY_CANCEL_REQUESTED,
    EXTRAS_KEY_NETTED_RESIDUAL_PENDING,
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
    from pynecore.core.broker.storage import OrderRow

logger = logging.getLogger(__name__)

#: Largest distance between a pending entry row's creation (taken just before
#: its POST) and the activity of the deal the POST opened, for the activity to
#: count as that POST's trace.
_ACTIVITY_MATCH_WINDOW_S = 3.0

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
        # tracker would raise ``UnexpectedCancelError`` → halt the bot
        # on a clean restart, even though there is nothing to recover.
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
        back from now past the row's creation by :data:`_ACTIVITY_LAG_S`, and
        is matched like the startup pass matches
        (:meth:`_CapitalComResumeHooks.inherited_submission_verdict`).

        A row older than the venue's longest activity read cannot be proven
        either way and stays held; so does a row whose trace another pending
        submission matches as well. Each is logged once per row. A row stamped
        :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_TAKE_OVER_UNPROVEN` was
        already traced to its deal: a later read without the trace is never
        an absence for it.

        :param row: The pending entry row.
        :return: :attr:`~ParkedOutcome.LANDED`
            with the deal id, ``ABSENT``, or ``INCONCLUSIVE`` for a row that
            is not an inherited entry submission, a failed read or an
            ambiguous match.
        """
        store_ctx = self.store_ctx
        if (store_ctx is None
                or not store_ctx.is_inherited_dispatch(row.client_order_id)
                or (row.extras or {}).get('kind') not in (ENTRY_KIND_POSITION,
                                                          ENTRY_KIND_WORKING)):
            return ParkedVerdict(ParkedOutcome.INCONCLUSIVE)
        age_s = max(0.0, epoch_time() - row.created_ts_ms / 1000.0)
        period = math.ceil(age_s) + _ACTIVITY_LAG_S
        if period > _ACTIVITY_MAX_PERIOD_S:
            if row.client_order_id not in self._inherited_unprovable_reported:
                self._inherited_unprovable_reported.add(row.client_order_id)
                broker_warning(
                    "inherited entry submission %s (intent %s) is older than the "
                    "venue's activity history (%d s): whether its POST opened a "
                    "deal cannot be read any more, so its entry stays held — "
                    "check the account and close the row by hand",
                    row.client_order_id, row.intent_key, _ACTIVITY_MAX_PERIOD_S,
                )
            return ParkedVerdict(ParkedOutcome.INCONCLUSIVE)
        try:
            activity_resp = await self._call(
                'history/activity',
                data={'lastPeriod': period, 'detailed': 'true'},
                method='get',
            )
        except (httpx.HTTPError, ConnectionError, ExchangeConnectionError,
                ExchangeRateLimitError, OrderNotFoundError, CapitalComError):
            return ParkedVerdict(ParkedOutcome.INCONCLUSIVE)
        activities = activity_resp.get('activities')
        if not isinstance(activities, list):
            return ParkedVerdict(ParkedOutcome.INCONCLUSIVE)
        hooks = _CapitalComResumeHooks(
            plugin=self, activities=activities, pos_by_ref={}, wo_by_ref={},
            wo_by_deal_id={}, pos_by_deal_id={},
        )
        verdict, ambiguous = hooks.inherited_submission_verdict(row)
        if ambiguous and row.client_order_id not in self._inherited_ambiguity_reported:
            self._inherited_ambiguity_reported.add(row.client_order_id)
            broker_warning(
                "inherited entry submission %s (intent %s) cannot be told apart from "
                "another pending submission by the venue's activity history: which "
                "of them opened the deal is unknown, so its entry stays held — check "
                "the account and close the row by hand",
                row.client_order_id, row.intent_key,
            )
        if (verdict.outcome is ParkedOutcome.ABSENT
                and (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None):
            # An earlier lookup traced this POST to its deal: a read that no
            # longer shows the trace (the deal claimed by another row, a read
            # cut short) does not undo that, and a rejection would send the
            # entry a second time.
            return ParkedVerdict(ParkedOutcome.INCONCLUSIVE)
        return verdict

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
        is never released for a second send. The operator is warned with a
        ``take_over_unproven_held_at_recovery`` audit event, once per row for
        the process.

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
            "it — check the account and settle the row by hand",
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
           raise :class:`UnexpectedCancelError`, halting a bot that has
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
        :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_TAKE_OVER_UNPROVEN` is
        never retired: it stays live and held.

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
                if extras.get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None:
                    # A held take-over: its order is gone because it executed,
                    # and retiring the row would release the entry for a second
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

    The ``submission_ambiguous`` branch is a deliberate side-channel:
    when the activity heuristic returns multiple candidates the hook
    writes ``state='submission_ambiguous'`` directly and raises
    :class:`BrokerManualInterventionError`. The journal does not catch
    the exception (intentional — operator intervention is required),
    so the propagation matches the legacy recovery semantics. A
    follow-up Core change is planned to canonicalise this status; M3
    keeps the current behaviour.
    """
    plugin: CapitalComHookCollaborator
    activities: list[dict]
    pos_by_ref: dict[str, dict]
    wo_by_ref: dict[str, dict]
    wo_by_deal_id: dict[str, dict]
    pos_by_deal_id: dict[str, dict]

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
            window_s: float | None = _ACTIVITY_MATCH_WINDOW_S,
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
        another journal row already tracks are not candidates.

        :param activity: One ``/history/activity`` row.
        :param row: The pending entry row.
        :param window_s: Largest distance between the activity's time and the
            row's creation; ``None`` matches at any time.
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
        if window_s is not None and row_created and abs(when - row_created) > window_s:
            return False
        deal_id = _wire_id(activity.get('dealId'))
        store_ctx = self.plugin.store_ctx
        if deal_id is not None and store_ctx is not None:
            tracked = store_ctx.find_by_ref('deal_id', deal_id)
            if tracked is not None and tracked.client_order_id != row.client_order_id:
                return False
        return True

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
        store_ctx = self.plugin.store_ctx
        if store_ctx is None:
            return False
        for other in find_pending_dispatch(store_ctx):
            if (other.client_order_id == row.client_order_id
                    or other.exchange_order_id
                    or (other.extras or {}).get('kind') not in (ENTRY_KIND_POSITION,
                                                                ENTRY_KIND_WORKING)):
                continue
            if self._activity_matches_row(activity, other):
                return True
        return False

    def inherited_submission_verdict(self, row: 'OrderRow') -> tuple[ParkedVerdict, bool]:
        """Judge an inherited entry submission from a complete activity read.

        * One deal traced inside :data:`_ACTIVITY_MATCH_WINDOW_S` of the row's
          creation, which no other pending submission could have left
          (:meth:`_trace_contested`): the POST landed there.
        * No trace at all: the POST opened nothing.
        * Anything else is inconclusive: several traced deals, a trace another
          pending submission matches too, a trace without a deal id, or a
          matching activity outside the window, which may be the POST's own
          trace under a venue clock that runs off the local one.

        :param row: The pending entry row; :attr:`activities` must cover its
            whole life.
        :return: The verdict, with the deal id when it landed, and whether the
            trace is ambiguous: several deals, or one another pending
            submission matches too. An ambiguous row stays held until the
            operator settles it.
        """
        traced: set[str | None] = set()
        contested = False
        outside_window = False
        for activity in self.activities:
            if self._activity_matches_row(activity, row):
                traced.add(_wire_id(activity.get('dealId')))
                contested = contested or self._trace_contested(activity, row)
            elif self._activity_matches_row(activity, row, window_s=None):
                outside_window = True
        ambiguous = contested or len(traced) > 1
        if len(traced) == 1 and not contested:
            deal_id = next(iter(traced))
            if deal_id is not None:
                return ParkedVerdict(ParkedOutcome.LANDED, order_id=deal_id), False
            return ParkedVerdict(ParkedOutcome.INCONCLUSIVE), False
        if traced or outside_window:
            return ParkedVerdict(ParkedOutcome.INCONCLUSIVE), ambiguous
        return ParkedVerdict(ParkedOutcome.ABSENT), False

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
           multiple matches → ambiguous side-channel (see class
           docstring); zero matches → ``still_unknown`` with
           ``recovery_path='no_match'``.
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
            # Another pending submission matches the same trace: which one
            # landed is unknown, so neither takes the deal. The row stays
            # pending and the in-run lookup keeps it held.
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

        # Multiple matches — cannot auto-resolve. Side-channel: write
        # the ambiguous state directly and raise. The journal does not
        # catch BrokerManualInterventionError, so the halt propagates
        # to the connect-flow caller — matching the legacy behaviour.
        assert self.plugin.store_ctx is not None
        self.plugin.store_ctx.set_order_state(
            row.client_order_id, 'submission_ambiguous',
        )
        raise BrokerManualInterventionError(
            f"Recovery ambiguous: {len(candidates)} activity matches for "
            f"coid={row.client_order_id!r}",
            intent_key=row.intent_key,
            context={'symbol': row.symbol, 'qty': row.qty, 'side': row.side,
                     'candidate_deal_ids': [c.get('dealId') for c in candidates]},
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
        return ResumeOutcome(
            status='rejected',
            reject_reason=(
                'accepted deal no longer present in any account snapshot '
                '(stale confirm for a dead deal)'
            ),
            recovery_path='confirm_get_stale_deal',
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
