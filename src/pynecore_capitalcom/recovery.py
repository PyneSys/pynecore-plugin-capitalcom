"""Restart recovery — runs once at ``connect()`` before the WS loop opens.

Capital.com's idempotency capability is SOFTWARE: the server generates
``dealReference`` from each POST, so the plugin must keep its own crash-
safe ledger of which dispatches reached the exchange and which rolled
back. This mix-in walks the BrokerStore at startup and resolves every
non-terminal row:

* ``_load_activity_cursor_from_events`` — replays the
  ``activity_processed`` events from the last 24h to rebuild
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

Bracket leg ``disposition_unknown`` rows are NOT handled here — they
are owned by ``_reconcile_snapshot``'s ``_resolve_bracket_leg_disposition``
since the resolution depends on the parent position's live state.

State touched: BrokerStore through ``self.store_ctx``,
``_activity_cursor`` (rebuilt).
"""
from collections.abc import Mapping
from dataclasses import dataclass
from time import time as epoch_time
from typing import TYPE_CHECKING

from pynecore.core.broker.exceptions import BrokerManualInterventionError
from pynecore.core.broker.journal import (
    DispatchJournal,
    ResumeOutcome,
)
from pynecore.core.broker.store_helpers import KIND_MODIFY_ENTRY
from pynecore.lib.log import broker_info

from ._base import _CapitalComBase
from .exceptions import OrderNotFoundError
from .helpers import _parse_iso_timestamp

if TYPE_CHECKING:
    from pynecore.core.broker.models import EntryIntent
    from pynecore.core.broker.storage import OrderRow


class _RecoveryMixin(_CapitalComBase):
    """Restart recovery mix-in — replays cursor + in-flight rows at connect."""

    async def _load_activity_cursor_from_events(self) -> None:
        """Rebuild :attr:`_activity_cursor` from persisted ``activity_processed``
        events written during previous process incarnations.

        Capital.com's ``/history/activity`` returns a rolling window;
        without cross-restart fingerprint awareness, a restart within
        the window would re-emit every activity row as a duplicate
        event. The plugin side-steps the issue by writing the
        fingerprint to the BrokerStore on every processed row and
        reloading the last 24 h on connect.

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
        """
        if self.store_ctx is None:
            return
        cutoff_ms = int((epoch_time() - 86400.0) * 1000)
        cursor = self._activity_cursor

        processed_payloads = list(self.store_ctx.iter_events_by_kind_since(
            'activity_processed', cutoff_ms,
        ))
        deferred_payloads = list(self.store_ctx.iter_events_by_kind_since(
            'activity_close_deferred_no_price', cutoff_ms,
        ))

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
            if fp:
                cursor.seen_fingerprints.add(fp)
            date_utc: str | None = payload.get('dateUTC')
            if not date_utc:
                continue
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
        positions_resp = await self._call('positions', method='get')
        working_resp = await self._call('workingorders', method='get')
        activity_resp = await self._call(
            'history/activity',
            data={'lastPeriod': 86400, 'detailed': 'true'},
            method='get',
        )

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

        resume_hooks = _CapitalComResumeHooks(
            plugin=self,
            activities=activities,
            pos_by_ref=pos_by_ref,
            wo_by_ref=wo_by_ref,
            wo_by_deal_id=wo_by_deal_id,
        )

        def hooks_for(row: 'OrderRow') -> '_CapitalComResumeHooks | None':
            # Bracket legs (``leg_kind in {'tp','sl'}``) are owned by
            # ``_resolve_bracket_leg_disposition``; everything else is
            # an entry row and goes through the journal. Filtering on
            # ``leg_kind`` (negative) rather than ``kind`` (positive)
            # matches the pre-M3 outer-loop behaviour: every non-bracket
            # row is recovered regardless of whether ``extras['kind']``
            # was explicitly stamped (older rows / test fixtures may
            # omit it).
            leg_kind = (row.extras or {}).get('leg_kind')
            if leg_kind in ('tp', 'sl'):
                return None  # bracket legs — own resolution path
            return resume_hooks

        journal = DispatchJournal(self.store_ctx)
        resolutions = await journal.recover_pending(hooks_for)

        # The ``pos_by_deal_id`` / ``wo_by_deal_id`` maps are snapshots
        # from *before* promotions; rows resolved via activity-heuristic
        # or the direct ``/confirms/{ref}`` call land a ``deal_id`` that
        # is not yet in those maps, so the subsequent orphan retirement
        # would otherwise close a freshly recovered live order.
        promoted_coids: set[str] = {
            r.coid for r in resolutions if r.status == 'confirmed'
        }

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
        )

    def _retire_startup_orphans(
            self,
            pos_by_deal_id: dict[str, dict],
            wo_by_deal_id: dict[str, dict],
            promoted_coids: set[str] | None = None,
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
           nothing left to recover.
        2. Bracket leg rows (``leg_kind in {'tp','sl'}``): Capital.com
           brackets are position attributes with no own ``dealId``,
           so the parent's state is the only handle. Retire the leg
           if its parent ``client_order_id`` was retired in pass 1, OR
           if ``parent_deal_id`` is set and missing from the positions
           snapshot.

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
        retired_parent_coids: set[str] = set()
        retired_count = 0

        for row in list(self.store_ctx.iter_live_orders()):
            extras = row.extras or {}
            if extras.get('leg_kind') in ('tp', 'sl'):
                continue
            if row.client_order_id in promoted:
                continue
            did = row.exchange_order_id
            if not did:
                continue
            if row.state not in ('confirmed', 'closing', 'rejected'):
                continue
            if did in pos_by_deal_id or did in wo_by_deal_id:
                continue
            self.store_ctx.log_event(
                'startup_orphan_retired',
                client_order_id=row.client_order_id,
                exchange_order_id=did,
                payload={'state': row.state, 'leg_kind': None},
            )
            self.store_ctx.close_order(row.client_order_id)
            retired_parent_coids.add(row.client_order_id)
            retired_count += 1

        for row in list(self.store_ctx.iter_live_orders()):
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
            retired_count += 1

        if retired_count:
            broker_info(
                "startup: retired %d orphan order row(s) — no matching "
                "position/working order on exchange",
                retired_count,
            )


# === Recovery hook =========================================================

@dataclass
class _CapitalComResumeHooks:
    """Recovery-time hook implementation for the entry journal.

    Constructed once per ``_recover_in_flight_submissions`` pass; the
    three REST snapshots are captured as attributes so each verdict
    call resolves locally without re-fetching. The journal's
    :meth:`pynecore.core.broker.journal.DispatchJournal.recover_pending`
    calls :meth:`resume_pending_dispatch` once per pending row; the
    other :class:`pynecore.core.broker.journal.EntryDispatchHooks`
    Protocol methods (``submit``, ``confirm_submission``,
    ``exchange_order_from_state``) defensively raise — they exist only
    to satisfy the Protocol and are never invoked on the recovery
    code path.

    The ``submission_ambiguous`` branch is a deliberate side-channel:
    when the activity heuristic returns multiple candidates the hook
    writes ``state='submission_ambiguous'`` directly and raises
    :class:`BrokerManualInterventionError`. The journal does not catch
    the exception (intentional — operator intervention is required),
    so the propagation matches the legacy recovery semantics. A
    follow-up Core change is planned to canonicalise this status; M3
    keeps the current behaviour.
    """
    plugin: _RecoveryMixin
    activities: list[dict]
    pos_by_ref: dict[str, dict]
    wo_by_ref: dict[str, dict]
    wo_by_deal_id: dict[str, dict]

    async def submit(
            self, *, coid: str, intent: 'EntryIntent', qty: float,
    ):  # pragma: no cover — Protocol-only
        raise RuntimeError(
            "_CapitalComResumeHooks.submit is not callable: recovery does "
            "not dispatch fresh submissions"
        )

    async def confirm_submission(
            self, *, coid: str, intent: 'EntryIntent', server_ref: str,
    ):  # pragma: no cover — Protocol-only
        raise RuntimeError(
            "_CapitalComResumeHooks.confirm_submission is not callable: "
            "recovery resolves via snapshots / /confirms GET inside "
            "_verdict_server_ref_seen, not the dispatch confirm path"
        )

    def exchange_order_from_state(
            self, *, row: 'OrderRow', intent: 'EntryIntent',
    ):  # pragma: no cover — Protocol-only
        raise RuntimeError(
            "_CapitalComResumeHooks.exchange_order_from_state is not "
            "callable: recovery does not synthesise ExchangeOrders"
        )

    async def resume_pending_dispatch(
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

    def _verdict_submitted(
            self, row: 'OrderRow', refs: Mapping[str, str],
    ) -> ResumeOutcome:
        """Verdict for a ``submitted`` / ``disposition_unknown`` row.

        Multi-factor confidence:

        1. Stored ``deal_reference`` (preferred from ``refs``, falling
           back to ``extras``) matches a positions / working snapshot →
           ``recovery_path='stored_ref'``, confirmed.
        2. Activity heuristic (epic + direction + size + ±3 s) →
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
                return ResumeOutcome(
                    status='confirmed',
                    exchange_id=str(deal_id) if deal_id else None,
                    is_filled=False,
                    filled_qty=0.0,
                    fill_price=0.0,
                    recovery_path='stored_ref',
                    recovery_context={
                        'deal_reference': stored_ref,
                        'matched_snapshot': matched_snapshot,
                    },
                )

        # Activity heuristic: epic + direction + size + ±3 s createdDateUTC.
        row_side = 'BUY' if row.side == 'buy' else 'SELL'
        row_created = row.created_ts_ms / 1000.0 if row.created_ts_ms else 0.0
        candidates: list[dict] = []
        for a in self.activities:
            if (a.get('epic') or '') != row.symbol:
                continue
            if (a.get('direction') or '').upper() != row_side:
                continue
            if abs(float(a.get('size') or 0.0) - row.qty) > 1e-9:
                continue
            when = _parse_iso_timestamp(a.get('dateUTC') or '')
            if row_created and abs(when - row_created) > 3.0:
                continue
            candidates.append(a)

        if not candidates:
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='no_match',
            )
        if len(candidates) == 1:
            deal_id = candidates[0].get('dealId')
            return ResumeOutcome(
                status='confirmed',
                exchange_id=str(deal_id) if deal_id else None,
                is_filled=False,
                filled_qty=0.0,
                fill_price=0.0,
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

        Direct ``/confirms/{ref}`` GET first
        (``recovery_path='confirm_get_direct'``); on
        :class:`OrderNotFoundError` (TTL expiry) the verdict falls back
        to the snapshot map (``recovery_path='ttl_fallback_snapshot'``).
        Without a usable ``deal_id`` the verdict is ``still_unknown``.

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
            confirm = await self.plugin._call(
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
            return ResumeOutcome(
                status='confirmed',
                exchange_id=str(deal_id),
                is_filled=False,
                filled_qty=0.0,
                fill_price=0.0,
                recovery_path='ttl_fallback_snapshot',
                recovery_context={
                    'deal_reference': ref,
                    'matched_snapshot': matched_snapshot,
                },
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
        return ResumeOutcome(
            status='confirmed',
            exchange_id=str(deal_id),
            is_filled=False,
            filled_qty=0.0,
            fill_price=0.0,
            recovery_path='confirm_get_direct',
            recovery_context={'deal_reference': ref},
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
        try:
            new_level = float(new_level_raw) if new_level_raw is not None else None
        except (TypeError, ValueError):
            new_level = None

        ref: str | None = (
            refs.get('deal_reference') or extras.get('deal_reference')
        )
        if ref:
            try:
                confirm = await self.plugin._call(
                    f'confirms/{ref}', method='get',
                )
            except OrderNotFoundError:
                confirm = None
            if confirm is not None:
                deal_status = (confirm.get('dealStatus') or '').upper()
                if deal_status == 'REJECTED':
                    return ResumeOutcome(
                        status='rejected',
                        reject_reason=confirm.get('reason') or 'unknown',
                        recovery_path='modify_entry_confirm_rejected',
                        recovery_context={'deal_reference': ref},
                    )
                echoed_level_raw = confirm.get('level')
                echoed_level = (
                    float(echoed_level_raw)
                    if echoed_level_raw is not None else None
                )
                deal_id = confirm.get('dealId')
                if (
                    new_level is not None
                    and echoed_level is not None
                    and abs(echoed_level - new_level) < 1e-9
                ):
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

        target_deal_id: str | None = None
        if target_coid and self.plugin.store_ctx is not None:
            target_row = self.plugin.store_ctx.get_order(str(target_coid))
            if target_row is not None and target_row.exchange_order_id:
                target_deal_id = str(target_row.exchange_order_id)

        if target_deal_id is None:
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='modify_entry_no_target',
            )

        wo = self.wo_by_deal_id.get(target_deal_id)
        if wo is None:
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='modify_entry_target_vanished',
            )
        wo_data = wo.get('workingOrderData') or {}
        wo_level_raw = wo_data.get('orderLevel')
        try:
            wo_level = float(wo_level_raw) if wo_level_raw is not None else None
        except (TypeError, ValueError):
            wo_level = None

        if (
            new_level is not None
            and wo_level is not None
            and abs(wo_level - new_level) < 1e-9
        ):
            return ResumeOutcome(
                status='confirmed',
                exchange_id=target_deal_id,
                is_filled=False,
                filled_qty=0.0,
                fill_price=0.0,
                recovery_path='modify_entry_snapshot_match',
                recovery_context={'echoed_level': wo_level},
            )

        return ResumeOutcome(
            status='still_unknown',
            recovery_path='modify_entry_snapshot_level_mismatch',
            recovery_context={
                'wo_level': wo_level,
                'expected_level': new_level,
            },
        )
