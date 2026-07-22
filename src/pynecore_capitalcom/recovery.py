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
from pynecore.types.strategy import ADOPTED_STARTUP_EXTRA_KEY
from pynecore.core.broker.journal import (
    DispatchJournal,
    ResumeOutcome,
)
from pynecore.core.broker.store_helpers import (
    KIND_CANCEL,
    KIND_FULL_CLOSE,
    KIND_MODIFY_ENTRY,
    KIND_MODIFY_EXIT,
    KIND_PARTIAL_CLOSE,
)
from pynecore.lib.log import broker_info

from ._base import _CapitalComBase
from .exceptions import OrderNotFoundError
from .helpers import _extract_reject_reason, _parse_iso_timestamp

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
            date_utc: str | None = payload.get('dateUTC')
            if fp:
                # A missing dateUTC replays as '' — such an entry sorts
                # below any real watermark and the first poll prunes it,
                # matching the poll-path guard that would skip its row
                # before the fingerprint is consulted anyway.
                cursor.seen_fingerprints[fp] = date_utc or ''
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
            pos_by_deal_id=pos_by_deal_id,
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

        # Symmetric counterpart to orphan retirement: adopt live exchange
        # positions the BrokerStore is NOT tracking into confirmed
        # per-leg ``position`` rows. A fresh process (or a recovery run
        # under a different ``run_id``) that finds an existing venue
        # position has no store row for it — the engine's startup
        # reconcile adopts the net size into ``_position``, but
        # ``execute_close`` / ``execute_exit`` derive their DELETE targets
        # from confirmed ``position`` rows, so a subsequent
        # ``strategy.close_all()`` would raise "no confirmed position
        # rows" and strand the adopted exposure. Reconcile each untracked
        # leg into a confirmed row so the normal close/exit paths can
        # flatten it.
        self._adopt_untracked_positions(positions_resp)

    def _adopt_untracked_positions(self, positions_resp: Mapping) -> None:
        """Seed confirmed ``position`` rows for untracked live venue legs.

        Netting-account only: on a hedging account the core one-way
        emulator closes per leg via ``close_leg`` reading
        :meth:`fetch_raw_positions` directly, so no store row is needed
        and seeding would double-count against the emulator's virtual
        FIFO. On a one-way account the DELETE-per-``dealId`` close path
        needs a confirmed ``position`` row per live leg.

        Idempotent: a leg already tracked by a live store row (matched by
        ``exchange_order_id`` or a ``deal_id`` ref — e.g. an entry row
        adopted across a same-``run_id`` restart, or a row just promoted
        by the in-flight recovery pass) is skipped, so no duplicate row is
        created. Rows are seeded ``filled_qty == qty`` with
        ``kind='position'`` + ``entry_filled_at`` so activity
        classification treats the eventual close leg as a reduction, and a
        ``deal_id`` ref mirrors the normal entry path so the activity loop
        can route the close-leg activity back to the row.
        """
        store_ctx = self.store_ctx
        if store_ctx is None or self._hedging_enabled:
            return

        tracked_deal_ids: set[str] = set()
        for row in store_ctx.iter_live_orders():
            if row.exchange_order_id:
                tracked_deal_ids.add(str(row.exchange_order_id))
        foreign_deal_ids = store_ctx.foreign_live_exchange_order_ids(
            symbol=self.symbol,
        )

        adopted_count = 0
        for raw in positions_resp.get('positions') or []:
            market = raw.get('market') or {}
            symbol = market.get('epic')
            position = raw.get('position') or {}
            direction = (position.get('direction') or '').upper()
            deal_id = position.get('dealId')
            size = float(position.get('size', 0.0))
            if (not symbol or not deal_id
                    or direction not in ('BUY', 'SELL') or size <= 0.0):
                continue
            deal_id = str(deal_id)
            if deal_id in tracked_deal_ids:
                continue
            if deal_id in foreign_deal_ids:
                continue
            if store_ctx.find_by_ref('deal_id', deal_id) is not None:
                continue
            synthetic_coid = f"__pyne_adopted__{symbol}__{deal_id}"
            store_ctx.upsert_order(
                synthetic_coid,
                symbol=symbol,
                side='buy' if direction == 'BUY' else 'sell',
                qty=size,
                filled_qty=size,
                state='confirmed',
                exchange_order_id=deal_id,
                extras={
                    'kind': 'position',
                    'entry_filled_at': epoch_time(),
                    ADOPTED_STARTUP_EXTRA_KEY: True,
                },
            )
            store_ctx.add_ref(synthetic_coid, 'deal_id', deal_id)
            store_ctx.log_event(
                'startup_position_adopted',
                client_order_id=synthetic_coid,
                exchange_order_id=deal_id,
                payload={
                    'symbol': symbol,
                    'direction': direction,
                    'size': size,
                },
            )
            tracked_deal_ids.add(deal_id)
            adopted_count += 1

        if adopted_count:
            broker_info(
                "startup: adopted %d untracked exchange position leg(s) "
                "into confirmed store rows for close/exit routing",
                adopted_count,
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
        retired_intent_keys: set[str] = set()
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
            # The envelope anchor in the ``envelopes`` table survives
            # ``close_order``; without this delete the next dispatch of
            # the same Pine intent reuses the stale ``bar_ts_ms`` and
            # regenerates the SAME client_order_id, hitting the
            # ``upsert_order`` UPDATE path on the just-closed row — and
            # since UPDATE does not touch ``closed_ts_ms``, the freshly
            # confirmed entry is invisible to ``iter_live_orders``,
            # which makes the immediately-following ``execute_exit``
            # raise ``no confirmed entry row``.
            if row.intent_key:
                self.store_ctx.record_complete(row.intent_key)
                retired_intent_keys.add(row.intent_key)
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
            # Leg rows persist ``intent_key`` as ``<exit_pine>\0<from_entry>\0TP/SL``;
            # strip the leg suffix to get the exit envelope key that
            # ``OrderSyncEngine._build_envelope`` looks up. TP and SL
            # legs share one exit envelope — ``record_complete`` is a
            # plain DELETE so the second call is a harmless no-op.
            leg_intent_key = row.intent_key
            if leg_intent_key and leg_intent_key.endswith(('\0TP', '\0SL')):
                exit_intent_key = leg_intent_key[:-3]
                if exit_intent_key not in retired_intent_keys:
                    self.store_ctx.record_complete(exit_intent_key)
                    retired_intent_keys.add(exit_intent_key)
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
    pos_by_deal_id: dict[str, dict]

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
                        reject_reason=_extract_reject_reason(confirm),
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

    async def _verdict_modify_exit(
            self, row: 'OrderRow', refs: Mapping[str, str],
    ) -> ResumeOutcome:
        """Verdict for a ``modify_exit`` command row.

        The command row carries ``extras['target_coid']`` (the entry
        whose bracket was being amended). Recovery question: did the
        ``PUT /positions/{dealId}`` land? Three resolution paths:

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
        target_coid = extras.get('target_coid')

        def _as_float(value):
            if value is None:
                return None
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        new_tp = _as_float(extras.get('new_tp'))
        new_sl = _as_float(extras.get('new_sl'))
        new_trail = _as_float(extras.get('new_trail'))
        new_trail_price = _as_float(extras.get('new_trail_price'))
        # Pine pending trailing (``trail_price`` + ``trail_offset``) leaves
        # broker ``trailingStop`` off — the local activation monitor PUTs the
        # native stop only after price crosses ``new_trail_price``. Recovery
        # therefore expects the broker snapshot to show ``trailingStop=False``
        # and the existing fixed/native stop cleared.
        pending_trail = new_trail is not None and new_trail_price is not None

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
                        reject_reason=_extract_reject_reason(confirm),
                        recovery_path='modify_exit_confirm_rejected',
                        recovery_context={'deal_reference': ref},
                    )
                if deal_status == 'ACCEPTED':
                    return ResumeOutcome(
                        status='confirmed',
                        is_filled=False,
                        filled_qty=0.0,
                        fill_price=0.0,
                        recovery_path='modify_exit_confirm_get',
                        recovery_context={'deal_reference': ref},
                    )

        target_deal_id: str | None = None
        if target_coid and self.plugin.store_ctx is not None:
            target_row = self.plugin.store_ctx.get_order(str(target_coid))
            if target_row is not None and target_row.exchange_order_id:
                target_deal_id = str(target_row.exchange_order_id)

        if target_deal_id is None:
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='modify_exit_no_target',
            )

        pos = self.pos_by_deal_id.get(target_deal_id)
        if pos is None:
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='modify_exit_target_vanished',
            )
        pos_data = pos.get('position') or {}
        pos_profit_level: float | None = _as_float(pos_data.get('profitLevel'))
        pos_stop_level: float | None = _as_float(pos_data.get('stopLevel'))
        pos_stop_distance: float | None = _as_float(pos_data.get('stopDistance'))
        pos_trailing_stop = bool(pos_data.get('trailingStop'))

        def _matches_tp() -> bool:
            if new_tp is None:
                return pos_profit_level is None
            if pos_profit_level is None:
                return False
            return abs(pos_profit_level - new_tp) < 1e-9

        def _matches_sl() -> bool:
            if new_sl is None and new_trail is None:
                return pos_stop_level is None and not pos_trailing_stop
            if new_trail is not None:
                # Pending trailing: the new bracket is purely local until the
                # activation monitor PUTs it. The broker side has no SL leg —
                # ``trailingStop`` must be off, ``stopLevel`` cleared.
                if pending_trail:
                    return pos_stop_level is None and not pos_trailing_stop
                if not pos_trailing_stop:
                    return False
                if pos_stop_distance is None:
                    return False
                return abs(pos_stop_distance - new_trail) < 1e-9
            if new_sl is None or pos_stop_level is None:
                return False
            return abs(pos_stop_level - new_sl) < 1e-9

        if _matches_tp() and _matches_sl():
            return ResumeOutcome(
                status='confirmed',
                exchange_id=target_deal_id,
                is_filled=False,
                filled_qty=0.0,
                fill_price=0.0,
                recovery_path='modify_exit_snapshot_match',
                recovery_context={
                    'profit_level': pos_profit_level,
                    'stop_level': pos_stop_level,
                    'stop_distance': pos_stop_distance,
                    'trailing_stop': pos_trailing_stop,
                },
            )

        return ResumeOutcome(
            status='still_unknown',
            recovery_path='modify_exit_snapshot_mismatch',
            recovery_context={
                'profit_level': pos_profit_level,
                'stop_level': pos_stop_level,
                'stop_distance': pos_stop_distance,
                'trailing_stop': pos_trailing_stop,
                'expected_tp': new_tp,
                'expected_sl': new_sl,
                'expected_trail': new_trail,
                'expected_trail_price': new_trail_price,
                'pending_trail': pending_trail,
            },
        )

    def _verdict_cancel(
            self, row: 'OrderRow', refs: Mapping[str, str],
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
        del refs  # cancel rows do not carry deal_reference
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
            self, row: 'OrderRow', refs: Mapping[str, str],
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

        The hook also mirrors each newly-confirmed target row to
        ``closing`` so the activity stream's subsequent ``CLOSED`` event
        matches a row in the same state the live dispatch would have
        left behind.
        """
        del refs  # full_close has no deal_reference
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

        still_alive: list[str] = []
        swept: list[str] = []
        for did in target_deal_ids:
            if str(did) in self.pos_by_deal_id:
                still_alive.append(str(did))
            else:
                swept.append(str(did))

        if still_alive:
            return ResumeOutcome(
                status='still_unknown',
                recovery_path='full_close_targets_partial',
                recovery_context={
                    'still_alive_targets': still_alive,
                    'swept_targets': swept,
                },
            )

        # All targets vanished — mirror each parent row to ``closing``
        # so the activity stream's CLOSED event can promote it cleanly.
        store_ctx = self.plugin.store_ctx
        mirrored: list[str] = []
        for r in list(store_ctx.iter_live_orders()):
            if r.exchange_order_id and r.exchange_order_id in target_deal_ids:
                if r.state not in ('closing', 'closed'):
                    store_ctx.set_order_state(r.client_order_id, 'closing')
                    store_ctx.log_event(
                        'close_dispatched',
                        client_order_id=r.client_order_id,
                        exchange_order_id=r.exchange_order_id,
                        intent_key=row.intent_key,
                        payload={'recovery_path': 'full_close_targets_vanished'},
                    )
                    mirrored.append(r.client_order_id)
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

        Partial close is the emulated opposite-direction POST. Resolution
        paths in priority order:

        1. **Stored ``deal_reference`` with live confirm** —
           ``/confirms/{ref}`` GET. ``REJECTED`` → rejected. ``ACCEPTED``
           — verify the POST actually netted by checking the live
           positions delta against the persisted ``pre_total_units`` /
           ``intent_units``: if the current symbol unit total matches the
           expected post-POST value → confirmed; otherwise
           ``still_unknown`` (the post-POST snapshot has not caught up or
           a race remains unresolved — operator intervention or next
           reconcile required). The corrective-DELETE time window (±3 s)
           is intentionally NOT reproduced on recovery: it cannot be
           reconstructed after the process boundary, so any race that
           landed mid-dispatch must be resolved by the live reconciler
           instead.

        2. **Ref missing or confirm TTL expired** — fall back to the
           persisted ``pre_total_units`` / ``intent_units`` versus the
           live positions snapshot. If the symbol total matches the
           expected post-POST value, the broker already netted the close
           and a retry would over-reduce the position, so we confirm.
           Without that delta context the verdict is ``still_unknown``
           and the engine-side reconciler retries on next sync. We
           deliberately do NOT re-issue an opposite POST from the
           verdict-builder — a duplicate opposite leg would compound any
           unresolved race.
        """
        extras = row.extras or {}
        ref: str | None = (
            refs.get('deal_reference') or extras.get('deal_reference')
        )
        if not ref:
            return await self._verdict_partial_close_by_units(
                row,
                ref=None,
                confirm=None,
                fallback_path='partial_close_missing_deal_reference',
            )
        try:
            confirm = await self.plugin._call(
                f'confirms/{ref}', method='get',
            )
        except OrderNotFoundError:
            return await self._verdict_partial_close_by_units(
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

        return await self._verdict_partial_close_by_units(
            row,
            ref=ref,
            confirm=confirm,
            fallback_path='partial_close_units_mismatch',
        )

    async def _verdict_partial_close_by_units(
            self,
            row: 'OrderRow',
            *,
            ref: str | None,
            confirm: dict | None,
            fallback_path: str,
    ) -> ResumeOutcome:
        """Resolve a partial-close verdict from the persisted unit delta.

        Used in three situations:

        * Confirm GET succeeded with ``ACCEPTED`` — we treat the broker's
          acknowledgement as truth only after the live positions snapshot
          shows the expected net.
        * Confirm TTL expired (``ref`` set, ``confirm`` ``None``) — the
          broker may have already netted before the crash; the unit
          delta is the only durable signal left.
        * No ``deal_reference`` was ever persisted (``ref`` ``None``,
          ``confirm`` ``None``) — same risk: a POST that landed at the
          broker but never made it back to local persistence would
          otherwise be retried and over-close the position.

        ``fallback_path`` names the ``still_unknown`` recovery path used
        when we lack the delta context or the units do not match.
        """
        extras = row.extras or {}
        pre_total_units = extras.get('pre_total_units')
        intent_units = extras.get('intent_units')
        recovery_context: dict = {}
        if ref is not None:
            recovery_context['deal_reference'] = ref
        if pre_total_units is None or intent_units is None:
            return ResumeOutcome(
                status='still_unknown',
                recovery_path=(
                    'partial_close_missing_delta_context'
                    if confirm is not None
                    else fallback_path
                ),
                recovery_context=recovery_context or None,
            )

        # Recovery runs during ``connect()`` before any execute path
        # would populate ``_instrument_rules_cache``, so reading the
        # cache directly leaves a fresh-restart recovery permanently
        # ``still_unknown``. Fetch the rules instead; on failure fall
        # back to ``still_unknown`` so the next sync / reconcile gets
        # another chance rather than halting recovery.
        try:
            rules = await self.plugin._get_instrument_rules(row.symbol)
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

        current_units = sum(
            round(float((r.get('position') or {}).get('size', 0.0))
                  / rules.lot_step)
            for r in self.pos_by_deal_id.values()
            if (r.get('market') or {}).get('epic') == row.symbol
        )
        expected = int(pre_total_units) - int(intent_units)
        deal_id = (confirm or {}).get('dealId')
        match_context = {
            **recovery_context,
            'current_units': current_units,
            'expected_units': expected,
        }
        if current_units == expected:
            # Confirm GET succeeded → ACCEPTED + delta match.
            # Confirm missing (TTL expired or no ref) but delta matches:
            # the broker already netted the close, so retrying would
            # over-reduce the position. Confirm it.
            if confirm is not None:
                path = 'partial_close_units_match'
            elif ref is not None:
                path = 'partial_close_units_match_ttl_expired'
            else:
                path = 'partial_close_units_match_no_ref'
            return ResumeOutcome(
                status='confirmed',
                exchange_id=str(deal_id) if deal_id else None,
                recovery_path=path,
                recovery_context=match_context,
            )
        return ResumeOutcome(
            status='still_unknown',
            recovery_path=fallback_path,
            recovery_context=match_context,
        )
