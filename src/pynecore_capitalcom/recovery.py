"""Restart recovery — runs once at ``connect()`` before the WS loop opens.

Capital.com's idempotency capability is SOFTWARE: the server generates
``dealReference`` from each POST, so the plugin must keep its own crash-
safe ledger of which dispatches reached the exchange and which rolled
back. This mix-in walks the BrokerStore at startup and resolves every
non-terminal row:

* ``_load_activity_cursor_from_events`` — replays the
  ``activity_processed`` events from the last 24h to rebuild
  ``_activity_cursor.seen_fingerprints`` + watermark.
* ``_recover_in_flight_submissions`` — outer loop: walks
  ``submitted`` / ``server_ref_seen`` / ``disposition_unknown`` rows
  and dispatches each to the appropriate single-row recoverer.
* ``_recover_submitted_row`` — multi-factor confidence match against
  ``GET /positions`` + ``GET /workingorders`` snapshots.
* ``_recover_server_ref_seen_row`` — calls ``GET /confirms/{ref}``
  directly first; falls back to snapshot match on TTL expiry.

Bracket leg ``disposition_unknown`` rows are NOT handled here — they
are owned by ``_reconcile_snapshot``'s ``_resolve_bracket_leg_disposition``
since the resolution depends on the parent position's live state.

State touched: BrokerStore through ``self.store_ctx``,
``_activity_cursor`` (rebuilt).
"""
from time import time as epoch_time

from pynecore.core.broker.exceptions import BrokerManualInterventionError
from pynecore.core.broker.store_helpers import mark_confirmed_with_fill
from pynecore.lib.log import broker_info

from ._base import _CapitalComBase
from .exceptions import OrderNotFoundError
from .helpers import _parse_iso_timestamp


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

        Batch-fetches the three REST snapshots once, then walks every
        live order row whose state is ``submitted`` or ``server_ref_seen``
        and resolves it against the batch. Three outcomes per row:

        - Stored ``deal_reference`` matches a position/working row →
          promote to ``confirmed`` with the exchange's ``dealId``.
        - ``submitted`` row with no stored ref → confidence-ranked match
          against recent activity (single match → promote; multiple →
          :class:`BrokerManualInterventionError`; none → leave
          submitted, sync engine re-dispatches with same CO-ID).
        - ``server_ref_seen`` row → call ``/confirms/{ref}`` directly;
          on TTL expiry fall back to the snapshot match.
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

        # Track CO-IDs promoted by this recovery pass. The
        # ``pos_by_deal_id`` / ``wo_by_deal_id`` maps are snapshots from
        # *before* the promotions; rows resolved via ``activities`` or
        # the direct ``/confirms/{ref}`` call land a ``deal_id`` that is
        # not yet in those maps, so the subsequent orphan retirement
        # would otherwise close a freshly recovered live order.
        promoted_coids: set[str] = set()

        for row in list(self.store_ctx.iter_live_orders()):
            # ``disposition_unknown`` rows are a superset of ``submitted``
            # for recovery purposes — the POST either never went out or
            # went out without a response. Same resolution strategy: try
            # a stored ref first, then fall back to a confidence-ranked
            # activity match.
            if row.state in ('submitted', 'disposition_unknown'):
                await self._recover_submitted_row(
                    row, activities, pos_by_ref, wo_by_ref,
                    promoted_coids,
                )
            elif row.state == 'server_ref_seen':
                await self._recover_server_ref_seen_row(
                    row, pos_by_ref, wo_by_ref,
                    promoted_coids,
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
        )

    async def _recover_submitted_row(
            self, row: 'OrderRow',
            activities: list[dict],
            pos_by_ref: dict[str, dict],
            wo_by_ref: dict[str, dict],
            promoted_coids: set[str],
    ) -> None:
        """Resolve a ``submitted`` row with multi-factor confidence."""
        assert self.store_ctx is not None
        stored_ref: str | None = (row.extras or {}).get('deal_reference')
        if stored_ref:
            hit = pos_by_ref.get(stored_ref) or wo_by_ref.get(stored_ref)
            if hit is not None:
                data = hit.get('position') or hit.get('workingOrderData') or {}
                deal_id = data.get('dealId')
                mark_confirmed_with_fill(
                    self.store_ctx,
                    coid=row.client_order_id,
                    exchange_id=str(deal_id) if deal_id else None,
                    is_filled=False,
                    filled_qty=0.0,
                    fill_price=0.0,
                )
                self.store_ctx.log_event(
                    'recovery_promoted_stored_ref',
                    client_order_id=row.client_order_id,
                    exchange_order_id=deal_id,
                )
                promoted_coids.add(row.client_order_id)
                return

        # Activity heuristic: epic + direction + size + ±3 s createdDateUTC.
        row_side = 'BUY' if row.side == 'buy' else 'SELL'
        row_created = row.created_ts_ms / 1000.0 if row.created_ts_ms else 0.0
        candidates: list[dict] = []
        for a in activities:
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
            self.store_ctx.log_event(
                'recovery_no_match',
                client_order_id=row.client_order_id,
            )
            return
        if len(candidates) == 1:
            deal_id = candidates[0].get('dealId')
            mark_confirmed_with_fill(
                self.store_ctx,
                coid=row.client_order_id,
                exchange_id=str(deal_id) if deal_id else None,
                is_filled=False,
                filled_qty=0.0,
                fill_price=0.0,
            )
            self.store_ctx.log_event(
                'recovery_promoted_single_match',
                client_order_id=row.client_order_id,
                exchange_order_id=deal_id,
            )
            promoted_coids.add(row.client_order_id)
            return

        # Multiple matches — cannot auto-resolve.
        self.store_ctx.set_order_state(
            row.client_order_id, 'submission_ambiguous',
        )
        raise BrokerManualInterventionError(
            f"Recovery ambiguous: {len(candidates)} activity matches for "
            f"coid={row.client_order_id!r}",
            intent_key=row.intent_key,
            context={'symbol': row.symbol, 'qty': row.qty, 'side': row.side,
                     'candidate_deal_ids': [c.get('dealId') for c in candidates]},
        )

    async def _recover_server_ref_seen_row(
            self, row: 'OrderRow',
            pos_by_ref: dict[str, dict],
            wo_by_ref: dict[str, dict],
            promoted_coids: set[str],
    ) -> None:
        """Resolve a ``server_ref_seen`` row — confirm first, snapshot fallback."""
        assert self.store_ctx is not None
        ref: str | None = (row.extras or {}).get('deal_reference')
        if not ref:
            return
        try:
            confirm = await self._call(f'confirms/{ref}', method='get')
        except OrderNotFoundError:
            # TTL expired — fall back to snapshot match on the ref.
            hit = pos_by_ref.get(ref) or wo_by_ref.get(ref)
            if hit is None:
                return
            data = hit.get('position') or hit.get('workingOrderData') or {}
            deal_id = data.get('dealId')
            if deal_id:
                mark_confirmed_with_fill(
                    self.store_ctx,
                    coid=row.client_order_id,
                    exchange_id=str(deal_id),
                    is_filled=False,
                    filled_qty=0.0,
                    fill_price=0.0,
                )
                self.store_ctx.log_event(
                    'recovery_snapshot_fallback',
                    client_order_id=row.client_order_id,
                    exchange_order_id=deal_id,
                )
                promoted_coids.add(row.client_order_id)
            return

        deal_id = None
        affected = confirm.get('affectedDeals') or []
        if affected:
            deal_id = affected[0].get('dealId')
        if not deal_id:
            deal_id = confirm.get('dealId')
        if deal_id:
            mark_confirmed_with_fill(
                self.store_ctx,
                coid=row.client_order_id,
                exchange_id=str(deal_id),
                is_filled=False,
                filled_qty=0.0,
                fill_price=0.0,
            )
            self.store_ctx.log_event(
                'recovery_confirm_resolved',
                client_order_id=row.client_order_id,
                exchange_order_id=deal_id,
            )
            promoted_coids.add(row.client_order_id)

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

