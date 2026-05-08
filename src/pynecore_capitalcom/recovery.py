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
        for row in positions_resp.get('positions') or []:
            pos_data = row.get('position') or {}
            ref = pos_data.get('dealReference')
            if ref:
                pos_by_ref[str(ref)] = row
        wo_by_ref: dict[str, dict] = {}
        for wo in working_resp.get('workingOrders') or []:
            data = wo.get('workingOrderData') or {}
            ref = data.get('dealReference')
            if ref:
                wo_by_ref[str(ref)] = wo
        activities = activity_resp.get('activities') or []

        for row in list(self.store_ctx.iter_live_orders()):
            # ``disposition_unknown`` rows are a superset of ``submitted``
            # for recovery purposes — the POST either never went out or
            # went out without a response. Same resolution strategy: try
            # a stored ref first, then fall back to a confidence-ranked
            # activity match.
            if row.state in ('submitted', 'disposition_unknown'):
                await self._recover_submitted_row(
                    row, activities, pos_by_ref, wo_by_ref,
                )
            elif row.state == 'server_ref_seen':
                await self._recover_server_ref_seen_row(
                    row, pos_by_ref, wo_by_ref,
                )

    async def _recover_submitted_row(
            self, row: 'OrderRow',
            activities: list[dict],
            pos_by_ref: dict[str, dict],
            wo_by_ref: dict[str, dict],
    ) -> None:
        """Resolve a ``submitted`` row with multi-factor confidence."""
        assert self.store_ctx is not None
        stored_ref: str | None = (row.extras or {}).get('deal_reference')
        if stored_ref:
            hit = pos_by_ref.get(stored_ref) or wo_by_ref.get(stored_ref)
            if hit is not None:
                data = hit.get('position') or hit.get('workingOrderData') or {}
                deal_id = data.get('dealId')
                if deal_id:
                    self.store_ctx.add_ref(
                        row.client_order_id, 'deal_id', str(deal_id),
                    )
                    self.store_ctx.set_exchange_id(
                        row.client_order_id, str(deal_id),
                    )
                self.store_ctx.set_order_state(
                    row.client_order_id, 'confirmed',
                )
                self.store_ctx.log_event(
                    'recovery_promoted_stored_ref',
                    client_order_id=row.client_order_id,
                    exchange_order_id=deal_id,
                )
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
            if deal_id:
                self.store_ctx.add_ref(
                    row.client_order_id, 'deal_id', str(deal_id),
                )
                self.store_ctx.set_exchange_id(
                    row.client_order_id, str(deal_id),
                )
            self.store_ctx.set_order_state(row.client_order_id, 'confirmed')
            self.store_ctx.log_event(
                'recovery_promoted_single_match',
                client_order_id=row.client_order_id,
                exchange_order_id=deal_id,
            )
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
                self.store_ctx.add_ref(
                    row.client_order_id, 'deal_id', str(deal_id),
                )
                self.store_ctx.set_exchange_id(
                    row.client_order_id, str(deal_id),
                )
                self.store_ctx.set_order_state(
                    row.client_order_id, 'confirmed',
                )
                self.store_ctx.log_event(
                    'recovery_snapshot_fallback',
                    client_order_id=row.client_order_id,
                    exchange_order_id=deal_id,
                )
            return

        deal_id = None
        affected = confirm.get('affectedDeals') or []
        if affected:
            deal_id = affected[0].get('dealId')
        if not deal_id:
            deal_id = confirm.get('dealId')
        if deal_id:
            self.store_ctx.add_ref(
                row.client_order_id, 'deal_id', str(deal_id),
            )
            self.store_ctx.set_exchange_id(
                row.client_order_id, str(deal_id),
            )
            self.store_ctx.set_order_state(row.client_order_id, 'confirmed')
            self.store_ctx.log_event(
                'recovery_confirm_resolved',
                client_order_id=row.client_order_id,
                exchange_order_id=deal_id,
            )

