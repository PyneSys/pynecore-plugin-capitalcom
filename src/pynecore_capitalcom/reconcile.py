"""Snapshot reconcile + missing-pending tracker + unexpected-cancel raiser.

The plugin's poll cycle reads ``GET /positions`` + ``GET /workingorders``
and reconciles them against the BrokerStore — the runtime equivalent of
``_recover_in_flight_submissions`` (which only runs at restart). Splits
into:

* ``_reconcile_snapshot`` — the workhorse. Walks every live position +
  working order, resolves bracket leg dispositions, emits partial-fill
  events, retires confirmed siblings on mixed-bracket rejection,
  handles natural-close detection, and writes resolution markers for
  ambiguous cases.
* ``_missing_pending_tracker`` — separately tracks bot-owned rows that
  vanish without a corresponding cancel event; stamps
  ``missing_pending_since`` so a grace window must elapse before
  raising.
* ``_maybe_raise_unexpected_cancel`` — applies the configured policy
  (stop / stop_and_cancel / re_place / ignore) when the grace window
  expires. ``stop`` raises :class:`UnexpectedCancelError`; the rest
  are quieter outcomes.

State touched: BrokerStore through ``self.store_ctx``,
``_current_poll_id`` (read).
"""
from time import time as epoch_time
from typing import AsyncIterator

from pynecore.core.broker.exceptions import (
    BrokerError,
    OrderDispositionUnknownError,
    UnexpectedCancelError,
)
from pynecore.core.broker.journal import (
    DispatchJournal,
    ReconcileOutcome,
)
from pynecore.core.broker.models import (
    ExchangeOrder,
    ExitIntent,
    LegType,
    OrderEvent,
    OrderStatus,
    OrderType,
)

from ._base import _CapitalComBase
from .exceptions import OrderNotFoundError
from .models import _compute_cumulative_fill


class _ReconcileMixin(_CapitalComBase):
    """Snapshot reconcile + missing-pending tracker mix-in."""

    async def _reconcile_snapshot(
            self,
            positions_by_deal: dict[str, dict],
            working_by_deal: dict[str, dict],
    ) -> AsyncIterator[OrderEvent]:
        """Authoritative snapshot reconcile.

        Walks every live order row in the BrokerStore and emits events
        for state transitions that ``_process_activity`` alone may miss
        (working → position flip on fill, partial fills via cumulative
        size decrease). Rows whose exchange counterpart has disappeared
        get a ``missing_pending_since`` stamp so the grace tracker can
        close them out after the grace window.
        """
        if self.store_ctx is None:
            return
        now_ts = epoch_time()
        # Per-poll bracket aggregator: parent_coid -> all_legs_attached.
        # Rationale: a bracket has TP + SL legs but only ONE parked
        # dispatch entry (under the parent entry COID). Writing
        # ``record_resolution`` separately per leg races with the engine's
        # next ``_consume_plugin_resolutions``: if leg #1 lands as
        # ``'attached'`` and the engine sync happens BEFORE leg #2 writes
        # its result, the engine deletes the row and leg #2's later
        # ``'rejected'`` UPDATE finds zero rows. The sticky-rejected SQL
        # in :meth:`record_resolution` cannot help once the row is gone.
        # Aggregating per parent_coid in a single pass within this poll —
        # then writing ONE ``record_resolution`` per parent — eliminates
        # the within-poll race entirely (both legs are visible in
        # ``iter_live_orders`` together because :meth:`execute_exit`
        # creates them atomically).
        bracket_resolutions: dict[str, bool] = {}
        # Mirror tracker for legs that ``_record_bracket_resolution`` flips
        # to ``confirmed`` in this batch — keyed on parent COID. Used by
        # the aggregate flush below to retire ``confirmed`` siblings when
        # a mixed bracket lands as ``rejected`` overall (the engine then
        # drops the parent dispatch's mapping and re-dispatches with
        # fresh COIDs; without this retirement the confirmed siblings
        # would survive alongside the new rows as duplicate bracket
        # legs under the same parent_deal_id).
        bracket_attached_coids: dict[str, list[str]] = {}
        for row in list(self.store_ctx.iter_live_orders()):
            # --- Bracket-leg disposition recovery -------------------------
            # Bracket TP/SL rows store ``parent_deal_id`` in extras and have
            # NO ``exchange_order_id`` of their own (Capital.com brackets are
            # position attributes — there is no separate order id to track).
            # When ``execute_exit``'s PUT or confirm GET times out we flip
            # them to ``disposition_unknown`` and raise
            # ``OrderDispositionUnknownError``; the engine then parks the
            # envelope. ``_verify_pending_dispatches`` only queries
            # ``get_open_orders`` which never returns brackets, and the
            # snapshot loop below skips rows without ``exchange_order_id``,
            # so without this branch the leg rows would be stuck in
            # ``disposition_unknown`` forever even when the bracket actually
            # attached. We resolve the disposition deterministically from
            # the parent position's ``profitLevel`` / ``stopLevel`` (or
            # ``trailingStop`` + ``stopDistance`` for trailing legs): if it
            # matches what we tried to set, the bracket is attached
            # (``confirmed``); otherwise it is definitively absent
            # (``rejected``). The aggregated resolution is also written
            # AFTER the loop (see ``bracket_resolutions``) to the persisted
            # park row keyed on the *parent* entry COID — the engine's
            # :meth:`_verify_pending_dispatches` consumes it on the next
            # sync, clearing the parked envelope and (for ``rejected``) the
            # active ExitIntent so the next sync re-dispatches the
            # protective order.
            rextras = row.extras or {}
            leg_kind = rextras.get('leg_kind')
            if (leg_kind in ('tp', 'sl')
                    and row.state == 'disposition_unknown'):
                self._resolve_bracket_leg_disposition(
                    row, leg_kind, positions_by_deal,
                    bracket_resolutions=bracket_resolutions,
                    bracket_attached_coids=bracket_attached_coids,
                )
                continue
            did = row.exchange_order_id
            if not did:
                continue
            # Rows flagged by ``_close_bracket_after_natural_close`` are
            # known-closed on the exchange (TP/SL fired and the position
            # vanished as expected). Skip the missing-pending accounting
            # — otherwise the grace window would still raise a false
            # ``UnexpectedCancelError``.
            if (row.extras or {}).get('natural_close_at') is not None:
                continue
            pos = positions_by_deal.get(did)
            work = working_by_deal.get(did)

            if pos is None and work is None:
                if (row.extras or {}).get('close_event_yielded_at') is not None:
                    # Race resolved: an earlier poll yielded a close
                    # fill on this row while ``/positions`` still
                    # carried the deal (so the eager teardown was
                    # deferred). The deal has now vanished — this is
                    # the same full-close path the immediate teardown
                    # covers, just one poll later. Promote to teardown
                    # here so the missing-pending grace tracker does
                    # not raise a false ``UnexpectedCancelError``.
                    self._close_bracket_after_natural_close(row)
                    continue
                extras = dict(row.extras or {})
                if 'missing_pending_since' not in extras:
                    extras['missing_pending_since'] = now_ts
                    self.store_ctx.upsert_order(
                        row.client_order_id, extras=extras,
                    )
                continue

            # Clear any stale missing_pending stamp — it came back.
            if 'missing_pending_since' in (row.extras or {}):
                extras = {k: v for k, v in (row.extras or {}).items()
                          if k != 'missing_pending_since'}
                self.store_ctx.upsert_order(
                    row.client_order_id, extras=extras,
                )

            # Clear any stale ``close_event_yielded_at`` breadcrumb on a
            # row whose deal is still present.  The breadcrumb's purpose
            # is the partial → full race window: a close activity yielded
            # in the SAME poll where ``/positions`` had not yet reflected
            # it.  If the deal is still alive on the next poll, the
            # original close event was either a genuine partial (deal
            # legitimately retained the remaining exposure) or a race
            # that has now decisively NOT resolved as a full close.
            # Either way the breadcrumb has outlived its valid window —
            # leaving it in place causes a later disappearance of the
            # remaining exposure (e.g. manual close whose activity rolls
            # out of Capital.com's 60s history before the next poll) to
            # trip the ``pos is None and work is None`` branch above and
            # tear the bracket down silently, suppressing the
            # missing-pending grace tracker that would otherwise raise
            # ``UnexpectedCancelError`` and re-sync the strategy.
            #
            # **Same-poll guard**: when ``_process_activity`` deferred a
            # full close in *this* cycle (``/positions`` returned the
            # deal still open while ``/history/activity`` already showed
            # the close), the same stale snapshot is what now hands us
            # ``pos is not None``. Clearing here would turn next poll's
            # legitimate disappearance into a ``missing_pending_since``
            # stamp and eventually a false :class:`UnexpectedCancelError`.
            # Skip the clear when the breadcrumb's poll-id matches the
            # current cycle; a fresh snapshot in a later poll will pass
            # the gate and clear normally.
            if 'close_event_yielded_at' in (row.extras or {}):
                stamp_poll_id = (row.extras or {}).get(
                    'close_event_yielded_at_poll_id',
                )
                if stamp_poll_id != self._current_poll_id:
                    extras = {k: v for k, v in (row.extras or {}).items()
                              if k not in (
                                  'close_event_yielded_at',
                                  'close_event_yielded_at_poll_id',
                              )}
                    self.store_ctx.upsert_order(
                        row.client_order_id, extras=extras,
                    )

            # Working → position transition (entry fill)
            if (row.state == 'server_ref_seen' and pos is not None
                    and (row.extras or {}).get('kind') == 'working'):
                pos_data = pos.get('position') or {}
                filled = float(pos_data.get('size') or row.qty)
                # Promote the working row via the journal. The Core
                # helper owns state + filled_qty + extras merge + audit
                # event in a single transaction. The extras_patch flips
                # ``kind`` to ``position`` and stamps ``entry_filled_at``
                # so the rest of the plugin treats this row as a filled
                # position. Several code paths gate on these fields:
                #   * :meth:`_find_active_entry_row` requires
                #     ``kind == 'position'`` — without it, subsequent
                #     ``modify_exit`` lookups would skip the row.
                #   * Partial-fill detection later in this same
                #     reconcile loop also requires ``kind == 'position'``.
                #   * Manual ``USER`` / ``DEALER`` close detection in
                #     :meth:`_activity_to_event` requires
                #     ``entry_filled_at`` truthy AND ``kind == 'position'``.
                #     Without these, a manual close activity for a
                #     limit-fill entry would route as
                #     :class:`LegType.ENTRY` and add to the local
                #     position instead of closing it.
                # The activity-stream stamp at :meth:`_process_activity`
                # only stamps ``entry_filled_at`` for rows that are
                # ALREADY ``kind == 'position'`` — limit-fill entries
                # that surface via this snapshot path are otherwise
                # never stamped.
                DispatchJournal(self.store_ctx).apply_reconcile_outcome(
                    row.client_order_id,
                    ReconcileOutcome(
                        kind='filled',
                        reason='working_promoted_position',
                        new_state='confirmed',
                        audit_event='working_to_position',
                        filled_qty=filled,
                        extras_patch={
                            'kind': 'position',
                            'entry_filled_at': now_ts,
                        },
                        audit_payload={'filled': filled},
                        exchange_order_id=did,
                    ),
                )
                yield OrderEvent(
                    order=ExchangeOrder(
                        id=did, symbol=row.symbol, side=row.side,
                        order_type=OrderType.MARKET,
                        qty=row.qty, filled_qty=filled,
                        remaining_qty=max(0.0, row.qty - filled),
                        price=None, stop_price=None,
                        average_fill_price=float(pos_data.get('level') or 0.0),
                        status=OrderStatus.FILLED,
                        timestamp=now_ts, fee=0.0, fee_currency='',
                        reduce_only=False, client_order_id=row.client_order_id,
                    ),
                    event_type='filled',
                    fill_price=float(pos_data.get('level') or 0.0),
                    fill_qty=filled,
                    timestamp=now_ts,
                    pine_id=row.pine_entry_id,
                    from_entry=row.from_entry,
                    leg_type=LegType.ENTRY,
                )

            # Partial fill detection: position size < row.qty - row.filled_qty.
            # Restricted to rows already in ``confirmed`` — any other live
            # state (``closing``, ``cancel_pending``) is owned by a
            # different lifecycle path, and the journal helper would
            # otherwise overwrite that state back to ``confirmed`` and
            # resurrect a row that is on its way out.
            if (pos is not None
                    and (row.extras or {}).get('kind') == 'position'
                    and row.state == 'confirmed'):
                pos_data = pos.get('position') or {}
                current_size = float(pos_data.get('size') or 0.0)
                cumulative = _compute_cumulative_fill(row.qty, current_size)
                if row.filled_qty + 1e-9 < cumulative < row.qty:
                    DispatchJournal(self.store_ctx).apply_reconcile_outcome(
                        row.client_order_id,
                        ReconcileOutcome(
                            kind='filled',
                            reason='partial_fill_progress',
                            new_state='confirmed',
                            audit_event='partial_fill_detected',
                            filled_qty=cumulative,
                            audit_payload={'cumulative': cumulative,
                                           'previous': row.filled_qty},
                            exchange_order_id=did,
                        ),
                    )
                    yield OrderEvent(
                        order=ExchangeOrder(
                            id=did, symbol=row.symbol, side=row.side,
                            order_type=OrderType.MARKET,
                            qty=row.qty, filled_qty=cumulative,
                            remaining_qty=row.qty - cumulative,
                            price=None, stop_price=None,
                            average_fill_price=float(pos_data.get('level') or 0.0),
                            status=OrderStatus.PARTIALLY_FILLED,
                            timestamp=now_ts, fee=0.0, fee_currency='',
                            reduce_only=False,
                            client_order_id=row.client_order_id,
                        ),
                        event_type='partial',
                        fill_price=float(pos_data.get('level') or 0.0),
                        fill_qty=cumulative - row.filled_qty,
                        timestamp=now_ts,
                        pine_id=row.pine_entry_id,
                        from_entry=row.from_entry,
                        leg_type=LegType.ENTRY,
                    )

        # Flush aggregated bracket resolutions: one ``record_resolution``
        # per parent COID, written AFTER every leg of that bracket has
        # been inspected this poll. ``True`` (all legs attached) ->
        # ``'attached'``; any leg ``False`` -> ``'rejected'`` (the engine
        # then drops the active ExitIntent and re-dispatches on the next
        # sync). Single write per parent eliminates the per-leg race
        # against ``_consume_plugin_resolutions`` described above.
        if bracket_resolutions:
            # Index live pending-trail SL rows by ``parent_coid``. These
            # rows are seeded as ``confirmed`` synchronously by
            # :meth:`attach_bracket` / :meth:`modify_exit` whenever Pine
            # combines ``trail_offset`` + ``trail_price`` (the PUT body
            # carries the TP only — the pending-trail SL is purely
            # local, owned by :meth:`_trailing_activation_monitor`), so
            # they never flow through :meth:`_record_bracket_resolution`
            # and therefore never appear in ``bracket_attached_coids``.
            # If the parent later resolves ``rejected`` (e.g. TP attach
            # was ambiguous and the next snapshot shows the broker did
            # not attach it), the engine drops the parent dispatch's
            # mapping and re-dispatches with fresh COIDs — leaving the
            # old pending-trail SL row live with the same
            # ``parent_deal_id``. The activation monitor would then PUT
            # ``trailingStop=true`` against the stale row when the
            # threshold crosses, racing the freshly seeded row.
            pending_trail_by_parent: dict[str, list[str]] = {}
            for live_row in self.store_ctx.iter_live_orders():
                lextras = live_row.extras or {}
                if lextras.get('leg_kind') != 'sl':
                    continue
                if lextras.get('trail_state') not in ('pending', 'activating'):
                    continue
                pcoid = lextras.get('parent_coid')
                if pcoid is None:
                    continue
                pending_trail_by_parent.setdefault(str(pcoid), []).append(
                    live_row.client_order_id,
                )
            for parent_coid, all_attached in bracket_resolutions.items():
                if not all_attached:
                    journal = DispatchJournal(self.store_ctx)
                    # Mixed bracket: any leg row that an earlier
                    # _record_bracket_resolution call promoted to
                    # ``confirmed`` in this same batch must be retired
                    # before the engine sees the parent ``rejected``.
                    # The engine's modify-rejected branch (also driving
                    # the entry-rejected path) drops the parent
                    # dispatch's mapping and re-dispatches the exit
                    # with a fresh envelope (new TP/SL COIDs); leaving
                    # the confirmed sibling alive would seed duplicate
                    # bracket leg rows under the same parent_deal_id —
                    # later fill / cancel lookups by parent could pick
                    # stale rows over the new dispatch's entries.
                    for sibling_coid in bracket_attached_coids.get(parent_coid, ()):
                        sibling = self.store_ctx.get_order(sibling_coid)
                        if sibling is None or sibling.closed_ts_ms is not None:
                            continue
                        sibling_extras = sibling.extras or {}
                        journal.apply_reconcile_outcome(
                            sibling_coid,
                            ReconcileOutcome(
                                kind='terminal_close',
                                reason='bracket_sibling_retired_on_mixed_rejection',
                                new_state='rejected',
                                audit_event='bracket_sibling_retired_on_mixed_rejection',
                                close_row=True,
                                audit_payload={
                                    'sibling_coid': sibling_coid,
                                    'parent_coid': parent_coid,
                                    'leg_kind': sibling_extras.get('leg_kind'),
                                    'reason': 'mixed_bracket_rejected',
                                },
                                exchange_order_id=sibling_extras.get(
                                    'parent_deal_id'
                                ),
                            ),
                        )
                    # Pending-trail SL siblings: same parent rejection
                    # consequences (engine re-dispatches with fresh
                    # COIDs), but the row was promoted to ``confirmed``
                    # outside _record_bracket_resolution, so it is not
                    # in ``bracket_attached_coids``. Walk the pre-built
                    # index and retire it through the same retirement
                    # path so the activation monitor cannot fire against
                    # the stale row.
                    for sibling_coid in pending_trail_by_parent.get(parent_coid, ()):
                        sibling = self.store_ctx.get_order(sibling_coid)
                        if sibling is None or sibling.closed_ts_ms is not None:
                            continue
                        sibling_extras = sibling.extras or {}
                        journal.apply_reconcile_outcome(
                            sibling_coid,
                            ReconcileOutcome(
                                kind='terminal_close',
                                reason='pending_trail_parent_rejected',
                                new_state='rejected',
                                audit_event='bracket_sibling_retired_on_mixed_rejection',
                                close_row=True,
                                audit_payload={
                                    'sibling_coid': sibling_coid,
                                    'parent_coid': parent_coid,
                                    'leg_kind': sibling_extras.get('leg_kind'),
                                    'reason': 'pending_trail_parent_rejected',
                                    'trail_state': sibling_extras.get(
                                        'trail_state'
                                    ),
                                },
                                exchange_order_id=sibling_extras.get(
                                    'parent_deal_id'
                                ),
                            ),
                        )
                self.store_ctx.record_resolution(
                    parent_coid,
                    'attached' if all_attached else 'rejected',
                )

    async def _missing_pending_tracker(
            self,
            working_by_deal: dict[str, dict],
            positions_by_deal: dict[str, dict],
    ) -> AsyncIterator[OrderEvent]:
        """Emit cancelled events for rows missing past the grace window.

        A row may temporarily disappear between polls (a fill in flight
        shows neither in working nor in positions for an instant). The
        grace window (``5 × cadence``, min 5 s) absorbs that noise.
        Rows missing past the window are treated as cancelled and fed
        into the :meth:`_maybe_raise_unexpected_cancel` policy branch.
        """
        if self.store_ctx is None:
            return
        grace = max(5.0, self.config.poll_interval_seconds * 5.0)
        now_ts = epoch_time()
        for row in list(self.store_ctx.iter_live_orders()):
            extras = row.extras or {}
            since: float | None = extras.get('missing_pending_since')
            if since is None:
                continue
            # Defensive: ``_reconcile_snapshot`` already skips
            # naturally-closed rows from being stamped, but double-guard
            # against any historical stamp surviving on a row that was
            # later flagged as natural close.
            if extras.get('natural_close_at') is not None:
                continue
            if (now_ts - float(since)) < grace:
                continue
            did = row.exchange_order_id
            if did and (did in working_by_deal or did in positions_by_deal):
                # Came back — already cleared in reconcile, skip.
                continue
            DispatchJournal(self.store_ctx).apply_reconcile_outcome(
                row.client_order_id,
                ReconcileOutcome(
                    kind='terminal_close',
                    reason='missing_pending_grace_expired',
                    new_state='rejected',
                    audit_event='unexpected_cancel',
                    close_row=True,
                    audit_payload={'missing_since': since, 'grace': grace},
                    exchange_order_id=did,
                ),
            )
            yield OrderEvent(
                order=ExchangeOrder(
                    id=did or '', symbol=row.symbol, side=row.side,
                    order_type=OrderType.MARKET,
                    qty=row.qty, filled_qty=row.filled_qty,
                    remaining_qty=max(0.0, row.qty - row.filled_qty),
                    price=None, stop_price=None,
                    average_fill_price=None,
                    status=OrderStatus.CANCELLED,
                    timestamp=now_ts, fee=0.0, fee_currency='',
                    reduce_only=False, client_order_id=row.client_order_id,
                ),
                event_type='cancelled',
                fill_price=None, fill_qty=None, timestamp=now_ts,
                pine_id=row.pine_entry_id,
                from_entry=row.from_entry,
            )
            await self._maybe_raise_unexpected_cancel(row)

    async def _maybe_raise_unexpected_cancel(self, row: 'OrderRow') -> None:
        """Apply the configured ``on_unexpected_cancel`` policy.

        - ``stop`` (default): raise :class:`UnexpectedCancelError` — the
          sync engine halts via its normal graceful-stop path.
        - ``stop_and_cancel``: best-effort cancel pass over the other
          bot-owned orders in the same symbol, then raise.
        - ``re_place``: no-op — the sync engine re-dispatches the
          protective order on the next diff cycle.
        - ``ignore``: no-op with an audit log.
        """
        policy = self.on_unexpected_cancel
        if policy == 'ignore':
            if self.store_ctx is not None:
                self.store_ctx.log_event(
                    'unexpected_cancel_ignored',
                    client_order_id=row.client_order_id,
                    exchange_order_id=row.exchange_order_id,
                )
            return
        if policy == 're_place':
            if self.store_ctx is not None:
                self.store_ctx.log_event(
                    'unexpected_cancel_re_place',
                    client_order_id=row.client_order_id,
                    exchange_order_id=row.exchange_order_id,
                )
            return
        if policy == 'stop_and_cancel' and self.store_ctx is not None:
            cascade_journal = DispatchJournal(self.store_ctx)
            for other in list(self.store_ctx.iter_live_orders(symbol=row.symbol)):
                if (other.client_order_id == row.client_order_id
                        or not other.exchange_order_id):
                    continue
                kind = (other.extras or {}).get('kind', 'working')
                endpoint = (f'workingorders/{other.exchange_order_id}'
                            if kind == 'working'
                            else f'positions/{other.exchange_order_id}')
                try:
                    await self._call(endpoint, method='delete')
                except (OrderNotFoundError, BrokerError):
                    pass
                cascade_journal.apply_reconcile_outcome(
                    other.client_order_id,
                    ReconcileOutcome(
                        kind='terminal_close',
                        reason='unexpected_cancel_cascade',
                        new_state='rejected',
                        audit_event='unexpected_cancel_cascade',
                        close_row=True,
                        audit_payload={
                            'origin_coid': row.client_order_id,
                            'origin_deal_id': row.exchange_order_id,
                        },
                        exchange_order_id=other.exchange_order_id,
                    ),
                )
        raise UnexpectedCancelError(
            f"Bot-owned order disappeared unexpectedly: "
            f"coid={row.client_order_id!r} deal_id={row.exchange_order_id!r}",
            context={
                'client_order_id': row.client_order_id,
                'exchange_order_id': row.exchange_order_id,
                'symbol': row.symbol,
                'policy': policy,
            },
        )

    # --- Trailing activation monitor --------------------------------------

