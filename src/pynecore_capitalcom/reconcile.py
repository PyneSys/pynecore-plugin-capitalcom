"""Snapshot reconcile + disappearance tracking over the core tracker.

The plugin's poll cycle reads ``GET /positions`` + ``GET /workingorders``
and reconciles them against the BrokerStore — the runtime equivalent of
``_recover_in_flight_submissions`` (which only runs at restart). Splits
into:

* ``_reconcile_snapshot`` — the workhorse. Walks every live position +
  working order, resolves bracket leg dispositions, emits partial-fill
  events, retires confirmed siblings on mixed-bracket rejection,
  handles natural-close detection, and writes resolution markers for
  ambiguous cases. Ends by feeding the poll's presence sets into the
  core :class:`DisappearanceTracker` (stamp / clear).
* ``_missing_pending_tracker`` — drives the tracker's grace-expiry
  protocol: rows missing past the grace window are retired as
  unexpected cancels (dual signal) and the configured
  ``on_unexpected_cancel`` policy is applied — ``stop`` /
  ``stop_and_cancel`` latch the engine quarantine through
  ``quarantine_sink``; ``halt`` (or an unwired sink) raises
  :class:`UnexpectedCancelError`.

State touched: BrokerStore through ``self.store_ctx``,
``_current_poll_id`` (read), ``_disappearance`` (lazy build).
"""
from time import time as epoch_time
from typing import TYPE_CHECKING, AsyncIterator

from pynecore.core.broker.disappearance import (
    DisappearanceTracker,
    MissingConfirmation,
    MissingResolution,
)
from pynecore.core.broker.exceptions import BrokerError
from pynecore.core.broker.journal import (
    DispatchJournal,
    ReconcileOutcome,
)
from pynecore.core.broker.models import (
    ExchangeOrder,
    LegType,
    OrderEvent,
    OrderStatus,
    OrderType,
)

from ._base import _CapitalComBase
from .exceptions import OrderNotFoundError
from .helpers import _POLL_INTERVAL_S
from .models import _compute_cumulative_fill

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow


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

            # §2.6.7 native fail-safe recovery feed: push the broker-observed
            # bracket triple for this live position into the engine so a parent
            # stuck in ``DEGRADING`` flips back to ``HEALTHY`` once the broker
            # confirms the desired worst-SL (see ``BrokerPlugin``'s
            # ``native_failsafe_observed_sink`` + ``_feed_native_failsafe_observed``).
            # Only positions carry brackets — a working order (``pos is None``)
            # has none yet. The parent ref is this row's ``client_order_id``
            # (the entry dispatch COID, which the engine keys the state on);
            # bracket-leg rows never reach here (no ``exchange_order_id``).
            if pos is not None:
                self._feed_native_failsafe_observed(row.client_order_id, pos)

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
                # No breadcrumb: the disappearance is the core tracker's
                # concern — ``observe_presence`` below stamps it.
                continue

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

        # Presence diff for the disappearance state machine: stamp rows
        # whose deal vanished from BOTH namespaces, clear rows that came
        # back. Runs AFTER the loop above so a natural-close teardown in
        # this same pass retires its rows before they could be stamped.
        # Both endpoints were fetched successfully when we got here (the
        # poll raises otherwise), so both namespaces are authoritative.
        self._disappearance_tracker().observe_presence(
            {
                'positions': set(positions_by_deal),
                'working': set(working_by_deal),
            },
            now_ts,
        )

    def _disappearance_tracker(self) -> DisappearanceTracker:
        """The lazily-built core disappearance tracker for this instance.

        Built on first use because its inputs — ``store_ctx``,
        ``on_unexpected_cancel``, ``quarantine_sink`` — are injected by
        the runner / CLI after ``__init__``. Venue wiring:

        * A bot order lives under ONE Capital.com ``dealId`` that
          migrates between ``/workingorders`` and ``/positions`` on fill,
          so every row tracks the same ref in both namespaces and is
          visible when either carries it. Bracket-leg rows have no deal
          id of their own → empty ref set → never tracked.
        * Rows flagged ``natural_close_at`` are known-closed on the
          exchange — exempt, otherwise the grace window would raise a
          false unexpected-cancel for an expected disappearance.
        * ``confirm_missing`` is the simple venue behavior: the poll
          snapshot is authoritative and phase 1 already cleared any row
          that came back, so a grace-expired stamp IS the confirmed
          external cancel.
        """
        tracker = self._disappearance
        if tracker is None:
            assert self.store_ctx is not None
            tracker = DisappearanceTracker(
                self.store_ctx,
                grace_s=max(5.0, _POLL_INTERVAL_S * 5.0),
                policy=self.on_unexpected_cancel,
                tracked_refs=lambda row: (
                    {('positions', row.exchange_order_id),
                     ('working', row.exchange_order_id)}
                    if row.exchange_order_id else set()
                ),
                confirm_missing=self._confirm_missing_cancelled,
                is_exempt=lambda row: (
                    (row.extras or {}).get('natural_close_at') is not None
                ),
                cancel_siblings=self._cancel_sibling_orders,
                request_quarantine=self.quarantine_sink,
                cancelled_event_factory=self._missing_pending_cancelled_event,
            )
            self._disappearance = tracker
        return tracker

    @staticmethod
    async def _confirm_missing_cancelled(
            _row: 'OrderRow',
    ) -> MissingConfirmation:
        """Grace-expiry verdict: a still-stamped row is an external cancel.

        Capital.com has no deal-history bridge to re-verify against — the
        per-poll snapshot is the authority, the tracker's phase 1 clears
        any row whose deal reappeared, and the stamp-version guard drops
        a verdict that raced a concurrent clear. What is left after the
        grace window is the genuine disappearance.
        """
        return MissingConfirmation(MissingResolution.CANCELLED)

    def _feed_native_failsafe_observed(
            self, parent_ref: str, pos: dict,
    ) -> None:
        """Forward one live position's broker-observed bracket triple to the
        engine's §2.6.7 fail-safe recovery feed.

        Maps the Capital.com ``/positions`` payload to the engine's
        ``(stop_level, profit_level, trailing_stop)`` shape:

        * a static stop is reported as ``stopLevel`` (an absolute price);
        * a trailing stop is reported as ``trailingStop=true`` + ``stopDistance``
          (a distance), and the absolute ``stopLevel`` slot is then irrelevant.

        The two are mutually exclusive at the broker for the SL slot, so we
        surface ``stop_level`` only when trailing is inactive and
        ``trailing_stop`` only when it is — exactly the split the engine's
        desired snapshot uses (``desired_level`` vs ``desired_trailing_stop``).
        A no-op when the runner has not wired the sink (state-only paths).
        """
        sink = self.native_failsafe_observed_sink
        if sink is None:
            return
        pos_data = pos.get('position') or {}
        raw_stop = pos_data.get('stopLevel')
        raw_profit = pos_data.get('profitLevel')
        raw_distance = pos_data.get('stopDistance')
        trailing_active = bool(pos_data.get('trailingStop'))
        sink(
            parent_ref,
            stop_level=(
                float(raw_stop)
                if raw_stop is not None and not trailing_active else None
            ),
            profit_level=float(raw_profit) if raw_profit is not None else None,
            trailing_stop=(
                float(raw_distance)
                if trailing_active and raw_distance is not None else None
            ),
        )

    async def _missing_pending_tracker(
            self,
            working_by_deal: dict[str, dict],
            positions_by_deal: dict[str, dict],
    ) -> AsyncIterator[OrderEvent]:
        """Drive the tracker's grace-expiry protocol for this poll.

        A row may temporarily disappear between polls (a fill in flight
        shows neither in working nor in positions for an instant). The
        grace window (``5 × cadence``, min 5 s) absorbs that noise. Rows
        missing past the window are retired with the tracker's dual
        signal: the synthetic ``cancelled`` event keeps the sync engine's
        order bookkeeping consistent (essential under the non-halting
        ``ignore`` / ``re_place`` policies, where the event is the ONLY
        signal), while the policy separately decides the operational
        reaction — ``stop`` / ``stop_and_cancel`` latch the engine
        quarantine, ``halt`` raises :class:`UnexpectedCancelError`
        through the poll loop.
        """
        if self.store_ctx is None:
            return
        async for event in self._disappearance_tracker().observe(
                {
                    'positions': set(positions_by_deal),
                    'working': set(working_by_deal),
                },
                epoch_time(),
        ):
            yield event

    @staticmethod
    def _missing_pending_cancelled_event(
            row: 'OrderRow', now_ts: float,
    ) -> OrderEvent:
        """Synthesised cancelled event for a vanished bot-owned row.

        Keyed on the row's ``dealId`` (``exchange_order_id``); carries no
        ``leg_type`` — the pre-tracker event shape this plugin's suite
        pins.
        """
        did = row.exchange_order_id
        return OrderEvent(
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

    async def _cancel_sibling_orders(self, row: 'OrderRow') -> None:
        """Best-effort cancel sweep for the ``stop_and_cancel`` policy.

        Deletes every other bot-owned working order / position in the
        origin row's symbol at the broker and retires its store row
        through the cascade audit path. Per-order failures are swallowed
        — this is a best-effort pass run while the quarantine / halt is
        already armed.
        """
        if self.store_ctx is None:
            return
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

    # --- Trailing activation monitor --------------------------------------

