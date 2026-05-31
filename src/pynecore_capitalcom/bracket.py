"""Bracket lifecycle helpers — leg row state transitions + trailing monitor.

Capital.com brackets are *position attributes* (the TP/SL levels on
``GET /positions``), not independent exchange orders. The plugin wraps
them in synthetic :class:`ExchangeOrder` rows so the rest of PyneCore
can reason about them; this mix-in owns every state transition for
those wrapper rows:

* ``_close_bracket_after_natural_close`` — flag legs as closed (not
  physically delete) when the parent position closes naturally so
  later ``modify_exit`` lookups don't see stale rows.
* ``_rollback_bracket_legs`` — synchronous reject path on a failed
  attach.
* ``_mark_bracket_legs_disposition_unknown`` — timeout path.
* ``_resolve_bracket_leg_disposition`` + ``_record_bracket_resolution``
  — snapshot reconcile resolves the disposition (attached / rejected
  / parent_gone) once the broker state is observable again.
* ``_trailing_activation_monitor`` — async task that promotes a Pine
  ``trail_price`` pending-trail leg to a native trailing stop once the
  activation threshold crosses (Capital.com has no native pending-
  trail; the plugin emulates it).

State touched: BrokerStore through ``self.store_ctx``.
"""
from time import time as epoch_time
from typing import TYPE_CHECKING

from pynecore.core.broker.exceptions import (
    BrokerError,
    OrderDispositionUnknownError,
    UnexpectedCancelError,
)
from pynecore.core.broker.models import ExitIntent

from ._base import _CapitalComBase
from .exceptions import CapitalComError
from .helpers import _extract_reject_reason

if TYPE_CHECKING:
    from pynecore.core.broker.native_failsafe_manager import NativeBracketSnapshot
    from pynecore.core.broker.storage import OrderRow


class _BracketMixin(_CapitalComBase):
    """Bracket lifecycle mix-in: leg row transitions + trailing monitor."""

    def _close_bracket_after_natural_close(self, entry_row: 'OrderRow') -> None:
        """Stamp the entry + bracket-leg rows with ``natural_close_at``.

        Called after a TP / SL / TRAILING_STOP fill on a position-attached
        bracket — Capital.com closes the position and auto-cancels the
        OCA partner.  Without this stamp, ``_reconcile_snapshot`` later
        sees the entry row's dealId missing from ``/positions`` +
        ``/workingorders``, stamps ``missing_pending_since``, and
        ``_missing_pending_tracker`` eventually raises a false
        :class:`UnexpectedCancelError`.

        The rows are NOT physically closed — :meth:`execute_exit` and
        :meth:`modify_exit` look up the entry row by ``pine_entry_id``
        on every Pine ``strategy.exit`` re-emission, and
        ``close_order`` would make those lookups fail.  The flag is
        instead read by :meth:`_reconcile_snapshot` and
        :meth:`_missing_pending_tracker` to skip the missing-pending
        accounting for these rows.
        """
        if self.store_ctx is None:
            return
        now_ts = epoch_time()
        deal_id = entry_row.exchange_order_id
        bracket_coids: list[str] = []
        if deal_id:
            for r in list(self.store_ctx.iter_live_orders(symbol=entry_row.symbol)):
                if (r.extras or {}).get('parent_deal_id') == deal_id:
                    bracket_coids.append(r.client_order_id)
        for coid in (*bracket_coids, entry_row.client_order_id):
            row = self.store_ctx.get_order(coid)
            if row is None:
                continue
            extras = dict(row.extras or {})
            extras['natural_close_at'] = now_ts
            self.store_ctx.upsert_order(coid, extras=extras)
        self.store_ctx.log_event(
            'bracket_natural_close',
            client_order_id=entry_row.client_order_id,
            exchange_order_id=deal_id,
            payload={'bracket_coids': bracket_coids, 'flagged_at': now_ts},
        )

    def _rollback_bracket_legs(
            self, *, intent: 'ExitIntent', parent_coid: str, deal_id: str,
            tp_coid: str | None, sl_coid: str | None, reason: str,
    ) -> None:
        """Retire the persisted TP/SL leg rows after a confirmed attach failure.

        Called when the broker either rejected the bracket synchronously
        (PUT 4xx / REJECTED confirm) or the response signals that no
        protective leg is in place.  The rows were upserted as
        ``submitted`` BEFORE the PUT so a crash mid-call would still
        replay deterministically; once we know the attach is dead, the
        rows must NOT linger in BrokerStore — otherwise a restart's
        recovery path treats them as live protective legs and the
        reconcile snapshot has no matching exchange artifact.

        Physically closes the rows (``close_order`` sets
        ``closed_ts_ms``); ``iter_live_orders`` will then skip them, and
        the audit event records the cause.
        """
        if self.store_ctx is None:
            return
        coids = [c for c in (tp_coid, sl_coid) if c is not None]
        for coid in coids:
            row = self.store_ctx.get_order(coid)
            if row is None:
                continue
            self.store_ctx.set_order_state(coid, 'rejected')
            self.store_ctx.close_order(coid)
        self.store_ctx.log_event(
            'bracket_attach_rollback',
            client_order_id=parent_coid,
            exchange_order_id=deal_id,
            intent_key=intent.intent_key,
            payload={'reason': reason, 'tp_coid': tp_coid, 'sl_coid': sl_coid,
                     'tp_level': intent.tp_price, 'sl_level': intent.sl_price,
                     'trail_offset': intent.trail_offset},
        )

    def _mark_bracket_legs_disposition_unknown(
            self, *, intent: 'ExitIntent', parent_coid: str, deal_id: str,
            tp_coid: str | None, sl_coid: str | None, reason: str,
            stage: str, deal_reference: str | None = None,
    ) -> None:
        """Flip TP/SL leg rows to ``disposition_unknown`` after an ambiguous PUT.

        Network failure mid-PUT (or mid-confirm) leaves the exchange's
        bracket state opaque until the next reconcile snapshot.  The
        rows stay live so the snapshot can promote them; the engine
        meanwhile receives :class:`OrderDispositionUnknownError` and
        parks the intent.
        """
        if self.store_ctx is None:
            return
        coids = [c for c in (tp_coid, sl_coid) if c is not None]
        for coid in coids:
            if self.store_ctx.get_order(coid) is None:
                continue
            self.store_ctx.set_order_state(coid, 'disposition_unknown')
        payload: dict = {'reason': reason, 'stage': stage,
                         'tp_coid': tp_coid, 'sl_coid': sl_coid}
        if deal_reference is not None:
            payload['deal_reference'] = deal_reference
        self.store_ctx.log_event(
            'bracket_attach_disposition_unknown',
            client_order_id=parent_coid,
            exchange_order_id=deal_id,
            intent_key=intent.intent_key,
            payload=payload,
        )

    @staticmethod
    def _levels_match(expected: object, actual: object,
                      tol: float = 1e-6) -> bool:
        """Float-compare two server-vs-local price levels with NaN tolerance.

        Both arguments may be ``None`` or non-numeric (Capital.com sometimes
        returns ``None`` for unset bracket legs). Returns ``False`` for any
        non-numeric or missing input.
        """
        if expected is None or actual is None:
            return False
        try:
            return abs(float(actual) - float(expected)) < tol  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return False

    def _resolve_bracket_leg_disposition(
            self, row: 'OrderRow', leg_kind: str,
            positions_by_deal: dict[str, dict],
            *,
            bracket_resolutions: dict[str, bool] | None = None,
            bracket_attached_coids: dict[str, list[str]] | None = None,
    ) -> None:
        """Resolve a ``disposition_unknown`` bracket leg row from the parent.

        Capital.com bracket legs share the parent position's deal_id and
        carry no ``exchange_order_id``; the standard snapshot recovery
        path (which keys off ``exchange_order_id``) cannot see them. This
        helper consults the parent position's ``profitLevel`` /
        ``stopLevel`` (or ``trailingStop`` / ``stopDistance`` for trailing
        legs) to decide whether the original PUT actually attached the
        bracket and updates the leg row accordingly.

        The leg-level row state change is applied immediately. The
        parent-level resolution intended for the engine's parked dispatch
        is *aggregated* (not written) when ``bracket_resolutions`` is
        provided — see the rationale in :meth:`_reconcile_snapshot`. The
        caller flushes the aggregate after processing every leg in the
        same poll cycle. When ``bracket_resolutions`` is ``None`` the
        helper falls back to writing per-leg directly (legacy callers
        and tests that drive the helper without an aggregator).

        :param bracket_resolutions: Aggregator keyed by parent COID. Each
            leg ANDs into the value (``True`` = all legs attached so far,
            ``False`` = at least one leg rejected — wins after flush).
        """
        if self.store_ctx is None:
            return
        rextras = row.extras or {}
        parent_deal_id = rextras.get('parent_deal_id')
        if not parent_deal_id:
            return
        parent_coid = rextras.get('parent_coid')

        is_trailing_leg = leg_kind == 'sl' and bool(
            row.trailing_distance or rextras.get('trail_offset')
        )

        parent = positions_by_deal.get(str(parent_deal_id))
        if parent is None:
            # Parent position vanished. Whether the bracket ever attached
            # is moot; either way the leg has nothing to protect now.
            self._record_bracket_resolution(
                row=row, leg_kind=leg_kind, resolution_label='parent_gone',
                attached=False, parent_deal_id=str(parent_deal_id),
                parent_coid=parent_coid,
                bracket_resolutions=bracket_resolutions,
                bracket_attached_coids=bracket_attached_coids,
            )
            return

        if rextras.get('force_disposition_rejected'):
            # ``modify_exit`` stamps this marker when an existing
            # broker-active SL leg is being transitioned into a
            # local-only pending-trail. The OLD row's level/distance
            # comparison can lie: if the broker did NOT apply the PUT
            # (timeout before reaching it), the old stop is still live
            # and a naïve compare would record ``attached``, clearing
            # the modify park while the new pending-trail intent
            # remains unsatisfied. Force a ``rejected`` resolution so
            # the engine's modify-rejected branch restores the
            # pre-modify ``_active_intents`` snapshot and re-runs
            # ``modify_exit`` next sync (whose retry will explicitly
            # clear the broker-side stop and persist the pending-trail
            # row).
            self._record_bracket_resolution(
                row=row, leg_kind=leg_kind,
                resolution_label='transition_force_rejected',
                attached=False, parent_deal_id=str(parent_deal_id),
                parent_coid=parent_coid,
                bracket_resolutions=bracket_resolutions,
                bracket_attached_coids=bracket_attached_coids,
            )
            return

        parent_pos = parent.get('position') or {}
        if is_trailing_leg:
            # Trailing SL on Capital.com is identified by the position's
            # ``trailingStop`` flag plus a matching ``stopDistance``; the
            # server-side level itself moves with price so a profit/stop
            # *level* comparison is meaningless here.
            expected_distance = (
                row.trailing_distance
                if row.trailing_distance is not None
                else rextras.get('trail_offset')
            )
            actual_distance = parent_pos.get('stopDistance')
            trailing_active = bool(parent_pos.get('trailingStop'))
            attached = trailing_active and self._levels_match(
                expected_distance, actual_distance,
            )
        elif leg_kind == 'tp':
            attached = self._levels_match(
                row.tp_level, parent_pos.get('profitLevel'),
            )
        else:
            attached = self._levels_match(
                row.sl_level, parent_pos.get('stopLevel'),
            )

        self._record_bracket_resolution(
            row=row, leg_kind=leg_kind,
            resolution_label='attached' if attached else 'not_attached',
            attached=attached, parent_deal_id=str(parent_deal_id),
            parent_coid=parent_coid,
            bracket_resolutions=bracket_resolutions,
            bracket_attached_coids=bracket_attached_coids,
        )

    def _record_bracket_resolution(
            self, *, row: 'OrderRow', leg_kind: str,
            resolution_label: str, attached: bool,
            parent_deal_id: str, parent_coid: str | None,
            bracket_resolutions: dict[str, bool] | None = None,
            bracket_attached_coids: dict[str, list[str]] | None = None,
    ) -> None:
        """Apply a bracket-leg resolution: row state, park, audit event.

        ``attached``: leg row → ``confirmed``; persisted park resolution
        → ``'attached'`` so the engine clears the park but keeps the
        ExitIntent active.

        ``not attached`` (level mismatch or parent gone): leg row →
        ``rejected`` + closed; persisted park resolution → ``'rejected'``
        so the engine drops the active ExitIntent and re-dispatches on
        the next sync, restoring protection.

        ``parent_coid`` is taken from the leg's extras (written by
        :meth:`execute_exit` at row creation time). When it is missing
        — pre-migration rows on a freshly upgraded process — the row
        state still flips correctly but the engine cannot re-dispatch
        until the next manual operator nudge; that is the safest
        degradation, the alternative would be guessing which COID to
        unpark.

        :param bracket_resolutions: When provided, the parent-level
            resolution is *aggregated* into this dict (one leg's
            ``False`` wins) instead of being written immediately. The
            caller flushes the aggregate to ``record_resolution`` after
            processing every leg in the same poll cycle. Eliminates the
            per-leg race against the engine's
            :meth:`_consume_plugin_resolutions`. Pass ``None`` for
            non-snapshot callers (e.g. unit tests).
        """
        assert self.store_ctx is not None  # caller guards this
        if attached:
            self.store_ctx.set_order_state(row.client_order_id, 'confirmed')
        else:
            self.store_ctx.set_order_state(row.client_order_id, 'rejected')
            self.store_ctx.close_order(row.client_order_id)
        if parent_coid is not None:
            if bracket_resolutions is not None:
                # AND-aggregate: any rejected leg wins over attached.
                # ``setdefault`` seeds with True so the first leg's
                # outcome holds; subsequent legs only flip True → False.
                bracket_resolutions[parent_coid] = (
                    bracket_resolutions.setdefault(parent_coid, True)
                    and attached
                )
                # Track every leg promoted to ``confirmed`` in this
                # batch so the aggregate flush can retire them if a
                # sibling later flips the parent to ``rejected``.
                # Without this, a mixed outcome (e.g. TP attached, SL
                # not_attached) leaves the TP row live while the engine
                # drops the parent dispatch and re-dispatches with new
                # COIDs — duplicate bracket rows under the same parent.
                if attached and bracket_attached_coids is not None:
                    bracket_attached_coids.setdefault(
                        parent_coid, [],
                    ).append(row.client_order_id)
            else:
                self.store_ctx.record_resolution(
                    parent_coid, 'attached' if attached else 'rejected',
                )
        self.store_ctx.log_event(
            'bracket_disposition_resolved',
            client_order_id=row.client_order_id,
            exchange_order_id=parent_deal_id,
            payload={
                'leg_kind': leg_kind,
                'resolution': resolution_label,
                'parent_coid': parent_coid,
            },
        )

    async def _trailing_activation_monitor(
            self, positions_by_deal: dict[str, dict],
    ) -> None:
        """3-state client-side trailing activation machine.

        Pine's ``trail_price`` + ``trail_offset`` semantics: the trailing
        stop only *activates* once the market crosses ``trail_price``.
        Capital.com's native ``trailingStop`` has no activation price
        — it starts tracking immediately. The gap is closed by this
        monitor:

        - ``pending``: row carries ``trail_activation_price``; the
          exchange is not yet trailing.  On the first tick where the
          mid-price crosses the threshold, transition to ``activating``
          and PUT ``trailingStop=true``.
        - ``activating``: idempotent PUT retry every tick until the
          snapshot confirms the exchange flipped to native trailing.
          Then transition to ``active`` and clear the activation fields.
        - ``active``: managed by the exchange; the monitor is a no-op.
        """
        if self.store_ctx is None:
            return
        for row in list(self.store_ctx.iter_live_orders()):
            state = (row.extras or {}).get('trail_state')
            if state not in ('pending', 'activating'):
                continue
            parent_deal_id = (row.extras or {}).get('parent_deal_id')
            if not parent_deal_id:
                continue
            pos = positions_by_deal.get(str(parent_deal_id))
            if pos is None:
                continue
            market = pos.get('market') or {}
            bid = float(market.get('bid') or 0.0)
            offer = float(market.get('offer') or 0.0)
            if not (bid and offer):
                continue
            mid = (bid + offer) / 2.0
            act_price = float((row.extras or {}).get('trail_activation_price') or 0.0)
            # ``row.side`` is the *exit* side. A long-entry SL has side='sell'
            # and activates when the mid rises to the activation price; a
            # short-entry SL has side='buy' and activates when the mid falls
            # to it. Pine's ``trail_price`` is always "the price at which the
            # trailing begins to follow the market", regardless of direction.
            if row.side == 'sell':
                crossed = mid >= act_price
            else:  # row.side == 'buy'
                crossed = mid <= act_price

            if state == 'pending':
                if not crossed:
                    continue
                # Send the PUT first; only flip to ``activating`` if the
                # broker accepted it. Otherwise a transient BrokerError
                # would leave the row in ``activating`` forever — the
                # snapshot branch below would never see ``trailingStop``
                # set, and the ``pending`` branch wouldn't re-enter, so
                # the protective trailing stop would stay inactive.
                body = {'trailingStop': True,
                        'stopDistance': float(row.trailing_distance or 0.0)}
                try:
                    await self._call(
                        f'positions/{parent_deal_id}', data=body, method='put',
                    )
                except BrokerError:
                    # Stay in pending; next tick re-tries.
                    continue
                new_extras = dict(row.extras or {})
                new_extras['trail_state'] = 'activating'
                self.store_ctx.upsert_order(
                    row.client_order_id, extras=new_extras,
                )
                self.store_ctx.log_event(
                    'trailing_activation_triggered',
                    client_order_id=row.client_order_id,
                    exchange_order_id=row.exchange_order_id,
                    payload={'mid': mid, 'activation_price': act_price},
                )

            # state 'activating' (possibly just transitioned) — verify
            # from the snapshot and promote to active on confirmation.
            pos_data = pos.get('position', {})
            if pos_data.get('trailingStop'):
                new_extras = {
                    k: v for k, v in (row.extras or {}).items()
                    if k not in ('trail_state', 'trail_activation_price')
                }
                self.store_ctx.set_risk(
                    row.client_order_id, trailing_stop=True,
                )
                self.store_ctx.upsert_order(
                    row.client_order_id, extras=new_extras,
                )
                self.store_ctx.log_event(
                    'trailing_activated',
                    client_order_id=row.client_order_id,
                    exchange_order_id=row.exchange_order_id,
                    payload={'distance': row.trailing_distance},
                )

    async def publish_native_failsafe_sl(self, snapshot: 'NativeBracketSnapshot') -> None:
        """§2.6.7 fail-safe actuator: PUT the worst-SL onto the parent dealId.

        The :class:`~pynecore.core.broker.native_failsafe_manager.NativeFailsafeManager`
        owns the worst-SL state; this method is the broker-side actuator the
        sync engine drives through ``set_native_bracket_dispatcher``. It does
        NOT touch BrokerStore leg rows — the protective stop here is the
        engine's own backstop behind the software partial-bracket WATCH loop,
        distinct from :meth:`execute_exit`'s bracket-leg accounting.

        Capital.com ``PUT /positions/{dealId}`` is *full replacement*
        (verified on demo 2026-05-29): a bracket field omitted from the body
        is cleared. The body therefore always carries an explicit
        ``stopLevel`` — a float to arm it, JSON ``null`` to remove it — never
        relying on omission for the SL, which keeps the body non-empty (an
        empty body is a no-op skip, not a clear). ``profitLevel`` and the
        trailing pair are sent only when the snapshot still wants them, so
        full replacement clears any coexisting leg the worst-SL recompute
        dropped (the snapshot carries the whole desired triple for exactly
        this reason).

        Raises (rather than returning) on an unresolved dealId, a mapped
        broker error, or a ``REJECTED`` confirm, so the engine records a PUT
        failure and degrades the fail-safe instead of believing the stop
        landed.

        :param snapshot: desired bracket triple + parent COID + generation.
        """
        if self.store_ctx is None:
            raise CapitalComError(
                "native fail-safe SL PUT refused: no store_ctx to resolve the "
                f"dealId for parent {snapshot.parent_entry_dispatch_ref!r}"
            )
        row = self.store_ctx.get_order(snapshot.parent_entry_dispatch_ref)
        deal_id = row.exchange_order_id if row is not None else None
        if not deal_id:
            raise CapitalComError(
                "native fail-safe SL PUT refused: unresolved dealId for parent "
                f"{snapshot.parent_entry_dispatch_ref!r} "
                f"({'no order row' if row is None else 'no exchange_order_id yet'})"
                " — refusing to PUT against a guessed position."
            )

        body: dict = {
            'stopLevel': (
                float(snapshot.stop_level)
                if snapshot.stop_level is not None else None
            ),
        }
        if snapshot.profit_level is not None:
            body['profitLevel'] = float(snapshot.profit_level)
        if snapshot.trailing_stop is not None:
            body['trailingStop'] = True
            body['stopDistance'] = float(snapshot.trailing_stop)

        resp = await self._call(f'positions/{deal_id}', data=body, method='put')
        deal_ref = resp.get('dealReference') if isinstance(resp, dict) else None
        self.store_ctx.log_event(
            'native_failsafe_sl_put',
            client_order_id=snapshot.parent_entry_dispatch_ref,
            exchange_order_id=deal_id,
            payload={
                'stop_level': snapshot.stop_level,
                'profit_level': snapshot.profit_level,
                'trailing_stop': snapshot.trailing_stop,
                'generation': snapshot.generation,
                'deal_reference': deal_ref,
            },
        )
        if not deal_ref:
            # PUT returned no dealReference to confirm; treat the round-trip
            # as success (the observed-reconcile pass verifies the actual
            # level later). Mirrors :meth:`execute_exit`, which only confirms
            # when a dealReference is present.
            return
        confirm = await self._call(f'confirms/{deal_ref}', method='get')
        if (confirm.get('dealStatus') or '').upper() == 'REJECTED':
            reason = _extract_reject_reason(confirm)
            raise CapitalComError(
                f"native fail-safe SL PUT REJECTED for dealId {deal_id} "
                f"(parent {snapshot.parent_entry_dispatch_ref!r}): {reason}"
            )

    # --- Connect / recovery -----------------------------------------------

