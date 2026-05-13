"""
@pyne

Journal-based ``modify_exit`` contract tests.

The plugin's ``modify_exit`` was migrated to the Core
:class:`~pynecore.core.broker.journal.DispatchJournal` in M4 phase 5: the
PUT/confirm lifecycle now flows through ``run_modify_exit`` with
``_CapitalComModifyExitHooks`` as the wire-format hook. The synthetic
TP/SL leg rows are still plugin-owned (they live outside the journal's
command-row scope for M4); the hook's ``mirror_bracket_legs`` callback
materialises them on the happy path and seeds them in
``disposition_unknown`` on the ambiguous path.

These tests pin the externally-observable contract of that migration:

* Happy path: command row reaches ``state='confirmed'`` and is closed
  out of ``iter_live_orders``; the leg rows are inserted as
  ``state='confirmed'``; the canonical audit chain
  (``dispatch_submitted`` → ``deal_reference_seen`` → ``confirmed`` →
  ``order_closed``) is emitted.
* Reject path: confirm ``REJECTED`` flips the command row to
  ``state='rejected'`` and surfaces an ``ExchangeOrderRejectedError``;
  no leg rows are written.
* Timeout paths (PUT and confirm): command row stays
  ``state='disposition_unknown'`` for recovery; added leg rows are
  seeded ``state='disposition_unknown'`` so the next reconcile snapshot
  resolves them against authoritative broker state.
* Recovery via snapshot match: a stale ``modify_exit`` command row with
  no ``deal_reference`` is resolved by matching ``new_tp`` / ``new_sl``
  against the live position's ``profitLevel`` / ``stopLevel``.
"""
import asyncio

import httpx
import pytest

from pynecore.core.broker.exceptions import (
    ExchangeOrderRejectedError,
    OrderDispositionUnknownError,
)
from pynecore.core.broker.idempotency import (
    KIND_ENTRY,
    KIND_EXIT_TP,
    KIND_EXIT_SL,
    KIND_MODIFY_EXIT as COID_KIND_MODIFY_EXIT,
)
from pynecore.core.broker.models import (
    DispatchEnvelope, ExitIntent,
)
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore
from pynecore.core.broker.store_helpers import KIND_MODIFY_EXIT

from pynecore_capitalcom import CapitalCom, CapitalComConfig


def main():
    pass


# Markets GET response that gives lot_step + a tradeable snapshot for mid.
_MARKETS_RESP = {
    'dealingRules': {
        'minStepDistance': {'value': 0.01},
        'minDealSize': {'value': 0.01},
        'minNormalStopOrLimitDistance': {'value': 0.0001},
    },
    'instrument': {'lotSize': 0.01},
    'snapshot': {'bid': 1.20, 'offer': 1.20, 'marketStatus': 'TRADEABLE'},
}


class _FakeBroker(CapitalCom):
    def __init__(self, *, config=None, responses=None):
        super().__init__(config=config)
        self._responses: dict = responses or {}
        self._calls: list = []

    async def _call(self, endpoint, *, data=None, method='post'):
        self._calls.append((endpoint, method, data))
        err = self._responses.get(('error', endpoint, method))
        if err is not None:
            raise err
        return self._responses.get((endpoint, method), {})


def _make_broker(tmp_path, *, responses=None):
    resp: dict = {('markets/EURUSD', 'get'): _MARKETS_RESP}
    if responses:
        resp.update(responses)
    config = CapitalComConfig(
        demo=True,
        user_email='test@example.com',
        api_key='k',
        api_password='p',
    )
    broker = _FakeBroker(config=config, responses=resp)
    store = BrokerStore(tmp_path / 'broker.sqlite', plugin_name=broker.plugin_name)
    identity = RunIdentity(
        strategy_id='test', symbol='EURUSD', timeframe='60',
        account_id='acc', label=None,
    )
    ctx = store.open_run(identity, script_source='// test')
    broker.store_ctx = ctx
    return broker, store, ctx


def _exit_envelope(*, tp=None, sl=None, trail_offset=None, trail_price=None,
                   pine_id='Long', from_entry='Long', qty=1.0):
    return DispatchEnvelope(
        intent=ExitIntent(
            pine_id=pine_id, symbol='EURUSD', side='sell', qty=qty,
            tp_price=tp, sl_price=sl,
            trail_offset=trail_offset, trail_price=trail_price,
            from_entry=from_entry,
            reduce_only=True,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )


def _seed_position(ctx, *, coid: str, exchange_id: str = 'pos-1',
                   from_entry: str = 'Long',
                   tp_level=None, sl_level=None):
    """Seed a confirmed entry row representing an open position."""
    extras: dict = {
        'kind': 'position',
        'order_type': 'market',
    }
    ctx.upsert_order(
        coid, symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id=from_entry,
        from_entry=from_entry,
        intent_key=from_entry,
        exchange_order_id=exchange_id,
        filled_qty=1.0,
        tp_level=tp_level, sl_level=sl_level,
        extras=extras,
    )


def _events_for(ctx, coid: str) -> list[tuple[str, dict]]:
    rows = list(ctx._store._conn.execute(
        "SELECT kind, payload FROM events "
        "WHERE run_instance_id = ? AND client_order_id = ? "
        "ORDER BY id ASC",
        (ctx.run_instance_id, coid),
    ))
    import json as _json
    return [(r['kind'], _json.loads(r['payload'] or '{}')) for r in rows]


def __test_modify_exit_happy_path_routes_through_journal__(tmp_path):
    """Add TP via PUT + confirm ACCEPTED → command CONFIRMED + leg row written."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/pos-1', 'put'): {'dealReference': 'amend-ref'},
        ('confirms/amend-ref', 'get'): {
            'dealStatus': 'ACCEPTED', 'status': 'OPEN',
            'dealId': 'pos-1',
            'profitLevel': 1.30, 'stopLevel': None,
            'trailingStop': False, 'stopDistance': None,
        },
    })
    entry_envelope = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Long', symbol='EURUSD', side='sell', qty=1.0,
            from_entry='Long', reduce_only=True,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    # Seed a confirmed entry row at the COID the find_active_entry_row scan
    # will encounter via the from_entry='Long' anchor.
    target_coid = 'entry-Long-1'
    _seed_position(ctx, coid=target_coid, exchange_id='pos-1',
                   from_entry='Long')

    old = _exit_envelope()
    new = _exit_envelope(tp=1.30)

    result = asyncio.run(broker.modify_exit(old, new))

    # Engine sees the new TP leg.
    assert len(result) == 1
    assert result[0].price == 1.30

    # Command row: confirmed and closed out of live iteration.
    cmd_coid = new.client_order_id(COID_KIND_MODIFY_EXIT)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'confirmed'
    live_coids = [r.client_order_id for r in ctx.iter_live_orders()]
    assert cmd_coid not in live_coids

    # TP leg row materialised by the mirror callback.
    tp_coid = new.client_order_id(KIND_EXIT_TP)
    tp_row = ctx.get_order(tp_coid)
    assert tp_row is not None
    assert tp_row.state == 'confirmed'
    assert tp_row.tp_level == 1.30
    assert (tp_row.extras or {}).get('leg_kind') == 'tp'
    assert (tp_row.extras or {}).get('parent_deal_id') == 'pos-1'

    # Audit event order pinned by journal contract.
    events = _events_for(ctx, cmd_coid)
    kinds = [k for k, _ in events]
    assert 'dispatch_submitted' in kinds
    assert 'deal_reference_seen' in kinds
    assert 'confirmed' in kinds
    assert 'order_closed' in kinds
    submit_payload = next(p for k, p in events if k == 'dispatch_submitted')
    assert submit_payload['kind'] == 'modify_exit'
    assert submit_payload['new_tp'] == 1.30
    assert submit_payload['new_sl'] is None
    assert submit_payload['from_entry'] == 'Long'

    store.close()


def __test_modify_exit_confirm_rejected_marks_command_row_rejected__(tmp_path):
    """Confirm REJECTED → command row REJECTED, no leg row inserted."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/pos-1', 'put'): {'dealReference': 'amend-ref'},
        ('confirms/amend-ref', 'get'): {
            'dealStatus': 'REJECTED',
            'reason': 'invalid level',
        },
    })
    target_coid = 'entry-Long-1'
    _seed_position(ctx, coid=target_coid, exchange_id='pos-1',
                   from_entry='Long')

    old = _exit_envelope()
    new = _exit_envelope(tp=1.30)

    with pytest.raises(ExchangeOrderRejectedError):
        asyncio.run(broker.modify_exit(old, new))

    cmd_coid = new.client_order_id(COID_KIND_MODIFY_EXIT)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'rejected'

    # No TP leg row written on reject (mirror is skipped).
    tp_coid = new.client_order_id(KIND_EXIT_TP)
    assert ctx.get_order(tp_coid) is None

    store.close()


def __test_modify_exit_put_timeout_flips_to_disposition_unknown__(tmp_path):
    """PUT timeout → command DISPOSITION_UNKNOWN + leg row seeded."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('error', 'positions/pos-1', 'put'):
            httpx.TimeoutException('PUT timeout'),
    })
    target_coid = 'entry-Long-1'
    _seed_position(ctx, coid=target_coid, exchange_id='pos-1',
                   from_entry='Long')

    old = _exit_envelope()
    new = _exit_envelope(tp=1.30)

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_exit(old, new))

    cmd_coid = new.client_order_id(COID_KIND_MODIFY_EXIT)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'disposition_unknown'

    # The newly-added TP leg row was seeded for snapshot reconciliation.
    tp_coid = new.client_order_id(KIND_EXIT_TP)
    tp_row = ctx.get_order(tp_coid)
    assert tp_row is not None
    assert tp_row.state == 'disposition_unknown'
    assert tp_row.tp_level == 1.30

    store.close()


def __test_modify_exit_confirm_timeout_flips_to_disposition_unknown__(tmp_path):
    """Confirm GET timeout after PUT 200 → command DISPOSITION_UNKNOWN."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/pos-1', 'put'): {'dealReference': 'amend-ref'},
        ('error', 'confirms/amend-ref', 'get'):
            httpx.TimeoutException('confirm timeout'),
    })
    target_coid = 'entry-Long-1'
    _seed_position(ctx, coid=target_coid, exchange_id='pos-1',
                   from_entry='Long')

    old = _exit_envelope()
    new = _exit_envelope(tp=1.30)

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_exit(old, new))

    cmd_coid = new.client_order_id(COID_KIND_MODIFY_EXIT)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'disposition_unknown'
    store.close()


def __test_modify_exit_recovery_via_snapshot_match__(tmp_path):
    """No deal_reference → recovery uses pos_by_deal_id snapshot.

    The new ``_verdict_modify_exit`` resolves a ``submitted`` modify
    command row by checking the parent position's current
    ``profitLevel`` / ``stopLevel``. A match → ``confirmed`` +
    ``recovery_path='modify_exit_snapshot_match'``.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        # Recovery snapshots
        ('positions', 'get'): {
            'positions': [{
                'position': {
                    'dealId': 'pos-1',
                    'profitLevel': 1.30,
                    'stopLevel': None,
                    'trailingStop': False,
                    'stopDistance': None,
                },
                'market': {'epic': 'EURUSD'},
            }],
        },
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
    })
    target_coid = 'target-pos'
    _seed_position(ctx, coid=target_coid, exchange_id='pos-1',
                   from_entry='Long')
    cmd_coid = 'modify-exit-cmd-1'
    ctx.upsert_order(
        cmd_coid, symbol='EURUSD', side='sell', qty=1.0,
        state='submitted', pine_entry_id='Long',
        extras={
            'kind': KIND_MODIFY_EXIT,
            'target_coid': target_coid,
            'new_tp': 1.30,
            'new_sl': None,
            'new_trail': None,
        },
    )

    asyncio.run(broker._recover_in_flight_submissions())

    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'confirmed'
    live_coids = [r.client_order_id for r in ctx.iter_live_orders()]
    assert cmd_coid not in live_coids
    store.close()


def __test_modify_exit_recovery_no_target_keeps_pending__(tmp_path):
    """Target row missing → still_unknown (engine retries on next sync)."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
    })
    cmd_coid = 'orphan-modify-exit'
    ctx.upsert_order(
        cmd_coid, symbol='EURUSD', side='sell', qty=1.0,
        state='submitted', pine_entry_id='Long',
        extras={
            'kind': KIND_MODIFY_EXIT,
            'target_coid': 'nonexistent',
            'new_tp': 1.30,
            'new_sl': None,
            'new_trail': None,
        },
    )

    asyncio.run(broker._recover_in_flight_submissions())

    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    # still_unknown → row stays as-is (engine reconciler retries).
    assert cmd_row.state == 'submitted'
    store.close()


def __test_modify_exit_recovery_confirm_rejected_marks_rejected__(tmp_path):
    """Stored deal_reference + confirm REJECTED → command rejected."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
        ('confirms/amend-ref', 'get'): {
            'dealStatus': 'REJECTED',
            'reason': 'invalid level',
        },
    })
    target_coid = 'target-pos'
    _seed_position(ctx, coid=target_coid, exchange_id='pos-1',
                   from_entry='Long')
    cmd_coid = 'modify-exit-rej-cmd'
    ctx.upsert_order(
        cmd_coid, symbol='EURUSD', side='sell', qty=1.0,
        state='disposition_unknown', pine_entry_id='Long',
        extras={
            'kind': KIND_MODIFY_EXIT,
            'target_coid': target_coid,
            'new_tp': 1.30,
            'new_sl': None,
            'new_trail': None,
            'deal_reference': 'amend-ref',
        },
    )
    ctx.add_ref(cmd_coid, 'deal_reference', 'amend-ref')

    asyncio.run(broker._recover_in_flight_submissions())

    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'rejected'
    store.close()
