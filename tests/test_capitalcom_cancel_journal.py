"""
@pyne

Journal-based ``execute_cancel`` contract tests.

The plugin's ``execute_cancel`` was migrated to the Core
:class:`~pynecore.core.broker.journal.DispatchJournal` in M4 phase 3:
the per-target sweep now flows through ``run_cancel`` with
``_CapitalComCancelHooks`` as the wire-format hook.

These tests pin the externally-observable contract of that migration:

* Happy path: command row reaches ``state='confirmed'``, gets closed
  out of ``iter_live_orders``, and ``extras`` carries
  ``reason_path='deleted'`` plus ``applied_target_coids``.
* Audit chain (``dispatch_submitted`` → ``confirmed`` → ``order_closed``)
  is emitted on the cancel command row, with ``target_coids`` mirrored
  into the ``dispatch_submitted`` payload.
* Mid-loop crash recovery: when the DELETE on one target succeeds but
  the second fails with a network error, the journal flips the command
  row to ``disposition_unknown``; on next-start recovery, if all
  remaining targets have vanished from the broker snapshots, the row
  promotes to ``confirmed`` (and gets closed).
* Mid-loop crash with a survivor: if any target is still alive on the
  broker side at recovery time, the dispatch verdict is
  ``still_unknown`` so the engine reconciler re-issues the cancel.
"""
import asyncio

import httpx
import pytest

from pynecore.core.broker.exceptions import OrderDispositionUnknownError
from pynecore.core.broker.idempotency import KIND_CANCEL as COID_KIND_CANCEL
from pynecore.core.broker.models import CancelIntent, DispatchEnvelope
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore
from pynecore.core.broker.store_helpers import KIND_CANCEL

from pynecore_capitalcom import CapitalCom, CapitalComConfig


def main():
    pass


_RULES_RESP = {
    'dealingRules': {
        'minStepDistance': {'value': 0.01},
        'minDealSize': {'value': 0.01},
        'minNormalStopOrLimitDistance': {'value': 0.0001},
    },
    'instrument': {'lotSize': 0.01},
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
    resp: dict = {('markets/EURUSD', 'get'): _RULES_RESP}
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


def _events_for(ctx, coid: str) -> list[tuple[str, dict]]:
    rows = list(ctx._store._conn.execute(
        "SELECT kind, payload FROM events "
        "WHERE run_instance_id = ? AND client_order_id = ? "
        "ORDER BY id ASC",
        (ctx.run_instance_id, coid),
    ))
    import json as _json
    return [(r['kind'], _json.loads(r['payload'] or '{}')) for r in rows]


def __test_execute_cancel_happy_path_routes_through_journal__(tmp_path):
    """DELETE works → command row CONFIRMED + reason_path='deleted'."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('workingorders/wo-1', 'delete'): {},
    })
    ctx.upsert_order('coid-w', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='wo-1',
                     extras={'kind': 'working'})
    env = DispatchEnvelope(
        intent=CancelIntent(pine_id='Long', symbol='EURUSD'),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    ok = asyncio.run(broker.execute_cancel(env))
    assert ok is True

    cmd_coid = env.client_order_id(COID_KIND_CANCEL)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'confirmed'
    extras = cmd_row.extras or {}
    assert extras.get('kind') == KIND_CANCEL
    assert extras.get('reason_path') == 'deleted'
    assert extras.get('target_coids') == ['coid-w']
    assert extras.get('applied_target_coids') == ['coid-w']

    # Command row is closed out of live (one-shot dispatch record).
    live_coids = [r.client_order_id for r in ctx.iter_live_orders()]
    assert cmd_coid not in live_coids
    # Target row is also closed by the hook.
    assert 'coid-w' not in live_coids

    events = _events_for(ctx, cmd_coid)
    kinds = [k for k, _ in events]
    assert kinds == ['dispatch_submitted', 'confirmed', 'order_closed']
    submit_payload = events[0][1]
    assert submit_payload['kind'] == KIND_CANCEL
    assert submit_payload['target_coids'] == ['coid-w']
    assert submit_payload['pine_id'] == 'Long'

    store.close()


def __test_execute_cancel_no_match_logs_noop_outside_journal__(tmp_path):
    """No live targets → ``cancel_noop`` event, NO command row created."""
    broker, store, ctx = _make_broker(tmp_path)
    env = DispatchEnvelope(
        intent=CancelIntent(pine_id='DoesNotExist', symbol='EURUSD'),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    ok = asyncio.run(broker.execute_cancel(env))
    assert ok is True

    cmd_coid = env.client_order_id(COID_KIND_CANCEL)
    # The no-match short-circuit bypasses the journal entirely — no
    # command row is created, only a ``cancel_noop`` audit event.
    assert ctx.get_order(cmd_coid) is None
    events = _events_for(ctx, cmd_coid)
    kinds = [k for k, _ in events]
    assert kinds == ['cancel_noop']
    store.close()


def __test_execute_cancel_delete_timeout_flips_to_disposition_unknown__(tmp_path):
    """Network timeout on DELETE → command row stays ``disposition_unknown``."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('error', 'workingorders/wo-1', 'delete'):
            httpx.TimeoutException('DELETE timeout'),
    })
    ctx.upsert_order('coid-w', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='wo-1',
                     extras={'kind': 'working'})
    env = DispatchEnvelope(
        intent=CancelIntent(pine_id='Long', symbol='EURUSD'),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_cancel(env))

    cmd_coid = env.client_order_id(COID_KIND_CANCEL)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'disposition_unknown'
    extras = cmd_row.extras or {}
    assert extras.get('target_coids') == ['coid-w']
    # The target row remained live because the hook never reached its
    # ``close_order`` after the timeout — recovery uses this to verify.
    live_coids = [r.client_order_id for r in ctx.iter_live_orders()]
    assert 'coid-w' in live_coids
    store.close()


def __test_execute_cancel_recovery_targets_vanished_confirms__(tmp_path):
    """All targets gone from broker snapshots → recovery promotes to confirmed."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        # Recovery snapshots — broker reports nothing left.
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
    })
    # Seed a target row mid-flight: it still has an exchange_order_id
    # (the DELETE was sent) but its state is still ``confirmed`` because
    # the hook crashed before closing it.
    ctx.upsert_order('coid-w', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='wo-stale',
                     extras={'kind': 'working'})
    # Seed the cancel command row in ``disposition_unknown`` as if the
    # journal had persisted it before the per-target loop failed.
    cmd_coid = 'cancel-cmd-1'
    ctx.upsert_order(
        cmd_coid, symbol='EURUSD', side='buy', qty=1.0,
        state='disposition_unknown', pine_entry_id='Long',
        extras={
            'kind': KIND_CANCEL,
            'target_coids': ['coid-w'],
        },
    )

    asyncio.run(broker._recover_in_flight_submissions())

    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'confirmed'
    assert (cmd_row.extras or {}).get('reason_path') == 'recovered'
    # Recovery closes the command row (kind-aware _apply_resume_outcome).
    live_coids = [r.client_order_id for r in ctx.iter_live_orders()]
    assert cmd_coid not in live_coids
    store.close()


def __test_execute_cancel_recovery_survivor_keeps_pending__(tmp_path):
    """Any target still alive on broker → still_unknown (engine retries)."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': []},
        # Broker still has wo-1 alive — DELETE never landed.
        ('workingorders', 'get'): {
            'workingOrders': [
                {'workingOrderData': {
                    'dealId': 'wo-1', 'orderLevel': 1.20,
                }},
            ],
        },
        ('history/activity', 'get'): {'activities': []},
    })
    ctx.upsert_order('coid-w', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='wo-1',
                     extras={'kind': 'working'})
    cmd_coid = 'cancel-cmd-2'
    ctx.upsert_order(
        cmd_coid, symbol='EURUSD', side='buy', qty=1.0,
        state='disposition_unknown', pine_entry_id='Long',
        extras={
            'kind': KIND_CANCEL,
            'target_coids': ['coid-w'],
        },
    )

    asyncio.run(broker._recover_in_flight_submissions())

    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    # still_unknown verdict leaves the row in disposition_unknown.
    assert cmd_row.state == 'disposition_unknown'
    # Engine reconciler will re-issue the cancel on next sync.
    store.close()


def __test_execute_cancel_recovery_mid_loop_partial_sweep__(tmp_path):
    """One target swept (row already closed), the other vanished on broker.

    The hook closed ``coid-a`` before the crash; recovery sees that row
    as ``closed`` (which counts as swept) and ``coid-b`` whose exchange
    id is absent from the snapshots (broker confirms it gone too). All
    targets accounted for → confirmed.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
    })
    # coid-a: hook closed it before crash (state='closed')
    ctx.upsert_order('coid-a', symbol='EURUSD', side='buy', qty=1.0,
                     state='closed', pine_entry_id='Long',
                     exchange_order_id='wo-a',
                     extras={'kind': 'working'})
    ctx.close_order('coid-a')
    # coid-b: hook never reached it (state='confirmed', still live).
    ctx.upsert_order('coid-b', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='wo-b',
                     extras={'kind': 'working'})
    cmd_coid = 'cancel-cmd-3'
    ctx.upsert_order(
        cmd_coid, symbol='EURUSD', side='buy', qty=1.0,
        state='disposition_unknown', pine_entry_id='Long',
        extras={
            'kind': KIND_CANCEL,
            'target_coids': ['coid-a', 'coid-b'],
        },
    )

    asyncio.run(broker._recover_in_flight_submissions())

    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'confirmed'
    extras = cmd_row.extras or {}
    assert extras.get('reason_path') == 'recovered'
    # Both targets are listed in applied_target_coids.
    applied = extras.get('applied_target_coids') or []
    assert set(applied) == {'coid-a', 'coid-b'}
    store.close()
