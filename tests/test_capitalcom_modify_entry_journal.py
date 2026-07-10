"""
@pyne

Journal-based ``modify_entry`` contract tests.

The plugin's ``modify_entry`` was migrated to the Core
:class:`~pynecore.core.broker.journal.DispatchJournal` in M4 phase 2:
the PUT/confirm lifecycle now flows through ``run_modify_entry`` with
``_CapitalComModifyEntryHooks`` as the wire-format hook.

These tests pin the externally-observable contract of that migration:

* Happy path: command row reaches ``state='confirmed'`` and is closed
  out of ``iter_live_orders`` (it is a one-shot dispatch record, not a
  persistent order); the target working order's ``extras`` carries the
  echoed ``amended_level``; the canonical audit chain
  (``dispatch_submitted`` → ``deal_reference_seen`` → ``confirmed`` →
  ``order_closed``) is emitted.
* Reject path: confirm ``REJECTED`` flips the command row to
  ``state='rejected'`` and surfaces an ``ExchangeOrderRejectedError``.
* PUT timeout: command row stays ``state='disposition_unknown'`` for
  recovery; the target row's level is NOT mutated.
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
    KIND_MODIFY_ENTRY as COID_KIND_MODIFY_ENTRY,
)
from pynecore.core.broker.models import (
    DispatchEnvelope, EntryIntent, OrderType,
)
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore
from pynecore.core.broker.store_helpers import KIND_MODIFY_ENTRY

from pynecore_capitalcom import CapitalCom, CapitalComConfig


def main():
    pass


_RULES_RESP = {
    'dealingRules': {
        'minStepDistance': {'value': 0.01},
        'minSizeIncrement': {'value': 0.01},
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


def _envelope(qty=1.0, limit=1.20, pine_id='Long'):
    return DispatchEnvelope(
        intent=EntryIntent(
            pine_id=pine_id, symbol='EURUSD', side='buy', qty=qty,
            order_type=OrderType.LIMIT, limit=limit, stop=None,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )


def _seed_target(ctx, *, coid: str, exchange_id: str = 'wo-1', level: float = 1.20):
    ctx.upsert_order(
        coid, symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id=exchange_id,
        extras={'kind': 'working', 'order_type': 'limit',
                'requested_level': level},
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


def __test_modify_entry_happy_path_routes_through_journal__(tmp_path):
    """PUT + confirm ACCEPTED → command row CONFIRMED + target carries new level."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('workingorders/wo-1', 'put'): {'dealReference': 'amend-ref'},
        ('confirms/amend-ref', 'get'): {
            'dealStatus': 'ACCEPTED', 'status': 'OPEN',
            'dealId': 'wo-1', 'level': 1.25,
        },
    })
    old = _envelope(qty=1.0, limit=1.20)
    new = _envelope(qty=1.0, limit=1.25)
    target_coid = old.client_order_id(KIND_ENTRY)
    _seed_target(ctx, coid=target_coid, exchange_id='wo-1', level=1.20)

    result = asyncio.run(broker.modify_entry(old, new))

    assert len(result) == 1
    eo = result[0]
    assert eo.client_order_id == target_coid
    assert eo.id == 'wo-1'
    assert eo.price == 1.25

    # Command row: confirmed and closed (one-shot dispatch, not persistent).
    cmd_coid = new.client_order_id(COID_KIND_MODIFY_ENTRY)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'confirmed'
    live_coids = [r.client_order_id for r in ctx.iter_live_orders()]
    assert cmd_coid not in live_coids
    # Target row stays live and carries the echoed level.
    target_row = ctx.get_order(target_coid)
    assert target_row is not None
    assert (target_row.extras or {}).get('amended_level') == 1.25

    # Audit event order pinned by journal contract.
    events = _events_for(ctx, cmd_coid)
    kinds = [k for k, _ in events]
    assert kinds == [
        'dispatch_submitted',
        'deal_reference_seen',
        'confirmed',
        'order_closed',
    ]
    submit_payload = events[0][1]
    assert submit_payload['kind'] == 'modify_entry'
    assert submit_payload['target_coid'] == target_coid
    assert submit_payload['new_level'] == 1.25
    assert submit_payload['order_type'] == 'limit'
    assert submit_payload['target_exchange_id'] == 'wo-1'

    store.close()


def __test_modify_entry_confirm_rejected_marks_command_row_rejected__(tmp_path):
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('workingorders/wo-1', 'put'): {'dealReference': 'amend-ref'},
        ('confirms/amend-ref', 'get'): {
            'dealStatus': 'REJECTED',
            'reason': 'invalid level',
        },
    })
    old = _envelope(limit=1.20)
    new = _envelope(limit=1.25)
    target_coid = old.client_order_id(KIND_ENTRY)
    _seed_target(ctx, coid=target_coid, exchange_id='wo-1', level=1.20)

    with pytest.raises(ExchangeOrderRejectedError):
        asyncio.run(broker.modify_entry(old, new))

    cmd_coid = new.client_order_id(COID_KIND_MODIFY_ENTRY)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'rejected'
    # Target row is NOT mutated on reject.
    target_row = ctx.get_order(target_coid)
    assert (target_row.extras or {}).get('amended_level') is None
    store.close()


def __test_modify_entry_put_timeout_flips_to_disposition_unknown__(tmp_path):
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('error', 'workingorders/wo-1', 'put'): httpx.TimeoutException('PUT timeout'),
    })
    old = _envelope(limit=1.20)
    new = _envelope(limit=1.25)
    target_coid = old.client_order_id(KIND_ENTRY)
    _seed_target(ctx, coid=target_coid, exchange_id='wo-1', level=1.20)

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_entry(old, new))

    cmd_coid = new.client_order_id(COID_KIND_MODIFY_ENTRY)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'disposition_unknown'
    target_row = ctx.get_order(target_coid)
    assert (target_row.extras or {}).get('amended_level') is None
    store.close()


def __test_modify_entry_confirm_timeout_flips_to_disposition_unknown__(tmp_path):
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('workingorders/wo-1', 'put'): {'dealReference': 'amend-ref'},
        ('error', 'confirms/amend-ref', 'get'):
            httpx.TimeoutException('confirm timeout'),
    })
    old = _envelope(limit=1.20)
    new = _envelope(limit=1.25)
    target_coid = old.client_order_id(KIND_ENTRY)
    _seed_target(ctx, coid=target_coid, exchange_id='wo-1', level=1.20)

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_entry(old, new))

    cmd_coid = new.client_order_id(COID_KIND_MODIFY_ENTRY)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'disposition_unknown'
    store.close()


def __test_modify_entry_recovery_via_snapshot_match__(tmp_path):
    """No deal_reference → recovery uses wo_by_deal_id snapshot.

    The new ``_verdict_modify_entry`` resolves a ``submitted`` modify
    command row by checking the target working-order's current level.
    A match → ``confirmed`` + ``recovery_path='modify_entry_snapshot_match'``.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        # Recovery snapshots
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {
            'workingOrders': [
                {'workingOrderData': {
                    'dealId': 'wo-1', 'orderLevel': 1.25,
                    'dealReference': 'old-ref',
                }},
            ],
        },
        ('history/activity', 'get'): {'activities': []},
    })
    target_coid = 'target-wo'
    _seed_target(ctx, coid=target_coid, exchange_id='wo-1', level=1.20)
    cmd_coid = 'modify-cmd-1'
    ctx.upsert_order(
        cmd_coid, symbol='EURUSD', side='buy', qty=1.0,
        state='submitted', pine_entry_id='Long',
        extras={
            'kind': KIND_MODIFY_ENTRY,
            'target_coid': target_coid,
            'new_level': 1.25,
        },
    )

    asyncio.run(broker._recover_in_flight_submissions())

    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'confirmed'
    # Recovery closes the command row (kind-aware _apply_resume_outcome).
    live_coids = [r.client_order_id for r in ctx.iter_live_orders()]
    assert cmd_coid not in live_coids
    store.close()


def __test_modify_entry_recovery_no_target_keeps_pending__(tmp_path):
    """Target row missing → still_unknown (engine retries on next sync)."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
    })
    cmd_coid = 'orphan-modify'
    ctx.upsert_order(
        cmd_coid, symbol='EURUSD', side='buy', qty=1.0,
        state='submitted', pine_entry_id='Long',
        extras={
            'kind': KIND_MODIFY_ENTRY,
            'target_coid': 'nonexistent',
            'new_level': 1.25,
        },
    )

    asyncio.run(broker._recover_in_flight_submissions())

    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    # still_unknown → row stays as-is (engine reconciler retries).
    assert cmd_row.state == 'submitted'
    store.close()
