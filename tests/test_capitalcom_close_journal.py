"""
@pyne

Journal-based ``execute_close`` contract tests.

The plugin's ``execute_close`` was migrated to the Core
:class:`~pynecore.core.broker.journal.DispatchJournal` in M4 phase 4:
both branches (full close and emulated partial close) now flow through
``run_close`` with ``_CapitalComCloseHooks`` as the wire-format hook.

These tests pin the externally-observable contract of that migration:

* **Full close happy path**: the command row reaches ``state='closing'``,
  every target's exchange ``dealId`` is recorded under
  ``extras['targets']``, and the audit chain
  (``dispatch_submitted`` → ``close_dispatched``) lands on the command
  row while per-target ``close_dispatched`` events mirror onto the
  underlying position rows.
* **Partial close happy path**: opposite-side POST → confirm GET →
  post-snapshot reconcile lands a clean unit-count delta → the command
  row reaches ``state='confirmed'`` and gets closed; the audit chain
  ends in ``confirmed`` + ``order_closed``.
* **Full close DELETE timeout**: the hook raises
  :class:`BrokerManualInterventionError` rather than parking — the
  sync engine cannot verify a parked close in-session
  (``get_open_orders`` only returns working orders, not positions), so
  halting hands the ambiguity to the operator while ``targets`` in
  extras + the next-restart positions snapshot drive recovery.
* **Partial close race outside ±3 s window**: the hook raises
  :class:`BrokerManualInterventionError`; the journal does NOT catch
  that — the propagation matches the pre-journal semantics.
* **Partial close race resolved via confirm ``affectedDeals``**: the
  fresh reverse leg is identified deterministically by the ``dealId``
  the confirm attributes to our POST — no clock involved — and the
  corrective DELETE goes out even when ``createdDateUTC`` is stale.
* **Partial close race, confirm names a different deal**: a fresh
  opposite row NOT listed in the confirm's ``affectedDeals`` is
  external (manual/other) — the hook halts instead of deleting it.
* **Partial close race, confirm TTL-expired**: fallback identification
  via the POST-anchored ``createdDateUTC`` band still issues the
  corrective DELETE.
* **Full close recovery**: when every target's ``dealId`` is gone from
  the positions snapshot, recovery promotes the command row to
  ``closing`` with ``recovery_path='full_close_targets_vanished'``.
* **Full close recovery survivor**: any target still in the snapshot
  → ``still_unknown``; the engine reconciler retries on next sync.
* **Partial close recovery via deal_reference + unit delta**: a
  ``server_ref_seen`` command row whose ``confirms`` GET reports
  ``ACCEPTED`` and whose persisted ``pre_total_units`` minus
  ``intent_units`` matches the current symbol unit total → promoted to
  ``confirmed`` with ``recovery_path='partial_close_units_match'``.
"""
import asyncio
from datetime import UTC, datetime

import httpx
import pytest

from pynecore.core.broker.exceptions import BrokerManualInterventionError
from pynecore.core.broker.idempotency import KIND_CLOSE
from pynecore.core.broker.models import CloseIntent, DispatchEnvelope
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore
from pynecore.core.broker.store_helpers import (
    KIND_FULL_CLOSE,
    KIND_PARTIAL_CLOSE,
)

from pynecore_capitalcom import CapitalCom, CapitalComConfig
from pynecore_capitalcom.exceptions import OrderNotFoundError


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
        value = self._responses.get((endpoint, method), {})
        # If the response is a list, pop the next entry (sequential
        # responses for repeated calls — e.g. partial-close pre- vs
        # post-snapshot ``GET /positions``).
        if isinstance(value, list):
            if not value:
                return {}
            return value.pop(0)
        return value


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


def __test_execute_close_full_happy_path_routes_through_journal__(tmp_path):
    """Full close → command row CONFIRMED→CLOSING + per-target audit events."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-L', 'delete'): {},
    })
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='deal-L', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='Long', symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    result = asyncio.run(broker.execute_close(env))
    assert result.id == 'deal-L'

    cmd_coid = env.client_order_id(KIND_CLOSE)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'closing'
    extras = cmd_row.extras or {}
    assert extras.get('kind') == KIND_FULL_CLOSE
    assert extras.get('targets') == ['deal-L']

    # Target row mirrored to ``closing`` and remains live until the
    # activity stream promotes it to ``closed``.
    target_row = ctx.get_order('coid-entry')
    assert target_row is not None
    assert target_row.state == 'closing'

    cmd_events = _events_for(ctx, cmd_coid)
    cmd_kinds = [k for k, _ in cmd_events]
    assert 'dispatch_submitted' in cmd_kinds
    assert 'close_dispatched' in cmd_kinds
    submit_payload = next(p for k, p in cmd_events if k == 'dispatch_submitted')
    assert submit_payload['kind'] == KIND_FULL_CLOSE
    assert submit_payload['targets'] == ['deal-L']
    assert submit_payload['pine_id'] == 'Long'

    target_events = _events_for(ctx, 'coid-entry')
    target_kinds = [k for k, _ in target_events]
    assert 'close_dispatched' in target_kinds

    # DELETE went out to the broker.
    assert any(
        c[0] == 'positions/deal-L' and c[1] == 'delete'
        for c in broker._calls
    )
    store.close()


def __test_execute_close_full_delete_timeout_halts__(tmp_path):
    """Network timeout on DELETE → ``BrokerManualInterventionError``.

    The sync engine cannot verify a parked close in-session
    (``get_open_orders`` only returns working orders, not positions),
    so the hook halts and lets recovery on next start drive the
    contract via the persisted ``targets`` extras + the positions
    snapshot.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('error', 'positions/deal-L', 'delete'):
            httpx.TimeoutException('DELETE timeout'),
    })
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='deal-L', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='Long', symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(BrokerManualInterventionError):
        asyncio.run(broker.execute_close(env))

    cmd_coid = env.client_order_id(KIND_CLOSE)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    # The journal does not catch ``BrokerManualInterventionError`` so
    # the command row stays at ``submitted`` — operator intervention
    # / next-restart recovery resolves the state.
    assert cmd_row.state == 'submitted'
    extras = cmd_row.extras or {}
    assert extras.get('kind') == KIND_FULL_CLOSE
    # Targets captured in extras so recovery can reconcile against the
    # next-start positions snapshot.
    assert extras.get('targets') == ['deal-L']

    # Target row was NOT advanced — DELETE never landed.
    target_row = ctx.get_order('coid-entry')
    assert target_row is not None
    assert target_row.state == 'confirmed'
    store.close()


def __test_execute_close_partial_happy_path_routes_through_journal__(tmp_path):
    """Partial close → command row CONFIRMED + audit + closed out."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        # Sequential snapshots: pre-POST = size 2.0, post-POST = size 1.0
        # (broker has netted the opposite leg internally).
        ('positions', 'get'): [
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY',
                              'size': 2.0}},
            ]},
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY',
                              'size': 1.0}},
            ]},
        ],
        ('positions', 'post'): {'dealReference': 'dr-1'},
        ('confirms/dr-1', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=2.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='orig', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='Long', symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    result = asyncio.run(broker.execute_close(env))
    assert result.id == 'dr-1'

    cmd_coid = env.client_order_id(KIND_CLOSE)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'confirmed'
    extras = cmd_row.extras or {}
    assert extras.get('kind') == KIND_PARTIAL_CLOSE
    assert extras.get('deal_reference') == 'dr-1'
    assert extras.get('pre_total_units') == 200  # 2.0 / 0.01 lot_step
    assert extras.get('intent_units') == 100

    live_coids = [r.client_order_id for r in ctx.iter_live_orders()]
    assert cmd_coid not in live_coids

    cmd_events = _events_for(ctx, cmd_coid)
    cmd_kinds = [k for k, _ in cmd_events]
    assert cmd_kinds[0] == 'dispatch_submitted'
    assert 'deal_reference_seen' in cmd_kinds
    assert 'confirmed' in cmd_kinds
    assert 'order_closed' in cmd_kinds

    store.close()


def __test_execute_close_partial_race_outside_window_halts__(tmp_path):
    """Race detected + no fresh-leg candidate within ±3 s → BrokerManualInterventionError."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'orig', 'direction': 'BUY', 'size': 2.0}},
            {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'fresh', 'direction': 'SELL', 'size': 1.0,
                          'createdDateUTC': '1970-01-01T00:00:00.000'}},
        ]},
        ('positions', 'post'): {'dealReference': 'dr-2'},
        ('confirms/dr-2', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=2.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='orig', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='Long', symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(BrokerManualInterventionError):
        asyncio.run(broker.execute_close(env))

    # ``BrokerManualInterventionError`` propagates uncaught through the
    # journal, so the command row's state did NOT advance past the
    # initial ``submitted`` write. The hook's mid-flow ``add_ref``
    # stored the ``deal_reference`` in ``order_refs`` so the operator
    # can trace the POST that landed.
    cmd_coid = env.client_order_id(KIND_CLOSE)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'submitted'
    extras = cmd_row.extras or {}
    assert extras.get('kind') == KIND_PARTIAL_CLOSE
    # pre_total_units / intent_units were captured before the POST.
    assert extras.get('pre_total_units') == 300
    assert extras.get('intent_units') == 100

    refs = dict(ctx.iter_refs_for_coid(cmd_coid))
    assert refs.get('deal_reference') == 'dr-2'

    store.close()


def _now_iso() -> str:
    """Capital.com-style ``createdDateUTC`` stamp for the current time."""
    return datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]


def __test_execute_close_partial_race_confirm_deal_id_corrects__(tmp_path):
    """Fresh reverse leg named by confirm ``affectedDeals`` → corrective DELETE.

    ``createdDateUTC`` is deliberately stale (epoch) — the deterministic
    confirm-``dealId`` discriminator must win without any time-band
    involvement.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): [
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY',
                              'size': 2.0}},
            ]},
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY',
                              'size': 2.0}},
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'fresh', 'direction': 'SELL',
                              'size': 1.0,
                              'createdDateUTC': '1970-01-01T00:00:00.000'}},
            ]},
        ],
        ('positions', 'post'): {'dealReference': 'dr-3'},
        ('confirms/dr-3', 'get'): {
            'dealStatus': 'ACCEPTED',
            'affectedDeals': [{'dealId': 'fresh', 'status': 'OPENED'}],
        },
        ('positions/fresh', 'delete'): {},
    })
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=2.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='orig', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='Long', symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    result = asyncio.run(broker.execute_close(env))
    assert result.id == 'dr-3'

    assert any(
        c[0] == 'positions/fresh' and c[1] == 'delete'
        for c in broker._calls
    )
    cmd_coid = env.client_order_id(KIND_CLOSE)
    cmd_kinds = [k for k, _ in _events_for(ctx, cmd_coid)]
    assert 'partial_close_corrective_delete' in cmd_kinds
    store.close()


def __test_execute_close_partial_race_confirm_mismatch_halts__(tmp_path):
    """Fresh opposite row NOT in confirm ``affectedDeals`` → halt, no DELETE.

    The row is inside the time band (created "now"), but the confirm
    attributes a different ``dealId`` to our POST — the fresh row is
    someone else's position and must not be deleted.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): [
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY',
                              'size': 2.0}},
            ]},
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY',
                              'size': 2.0}},
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'external', 'direction': 'SELL',
                              'size': 1.0,
                              'createdDateUTC': _now_iso()}},
            ]},
        ],
        ('positions', 'post'): {'dealReference': 'dr-4'},
        ('confirms/dr-4', 'get'): {
            'dealStatus': 'ACCEPTED',
            'affectedDeals': [
                {'dealId': 'netted-elsewhere', 'status': 'PARTIALLY_CLOSED'},
            ],
        },
    })
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=2.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='orig', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='Long', symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(BrokerManualInterventionError):
        asyncio.run(broker.execute_close(env))

    assert not any(c[1] == 'delete' for c in broker._calls)
    store.close()


def __test_execute_close_partial_race_ttl_fallback_post_anchor__(tmp_path):
    """Confirm TTL-expired → POST-anchored time-band fallback still corrects."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): [
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY',
                              'size': 2.0}},
            ]},
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY',
                              'size': 2.0}},
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'fresh', 'direction': 'SELL',
                              'size': 1.0,
                              'createdDateUTC': _now_iso()}},
            ]},
        ],
        ('positions', 'post'): {'dealReference': 'dr-5'},
        ('error', 'confirms/dr-5', 'get'): OrderNotFoundError(
            'confirm TTL expired', ref_type='deal_reference',
        ),
        ('positions/fresh', 'delete'): {},
    })
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=2.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='orig', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='Long', symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    result = asyncio.run(broker.execute_close(env))
    assert result.id == 'dr-5'

    assert any(
        c[0] == 'positions/fresh' and c[1] == 'delete'
        for c in broker._calls
    )
    store.close()


def __test_execute_close_full_recovery_targets_vanished_confirms__(tmp_path):
    """All targets gone from positions snapshot → recovery promotes to ``closing``."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
    })
    # Live entry row whose dealId vanished from the broker.
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='deal-vanished',
                     extras={'kind': 'position'})
    # Seed the full_close command row in ``disposition_unknown``.
    cmd_coid = 'close-cmd-1'
    ctx.upsert_order(
        cmd_coid, symbol='EURUSD', side='sell', qty=1.0,
        state='disposition_unknown', pine_entry_id='Long',
        extras={
            'kind': KIND_FULL_CLOSE,
            'targets': ['deal-vanished'],
        },
    )

    asyncio.run(broker._recover_in_flight_submissions())

    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'closing'
    extras = cmd_row.extras or {}
    assert extras.get('recovery_path') == 'full_close_targets_vanished'
    assert extras.get('targets') == ['deal-vanished']

    # Target entry row mirrored to ``closing``.
    target = ctx.get_order('coid-entry')
    assert target is not None
    assert target.state == 'closing'
    store.close()


def __test_execute_close_full_recovery_survivor_keeps_pending__(tmp_path):
    """Any target still in snapshots → still_unknown (engine retries)."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'deal-alive', 'direction': 'BUY',
                          'size': 1.0}},
        ]},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
    })
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='deal-alive',
                     extras={'kind': 'position'})
    cmd_coid = 'close-cmd-2'
    ctx.upsert_order(
        cmd_coid, symbol='EURUSD', side='sell', qty=1.0,
        state='disposition_unknown', pine_entry_id='Long',
        extras={
            'kind': KIND_FULL_CLOSE,
            'targets': ['deal-alive'],
        },
    )

    asyncio.run(broker._recover_in_flight_submissions())

    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    # still_unknown leaves the row in disposition_unknown.
    assert cmd_row.state == 'disposition_unknown'
    # Target row is untouched — broker still has it alive.
    target = ctx.get_order('coid-entry')
    assert target is not None
    assert target.state == 'confirmed'
    store.close()


def __test_execute_close_partial_recovery_units_match_confirms__(tmp_path):
    """confirm GET ACCEPTED + units == pre - intent → confirmed."""
    from time import time as epoch_time

    from pynecore_capitalcom.models import _InstrumentRules

    broker, store, ctx = _make_broker(tmp_path, responses={
        # Recovery snapshots — broker now reports the netted total only.
        ('positions', 'get'): {'positions': [
            {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'orig', 'direction': 'BUY', 'size': 1.0}},
        ]},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
        ('confirms/dr-rec', 'get'): {'dealStatus': 'ACCEPTED',
                                     'dealId': 'opposite-leg'},
    })
    # Seed the instrument rules cache so the verdict-builder can compute
    # the unit delta without a separate REST fetch.
    broker._instrument_rules_cache['EURUSD'] = _InstrumentRules(
        epic='EURUSD', lot_step=0.01, min_size=0.01,
        min_stop_or_limit_distance=0.0001, fetched_at=epoch_time(),
    )

    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=2.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='orig', extras={'kind': 'position'})
    cmd_coid = 'close-cmd-partial-1'
    ctx.upsert_order(
        cmd_coid, symbol='EURUSD', side='sell', qty=1.0,
        state='server_ref_seen', pine_entry_id='Long',
        extras={
            'kind': KIND_PARTIAL_CLOSE,
            'deal_reference': 'dr-rec',
            'pre_total_units': 200,
            'intent_units': 100,
        },
    )
    ctx.add_ref(cmd_coid, 'deal_reference', 'dr-rec')

    asyncio.run(broker._recover_in_flight_submissions())

    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'confirmed'
    extras = cmd_row.extras or {}
    assert extras.get('recovery_path') == 'partial_close_units_match'

    # Recovery closes the command row (kind-aware _apply_resume_outcome).
    live_coids = [r.client_order_id for r in ctx.iter_live_orders()]
    assert cmd_coid not in live_coids
    store.close()
