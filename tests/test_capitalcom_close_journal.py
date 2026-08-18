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
* **Full close DELETE timeout**: the hook parks the command as
  ``disposition_unknown``; ``targets`` in extras and the next-restart
  positions snapshot drive deterministic recovery without a duplicate DELETE.
* **Partial close race without confirm attribution**: the hook raises
  :class:`BrokerManualInterventionError` and never deletes a candidate
  reverse row from timestamp proximity alone.
* **Partial close race resolved via confirm ``affectedDeals``**: the
  fresh reverse leg is identified deterministically by the ``dealId``
  the confirm attributes to our POST — no clock involved — and the
  corrective DELETE goes out even when ``createdDateUTC`` is stale.
* **Partial close race, confirm names a different deal**: a fresh
  opposite row NOT listed in the confirm's ``affectedDeals`` is
  external (manual/other) — the hook halts instead of deleting it.
* **Partial close race, confirm TTL-expired**: no corrective DELETE is
  authorized because the venue no longer supplies exact deal attribution.
* **Full close recovery**: when every target's ``dealId`` is gone from
  the positions snapshot, recovery promotes the command row to
  ``closing`` with ``recovery_path='full_close_targets_vanished'``.
* **Full close recovery survivor**: any target still in the snapshot
  → ``still_unknown``; the engine reconciler retries on next sync.
* **Partial close recovery via exact target residual**: a
  ``server_ref_seen`` command row whose ``confirms`` GET reports
  ``ACCEPTED`` and whose persisted target deal remains in the original
  direction at ``pre_target_units - intent_units`` → promoted to
  ``confirmed`` with ``recovery_path='partial_close_target_match'``.
"""
import asyncio
import httpx
import pytest

from pynecore.core.broker.exceptions import (
    BrokerManualInterventionError,
    ExchangeOrderRejectedError,
    OrderDispositionUnknownError,
)
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

    def _fake_response(self, endpoint, *, data=None, method='post'):
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

    def _call_serialized_write(self, endpoint, *, data=None, method='post'):
        return self._fake_response(endpoint, data=data, method=method)

    async def _call(self, endpoint, *, data=None, method='post'):
        return self._fake_response(endpoint, data=data, method=method)


class _InjectSameSideBeforePostBroker(_FakeBroker):
    """Expose another same-side deal only at the hook's pre-POST snapshot."""

    def __init__(self, *, config=None, responses=None):
        super().__init__(config=config, responses=responses)
        self._position_gets = 0

    async def _call(self, endpoint, *, data=None, method='post'):
        if endpoint == 'positions' and method == 'get':
            self._position_gets += 1
        return await super()._call(endpoint, data=data, method=method)

    def _call_serialized_write(self, endpoint, *, data=None, method='post'):
        if endpoint == 'positions' and method == 'get':
            self._position_gets += 1
            self._calls.append((endpoint, method, data))
            positions = [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'deal-A', 'direction': 'BUY', 'size': 2.0}},
            ]
            if self._position_gets >= 2:
                positions.append(
                    {'market': {'epic': 'EURUSD'},
                     'position': {'dealId': 'deal-B', 'direction': 'BUY', 'size': 1.0}},
                )
            return {'positions': positions}
        if endpoint == 'positions' and method == 'post':
            self._calls.append((endpoint, method, data))
            return {'dealReference': 'should-not-write'}
        return self._fake_response(endpoint, data=data, method=method)


class _ShrinkBeforePartialPostBroker(_FakeBroker):
    """Report a smaller target only at the serialized pre-POST snapshot."""

    def __init__(self, *, config=None, responses=None):
        super().__init__(config=config, responses=responses)
        self._position_gets = 0

    async def _call(self, endpoint, *, data=None, method='post'):
        if endpoint == 'positions' and method == 'get':
            self._position_gets += 1
        return await super()._call(endpoint, data=data, method=method)

    def _call_serialized_write(self, endpoint, *, data=None, method='post'):
        if endpoint == 'positions' and method == 'get':
            self._position_gets += 1
            self._calls.append((endpoint, method, data))
            size = 2.0 if self._position_gets == 1 else 1.0
            return {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'deal-A', 'direction': 'BUY', 'size': size}},
            ]}
        if endpoint == 'positions' and method == 'post':
            self._calls.append((endpoint, method, data))
            return {'dealReference': 'should-not-write'}
        return self._fake_response(endpoint, data=data, method=method)


def _make_broker(tmp_path, *, responses=None, broker_type=_FakeBroker):
    resp: dict = {('markets/EURUSD', 'get'): _RULES_RESP}
    if responses:
        resp.update(responses)
    config = CapitalComConfig(
        demo=True,
        user_email='test@example.com',
        api_key='k',
        api_password='p',
    )
    broker = broker_type(config=config, responses=resp)
    # A live plugin always carries its provider symbol; startup adoption is
    # scoped to it, so the fixture must set it the way ``pyne run`` does.
    broker.symbol = 'EURUSD'
    broker._account_id = 'capitalcom-demo-test-account'
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


def __test_execute_close_keyed_targets_only_owned_entry__(tmp_path):
    """A keyed close DELETEs only the position opened by that Pine entry id."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-A', 'delete'): {},
    })
    ctx.upsert_order('coid-entry-a', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='A',
                     exchange_order_id='deal-A', extras={'kind': 'position'})
    ctx.upsert_order('coid-entry-b', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='B',
                     exchange_order_id='deal-B', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='A', symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    result = asyncio.run(broker.execute_close(env))
    assert result.id == 'deal-A'
    assert [call for call in broker._calls if call[1] in {'delete', 'post'}] == [
        ('positions/deal-A', 'delete', None),
    ]

    cmd_row = ctx.get_order(env.client_order_id(KIND_CLOSE))
    assert cmd_row is not None
    assert (cmd_row.extras or {}).get('kind') == KIND_FULL_CLOSE
    assert (cmd_row.extras or {}).get('targets') == ['deal-A']
    entry_a = ctx.get_order('coid-entry-a')
    entry_b = ctx.get_order('coid-entry-b')
    assert entry_a is not None and entry_a.state == 'closing'
    assert entry_b is not None and entry_b.state == 'confirmed'
    store.close()


def __test_execute_close_all_targets_every_owned_entry__(tmp_path):
    """An empty Pine id keeps ``strategy.close_all`` symbol-wide."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-A', 'delete'): {},
        ('positions/deal-B', 'delete'): {},
    })
    ctx.upsert_order('coid-entry-a', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='A',
                     exchange_order_id='deal-A', extras={'kind': 'position'})
    ctx.upsert_order('coid-entry-b', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='B',
                     exchange_order_id='deal-B', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='', symbol='EURUSD', side='sell', qty=2.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    result = asyncio.run(broker.execute_close(env))
    assert result.id == 'deal-A'
    assert [call for call in broker._calls if call[1] in {'delete', 'post'}] == [
        ('positions/deal-A', 'delete', None),
        ('positions/deal-B', 'delete', None),
    ]

    cmd_row = ctx.get_order(env.client_order_id(KIND_CLOSE))
    assert cmd_row is not None
    assert (cmd_row.extras or {}).get('kind') == KIND_FULL_CLOSE
    assert (cmd_row.extras or {}).get('targets') == ['deal-A', 'deal-B']
    store.close()


def __test_execute_close_unknown_key_does_not_fall_back_symbol_wide__(tmp_path):
    """An unknown keyed close cannot reduce another entry on the symbol."""
    broker, store, ctx = _make_broker(tmp_path)
    ctx.upsert_order('coid-entry-a', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='A',
                     exchange_order_id='deal-A', extras={'kind': 'position'})
    ctx.upsert_order('coid-entry-b', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='B',
                     exchange_order_id='deal-B', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='missing', symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(ExchangeOrderRejectedError,
                       match='no confirmed position rows'):
        asyncio.run(broker.execute_close(env))

    assert not any(call[1] in {'delete', 'post'} for call in broker._calls)
    entry_a = ctx.get_order('coid-entry-a')
    entry_b = ctx.get_order('coid-entry-b')
    assert entry_a is not None and entry_a.state == 'confirmed'
    assert entry_b is not None and entry_b.state == 'confirmed'
    store.close()


def __test_execute_close_keyed_fractional_multi_deal_rejects_without_write__(tmp_path):
    """Fractional keyed close cannot use a symbol-wide POST across two deals."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'deal-A1', 'direction': 'BUY', 'size': 1.0}},
            {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'deal-A2', 'direction': 'BUY', 'size': 1.0}},
        ]},
    })
    ctx.upsert_order('coid-entry-a1', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='A',
                     exchange_order_id='deal-A1', extras={'kind': 'position'})
    ctx.upsert_order('coid-entry-a2', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='A',
                     exchange_order_id='deal-A2', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='A', symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(ExchangeOrderRejectedError,
                       match='partial close requires one exclusive'):
        asyncio.run(broker.execute_close(env))

    assert not any(call[1] in {'delete', 'post'} for call in broker._calls)
    entry_a1 = ctx.get_order('coid-entry-a1')
    entry_a2 = ctx.get_order('coid-entry-a2')
    assert entry_a1 is not None and entry_a1.state == 'confirmed'
    assert entry_a2 is not None and entry_a2.state == 'confirmed'
    store.close()


def __test_execute_close_adopted_position_accepts_restart_key__(tmp_path):
    """A startup-adopted row remains closeable through the script's entry id."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-adopted', 'delete'): {},
    })
    ctx.upsert_order(
        '__pyne_adopted__EURUSD__deal-adopted',
        symbol='EURUSD', side='buy', qty=1.0, filled_qty=1.0,
        state='confirmed', exchange_order_id='deal-adopted',
        extras={
            'kind': 'position',
            'entry_filled_at': 1.0,
            'adopted_startup': True,
        },
    )
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='LAB-CAP-RESTART-POSITION',
            symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    result = asyncio.run(broker.execute_close(env))
    assert result.id == 'deal-adopted'
    assert [call for call in broker._calls if call[1] in {'delete', 'post'}] == [
        ('positions/deal-adopted', 'delete', None),
    ]
    store.close()


def __test_execute_close_defensive_targets_exact_position_coid__(tmp_path):
    """A defensive close cannot sweep another same-symbol position row."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-A', 'delete'): {},
    })
    ctx.upsert_order('coid-entry-a', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='A',
                     exchange_order_id='deal-A', extras={'kind': 'position'})
    ctx.upsert_order('coid-entry-b', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='B',
                     exchange_order_id='deal-B', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='__pyne_defensive_close__coid-entry-a',
            symbol='EURUSD', side='sell', qty=1.0,
            synthetic_kind='defensive_close',
            target_position_coid='coid-entry-a',
            target_exchange_id='deal-A',
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    result = asyncio.run(broker.execute_close(env))
    assert result.id == 'deal-A'
    assert [call for call in broker._calls if call[1] in {'delete', 'post'}] == [
        ('positions/deal-A', 'delete', None),
    ]
    entry_a = ctx.get_order('coid-entry-a')
    entry_b = ctx.get_order('coid-entry-b')
    assert entry_a is not None and entry_a.state == 'closing'
    assert entry_b is not None and entry_b.state == 'confirmed'
    store.close()


def __test_execute_close_orphan_defensive_missing_exact_deal_does_not_replace__(tmp_path):
    """An orphan defensive close never substitutes an equal-sized new deal."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'replacement', 'direction': 'BUY',
                          'size': 1.0}},
        ]},
    })
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='__pyne_defensive_close__orphan',
            symbol='EURUSD', side='sell', qty=1.0,
            synthetic_kind='defensive_close',
            target_position_coid='__pyne_orphan__EURUSD__Long',
            target_exchange_id='original',
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(ExchangeOrderRejectedError,
                       match='no confirmed position rows'):
        asyncio.run(broker.execute_close(env))

    assert not any(call[1] == 'delete' for call in broker._calls)
    assert ctx.find_by_ref('deal_id', 'replacement') is None
    store.close()


def __test_execute_close_keyed_fractional_with_other_entry_rejects__(tmp_path):
    """A one-deal target still cannot be partially reduced beside another entry."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'deal-A', 'direction': 'BUY', 'size': 2.0}},
            {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'deal-B', 'direction': 'BUY', 'size': 1.0}},
        ]},
    })
    ctx.upsert_order('coid-entry-a', symbol='EURUSD', side='buy', qty=2.0,
                     state='confirmed', pine_entry_id='A',
                     exchange_order_id='deal-A', extras={'kind': 'position'})
    ctx.upsert_order('coid-entry-b', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='B',
                     exchange_order_id='deal-B', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='A', symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(ExchangeOrderRejectedError,
                       match='partial close requires one exclusive'):
        asyncio.run(broker.execute_close(env))

    assert not any(call[1] in {'delete', 'post'} for call in broker._calls)
    entry_a = ctx.get_order('coid-entry-a')
    entry_b = ctx.get_order('coid-entry-b')
    assert entry_a is not None and entry_a.state == 'confirmed'
    assert entry_b is not None and entry_b.state == 'confirmed'
    store.close()


def __test_execute_close_partial_revalidates_exclusivity_before_post__(tmp_path):
    """A newly-visible same-side deal blocks the non-targetable reduce POST."""
    broker, store, ctx = _make_broker(
        tmp_path, broker_type=_InjectSameSideBeforePostBroker,
    )
    ctx.upsert_order('coid-entry-a', symbol='EURUSD', side='buy', qty=2.0,
                     state='confirmed', pine_entry_id='A',
                     exchange_order_id='deal-A', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='A', symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(ExchangeOrderRejectedError,
                       match='immediately before dispatch'):
        asyncio.run(broker.execute_close(env))

    assert not any(
        call[0] == 'positions' and call[1] == 'post'
        for call in broker._calls
    )
    entry_a = ctx.get_order('coid-entry-a')
    assert entry_a is not None and entry_a.state == 'confirmed'
    store.close()


def __test_execute_close_partial_rejects_if_target_shrinks_before_post__(tmp_path):
    """A close equal to the fresh target size must not reverse the account."""
    broker, store, ctx = _make_broker(
        tmp_path, broker_type=_ShrinkBeforePartialPostBroker,
    )
    ctx.upsert_order('coid-entry-a', symbol='EURUSD', side='buy', qty=2.0,
                     state='confirmed', pine_entry_id='A',
                     exchange_order_id='deal-A', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='A', symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(ExchangeOrderRejectedError,
                       match='smaller than the target'):
        asyncio.run(broker.execute_close(env))

    assert not any(
        call[0] == 'positions' and call[1] == 'post'
        for call in broker._calls
    )
    entry_a = ctx.get_order('coid-entry-a')
    assert entry_a is not None and entry_a.state == 'confirmed'
    store.close()


def __test_execute_close_reserved_prefix_adopted_key_is_not_defensive__(tmp_path):
    """An unmatched script key with a reserved prefix still closes adopted exposure."""
    real_key = '__pyne_defensive_close__coid-entry-b'
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-adopted', 'delete'): {},
    })
    ctx.upsert_order(
        '__pyne_adopted__EURUSD__deal-adopted',
        symbol='EURUSD', side='buy', qty=1.0, filled_qty=1.0,
        state='confirmed', exchange_order_id='deal-adopted',
        extras={'kind': 'position', 'adopted_startup': True},
    )
    ctx.upsert_order('coid-entry-b', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='B',
                     exchange_order_id='deal-B', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id=real_key, symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    result = asyncio.run(broker.execute_close(env))
    assert result.id == 'deal-adopted'
    assert [call for call in broker._calls if call[1] in {'delete', 'post'}] == [
        ('positions/deal-adopted', 'delete', None),
    ]
    adopted = ctx.get_order('__pyne_adopted__EURUSD__deal-adopted')
    entry_b = ctx.get_order('coid-entry-b')
    assert adopted is not None and adopted.state == 'closing'
    assert entry_b is not None and entry_b.state == 'confirmed'
    store.close()


def __test_execute_close_synthetic_exit_targets_parent_entry__(tmp_path):
    """A marketable synthetic exit inherits keyed ownership from ``from_entry``."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-A', 'delete'): {},
    })
    ctx.upsert_order('coid-entry-a', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='A',
                     exchange_order_id='deal-A', extras={'kind': 'position'})
    ctx.upsert_order('coid-entry-b', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='B',
                     exchange_order_id='deal-B', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='__pyne_marketable_exit__exit-a\0A',
            symbol='EURUSD', side='sell', qty=1.0,
            synthetic_kind='marketable_exit',
            target_entry_id='A',
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    result = asyncio.run(broker.execute_close(env))
    assert result.id == 'deal-A'
    assert [call for call in broker._calls if call[1] in {'delete', 'post'}] == [
        ('positions/deal-A', 'delete', None),
    ]
    entry_a = ctx.get_order('coid-entry-a')
    entry_b = ctx.get_order('coid-entry-b')
    assert entry_a is not None and entry_a.state == 'closing'
    assert entry_b is not None and entry_b.state == 'confirmed'
    store.close()


def __test_execute_close_full_delete_timeout_parks_for_recovery__(tmp_path):
    """Network timeout on DELETE parks the persisted close command."""
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

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_close(env))

    cmd_coid = env.client_order_id(KIND_CLOSE)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'disposition_unknown'
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
        # Sequential snapshots: route-selection = size 2.0, pre-POST =
        # size 2.0, post-POST = size 1.0 (broker netted the opposite leg).
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
    assert extras.get('target_deal_id') == 'orig'
    assert extras.get('target_direction') == 'BUY'
    assert extras.get('pre_target_units') == 200  # 2.0 / 0.01 lot_step
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


def __test_execute_close_partial_retires_journal_exposure_cursor__(tmp_path):
    """Partial close reduces the target row's ``filled_qty`` cursor.

    The journal's ``filled_qty`` doubles as the run-owned exposure cursor
    (K3 cycle-end book, adoption clamp, fold-netting ``retired_exposure``).
    Measured live on cycle 32: without this retirement the next reversal
    fold over-netted against the stale pre-partial cursor, closed the fold
    row at retained=0, orphaned its own entry activity and ended the cycle
    at venue 0.01 vs journal 0. ``qty`` must stay the original volume.
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
                     filled_qty=2.0, state='confirmed', pine_entry_id='Long',
                     exchange_order_id='orig', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='Long', symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    asyncio.run(broker.execute_close(env))

    target_row = ctx.get_order('coid-entry')
    assert target_row is not None
    assert abs(target_row.filled_qty - 1.0) < 1e-9, (
        "the exposure cursor must drop by the closed amount"
    )
    assert abs(target_row.qty - 2.0) < 1e-9, (
        "qty stays the original traded volume"
    )
    extras = target_row.extras or {}
    assert extras.get('partial_close_retired_at') is not None

    events = _events_for(ctx, 'coid-entry')
    retired = [p for k, p in events if k == 'partial_close_journal_retired']
    assert len(retired) == 1
    assert abs(retired[0]['retired_exposure'] - 1.0) < 1e-9
    assert abs(retired[0]['remaining_exposure'] - 1.0) < 1e-9

    store.close()


def __test_execute_close_partial_target_disappears_reverse_is_not_fill__(tmp_path):
    """A same-total reverse row is corrected but never reported as a fill."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): [
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY', 'size': 2.0}},
            ]},
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY', 'size': 2.0}},
            ]},
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'reverse', 'direction': 'SELL', 'size': 1.0}},
            ]},
            {'positions': []},
        ],
        ('positions', 'post'): {'dealReference': 'dr-reverse'},
        ('confirms/dr-reverse', 'get'): {
            'dealStatus': 'ACCEPTED',
            'affectedDeals': [{'dealId': 'reverse', 'status': 'OPENED'}],
        },
        ('positions/reverse', 'delete'): {},
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

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_close(env))

    assert ('positions/reverse', 'delete', None) in broker._calls
    cmd_coid = env.client_order_id(KIND_CLOSE)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None and cmd_row.state == 'disposition_unknown'
    assert 'confirmed' not in [kind for kind, _ in _events_for(ctx, cmd_coid)]
    store.close()


def __test_execute_close_partial_replacement_row_cannot_prove_target__(tmp_path):
    """An equal-sized replacement row cannot satisfy exact-target proof."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): [
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY', 'size': 2.0}},
            ]},
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY', 'size': 2.0}},
            ]},
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'replacement', 'direction': 'BUY',
                              'size': 1.0}},
            ]},
        ],
        ('positions', 'post'): {'dealReference': 'dr-replacement'},
        ('confirms/dr-replacement', 'get'): {'dealStatus': 'ACCEPTED'},
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

    with pytest.raises(BrokerManualInterventionError,
                       match='exact target deal'):
        asyncio.run(broker.execute_close(env))

    assert not any(call[1] == 'delete' for call in broker._calls)
    cmd_coid = env.client_order_id(KIND_CLOSE)
    assert 'confirmed' not in [kind for kind, _ in _events_for(ctx, cmd_coid)]
    store.close()


def __test_execute_close_partial_race_outside_window_halts__(tmp_path):
    """An unattributed reverse row halts without a corrective DELETE."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): [
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY', 'size': 2.0}},
            ]},
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY', 'size': 2.0}},
            ]},
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY', 'size': 2.0}},
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'fresh', 'direction': 'SELL', 'size': 1.0,
                              'createdDateUTC': '1970-01-01T00:00:00.000'}},
            ]},
        ],
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
    # Exact target provenance was captured before the POST.
    assert extras.get('target_deal_id') == 'orig'
    assert extras.get('target_direction') == 'BUY'
    assert extras.get('pre_target_units') == 200
    assert extras.get('intent_units') == 100

    refs = dict(ctx.iter_refs_for_coid(cmd_coid))
    assert refs.get('deal_reference') == 'dr-2'

    store.close()


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

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_close(env))

    assert any(
        c[0] == 'positions/fresh' and c[1] == 'delete'
        for c in broker._calls
    )
    cmd_coid = env.client_order_id(KIND_CLOSE)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None and cmd_row.state == 'disposition_unknown'
    cmd_kinds = [k for k, _ in _events_for(ctx, cmd_coid)]
    assert 'partial_close_corrective_delete_dispatched' in cmd_kinds
    assert 'partial_close_corrective_delete' in cmd_kinds
    assert (cmd_row.extras or {}).get('corrected_reverse_deal_id') == 'fresh'
    assert (cmd_row.extras or {}).get('corrective_delete_pending') is False
    assert 'confirmed' not in cmd_kinds
    store.close()


def __test_execute_close_partial_corrective_delete_timeout_persists_target__(
        tmp_path,
):
    """An ambiguous corrective DELETE retains exact recovery identity."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): [
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY', 'size': 2.0}},
            ]},
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY', 'size': 2.0}},
            ]},
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY', 'size': 2.0}},
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'fresh', 'direction': 'SELL', 'size': 1.0}},
            ]},
        ],
        ('positions', 'post'): {'dealReference': 'dr-delete-timeout'},
        ('confirms/dr-delete-timeout', 'get'): {
            'dealStatus': 'ACCEPTED',
            'affectedDeals': [{'dealId': 'fresh', 'status': 'OPENED'}],
        },
        ('error', 'positions/fresh', 'delete'):
            httpx.TimeoutException('corrective DELETE timeout'),
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

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_close(env))

    cmd_row = ctx.get_order(env.client_order_id(KIND_CLOSE))
    assert cmd_row is not None and cmd_row.state == 'disposition_unknown'
    extras = cmd_row.extras or {}
    assert extras.get('corrected_reverse_deal_id') == 'fresh'
    assert extras.get('corrective_delete_pending') is True
    store.close()


def __test_execute_close_partial_race_confirm_mismatch_halts__(tmp_path):
    """Fresh opposite row absent from ``affectedDeals`` is never deleted."""
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
            ]},
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY',
                              'size': 2.0}},
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'external', 'direction': 'SELL',
                              'size': 1.0}},
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


def __test_execute_close_partial_race_ttl_without_deal_id_does_not_delete__(tmp_path):
    """A TTL-expired confirm cannot authorize a timestamp-only DELETE."""
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
            ]},
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY',
                              'size': 2.0}},
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'fresh', 'direction': 'SELL',
                              'size': 1.0}},
            ]},
        ],
        ('positions', 'post'): {'dealReference': 'dr-5'},
        ('error', 'confirms/dr-5', 'get'): OrderNotFoundError(
            'confirm TTL expired', ref_type='deal_reference',
        ),
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
    cmd_row = ctx.get_order(env.client_order_id(KIND_CLOSE))
    assert cmd_row is not None and cmd_row.state == 'submitted'
    store.close()


def __test_execute_close_partial_netting_pending_no_reverse_leg__(tmp_path):
    """Accepted but unchanged exact target is parked, not reported filled."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        # Both pre- and post-POST snapshots still show the FULL BUY 2.0
        # (netting reduction not yet reflected); no SELL row exists.
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
            ]},
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY',
                              'size': 2.0}},
            ]},
        ],
        ('positions', 'post'): {'dealReference': 'dr-pending'},
        ('confirms/dr-pending', 'get'): {'dealStatus': 'ACCEPTED'},
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

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_close(env))

    cmd_coid = env.client_order_id(KIND_CLOSE)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'disposition_unknown'
    assert (cmd_row.extras or {}).get('kind') == KIND_PARTIAL_CLOSE

    assert not any(c[1] == 'delete' for c in broker._calls)
    cmd_kinds = [k for k, _ in _events_for(ctx, cmd_coid)]
    assert 'partial_close_netting_pending' in cmd_kinds
    assert 'confirmed' not in cmd_kinds
    store.close()


def __test_execute_close_partial_confirm_rejected_routes_reject__(tmp_path):
    """Confirm ``dealStatus == 'REJECTED'`` → reject, NOT silent success.

    A genuinely rejected reduce POST (market closed/suspended, below min
    deal size) leaves the venue position unchanged and creates no row —
    observationally identical to benign netting lag. The confirm's
    ``dealStatus`` is the discriminator: REJECTED must raise
    ``ExchangeOrderRejectedError`` so the journal marks the command row
    rejected and the engine's internal position stays in sync with the
    venue, instead of the benign fall-through reporting a successful
    reduction that never happened.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'orig', 'direction': 'BUY',
                          'size': 2.0}},
        ]},
        ('positions', 'post'): {'dealReference': 'dr-rej'},
        ('confirms/dr-rej', 'get'): {
            'dealStatus': 'REJECTED',
            'rejectReason': 'MARKET_CLOSED',
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

    with pytest.raises(ExchangeOrderRejectedError, match='MARKET_CLOSED'):
        asyncio.run(broker.execute_close(env))

    # The journal persisted the rejection — the command row is terminal
    # ``rejected``, not ``confirmed``.
    cmd_coid = env.client_order_id(KIND_CLOSE)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'rejected'

    cmd_events = _events_for(ctx, cmd_coid)
    cmd_kinds = [k for k, _ in cmd_events]
    assert 'rejected' in cmd_kinds
    assert 'partial_close_netting_pending' not in cmd_kinds
    assert 'confirmed' not in cmd_kinds
    reject_payload = dict(cmd_events)['rejected']
    assert reject_payload.get('phase') == 'partial_close_post'

    # The rejected POST executed no deal, so nothing was swept.
    assert not any(c[1] == 'delete' for c in broker._calls)
    store.close()


def __test_execute_close_partial_unsettled_no_confirm_verdict_halts__(tmp_path):
    """TTL-expired confirm with unchanged target parks for recovery."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        # Pre- and post-snapshot identical: reduction not reflected,
        # no opposite-direction row.
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
            ]},
            {'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'dealId': 'orig', 'direction': 'BUY',
                              'size': 2.0}},
            ]},
        ],
        ('positions', 'post'): {'dealReference': 'dr-ttl'},
        ('error', 'confirms/dr-ttl', 'get'): OrderNotFoundError(
            'confirm TTL expired', ref_type='deal_reference',
        ),
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

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_close(env))

    cmd_coid = env.client_order_id(KIND_CLOSE)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None and cmd_row.state == 'disposition_unknown'
    cmd_kinds = [k for k, _ in _events_for(ctx, cmd_coid)]
    assert 'partial_close_netting_pending' in cmd_kinds
    assert 'confirmed' not in cmd_kinds
    assert not any(c[1] == 'delete' for c in broker._calls)

    # The mid-flow add_ref preserved the deal_reference for the operator.
    refs = dict(ctx.iter_refs_for_coid(cmd_coid))
    assert refs.get('deal_reference') == 'dr-ttl'
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
    """Accepted confirm plus exact target residual confirms recovery."""
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
            'target_deal_id': 'orig',
            'target_direction': 'BUY',
            'pre_target_units': 200,
            'intent_units': 100,
        },
    )
    ctx.add_ref(cmd_coid, 'deal_reference', 'dr-rec')

    asyncio.run(broker._recover_in_flight_submissions())

    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'confirmed'
    extras = cmd_row.extras or {}
    assert extras.get('recovery_path') == 'partial_close_target_match'

    # Recovery closes the command row (kind-aware _apply_resume_outcome).
    live_coids = [r.client_order_id for r in ctx.iter_live_orders()]
    assert cmd_coid not in live_coids
    store.close()


def __test_execute_close_partial_recovery_aggregate_match_wrong_deal_stays_unknown(
        tmp_path,
):
    """Matching symbol units on a replacement deal are not ownership proof."""
    from time import time as epoch_time

    from pynecore_capitalcom.models import _InstrumentRules

    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'replacement', 'direction': 'BUY',
                          'size': 1.0}},
        ]},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
        ('confirms/dr-wrong', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    broker._instrument_rules_cache['EURUSD'] = _InstrumentRules(
        epic='EURUSD', lot_step=0.01, min_size=0.01,
        min_stop_or_limit_distance=0.0001, fetched_at=epoch_time(),
    )
    cmd_coid = 'close-cmd-partial-wrong-deal'
    ctx.upsert_order(
        cmd_coid, symbol='EURUSD', side='sell', qty=1.0,
        state='server_ref_seen', pine_entry_id='Long',
        extras={
            'kind': KIND_PARTIAL_CLOSE,
            'deal_reference': 'dr-wrong',
            'target_deal_id': 'orig',
            'target_direction': 'BUY',
            'pre_target_units': 200,
            'intent_units': 100,
        },
    )
    ctx.add_ref(cmd_coid, 'deal_reference', 'dr-wrong')

    asyncio.run(broker._recover_in_flight_submissions())

    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'server_ref_seen'
    recovery_event = dict(_events_for(ctx, cmd_coid))['recovery_pending']
    assert recovery_event.get('recovery_path') == 'partial_close_units_mismatch'
    recovery_context = recovery_event.get('recovery_context') or {}
    assert recovery_context.get('target_deal_id') == 'orig'
    assert recovery_context.get('target_units') is None
    store.close()


def __test_recovery_adopts_untracked_position_then_close_all__(tmp_path):
    """Untracked venue position → confirmed rows → ``close_all`` flattens it.

    Regression for the post-restart recovery failure: a fresh process
    adopts an existing 200-unit EURUSD position (reported as two 100-unit
    netting lots) that the BrokerStore never tracked. Startup recovery
    must reconcile each live leg into a confirmed ``position`` row so a
    subsequent normal ``strategy.close_all()`` no longer raises
    ``ExchangeOrderRejectedError: no confirmed position rows``.

    A confirmed market position carries ``filled_qty == qty`` because that
    field records the entry fill; it does not reduce the live venue quantity.
    Closing the complete adopted quantity therefore routes through the native
    full-close path and DELETEs every adopted ``dealId`` exactly once.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'lot-a', 'direction': 'BUY',
                          'size': 1.0}},
            {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'lot-b', 'direction': 'BUY',
                          'size': 1.0}},
        ]},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
        ('positions/lot-a', 'delete'): {},
        ('positions/lot-b', 'delete'): {},
    })

    # Fresh store: no rows track the live position.
    asyncio.run(broker._recover_in_flight_submissions())

    # Both legs are now confirmed ``position`` rows with the load-bearing
    # ``deal_id`` ref so the activity loop can route the close-leg fills.
    for deal_id in ('lot-a', 'lot-b'):
        row = ctx.find_by_ref('deal_id', deal_id)
        assert row is not None, f"leg {deal_id!r} was not adopted"
        assert row.state == 'confirmed'
        assert row.exchange_order_id == deal_id
        assert (row.extras or {}).get('kind') == 'position'
        assert (row.extras or {}).get('adopted_startup') is True

    # A normal full close of the adopted 2.0 total must now succeed instead
    # of raising "no confirmed position rows".
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='Long', symbol='EURUSD', side='sell', qty=2.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    result = asyncio.run(broker.execute_close(env))
    assert result.id == 'lot-a'

    delete_calls = [
        call for call in broker._calls if call[1] == 'delete'
    ]
    assert delete_calls == [
        ('positions/lot-a', 'delete', None),
        ('positions/lot-b', 'delete', None),
    ]
    assert not any(
        call[0] == 'positions' and call[1] == 'post'
        for call in broker._calls
    )

    cmd_coid = env.client_order_id(KIND_CLOSE)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'closing'
    assert (cmd_row.extras or {}).get('kind') == KIND_FULL_CLOSE
    assert (cmd_row.extras or {}).get('targets') == ['lot-a', 'lot-b']
    for deal_id in ('lot-a', 'lot-b'):
        row = ctx.find_by_ref('deal_id', deal_id)
        assert row is not None
        assert row.state == 'closing'
    store.close()


def __test_recovery_skips_tracked_position_no_duplicate_adopt__(tmp_path):
    """A leg already tracked by a live row is not re-adopted (idempotent)."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'orig', 'direction': 'BUY',
                          'size': 1.0}},
        ]},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
    })
    # Existing tracked entry row for the same dealId.
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='orig', extras={'kind': 'position'})

    asyncio.run(broker._recover_in_flight_submissions())

    # No synthetic adoption row was created for the already-tracked leg.
    live = list(ctx.iter_live_orders())
    adopted = [r for r in live
               if (r.extras or {}).get('adopted_startup') is True]
    assert adopted == []
    assert any(r.client_order_id == 'coid-entry' for r in live)
    store.close()
