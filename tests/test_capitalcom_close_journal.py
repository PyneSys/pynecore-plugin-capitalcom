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
* **Partial close race without confirm attribution**: the hook never deletes
  a candidate reverse row from timestamp proximity alone; it reports the
  opposite deal (ERROR plus an audit event) and parks the close. Nothing
  proves someone else changed the bot's state, so nothing is latched and the
  process is not halted.
* **Partial close race resolved via confirm ``affectedDeals``**: the
  fresh reverse leg is identified deterministically by the ``dealId``
  the confirm attributes to our POST — no clock involved — and the
  corrective DELETE goes out even when ``createdDateUTC`` is stale.
* **Partial close race, confirm names a different deal**: a fresh
  opposite row NOT listed in the confirm's ``affectedDeals`` is
  external (manual/other) — the hook reports it and parks the close
  instead of deleting it; no quarantine of its own.
* **Partial close race, confirm TTL-expired**: no corrective DELETE is
  authorized because the venue no longer supplies exact deal attribution;
  the close parks, nothing is latched.
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
import time

import httpx
import pytest

from pynecore.core.broker.exceptions import (
    ExchangeOrderRejectedError,
    MarketClosedError,
    OrderDispositionUnknownError,
    OrderSkippedByPlugin,
)
from pynecore.core.broker.idempotency import KIND_CLOSE
from pynecore.core.broker.models import CloseIntent, DispatchEnvelope, QuarantineKind
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore
from pynecore.core.broker.store_helpers import (
    KIND_FULL_CLOSE,
    KIND_PARTIAL_CLOSE,
)

from pynecore_capitalcom import CapitalCom, CapitalComConfig, foreign
from pynecore_capitalcom.exceptions import CapitalComApiError, OrderNotFoundError


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

    injected_position = {'dealId': 'deal-B', 'direction': 'BUY', 'size': 1.0}

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
                    {'market': {'epic': 'EURUSD'}, 'position': self.injected_position},
                )
            return {'positions': positions}
        if endpoint == 'positions' and method == 'post':
            self._calls.append((endpoint, method, data))
            return {'dealReference': 'should-not-write'}
        return self._fake_response(endpoint, data=data, method=method)


class _InjectOwnFilledWorkingOrderBeforePostBroker(_InjectSameSideBeforePostBroker):
    """The pre-POST snapshot shows a deal an own working order just filled into."""

    injected_position = {'dealId': 'deal-B', 'direction': 'BUY', 'size': 1.0,
                         'workingOrderId': 'wo-B'}


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


def __test_execute_close_reversal_close_is_symbol_wide_full_delete__(tmp_path):
    """A ``reversal_close`` targets every owned row despite its synthetic id.

    The engine's stop-and-reverse close leg carries the synthetic
    ``__pyne_reversal_close__*`` pine id, which matches no position row —
    keyed targeting would reject it. The synthetic kind routes it
    symbol-wide (the engine flattens the whole book before the raw
    reversing entry) through the native per-dealId DELETE.
    """
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
            pine_id='__pyne_reversal_close__S', symbol='EURUSD', side='sell',
            qty=2.0, immediately=True, synthetic_kind='reversal_close',
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


def __test_execute_close_reversal_close_never_takes_the_partial_post_path__(tmp_path):
    """A quantity mismatch must not push a ``reversal_close`` onto the POST.

    The opposite-direction partial-close POST could open opposite exposure
    against a protective fill racing the reversal — exactly what the
    close-then-open protocol exists to prevent. Even when the intent
    quantity is smaller than the live row (the venue book moved after the
    engine sized the close), the reversal close stays on the per-dealId
    DELETE: the reversal wants the book flat, and the DELETE cannot
    over-open.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-A', 'delete'): {},
    })
    ctx.upsert_order('coid-entry-a', symbol='EURUSD', side='buy', qty=2.0,
                     state='confirmed', pine_entry_id='A',
                     exchange_order_id='deal-A', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='__pyne_reversal_close__S', symbol='EURUSD', side='sell',
            qty=1.0, immediately=True, synthetic_kind='reversal_close',
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

    # A script-emitted close with no owned rows on its reducing side is a
    # non-halting decline (the book is already flat there), never a fatal
    # reject — but it must still touch nothing on the symbol.
    with pytest.raises(OrderSkippedByPlugin,
                       match='nothing to close'):
        asyncio.run(broker.execute_close(env))

    assert not any(call[1] in {'delete', 'post'} for call in broker._calls)
    entry_a = ctx.get_order('coid-entry-a')
    entry_b = ctx.get_order('coid-entry-b')
    assert entry_a is not None and entry_a.state == 'confirmed'
    assert entry_b is not None and entry_b.state == 'confirmed'
    store.close()


def __test_close_all_skips_when_reducing_side_already_flat__(tmp_path):
    """Incident shape (cycle 60, 2026-08-23): a stale symbol-wide close_all
    whose sell book was already flattened must DECLINE, not grab the fresh
    opposite-direction entry as a target and crash on the partial path."""
    broker, store, ctx = _make_broker(tmp_path)
    # Only a freshly filled LONG row is live — the shorts the close_all was
    # sized for are gone (their DELETEs settled between re-emissions).
    ctx.upsert_order('coid-fresh-long', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='L',
                     exchange_order_id='deal-L', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='', symbol='EURUSD', side='buy', qty=0.5,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(OrderSkippedByPlugin, match='nothing to close') as exc:
        asyncio.run(broker.execute_close(env))
    assert exc.value.reason == 'nothing_to_close'

    # No wire write went out and the long row is untouched.
    assert not any(call[1] in {'delete', 'post'} for call in broker._calls)
    fresh = ctx.get_order('coid-fresh-long')
    assert fresh is not None and fresh.state == 'confirmed'
    store.close()


def __test_close_all_targets_only_opposite_side_rows__(tmp_path):
    """A buy-side close_all reduces the SELL rows only — a same-direction BUY
    row on the symbol can never become its target."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-S', 'delete'): {},
    })
    ctx.upsert_order('coid-short', symbol='EURUSD', side='sell', qty=1.0,
                     state='confirmed', pine_entry_id='S',
                     exchange_order_id='deal-S', extras={'kind': 'position'})
    ctx.upsert_order('coid-long', symbol='EURUSD', side='buy', qty=2.0,
                     state='confirmed', pine_entry_id='L',
                     exchange_order_id='deal-L', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='', symbol='EURUSD', side='buy', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    result = asyncio.run(broker.execute_close(env))
    assert result.id == 'deal-S'

    # Exactly one DELETE, aimed at the short; the long was never touched.
    deletes = [c for c in broker._calls if c[1] == 'delete']
    assert [c[0] for c in deletes] == ['positions/deal-S']
    assert not any(call[1] == 'post' for call in broker._calls)
    long_row = ctx.get_order('coid-long')
    assert long_row is not None and long_row.state == 'confirmed'
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


def __test_execute_close_never_targets_a_startup_adopted_row__(tmp_path):
    """A row seeded for an untracked venue deal is not the run's exposure.

    The deal was opened outside this run's journal, so no close — keyed or
    ``close_all`` — may DELETE it; the close finds no owned exposure and is
    declined.
    """
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

    with pytest.raises(OrderSkippedByPlugin, match='no owned position rows'):
        asyncio.run(broker.execute_close(env))
    assert not any(call[1] in {'delete', 'post'} for call in broker._calls)
    adopted = ctx.get_order('__pyne_adopted__EURUSD__deal-adopted')
    assert adopted is not None and adopted.state == 'confirmed'
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
    """A newly-visible own same-side deal blocks the non-targetable reduce POST."""
    broker, store, ctx = _make_broker(
        tmp_path, broker_type=_InjectOwnFilledWorkingOrderBeforePostBroker,
    )
    ctx.upsert_order('coid-entry-a', symbol='EURUSD', side='buy', qty=2.0,
                     state='confirmed', pine_entry_id='A',
                     exchange_order_id='deal-A', extras={'kind': 'position'})
    # The run's own resting order: it fills into deal-B between the plan's
    # snapshot and the locked pre-POST snapshot.
    ctx.upsert_order('coid-entry-wo', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='B',
                     exchange_order_id='wo-B', extras={'kind': 'working'})
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


def __test_execute_close_partial_foreign_deal_before_post_is_refused__(
        tmp_path, monkeypatch):
    """A foreign same-side deal visible only at the locked pre-POST snapshot.

    The reduce POST cannot target a ``dealId`` and could net that deal FIFO,
    so nothing is sent: the close is declined (re-evaluated next bar), the
    command row is rejected and the refusal is logged once — never retried
    into the foreign deal. A single refusal latches nothing (the deal may be
    gone in seconds); one still blocked past the latch bound latches the entry
    block once.
    """
    broker, store, ctx = _make_broker(
        tmp_path, broker_type=_InjectSameSideBeforePostBroker,
    )
    latched: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: latched.append(
        (reason, context, kind))
    ctx.upsert_order('coid-entry-a', symbol='EURUSD', side='buy', qty=2.0,
                     state='confirmed', pine_entry_id='A',
                     exchange_order_id='deal-A', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='A', symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(OrderSkippedByPlugin) as raised:
        asyncio.run(broker.execute_close(env))

    assert raised.value.reason == 'foreign_deal_blocks_partial_close'
    assert not any(
        call[0] == 'positions' and call[1] == 'post'
        for call in broker._calls
    )
    entry_a = ctx.get_order('coid-entry-a')
    assert entry_a is not None and entry_a.state == 'confirmed'
    assert not (entry_a.extras or {}).get('own_close_expected')
    cmd_row = ctx.get_order(env.client_order_id(KIND_CLOSE))
    assert cmd_row is None or cmd_row.state == 'rejected'
    assert latched == [], "one refusal is no proof the exit is impossible"

    # The foreign deal is still there on later bars, the poll in between
    # shows it too: the fresh read refuses and the block is continuous.
    blocked = [
        {'market': {'epic': 'EURUSD'},
         'position': {'dealId': 'deal-A', 'direction': 'BUY', 'size': 2.0}},
        {'market': {'epic': 'EURUSD'}, 'position': broker.injected_position},
    ]
    broker._responses[('positions', 'get')] = {'positions': blocked}
    broker._remember_positions_snapshot(
        {raw['position']['dealId']: raw for raw in blocked}, time.time() + 60.0)
    later = time.time() + 121.0
    monkeypatch.setattr(foreign, 'epoch_time', lambda: later)
    for bar_ts_ms in (1700000060000, 1700000120000):
        with pytest.raises(OrderSkippedByPlugin):
            asyncio.run(broker.execute_close(DispatchEnvelope(
                intent=CloseIntent(pine_id='A', symbol='EURUSD', side='sell', qty=1.0),
                run_tag='test', bar_ts_ms=bar_ts_ms,
            )))
    assert not any(call[:2] == ('positions', 'post') for call in broker._calls)
    assert len(latched) == 1, "the block latches once"
    assert latched[0][2] is QuarantineKind.ENTRY_BLOCK
    assert 'blocked for' in latched[0][0]
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
    """An unmatched script key with a reserved prefix is not a defensive close.

    It neither closes the row its suffix names nor falls back to a row seeded
    for an untracked venue deal: nothing is sent.
    """
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

    with pytest.raises(OrderSkippedByPlugin, match='no owned position rows'):
        asyncio.run(broker.execute_close(env))
    assert not any(call[1] in {'delete', 'post'} for call in broker._calls)
    adopted = ctx.get_order('__pyne_adopted__EURUSD__deal-adopted')
    entry_b = ctx.get_order('coid-entry-b')
    assert adopted is not None and adopted.state == 'confirmed'
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


def _full_close_env() -> DispatchEnvelope:
    return DispatchEnvelope(
        intent=CloseIntent(
            pine_id='Long', symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )


def __test_execute_close_full_delete_market_closed_is_non_terminal__(tmp_path):
    """A session-pause refusal of the DELETE maps to ``MarketClosedError``.

    Measured live: the reversal close dispatched into Capital.com's 21:00 UTC
    maintenance break was answered ``Rejected. BTCUSD is currently closed.
    Timetable in place: ...``; the raw API error escaped the hook and killed
    the run. The venue reopens on its own, so the engine must retry instead.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('error', 'positions/deal-L', 'delete'): CapitalComApiError(
            "API error occured: Rejected. BTCUSD is currently closed. "
            "Timetable in place: UTC; Mon - 21:00, 21:05 -;",
            error_code="Rejected. BTCUSD is currently closed.",
        ),
    })
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='deal-L', extras={'kind': 'position'})

    with pytest.raises(MarketClosedError):
        asyncio.run(broker.execute_close(_full_close_env()))
    # The target stays an untouched live position.
    target_row = ctx.get_order('coid-entry')
    assert target_row is not None
    assert target_row.state == 'confirmed'
    store.close()


def __test_execute_close_full_delete_api_error_is_a_reject_not_raw__(tmp_path):
    """Any other synchronous API verdict on the DELETE is a definitive reject."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('error', 'positions/deal-L', 'delete'): CapitalComApiError(
            "API error occured: error.invalid.dealId", error_code="error.invalid.dealId",
        ),
    })
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='deal-L', extras={'kind': 'position'})

    with pytest.raises(ExchangeOrderRejectedError) as info:
        asyncio.run(broker.execute_close(_full_close_env()))
    assert not isinstance(info.value, MarketClosedError)
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
    """An equal-sized replacement row cannot satisfy exact-target proof.

    Nothing shows who moved the target, so the close is parked as
    disposition-unknown with an ERROR and a ``partial_close_target_diverged``
    audit event — no halt, no corrective write. The activity stream settles
    it against the own-close record.
    """
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
    ctx.add_ref('coid-entry', 'deal_id', 'orig')
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='Long', symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(OrderDispositionUnknownError, match='exact target deal'):
        asyncio.run(broker.execute_close(env))

    assert not any(call[1] == 'delete' for call in broker._calls)
    cmd_coid = env.client_order_id(KIND_CLOSE)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None and cmd_row.state == 'disposition_unknown'
    # The resolver finds the own-close record the POST left on its target.
    assert (cmd_row.extras or {}).get('target_coid') == 'coid-entry'
    assert (cmd_row.extras or {}).get('own_close_sent_at') is not None
    cmd_events = [kind for kind, _ in _events_for(ctx, cmd_coid)]
    assert 'confirmed' not in cmd_events
    assert 'partial_close_target_diverged' in cmd_events
    store.close()


def __test_execute_close_partial_race_outside_window_parks__(tmp_path):
    """An unattributed reverse row parks the close without a corrective DELETE."""
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

    latched: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: latched.append(kind)
    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_close(env))

    assert not any(call[1] == 'delete' for call in broker._calls)
    # The confirm names no deal: the opposite row may be the bot's own
    # reverse leg or someone else's. Neither is proof of an intervention:
    # nothing is latched, the process is not halted, and the parked command
    # keeps the POST's provenance for the resolver.
    assert latched == []
    cmd_coid = env.client_order_id(KIND_CLOSE)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert cmd_row.state == 'disposition_unknown'
    reported = [payload for kind, payload in _events_for(ctx, cmd_coid)
                if kind == 'partial_close_unattributed_opposite']
    assert len(reported) == 1
    assert reported[0]['opposite_deal_ids'] == ['fresh']
    assert reported[0]['attributable'] is False
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


def _reverse_leg_not_listed_yet(tmp_path, *, delete_response):
    """A partial close whose target an SL closed before the POST: the POST
    opens a fresh SELL deal the confirm names ``OPENED``, while ``/positions``
    does not list it yet."""
    pre = {'positions': [
        {'market': {'epic': 'EURUSD'},
         'position': {'dealId': 'orig', 'direction': 'BUY', 'size': 2.0}},
    ]}
    responses: dict = {
        ('positions', 'get'): [pre, pre, {'positions': []}, {'positions': []}],
        ('positions', 'post'): {'dealReference': 'dr-lag'},
        ('confirms/dr-lag', 'get'): {
            'dealStatus': 'ACCEPTED', 'dealId': 'fresh',
            'affectedDeals': [{'dealId': 'fresh', 'status': 'OPENED'}],
        },
    }
    responses.update(delete_response)
    broker, store, ctx = _make_broker(tmp_path, responses=responses)
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=2.0,
                     filled_qty=2.0, state='confirmed', pine_entry_id='Long',
                     exchange_order_id='orig', extras={'kind': 'position'})
    ctx.add_ref('coid-entry', 'deal_id', 'orig')
    env = DispatchEnvelope(
        intent=CloseIntent(pine_id='Long', symbol='EURUSD', side='sell', qty=1.0),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_close(env))
    return broker, store, ctx, env.client_order_id(KIND_CLOSE)


def __test_execute_close_partial_reverse_leg_the_snapshot_does_not_list_yet_is_corrected__(
        tmp_path):
    """The deal the confirm says this POST opened is its own reverse leg even
    before ``/positions`` lists it: it is deleted and recorded, never left on
    the venue as unbooked exposure the bot takes for someone else's."""
    broker, store, ctx, cmd_coid = _reverse_leg_not_listed_yet(
        tmp_path, delete_response={('positions/fresh', 'delete'): {}})

    assert ('positions/fresh', 'delete', None) in broker._calls
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None and cmd_row.state == 'disposition_unknown'
    extras = cmd_row.extras or {}
    assert extras.get('corrected_reverse_deal_id') == 'fresh'
    assert extras.get('corrective_delete_pending') is False
    cmd_kinds = [k for k, _ in _events_for(ctx, cmd_coid)]
    assert 'partial_close_corrective_delete' in cmd_kinds
    assert 'confirmed' not in cmd_kinds
    store.close()


def __test_parked_partial_close_repeats_its_corrective_delete_while_the_reverse_leg_shows__(
        tmp_path):
    """A corrective DELETE the venue cannot route yet (a fresh deal) is
    repeated while ``/positions`` lists the reverse leg; once it is gone the
    parked close settles instead of staying parked forever."""
    broker, store, ctx, cmd_coid = _reverse_leg_not_listed_yet(
        tmp_path, delete_response={('error', 'positions/fresh', 'delete'): OrderNotFoundError(
            'not routable yet', ref_type='deal_id')})
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None
    assert (cmd_row.extras or {}).get('corrective_delete_pending') is True

    del broker._responses[('error', 'positions/fresh', 'delete')]
    broker._responses[('positions/fresh', 'delete')] = {}
    deletes_before = broker._calls.count(('positions/fresh', 'delete', None))
    listed = {'fresh': {'market': {'epic': 'EURUSD'},
                        'position': {'dealId': 'fresh', 'direction': 'SELL', 'size': 1.0}}}
    asyncio.run(broker._resolve_partial_close_dispositions(listed))
    assert broker._calls.count(('positions/fresh', 'delete', None)) == deletes_before + 1
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None and cmd_row.state == 'disposition_unknown'
    assert (cmd_row.extras or {}).get('corrective_delete_pending') is False

    asyncio.run(broker._resolve_partial_close_dispositions(listed))
    assert broker._calls.count(('positions/fresh', 'delete', None)) == deletes_before + 1, (
        "a landed DELETE is not repeated while the venue still lists the deal")
    broker.fill_report_grace_s = 0.0
    asyncio.run(broker._resolve_partial_close_dispositions({}))
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None and cmd_row.state == 'rejected'
    assert len([k for k, _ in _events_for(ctx, cmd_coid)
                if k == 'partial_close_no_own_reduction']) == 1
    store.close()


def __test_execute_close_partial_race_confirm_mismatch_parks__(tmp_path):
    """Fresh opposite row absent from ``affectedDeals`` is never deleted.

    The confirm names what the POST touched, so the opposite row is someone
    else's: reported and parked, no quarantine of its own (someone else's
    close of the bot's deal is judged from the target's activity).
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

    latched: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: latched.append(kind)
    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_close(env))

    assert not any(c[1] == 'delete' for c in broker._calls)
    assert latched == []
    cmd_coid = env.client_order_id(KIND_CLOSE)
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None and cmd_row.state == 'disposition_unknown'
    reported = [payload for kind, payload in _events_for(ctx, cmd_coid)
                if kind == 'partial_close_unattributed_opposite']
    assert len(reported) == 1 and reported[0]['attributable'] is True
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

    latched: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: latched.append(kind)
    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_close(env))

    assert not any(c[1] == 'delete' for c in broker._calls)
    assert latched == []
    cmd_row = ctx.get_order(env.client_order_id(KIND_CLOSE))
    assert cmd_row is not None and cmd_row.state == 'disposition_unknown'
    store.close()


def __test_parked_partial_close_beside_an_unattributable_opposite_deal_settles_after_the_grace__(
        tmp_path):
    """The venue closed the whole target between the final snapshot and the
    reduce POST, so the POST opened a fresh opposite deal, and its confirm is
    gone: nothing tells that deal from someone else's. It is never deleted,
    but it does not keep the close parked for good either: past the
    fill-report grace without an activity of its own the close settles as not
    executed, with an ERROR and an audit event naming the deal, and nothing
    latches."""
    pre = {'positions': [
        {'market': {'epic': 'EURUSD'},
         'position': {'dealId': 'orig', 'direction': 'BUY', 'size': 2.0}},
    ]}
    fresh = {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'fresh', 'direction': 'SELL', 'size': 1.0}}
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): [pre, pre, {'positions': [fresh]}],
        ('positions', 'post'): {'dealReference': 'dr-gone'},
        ('error', 'confirms/dr-gone', 'get'): OrderNotFoundError(
            'confirm TTL expired', ref_type='deal_reference',
        ),
    })
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=2.0,
                     filled_qty=2.0, state='confirmed', pine_entry_id='Long',
                     exchange_order_id='orig', extras={'kind': 'position'})
    ctx.add_ref('coid-entry', 'deal_id', 'orig')
    env = DispatchEnvelope(
        intent=CloseIntent(pine_id='Long', symbol='EURUSD', side='sell', qty=1.0),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    latched: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: latched.append(kind)
    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_close(env))
    cmd_coid = env.client_order_id(KIND_CLOSE)

    listed = {'fresh': fresh}
    asyncio.run(broker._resolve_partial_close_dispositions(listed))
    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None and cmd_row.state == 'disposition_unknown', \
        "parked while its own activity may still arrive"

    broker.fill_report_grace_s = 0.0
    asyncio.run(broker._resolve_partial_close_dispositions(listed))

    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None and cmd_row.state == 'rejected'
    settled = [payload for kind, payload in _events_for(ctx, cmd_coid)
               if kind == 'partial_close_settled_beside_unattributed_opposite']
    assert len(settled) == 1 and settled[0]['opposite_deal_ids'] == ['fresh']
    assert not any(call[1] == 'delete' for call in broker._calls)
    assert latched == []
    store.close()


def __test_parked_partial_close_stays_parked_while_its_own_reverse_leg_is_listed__(tmp_path):
    """The close's own reverse leg (the confirm named it) still shows while
    its corrective DELETE settles: the close is not settled beside it."""
    broker, store, ctx, cmd_coid = _reverse_leg_not_listed_yet(
        tmp_path, delete_response={('positions/fresh', 'delete'): {}})
    broker.fill_report_grace_s = 0.0
    listed = {'fresh': {'market': {'epic': 'EURUSD'},
                        'position': {'dealId': 'fresh', 'direction': 'SELL', 'size': 1.0}}}
    asyncio.run(broker._resolve_partial_close_dispositions(listed))

    cmd_row = ctx.get_order(cmd_coid)
    assert cmd_row is not None and cmd_row.state == 'disposition_unknown'
    assert not [kind for kind, _ in _events_for(ctx, cmd_coid)
                if kind == 'partial_close_settled_beside_unattributed_opposite']
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


def __test_execute_close_partial_recovery_lowers_the_target_cursor_once__(tmp_path):
    """A partial close confirmed at restart never returned from its dispatch:
    recovery lowers the target's run-owned exposure cursor, exactly once."""
    from time import time as epoch_time

    from pynecore_capitalcom.helpers import _retire_parked_partial_close_exposure
    from pynecore_capitalcom.models import _InstrumentRules

    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'orig', 'direction': 'BUY', 'size': 1.0}},
        ]},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
        ('confirms/dr-rec', 'get'): {'dealStatus': 'ACCEPTED',
                                     'dealId': 'opposite-leg'},
    })
    broker._instrument_rules_cache['EURUSD'] = _InstrumentRules(
        epic='EURUSD', lot_step=0.01, min_size=0.01,
        min_stop_or_limit_distance=0.0001, fetched_at=epoch_time(),
    )
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=2.0,
                     filled_qty=2.0, state='confirmed', pine_entry_id='Long',
                     exchange_order_id='orig', extras={'kind': 'position'})
    ctx.add_ref('coid-entry', 'deal_id', 'orig')
    cmd_coid = 'close-cmd-partial-1'
    ctx.upsert_order(
        cmd_coid, symbol='EURUSD', side='sell', qty=1.0,
        state='disposition_unknown', pine_entry_id='Long',
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

    target = ctx.get_order('coid-entry')
    assert target is not None and target.filled_qty == pytest.approx(1.0)
    command = ctx.get_order(cmd_coid)
    assert command is not None and command.closed_ts_ms is not None
    _retire_parked_partial_close_exposure(ctx, command, 100, 0.01, path='again')
    target = ctx.get_order('coid-entry')
    assert target is not None and target.filled_qty == pytest.approx(1.0)
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


def __test_recovery_leaves_untracked_position_alone__(tmp_path):
    """An untracked venue position is never seeded, closed or booked.

    A fresh process finds a 200-unit EURUSD position (two 100-unit netting
    lots) no row of this run tracks. It was opened outside the run's journal,
    so it is not the run's: startup only logs each deal with a
    ``foreign_deal_on_epic_at_startup`` audit event, and a later
    ``strategy.close_all()`` finds no owned exposure and is declined — it
    neither DELETEs the lots nor crashes the run.
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

    asyncio.run(broker._recover_in_flight_submissions())

    assert list(ctx.iter_live_orders()) == []
    noted = [
        row['exchange_order_id'] for row in ctx._store._conn.execute(
            "SELECT exchange_order_id FROM events "
            "WHERE run_instance_id = ? AND kind = 'foreign_deal_on_epic_at_startup' "
            "ORDER BY id ASC",
            (ctx.run_instance_id,),
        )
    ]
    assert noted == ['lot-a', 'lot-b']

    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='Long', symbol='EURUSD', side='sell', qty=2.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    with pytest.raises(OrderSkippedByPlugin, match='no owned position rows'):
        asyncio.run(broker.execute_close(env))
    assert not any(call[1] in {'delete', 'post'} for call in broker._calls)
    store.close()


def __test_recovery_retires_a_legacy_adopted_row_without_touching_its_deal__(tmp_path):
    """A row an earlier version seeded for an untracked deal is retired at startup."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'deal-adopted', 'direction': 'BUY',
                          'size': 1.0}},
        ]},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
    })
    ctx.upsert_order(
        '__pyne_adopted__EURUSD__deal-adopted',
        symbol='EURUSD', side='buy', qty=1.0, filled_qty=1.0,
        state='confirmed', exchange_order_id='deal-adopted',
        extras={'kind': 'position', 'entry_filled_at': 1.0,
                'adopted_startup': True},
    )

    asyncio.run(broker._recover_in_flight_submissions())

    assert list(ctx.iter_live_orders()) == []
    kinds = [kind for kind, _ in _events_for(ctx, '__pyne_adopted__EURUSD__deal-adopted')]
    assert 'startup_adopted_row_retired' in kinds
    assert not any(call[1] in {'delete', 'post', 'put'} for call in broker._calls)
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


def __test_full_close_persists_the_delete_deal_reference_on_the_target__(tmp_path):
    """The DELETE's ``dealReference`` lands on the closing row for later pricing."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-L', 'delete'): {'dealReference': 'p_close-ref'},
    })
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=1.0,
                     filled_qty=1.0, state='confirmed', pine_entry_id='Long',
                     exchange_order_id='deal-L', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CloseIntent(
            pine_id='Long', symbol='EURUSD', side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    asyncio.run(broker.execute_close(env))

    target = ctx.get_order('coid-entry')
    assert target is not None
    assert target.state == 'closing'
    assert (target.extras or {}).get('close_deal_reference') == 'p_close-ref'
    kinds = [(k, p) for k, p in _events_for(ctx, 'coid-entry') if k == 'close_dispatched']
    assert kinds == [('close_dispatched', {'close_deal_reference': 'p_close-ref'})]
    store.close()
