"""
@pyne
"""
import asyncio

import pytest

from pynecore.core.broker.exceptions import (
    AuthenticationError,
    ExchangeCapabilityError,
    ExchangeRateLimitError,
    ExchangeOrderRejectedError,
)
from pynecore.core.broker.models import (
    DispatchEnvelope, EntryIntent, OrderType,
)
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore

from pynecore_capitalcom import (
    CapitalCom,
    CapitalComConfig,
    CapitalComError,
)


def main():
    pass


class _FakeBroker(CapitalCom):
    """Broker subclass that bypasses the real REST helper.

    The production ``CapitalCom._call`` delegates to ``self(...)``
    (inherited from :class:`CapitalComProvider`) which issues a live HTTP
    request via ``httpx``. Overriding ``_call`` lets the tests inject
    pre-canned responses per endpoint without touching the network layer.
    """

    def __init__(self, *, symbol=None, timeframe=None, ohlcv_dir=None,
                 config=None, responses=None):
        super().__init__(symbol=symbol, timeframe=timeframe,
                         ohlcv_dir=ohlcv_dir, config=config)
        self._responses: dict = responses or {}
        self._calls: list = []

    async def _call(self, endpoint, *, data=None, method='post'):
        self._calls.append((endpoint, method, data))
        err = self._responses.get(('error', endpoint, method))
        if err is not None:
            raise err
        return self._responses.get((endpoint, method), {})


def _make_config(**overrides) -> CapitalComConfig:
    defaults = dict(
        demo=True,
        user_email="test@example.com",
        api_key="test-key",
        api_password="test-password",
    )
    defaults.update(overrides)
    return CapitalComConfig(**defaults)


def _open_store_ctx(tmp_path, broker: CapitalCom, *, account_id="test-account"):
    """Helper: open a BrokerStore + RunContext for a broker instance.

    Used by tests that exercise alias-lookup paths (``get_open_orders``).
    The caller is responsible for closing the returned (store, ctx) pair.
    """
    store = BrokerStore(
        tmp_path / "broker.sqlite",
        plugin_name=broker.plugin_name,
    )
    identity = RunIdentity(
        strategy_id="test", symbol="EURUSD", timeframe="60",
        account_id=account_id, label=None,
    )
    ctx = store.open_run(identity, script_source="// test")
    broker.store_ctx = ctx
    return store, ctx


def __test_broker_capabilities_match_dossier__():
    """Capability struct mirrors the research dossier §3."""
    broker = _FakeBroker(config=_make_config())
    caps = broker.get_capabilities()
    assert caps.stop_order is True
    assert caps.stop_limit_order is False
    assert caps.trailing_stop is True
    assert caps.tp_sl_bracket is True
    assert caps.tp_sl_bracket_native is True
    assert caps.oca_cancel_native is False
    assert caps.amend_order is True
    assert caps.cancel_all is False
    assert caps.reduce_only is True
    assert caps.watch_orders is True
    assert caps.fetch_position is True
    assert caps.client_id_echo is False
    assert caps.idempotency_native is False
    assert caps.partial_qty_bracket_exit is False


def __test_broker_get_balance_selects_preferred_account__():
    broker = _FakeBroker(config=_make_config(), responses={
        ('accounts', 'get'): {
            'accounts': [
                {'preferred': False, 'currency': 'EUR',
                 'balance': {'available': 1.0}},
                {'preferred': True, 'currency': 'USD',
                 'balance': {'available': 12345.67}},
            ],
        },
    })
    result = asyncio.run(broker.get_balance())
    assert result == {'USD': 12345.67}


def __test_broker_get_position_aggregates_same_direction_rows__():
    """Pyramiding creates multiple BUY rows; get_position averages them."""
    broker = _FakeBroker(config=_make_config(), responses={
        ('positions', 'get'): {
            'positions': [
                {'market': {'epic': 'EURUSD'},
                 'position': {'direction': 'BUY', 'size': 1.0,
                              'level': 1.10, 'upl': 5.0, 'leverage': 30}},
                {'market': {'epic': 'EURUSD'},
                 'position': {'direction': 'BUY', 'size': 2.0,
                              'level': 1.12, 'upl': 3.0, 'leverage': 30}},
                {'market': {'epic': 'GBPUSD'},
                 'position': {'direction': 'SELL', 'size': 1.0,
                              'level': 1.25, 'upl': 0.0}},
            ],
        },
    })
    pos = asyncio.run(broker.get_position('EURUSD'))
    assert pos is not None
    assert pos.side == 'long'
    assert pos.size == 3.0
    assert abs(pos.entry_price - (1.10 + 2 * 1.12) / 3) < 1e-9
    assert pos.unrealized_pnl == 8.0


def __test_broker_get_position_returns_none_when_no_row__():
    broker = _FakeBroker(config=_make_config(), responses={
        ('positions', 'get'): {'positions': []},
    })
    assert asyncio.run(broker.get_position('EURUSD')) is None


def __test_broker_get_open_orders_parses_working_orders__(tmp_path):
    """Working orders map to ExchangeOrder; BrokerStore deal_id ref → client_order_id."""
    broker = _FakeBroker(config=_make_config(), responses={
        ('workingorders', 'get'): {
            'workingOrders': [
                {'market': {'epic': 'EURUSD'},
                 'workingOrderData': {
                     'dealId': 'deal_42',
                     'direction': 'BUY',
                     'orderType': 'LIMIT',
                     'orderLevel': 1.0800,
                     'orderSize': 1.5,
                     'createdDateUTC': '2026-04-20T10:00:00',
                 }},
            ],
        },
    })
    store, ctx = _open_store_ctx(tmp_path, broker)
    try:
        ctx.upsert_order(
            'coid-xyz', symbol='EURUSD', side='buy', qty=1.5,
            state='confirmed',
        )
        ctx.add_ref('coid-xyz', 'deal_id', 'deal_42')

        orders = asyncio.run(broker.get_open_orders(symbol='EURUSD'))
    finally:
        ctx.close()
        store.close()
    assert len(orders) == 1
    o = orders[0]
    assert o.id == 'deal_42'
    assert o.side == 'buy'
    assert o.order_type == OrderType.LIMIT
    assert o.qty == 1.5
    assert o.price == 1.08
    assert o.client_order_id == 'coid-xyz'


def __test_broker_hedging_mode_startup_gate__():
    broker = _FakeBroker(config=_make_config(), responses={
        ('accounts/preferences', 'get'): {'hedgingMode': True},
    })
    with pytest.raises(ExchangeCapabilityError):
        asyncio.run(broker.assert_one_way_mode())


def __test_broker_hedging_mode_gate_passes_when_one_way__():
    broker = _FakeBroker(config=_make_config(), responses={
        ('accounts/preferences', 'get'): {'hedgingMode': False},
    })
    asyncio.run(broker.assert_one_way_mode())


# noinspection PyProtectedMember
def __test_broker_map_exception_classifies_codes__():
    broker = _FakeBroker(config=_make_config())
    assert isinstance(
        broker._map_exception(
            CapitalComError("API error occured: error.null.api.key"),
        ),
        AuthenticationError,
    )
    assert isinstance(
        broker._map_exception(
            CapitalComError("API error occured: error.too-many.requests"),
        ),
        ExchangeRateLimitError,
    )
    assert isinstance(
        broker._map_exception(
            CapitalComError(
                "API error occured: error.invalid.stoploss.minvalue: 17.146",
            ),
        ),
        ExchangeOrderRejectedError,
    )


def __test_watch_orders_is_async_iterator__():
    """watch_orders is advertised as True in capabilities, so it must exist as
    an async generator callable — not a NotImplementedError stub."""
    broker = _FakeBroker(config=_make_config())
    import inspect
    assert inspect.isasyncgenfunction(broker.watch_orders)


# =======================================================================
# Execute-path unit tests — exercise every ``execute_*`` method against a
# mocked ``_call`` to verify PERSIST-FIRST ordering, state transitions,
# and error taxonomy.
# =======================================================================

from pynecore.core.broker.exceptions import (  # noqa: E402
    BrokerManualInterventionError,
    OrderDispositionUnknownError,
    UnexpectedCancelError,
    InsufficientMarginError,
)
from pynecore.core.broker.models import (  # noqa: E402
    CancelIntent, CloseIntent, ExitIntent, OrderStatus,
)
from pynecore_capitalcom import (  # noqa: E402
    InvalidStopDistanceError, OrderNotFoundError,
)

_RULES_RESP = {
    'dealingRules': {
        'minStepDistance': {'value': 0.01},
        'minDealSize': {'value': 0.01},
        'minNormalStopOrLimitDistance': {'value': 0.0001},
    },
    'instrument': {'lotSize': 0.01},
}


def _make_broker(tmp_path, *, responses=None, config=None):
    """Construct a _FakeBroker with an opened BrokerStore and a pre-seeded
    ``markets/EURUSD`` response so :meth:`_get_instrument_rules` always has
    something to return without every test re-declaring the fixture."""
    resp: dict = {('markets/EURUSD', 'get'): _RULES_RESP}
    if responses:
        resp.update(responses)
    broker = _FakeBroker(config=config or _make_config(), responses=resp)
    store, ctx = _open_store_ctx(tmp_path, broker)
    return broker, store, ctx


def _entry_envelope(symbol='EURUSD', qty=1.0,
                    order_type=OrderType.MARKET,
                    limit=None, stop=None, pine_id='Long', side='buy'):
    return DispatchEnvelope(
        intent=EntryIntent(
            pine_id=pine_id, symbol=symbol, side=side, qty=qty,
            order_type=order_type, limit=limit, stop=stop,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )


def __test_execute_entry_market_happy_path__(tmp_path):
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'post'): {'dealReference': 'o-ref'},
        ('confirms/o-ref', 'get'): {
            'dealStatus': 'ACCEPTED', 'status': 'OPEN',
            'dealId': 'deal42', 'level': 1.1005, 'size': 1.0,
        },
    })
    env = _entry_envelope()
    result = asyncio.run(broker.execute_entry(env))
    assert len(result) == 1
    eo = result[0]
    assert eo.id == 'deal42'
    assert eo.status == OrderStatus.FILLED
    assert eo.filled_qty == 1.0
    assert eo.average_fill_price == 1.1005
    row = ctx.get_order(eo.client_order_id)
    assert row is not None
    assert row.state == 'confirmed'
    assert row.exchange_order_id == 'deal42'
    assert ctx.find_by_ref('deal_id', 'deal42') is not None
    store.close()


def __test_execute_entry_limit_creates_working_order__(tmp_path):
    broker, store, _ = _make_broker(tmp_path, responses={
        ('workingorders', 'post'): {'dealReference': 'wo-ref'},
        ('confirms/wo-ref', 'get'): {
            'dealStatus': 'ACCEPTED', 'status': 'OPEN', 'dealId': 'w1',
        },
    })
    env = _entry_envelope(order_type=OrderType.LIMIT, limit=1.2000)
    result = asyncio.run(broker.execute_entry(env))
    assert result[0].order_type == OrderType.LIMIT
    assert result[0].status == OrderStatus.OPEN  # working order, not filled
    # POST body must carry level + type
    call = [c for c in broker._calls if c[0] == 'workingorders' and c[1] == 'post'][0]
    assert call[2]['level'] == 1.2000
    assert call[2]['type'] == 'LIMIT'
    store.close()


def __test_execute_entry_stop_limit_rejects_as_capability__(tmp_path):
    broker, store, _ = _make_broker(tmp_path)
    env = _entry_envelope(order_type=OrderType.STOP_LIMIT,
                          limit=1.2, stop=1.1)
    with pytest.raises(ExchangeCapabilityError):
        asyncio.run(broker.execute_entry(env))
    store.close()


def __test_execute_entry_no_deal_reference_parks_as_unknown__(tmp_path):
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'post'): {},  # no dealReference
    })
    env = _entry_envelope()
    with pytest.raises(OrderDispositionUnknownError) as exc:
        asyncio.run(broker.execute_entry(env))
    assert exc.value.client_order_id.startswith('test-')
    row = ctx.get_order(exc.value.client_order_id)
    assert row is not None and row.state == 'disposition_unknown'
    store.close()


def __test_execute_entry_rejected_margin_maps_to_margin_error__(tmp_path):
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'post'): {'dealReference': 'ref'},
        ('confirms/ref', 'get'): {
            'dealStatus': 'REJECTED', 'reason': 'Insufficient margin',
        },
    })
    env = _entry_envelope()
    with pytest.raises(InsufficientMarginError):
        asyncio.run(broker.execute_entry(env))
    # Row should be persisted as rejected
    rows = [r for r in ctx.iter_live_orders() if r.state == 'rejected']
    # rejected rows are still "live" until closed; we just verify state flipped.
    assert len(rows) == 1
    store.close()


def __test_execute_entry_quantizes_qty_to_lot_step__(tmp_path):
    broker, store, _ = _make_broker(tmp_path, responses={
        ('positions', 'post'): {'dealReference': 'r'},
        ('confirms/r', 'get'): {
            'dealStatus': 'ACCEPTED', 'status': 'OPEN',
            'dealId': 'd', 'level': 1.0, 'size': 1.24,
        },
    })
    env = _entry_envelope(qty=1.237)  # lot_step=0.01 → 1.24
    asyncio.run(broker.execute_entry(env))
    call = [c for c in broker._calls if c[0] == 'positions' and c[1] == 'post'][0]
    assert abs(call[2]['size'] - 1.24) < 1e-9
    store.close()


def __test_execute_exit_full_bracket_returns_two_legs__(tmp_path):
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-L', 'put'): {'dealReference': 'attach'},
        ('confirms/attach', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    # Seed: confirmed entry row for Long
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='deal-L',
                     extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='TP_SL', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.2, sl_price=1.05,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    legs = asyncio.run(broker.execute_exit(env))
    assert len(legs) == 2
    assert legs[0].id.endswith(':tp') and legs[1].id.endswith(':sl')
    assert legs[0].reduce_only is True and legs[1].reduce_only is True
    store.close()


def __test_execute_exit_partial_qty_runtime_rejects__(tmp_path):
    broker, store, ctx = _make_broker(tmp_path)
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=2.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='deal-L', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Partial', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.2,  # qty < row.qty
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    with pytest.raises(ExchangeCapabilityError):
        asyncio.run(broker.execute_exit(env))
    store.close()


def __test_execute_exit_trailing_with_activation_defers_put__(tmp_path):
    # body stays empty (trail_price present, no tp/sl), so no PUT is issued.
    broker, store, ctx = _make_broker(tmp_path)
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='deal-L', extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Trail', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, trail_offset=0.0050, trail_price=1.15,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    legs = asyncio.run(broker.execute_exit(env))
    assert len(legs) == 1  # only SL leg synthesized
    # No PUT issued since body empty
    assert not any(c[0] == 'positions/deal-L' and c[1] == 'put'
                   for c in broker._calls)
    # SL row carries pending trail_state
    sl_row = ctx.get_order(legs[0].client_order_id)
    assert sl_row.extras.get('trail_state') == 'pending'
    store.close()


def __test_execute_close_full_deletes_position__(tmp_path):
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
    # DELETE called
    assert any(c[0] == 'positions/deal-L' and c[1] == 'delete'
               for c in broker._calls)
    store.close()


def __test_execute_close_partial_race_outside_window_halts__(tmp_path):
    # Pre-snapshot = 1 row size=2.0. POST partial close size=1.0.
    # Post-snapshot = 2 rows (original + fresh opposite) — expected 1 unit,
    # but createdDateUTC is stale (outside ±3s) → manual intervention.
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'orig', 'direction': 'BUY', 'size': 2.0}},
            {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'fresh', 'direction': 'SELL', 'size': 1.0,
                          'createdDateUTC': '1970-01-01T00:00:00.000'}},
        ]},
        ('positions', 'post'): {'dealReference': 'x'},
        ('confirms/x', 'get'): {'dealStatus': 'ACCEPTED'},
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
    store.close()


def __test_execute_cancel_working_order_deletes__(tmp_path):
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('workingorders/w1', 'delete'): {},
    })
    ctx.upsert_order('coid-w', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='w1', extras={'kind': 'working'})
    env = DispatchEnvelope(
        intent=CancelIntent(pine_id='Long', symbol='EURUSD'),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    ok = asyncio.run(broker.execute_cancel(env))
    assert ok is True
    assert any(c[0] == 'workingorders/w1' and c[1] == 'delete'
               for c in broker._calls)
    store.close()


def __test_execute_cancel_no_match_is_noop__(tmp_path):
    broker, store, _ = _make_broker(tmp_path)
    env = DispatchEnvelope(
        intent=CancelIntent(pine_id='DoesNotExist', symbol='EURUSD'),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    ok = asyncio.run(broker.execute_cancel(env))
    assert ok is True
    store.close()


def __test_modify_entry_size_change_falls_back_to_super__(tmp_path):
    # With size change the override delegates to super().modify_entry,
    # which is cancel+execute. We assert that by verifying a DELETE was
    # attempted (base class cancel path).
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('workingorders/w1', 'delete'): {},
        ('workingorders', 'post'): {'dealReference': 'newref'},
        ('confirms/newref', 'get'): {
            'dealStatus': 'ACCEPTED', 'status': 'OPEN', 'dealId': 'w2',
        },
    })
    ctx.upsert_order('coid-w', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='w1',
                     extras={'kind': 'working', 'order_type': 'limit'})
    old = _entry_envelope(qty=1.0, order_type=OrderType.LIMIT, limit=1.2)
    new = _entry_envelope(qty=2.0, order_type=OrderType.LIMIT, limit=1.2)
    asyncio.run(broker.modify_entry(old, new))
    # super().modify_entry → cancel path → DELETE working order
    assert any(c[0] == 'workingorders/w1' and c[1] == 'delete'
               for c in broker._calls)
    store.close()


def __test_process_activity_external_is_logged_and_skipped__(tmp_path):
    broker, store, ctx = _make_broker(tmp_path)
    events_before = 0
    events = list(ctx._store._conn.execute(
        "SELECT id FROM events WHERE run_instance_id = ?",
        (ctx.run_instance_id,),
    ))
    events_before = len(events)
    activities = [{
        'dateUTC': '2026-04-21T10:00:00.000', 'dealId': 'external-deal',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'DEALER',
        'epic': 'EURUSD', 'direction': 'BUY', 'size': 1.0,
    }]

    async def drain():
        out = []
        async for ev in broker._process_activity(activities):
            out.append(ev)
        return out

    emitted = asyncio.run(drain())
    assert emitted == []  # external, not emitted
    events_after = list(ctx._store._conn.execute(
        "SELECT kind FROM events WHERE run_instance_id = ?",
        (ctx.run_instance_id,),
    ))
    kinds = [r['kind'] for r in events_after]
    assert 'external_activity_ignored' in kinds
    store.close()


def __test_process_activity_fingerprint_dedups_in_session__(tmp_path):
    broker, store, ctx = _make_broker(tmp_path)
    # Seed an owned row
    ctx.upsert_order('coid', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='d1', extras={'kind': 'position'})
    ctx.add_ref('coid', 'deal_id', 'd1')
    activity = {
        'dateUTC': '2026-04-21T10:00:00.000', 'dealId': 'd1',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'DEALER',
        'size': 1.0, 'level': 1.10,
    }

    async def drain(acts):
        out = []
        async for ev in broker._process_activity(acts):
            out.append(ev)
        return out

    first = asyncio.run(drain([activity]))
    second = asyncio.run(drain([activity]))
    assert len(first) == 1
    assert second == []  # dedup'd via fingerprint
    store.close()


def __test_recover_submitted_row_single_activity_match_promotes__(tmp_path):
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': [
            {'epic': 'EURUSD', 'direction': 'BUY', 'size': 1.0,
             'dateUTC': '2026-04-21T10:00:00.000', 'dealId': 'matched',
             'type': 'POSITION', 'status': 'EXECUTED'},
        ]},
    })
    # Submitted row whose created_ts aligns with activity dateUTC
    import datetime as _dt
    ts_ms = int(_dt.datetime(2026, 4, 21, 10, 0, 0,
                             tzinfo=_dt.UTC).timestamp() * 1000)
    ctx.upsert_order('lost-coid', symbol='EURUSD', side='buy', qty=1.0,
                     state='submitted', pine_entry_id='Long',
                     intent_key='Long')
    ctx._store._conn.execute(
        "UPDATE orders SET created_ts_ms = ? WHERE client_order_id = ?",
        (ts_ms, 'lost-coid'),
    )
    asyncio.run(broker._recover_in_flight_submissions())
    row = ctx.get_order('lost-coid')
    assert row.state == 'confirmed'
    assert row.exchange_order_id == 'matched'
    store.close()


def __test_recover_submitted_row_multi_match_raises_manual_intervention__(tmp_path):
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': [
            {'epic': 'EURUSD', 'direction': 'BUY', 'size': 1.0,
             'dateUTC': '2026-04-21T10:00:00.000', 'dealId': 'm1',
             'type': 'POSITION', 'status': 'EXECUTED'},
            {'epic': 'EURUSD', 'direction': 'BUY', 'size': 1.0,
             'dateUTC': '2026-04-21T10:00:00.500', 'dealId': 'm2',
             'type': 'POSITION', 'status': 'EXECUTED'},
        ]},
    })
    import datetime as _dt
    ts_ms = int(_dt.datetime(2026, 4, 21, 10, 0, 0,
                             tzinfo=_dt.UTC).timestamp() * 1000)
    ctx.upsert_order('ambiguous', symbol='EURUSD', side='buy', qty=1.0,
                     state='submitted', pine_entry_id='Long',
                     intent_key='Long')
    ctx._store._conn.execute(
        "UPDATE orders SET created_ts_ms = ? WHERE client_order_id = ?",
        (ts_ms, 'ambiguous'),
    )
    with pytest.raises(BrokerManualInterventionError):
        asyncio.run(broker._recover_in_flight_submissions())
    store.close()


def __test_trailing_monitor_pending_crosses_activation__(tmp_path):
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-L', 'put'): {'dealReference': 'putref'},
    })
    # SL leg on a long entry activates when mid rises above trail_price.
    ctx.upsert_order('sl-coid', symbol='EURUSD', side='sell', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     from_entry='Long',
                     trailing_distance=0.0050,
                     extras={'leg_kind': 'sl',
                             'parent_deal_id': 'deal-L',
                             'trail_state': 'pending',
                             'trail_activation_price': 1.15})
    positions = {'deal-L': {
        'market': {'epic': 'EURUSD', 'bid': 1.1502, 'offer': 1.1504},
        'position': {'trailingStop': False},
    }}

    async def drain():
        out = []
        async for ev in broker._trailing_activation_monitor(positions):
            out.append(ev)
        return out

    asyncio.run(drain())
    row = ctx.get_order('sl-coid')
    assert row.extras.get('trail_state') == 'activating'
    assert any(c[0] == 'positions/deal-L' and c[1] == 'put'
               for c in broker._calls)
    store.close()


def __test_trailing_monitor_activating_to_active_on_snapshot__(tmp_path):
    broker, store, ctx = _make_broker(tmp_path)
    ctx.upsert_order('sl-coid', symbol='EURUSD', side='sell', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     from_entry='Long', trailing_distance=0.005,
                     extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                             'trail_state': 'activating',
                             'trail_activation_price': 1.15})
    positions = {'deal-L': {
        'market': {'epic': 'EURUSD', 'bid': 1.16, 'offer': 1.1602},
        'position': {'trailingStop': True},  # exchange confirms
    }}

    async def drain():
        async for _ in broker._trailing_activation_monitor(positions):
            pass

    asyncio.run(drain())
    row = ctx.get_order('sl-coid')
    assert 'trail_state' not in (row.extras or {})
    assert row.trailing_stop is True
    store.close()


def __test_missing_pending_tracker_fires_unexpected_cancel__(tmp_path):
    # stop policy → UnexpectedCancelError bubbles up.
    broker, store, ctx = _make_broker(
        tmp_path,
        config=_make_config(
            on_unexpected_cancel='stop',
            poll_interval_seconds=0.1,  # grace = max(5s, 0.5s) = 5s
        ),
    )
    old_ts = epoch_time_compat = __import__('time').time() - 120.0
    ctx.upsert_order('lost', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='gone',
                     extras={'missing_pending_since': old_ts,
                             'kind': 'position'})

    async def drain():
        events = []
        async for ev in broker._missing_pending_tracker({}, {}):
            events.append(ev)
        return events

    with pytest.raises(UnexpectedCancelError):
        asyncio.run(drain())
    store.close()


def __test_missing_pending_tracker_ignore_policy_suppresses__(tmp_path):
    broker, store, ctx = _make_broker(
        tmp_path,
        config=_make_config(on_unexpected_cancel='ignore'),
    )
    old_ts = __import__('time').time() - 120.0
    ctx.upsert_order('lost', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='gone',
                     extras={'missing_pending_since': old_ts,
                             'kind': 'position'})

    async def drain():
        out = []
        async for ev in broker._missing_pending_tracker({}, {}):
            out.append(ev)
        return out

    events = asyncio.run(drain())
    # Event is still emitted (cancelled), but the policy does NOT raise.
    assert len(events) == 1
    store.close()


def __test_map_exception_invalid_stop_distance_carries_min_value__():
    broker = _FakeBroker(config=_make_config())
    err = CapitalComError(
        "API error occured: error.invalid.stoploss.minvalue: 0.00050",
    )
    mapped = broker._map_exception(err)
    assert isinstance(mapped, InvalidStopDistanceError)
    assert abs(mapped.min_distance - 0.00050) < 1e-9


def __test_map_exception_not_found_deal_id__():
    broker = _FakeBroker(config=_make_config())
    err = CapitalComError("API error occured: error.not-found.dealId")
    mapped = broker._map_exception(err)
    assert isinstance(mapped, OrderNotFoundError)
    assert mapped.ref_type == 'deal_id'


def __test_map_exception_httpx_timeout_becomes_connection_error__():
    import httpx
    broker = _FakeBroker(config=_make_config())
    err = httpx.TimeoutException("read timeout")
    from pynecore.core.broker.exceptions import ExchangeConnectionError
    mapped = broker._map_exception(err)
    assert isinstance(mapped, ExchangeConnectionError)
