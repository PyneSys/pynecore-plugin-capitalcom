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
    CancelIntent, CloseIntent, ExitIntent, LegType, OrderStatus,
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


def __test_process_activity_unmatched_then_matched_emits_on_second_poll__(tmp_path):
    """Race: ``execute_entry``'s confirm step has not yet attached the
    deal_id when the watch_orders poller first sees the EXECUTED activity.
    The first poll classifies it as external (logged once, fingerprint NOT
    cached). The bot then runs ``add_ref('deal_id', …)``. The second poll
    re-evaluates the same activity, finds the row, and emits the fill.
    """
    broker, store, ctx = _make_broker(tmp_path)
    # Bot has dispatched the entry but not yet attached the deal_id ref.
    ctx.upsert_order('coid', symbol='EURUSD', side='buy', qty=1.0,
                     state='server_ref_seen', pine_entry_id='Long',
                     extras={'kind': 'position'})
    activity = {
        'dateUTC': '2026-04-21T10:00:00.000', 'dealId': 'd-late',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'DEALER',
        'size': 1.0, 'level': 1.10,
    }

    async def drain(acts):
        out = []
        async for ev in broker._process_activity(acts):
            out.append(ev)
        return out

    first = asyncio.run(drain([activity]))
    assert first == []  # row not yet linked to deal_id

    # Bot's confirm step lands.
    ctx.add_ref('coid', 'deal_id', 'd-late')
    ctx.set_exchange_id('coid', 'd-late')
    ctx.set_order_state('coid', 'confirmed')

    second = asyncio.run(drain([activity]))
    assert len(second) == 1
    assert second[0].event_type == 'filled'
    assert second[0].pine_id == 'Long'

    # And a third poll should NOT re-emit (now in seen_fingerprints).
    third = asyncio.run(drain([activity]))
    assert third == []

    # external_activity_ignored logged exactly once across the three polls.
    kinds = [
        r['kind'] for r in ctx._store._conn.execute(
            "SELECT kind FROM events WHERE run_instance_id = ?",
            (ctx.run_instance_id,),
        )
    ]
    assert kinds.count('external_activity_ignored') == 1
    assert kinds.count('activity_processed') == 1
    store.close()


def __test_process_activity_fills_zero_level_from_position_snapshot__(tmp_path):
    """Capital.com sometimes returns a market-entry activity row with an
    empty/zero ``level`` field.  The fill price must be back-filled from
    the same poll cycle's ``GET /positions`` snapshot — otherwise the
    resulting :class:`OrderEvent` carries ``fill_price=0.0`` and
    :meth:`BrokerPosition.record_fill` silently drops it, stranding
    ``position.size`` at zero.
    """
    broker, store, ctx = _make_broker(tmp_path)
    ctx.upsert_order('coid', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='d1', extras={'kind': 'position'})
    ctx.add_ref('coid', 'deal_id', 'd1')
    activity = {
        'dateUTC': '2026-04-21T10:00:00.000', 'dealId': 'd1',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'DEALER',
        'size': 1.0, 'level': 0.0,
    }
    positions_by_deal = {'d1': {'position': {'dealId': 'd1', 'level': 1.17341}}}

    async def drain(acts, snap):
        out = []
        async for ev in broker._process_activity(acts, snap):
            out.append(ev)
        return out

    events = asyncio.run(drain([activity], positions_by_deal))
    assert len(events) == 1
    assert events[0].event_type == 'filled'
    assert events[0].fill_price == 1.17341
    store.close()


def __test_process_activity_fills_zero_level_from_extras_confirm_level__(tmp_path):
    """Poll-cycle race: the fill lands *between* the same poll's
    ``/positions`` and ``/history/activity`` fetches, so the activity
    row reports ``level=0`` AND the snapshot has no row for this
    ``dealId`` either. The price is recovered from
    ``row.extras['confirm_level']`` — persisted by :meth:`execute_entry`
    after the synchronous confirm step. Without this fallback the
    resulting :class:`OrderEvent` carries ``fill_price=0.0``, the
    sync engine emits a broker-warning, and ``position.size`` strands
    at zero.
    """
    broker, store, ctx = _make_broker(tmp_path)
    ctx.upsert_order('coid', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='d1',
                     extras={'kind': 'position', 'confirm_level': 1.17350})
    ctx.add_ref('coid', 'deal_id', 'd1')
    activity = {
        'dateUTC': '2026-04-21T10:00:00.000', 'dealId': 'd1',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'DEALER',
        'size': 1.0, 'level': 0.0,
    }
    # Empty positions snapshot — the race scenario this fallback covers.
    positions_by_deal: dict[str, dict] = {}

    async def drain(acts, snap):
        out = []
        async for ev in broker._process_activity(acts, snap):
            out.append(ev)
        return out

    events = asyncio.run(drain([activity], positions_by_deal))
    assert len(events) == 1
    assert events[0].event_type == 'filled'
    assert events[0].fill_price == 1.17350
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


def __test_recover_disposition_unknown_promotes_via_activity_match__(tmp_path):
    """Rows left in ``disposition_unknown`` after a POST timeout are
    treated as a superset of ``submitted`` by the recovery loop — the
    POST may or may not have landed, so an activity-match resolution is
    the same call the dossier §5.1 #3 prescribes for the pre-ref case.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': [
            {'epic': 'EURUSD', 'direction': 'BUY', 'size': 1.0,
             'dateUTC': '2026-04-21T10:00:00.000', 'dealId': 'recovered',
             'type': 'POSITION', 'status': 'EXECUTED'},
        ]},
    })
    import datetime as _dt
    ts_ms = int(_dt.datetime(2026, 4, 21, 10, 0, 0,
                             tzinfo=_dt.UTC).timestamp() * 1000)
    ctx.upsert_order('parked-coid', symbol='EURUSD', side='buy', qty=1.0,
                     state='disposition_unknown', pine_entry_id='Long',
                     intent_key='Long')
    ctx._store._conn.execute(
        "UPDATE orders SET created_ts_ms = ? WHERE client_order_id = ?",
        (ts_ms, 'parked-coid'),
    )
    asyncio.run(broker._recover_in_flight_submissions())
    row = ctx.get_order('parked-coid')
    assert row.state == 'confirmed'
    assert row.exchange_order_id == 'recovered'
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

    asyncio.run(broker._trailing_activation_monitor(positions))
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

    asyncio.run(broker._trailing_activation_monitor(positions))
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


# =======================================================================
# Natural-close regression tests — TP/SL fill on a bracket-attached entry.
# Three structural bugs are covered:
#   (A) Entry + bracket rows must be closed in BrokerStore so
#       ``_reconcile_snapshot`` does not stamp ``missing_pending_since``
#       and ``_missing_pending_tracker`` does not raise a false
#       ``UnexpectedCancelError`` after the grace window.
#   (B) The emitted ``OrderEvent.order.side`` for a closing leg must come
#       from the activity's ``direction`` field (opposite of the entry's
#       side), not from the matched entry row's side. Otherwise
#       ``BrokerPosition.record_fill`` adds to the position instead of
#       reducing it, doubling the local Pine-side size on every TP/SL
#       fill while the exchange already closed the position.
#   (C) The fill-price ``level<=0`` fallback must NOT use the entry row's
#       ``extras['confirm_level']`` for a closing leg — that is the
#       entry price, not the close price. For closing legs the bracket
#       leg row's ``tp_level`` / ``sl_level`` is the right source.
# =======================================================================


def _seed_bracket_setup(ctx, *, entry_side='buy', tp_price=1.17699,
                        sl_price=1.17636, confirm_level=1.17667,
                        deal_id='deal-L', entry_coid='coid-entry',
                        tp_coid='coid-tp', sl_coid='coid-sl',
                        symbol='EURUSD', qty=1.0, pine_id='Long'):
    """Seed a confirmed entry row + bracket TP/SL rows mirroring what
    :meth:`execute_entry` + :meth:`execute_exit` would produce.

    Returns the entry / TP / SL CO-IDs so the caller can assert on them.
    """
    ctx.upsert_order(
        entry_coid, symbol=symbol, side=entry_side, qty=qty,
        state='confirmed', pine_entry_id=pine_id,
        exchange_order_id=deal_id,
        extras={'kind': 'position', 'confirm_level': confirm_level},
    )
    ctx.add_ref(entry_coid, 'deal_id', deal_id)
    exit_side = 'sell' if entry_side == 'buy' else 'buy'
    ctx.upsert_order(
        tp_coid, symbol=symbol, side=exit_side, qty=qty,
        state='confirmed', pine_entry_id=pine_id, from_entry=pine_id,
        tp_level=tp_price,
        extras={'leg_kind': 'tp', 'parent_deal_id': deal_id},
    )
    ctx.upsert_order(
        sl_coid, symbol=symbol, side=exit_side, qty=qty,
        state='confirmed', pine_entry_id=pine_id, from_entry=pine_id,
        sl_level=sl_price,
        extras={'leg_kind': 'sl', 'parent_deal_id': deal_id},
    )
    return entry_coid, tp_coid, sl_coid


def __test_process_activity_tp_fill_flags_entry_and_bracket_rows__(tmp_path):
    """Bug A: a TP fill on a Capital.com native bracket closes the
    position and auto-cancels the OCA partner leg. The plugin must
    stamp the entry + both bracket rows with ``extras['natural_close_at']``
    so the next ``_reconcile_snapshot`` poll does not mistake the
    now-vanished ``dealId`` for a missing protective order.

    The rows are kept LIVE in BrokerStore on purpose: subsequent
    ``execute_exit`` / ``modify_exit`` calls (Pine re-emits
    ``strategy.exit`` on every bar even after a close) need to find
    the entry row by ``pine_entry_id`` until Pine's next entry
    replaces it. Closing the row physically would break that lookup.
    """
    broker, store, ctx = _make_broker(tmp_path)
    e_coid, tp_coid, sl_coid = _seed_bracket_setup(ctx)
    activity = {
        'dateUTC': '2026-04-21T10:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'TP',
        'epic': 'EURUSD', 'direction': 'SELL',
        'size': 1.0, 'level': 1.17700,
    }

    async def drain():
        out = []
        async for ev in broker._process_activity([activity]):
            out.append(ev)
        return out

    events = asyncio.run(drain())
    assert len(events) == 1
    assert events[0].leg_type == LegType.TAKE_PROFIT
    assert events[0].event_type == 'filled'

    # All three rows stay live, but each carries the natural_close_at flag.
    for coid in (e_coid, tp_coid, sl_coid):
        row = ctx.get_order(coid)
        assert row is not None, f"row {coid!r} must remain live in BrokerStore"
        assert row.closed_ts_ms is None, (
            f"row {coid!r} must NOT be physically closed — "
            "execute_exit/modify_exit need to find it"
        )
        assert (row.extras or {}).get('natural_close_at') is not None, (
            f"row {coid!r} must carry the natural_close_at flag so "
            "_reconcile_snapshot skips its missing-pending accounting"
        )
    store.close()


def __test_execute_exit_skips_natural_close_flagged_row_picks_new_entry__(tmp_path):
    """Re-entry after natural close: the OLD entry row stays live in
    BrokerStore but flagged with ``natural_close_at`` (so prior
    ``modify_exit`` lookups keep working until Pine actually re-emits
    a new bracket). Once Pine opens a NEW entry under the same
    ``pine_entry_id``, two rows match the search. The plugin must
    pick the new (active, unflagged) row and PUT against its dealId,
    NOT the old one — otherwise Capital.com returns
    ``error.not-found.dealId`` (404) and the dispatch crashes.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-NEW', 'put'): {'dealReference': 'attach-new'},
        ('confirms/attach-new', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    # Old entry row, naturally closed (TP/SL fired earlier).
    ctx.upsert_order(
        'coid-old', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-OLD',
        extras={'kind': 'position',
                'confirm_level': 1.17500,
                'natural_close_at': 1700000000.0},
    )
    ctx.add_ref('coid-old', 'deal_id', 'deal-OLD')
    # New entry row from Pine's re-entry on a later bar.
    ctx.upsert_order(
        'coid-new', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-NEW',
        extras={'kind': 'position', 'confirm_level': 1.17800},
    )
    ctx.add_ref('coid-new', 'deal_id', 'deal-NEW')

    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.17900, sl_price=1.17750,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    legs = asyncio.run(broker.execute_exit(env))
    assert len(legs) == 2

    # The PUT must target the NEW dealId, not the flagged OLD one.
    puts = [c for c in broker._calls
            if c[0].startswith('positions/') and c[1] == 'put']
    assert len(puts) == 1
    assert puts[0][0] == 'positions/deal-NEW', (
        f"PUT must target the active entry (deal-NEW), got {puts[0][0]!r}"
    )
    store.close()


def __test_execute_exit_rejects_when_only_flagged_entry_exists__(tmp_path):
    """Counterpart to the re-entry test: when the ONLY entry row for
    the requested ``pine_entry_id`` is flagged with ``natural_close_at``
    (the position closed on the broker, no new entry yet), the plugin
    must reject the exit dispatch — PUT'ing against a closed dealId
    would 404 with ``error.not-found.dealId`` and crash the engine.

    The sync engine's ``_cleanup_closed_position`` normally clears
    the matching exit intent before this state is observable, but
    a same-bar race could still feed an exit dispatch through; this
    test pins the fail-loud behaviour.
    """
    broker, store, ctx = _make_broker(tmp_path)
    _seed_bracket_setup(ctx)
    activity = {
        'dateUTC': '2026-04-21T10:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'SL',
        'epic': 'EURUSD', 'direction': 'BUY', 'size': 1.0, 'level': 1.17630,
    }

    async def feed_close():
        async for _ in broker._process_activity([activity]):
            pass

    asyncio.run(feed_close())

    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000, sl_price=1.17000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    with pytest.raises(ExchangeOrderRejectedError):
        asyncio.run(broker.execute_exit(new_env))
    store.close()


def __test_process_activity_closing_leg_side_is_opposite_of_entry__(tmp_path):
    """Bug B (corrected): Capital.com's ``direction`` field on a TP/SL
    close activity reports the POSITION direction (``'BUY'`` for a long
    being closed), NOT the closing trade direction. So ``side`` cannot
    be read off ``direction`` alone — the closing-leg side must be the
    opposite of ``row.side`` (the matched entry row's open direction).

    Without this flip ``BrokerPosition.record_fill`` reads
    ``side='buy'`` on a long-closing fill, computes a positive
    ``signed_delta``, and ADDS to the existing long instead of
    reducing it — doubling the local Pine-side size while the
    exchange already closed the position.

    The test feeds the realistic Capital.com payload — ``direction='BUY'``
    on a long position's SL fill — and asserts the emitted event side
    is ``'sell'``.
    """
    broker, store, ctx = _make_broker(tmp_path)
    _seed_bracket_setup(ctx)  # long entry, side='buy'
    activity = {
        'dateUTC': '2026-04-21T10:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'SL',
        'epic': 'EURUSD', 'direction': 'BUY',  # position direction, not trade
        'size': 1.0, 'level': 1.17630,
    }

    async def drain():
        out = []
        async for ev in broker._process_activity([activity]):
            out.append(ev)
        return out

    events = asyncio.run(drain())
    assert len(events) == 1
    assert events[0].leg_type == LegType.STOP_LOSS
    assert events[0].order.side == 'sell', (
        "closing-leg side must be the opposite of the entry's side; "
        f"got {events[0].order.side!r} (record_fill would add to the "
        "position instead of reducing it)"
    )
    store.close()


def __test_process_activity_short_closing_leg_side_is_buy__(tmp_path):
    """Mirror of the long-close case: a short entry (``side='sell'``)
    closing via TP fill emits ``side='buy'`` regardless of what
    Capital.com puts in ``direction``.
    """
    broker, store, ctx = _make_broker(tmp_path)
    _seed_bracket_setup(ctx, entry_side='sell')
    activity = {
        'dateUTC': '2026-04-21T10:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'TP',
        'epic': 'EURUSD', 'direction': 'SELL',  # position direction
        'size': 1.0, 'level': 1.17699,
    }

    async def drain():
        out = []
        async for ev in broker._process_activity([activity]):
            out.append(ev)
        return out

    events = asyncio.run(drain())
    assert len(events) == 1
    assert events[0].leg_type == LegType.TAKE_PROFIT
    assert events[0].order.side == 'buy'
    store.close()


def __test_process_activity_tp_fill_uses_leg_tp_level_not_entry_confirm__(tmp_path):
    """Bug C: ``level=0`` fallback chain must NOT use the entry's
    ``extras['confirm_level']`` for a closing leg — that is the entry
    price, not the close price. The bracket TP row's ``tp_level`` is
    the correct fallback when Capital.com leaves the activity row's
    ``level`` blank on a TP fill.
    """
    broker, store, ctx = _make_broker(tmp_path)
    _seed_bracket_setup(
        ctx,
        confirm_level=1.17667,  # entry price — must NOT be reused for TP
        tp_price=1.17699,        # bracket TP — this is the correct fallback
    )
    activity = {
        'dateUTC': '2026-04-21T10:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'TP',
        'epic': 'EURUSD', 'direction': 'SELL',
        'size': 1.0, 'level': 0.0,   # the offending Capital.com gap
    }

    async def drain():
        out = []
        async for ev in broker._process_activity([activity], None):
            out.append(ev)
        return out

    events = asyncio.run(drain())
    assert len(events) == 1
    assert events[0].fill_price == 1.17699, (
        f"TP fill price must fall back to bracket TP row's tp_level, "
        f"got {events[0].fill_price!r} (likely entry's confirm_level — wrong)"
    )
    store.close()


def __test_natural_close_does_not_stamp_missing_pending__(tmp_path):
    """Integration: full TP fill sequence must not stamp the entry row
    with ``missing_pending_since``, so the grace-window watchdog does
    not later raise a false ``UnexpectedCancelError``.

    Arranges the same poll cycle the production code runs:
      1. ``_process_activity`` consumes the TP fill (and must close all
         three bracket rows as a side-effect).
      2. ``_reconcile_snapshot`` runs against empty positions / working
         maps (the position is gone — that is what triggers the bug
         today). With the rows already closed, the snapshot must NOT
         see them in ``iter_live_orders`` and therefore must NOT stamp
         ``missing_pending_since``.
      3. ``_missing_pending_tracker`` must not raise.
    """
    broker, store, ctx = _make_broker(
        tmp_path,
        config=_make_config(
            on_unexpected_cancel='stop',
            poll_interval_seconds=0.1,
        ),
    )
    e_coid, _, _ = _seed_bracket_setup(ctx)
    activity = {
        'dateUTC': '2026-04-21T10:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'SL',
        'epic': 'EURUSD', 'direction': 'SELL',
        'size': 1.0, 'level': 1.17630,
    }

    async def run_poll():
        async for _ in broker._process_activity([activity]):
            pass
        # /positions and /workingorders are now empty (position closed).
        async for _ in broker._reconcile_snapshot({}, {}):
            pass
        async for _ in broker._missing_pending_tracker({}, {}):
            pass

    asyncio.run(run_poll())  # must not raise

    row = ctx.get_order(e_coid)
    assert row is None or 'missing_pending_since' not in (row.extras or {}), (
        f"entry row must not be stamped missing — extras={row.extras!r}"
    )
