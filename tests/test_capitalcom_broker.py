"""
@pyne
"""
import asyncio
import json

import httpx
import pytest

from pynecore.core.broker.exceptions import (
    AuthenticationError,
    BracketAttachAfterFillRejectedError,
    BrokerError,
    ExchangeCapabilityError,
    ExchangeRateLimitError,
    ExchangeOrderRejectedError,
)
from pynecore.core.broker.models import (
    CapabilityLevel, DispatchEnvelope, EntryIntent, OrderType,
)
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore

from pynecore_capitalcom import (
    CapitalCom,
    CapitalComConfig,
    CapitalComError,
)
from pynecore_capitalcom.plugin import _activity_fingerprint


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
    assert caps.stop_order is CapabilityLevel.NATIVE
    assert caps.stop_limit_order is CapabilityLevel.UNSUPPORTED
    assert caps.trailing_stop is CapabilityLevel.NATIVE
    assert caps.tp_sl_bracket is CapabilityLevel.NATIVE
    assert caps.partial_qty_bracket_exit is CapabilityLevel.UNSUPPORTED
    assert caps.oca_cancel is CapabilityLevel.SOFTWARE
    assert caps.amend_order is CapabilityLevel.PARTIAL_NATIVE
    assert caps.cancel_all is CapabilityLevel.SOFTWARE
    assert caps.reduce_only is CapabilityLevel.SOFTWARE
    assert caps.watch_orders is CapabilityLevel.SOFTWARE
    assert caps.fetch_position is CapabilityLevel.NATIVE
    assert caps.idempotency is CapabilityLevel.SOFTWARE


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


# noinspection PyProtectedMember
def __test_map_exception_session_token_routes_to_connection_error__():
    """``error.invalid.session.token`` must NOT surface as an order rejection.

    The reactive retry in ``__call__`` recreates the session and retries,
    but if retries are exhausted (or the failure was on a bootstrap
    endpoint the retry path skips), the error reaches ``_map_exception``.
    The code starts with ``error.invalid.``, so the generic prefix branch
    would historically classify it as :class:`ExchangeOrderRejectedError`
    — falsely reporting a dead REST session as a rejected trade. The fix
    is to route session-recreate codes through the retryable branch
    (``ExchangeConnectionError``) before the prefix fallthrough.
    """
    from pynecore.core.broker.exceptions import ExchangeConnectionError

    broker = _FakeBroker(config=_make_config())
    mapped = broker._map_exception(
        CapitalComError("API error occured: error.invalid.session.token"),
    )
    assert isinstance(mapped, ExchangeConnectionError)
    assert not isinstance(mapped, ExchangeOrderRejectedError)


# noinspection PyProtectedMember
def __test_broker_map_exception_falls_through_to_broker_base__():
    """Stdlib ``ConnectionError`` must be mapped by ``BrokerPlugin._map_exception``.

    Capital.com's ``_map_exception`` only handles ``httpx`` transport errors
    and ``CapitalComError`` payloads itself; everything else falls through
    via ``super()``. The MRO from the Capital.com mix-in must reach the
    ``BrokerPlugin`` base — any intermediate class declaring an ``...``
    stub for ``_map_exception`` would silently break this fallthrough and
    classify a connection drop as ``None`` (re-raise as-is) instead of
    :class:`ExchangeConnectionError`.
    """
    from pynecore.core.broker.exceptions import ExchangeConnectionError

    broker = _FakeBroker(config=_make_config())
    mapped = broker._map_exception(ConnectionError("peer reset"))
    assert isinstance(mapped, ExchangeConnectionError)


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
    InvalidStopDistanceError,
    InvalidStopMaxValueError,
    InvalidTakeProfitDistanceError,
    InvalidTakeProfitMaxValueError,
    OrderNotFoundError,
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


def __test_get_instrument_rules_ttl_expiry_refetches__(tmp_path, monkeypatch):
    """Cache honours ``instrument_rules_ttl_seconds``: a stale entry forces
    a re-fetch instead of indefinite reuse."""
    broker, store, _ = _make_broker(
        tmp_path,
        config=_make_config(instrument_rules_ttl_seconds=30.0),
    )
    fake_now = {'t': 1_000_000.0}
    monkeypatch.setattr(
        'pynecore_capitalcom.provider.epoch_time', lambda: fake_now['t'],
    )
    asyncio.run(broker._get_instrument_rules('EURUSD'))
    fetch_calls = [c for c in broker._calls if c[0] == 'markets/EURUSD']
    assert len(fetch_calls) == 1
    fake_now['t'] += 5.0
    asyncio.run(broker._get_instrument_rules('EURUSD'))
    assert len([c for c in broker._calls if c[0] == 'markets/EURUSD']) == 1
    fake_now['t'] += 60.0
    asyncio.run(broker._get_instrument_rules('EURUSD'))
    assert len([c for c in broker._calls if c[0] == 'markets/EURUSD']) == 2
    store.close()


def _rules_resp_with_snapshot(bid: float, offer: float) -> dict:
    """Variant of :data:`_RULES_RESP` carrying a ``snapshot`` block so the
    plugin's pre-check can read a live mid quote during pre-validation tests."""
    return {**_RULES_RESP, 'snapshot': {'bid': bid, 'offer': offer}}


def __test_execute_exit_pre_validates_sl_distance__(tmp_path):
    """SL closer than ``minNormalStopOrLimitDistance`` from the live mid
    raises before any PUT is issued."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.10000, 1.10002),
        ('positions/deal-L', 'put'): {'dealReference': 'attach'},
        ('confirms/attach', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    # min_stop_or_limit_distance from _RULES_RESP = 0.0001; mid = 1.10001;
    # SL at 1.10005 → distance 0.00004 < 0.0001 ⇒ rejected.
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='deal-L',
                     extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='SL', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, sl_price=1.10005,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    with pytest.raises(InvalidStopDistanceError) as exc:
        asyncio.run(broker.execute_exit(env))
    assert abs(exc.value.min_distance - 0.0001) < 1e-9
    assert not any(
        c[0] == 'positions/deal-L' and c[1] == 'put' for c in broker._calls
    ), "PUT must not be issued when pre-check rejects"
    store.close()


def __test_execute_exit_pre_validates_tp_distance__(tmp_path):
    """TP closer than the symmetric minimum raises
    :class:`InvalidTakeProfitDistanceError` before any PUT."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.10000, 1.10002),
        ('positions/deal-L', 'put'): {'dealReference': 'attach'},
        ('confirms/attach', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='deal-L',
                     extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='TP', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.10003,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    with pytest.raises(InvalidTakeProfitDistanceError) as exc:
        asyncio.run(broker.execute_exit(env))
    assert abs(exc.value.min_distance - 0.0001) < 1e-9
    assert not any(
        c[0] == 'positions/deal-L' and c[1] == 'put' for c in broker._calls
    )
    store.close()


def __test_execute_exit_pre_check_uses_live_mid_not_entry_fill__(tmp_path):
    """When price has drifted away from the entry, a valid SL near current
    mid must NOT be rejected on the basis of distance to ``confirm_level``."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        # Entry filled at 1.10000, market now at 1.11000 — SL at 1.10900
        # is 100 pips from the entry but only 10 pips from the live mid;
        # min distance is 0.0001 so this is well above the floor.
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.10999, 1.11001),
        ('positions/deal-L', 'put'): {'dealReference': 'attach'},
        ('confirms/attach', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='deal-L',
                     extras={'kind': 'position', 'confirm_level': 1.10000})
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='SL', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, sl_price=1.10900,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    legs = asyncio.run(broker.execute_exit(env))
    assert any(leg.id.endswith(':sl') for leg in legs)
    assert any(
        c[0] == 'positions/deal-L' and c[1] == 'put' for c in broker._calls
    ), "PUT must be issued when SL distance is valid against live mid"
    store.close()


def __test_modify_exit_pre_validates_distance__(tmp_path):
    """``modify_exit`` runs the same proactive distance check before PUT."""
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.20000, 1.20002),
        ('positions/deal-L', 'put'): {'dealReference': 'amend'},
        ('confirms/amend', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='deal-L',
                     extras={'kind': 'position'})
    # mid = 1.20001; new SL = 1.19996 → distance 0.00005 < 0.0001 ⇒ rejected.
    old_intent = ExitIntent(
        pine_id='Br', from_entry='Long', symbol='EURUSD',
        side='sell', qty=1.0, sl_price=1.19500, tp_price=1.20800,
    )
    new_intent = ExitIntent(
        pine_id='Br', from_entry='Long', symbol='EURUSD',
        side='sell', qty=1.0, sl_price=1.19996, tp_price=1.20800,
    )
    old_env = DispatchEnvelope(
        intent=old_intent, run_tag='test', bar_ts_ms=1700000000000,
    )
    new_env = DispatchEnvelope(
        intent=new_intent, run_tag='test', bar_ts_ms=1700000000000,
    )
    with pytest.raises(InvalidStopDistanceError):
        asyncio.run(broker.modify_exit(old_env, new_env))
    assert not any(
        c[0] == 'positions/deal-L' and c[1] == 'put' for c in broker._calls
    )
    store.close()


def __test_get_instrument_rules_prefers_normal_distance_over_controlled_risk__(tmp_path):
    """Capital.com brackets are normal stops; the wider controlled-risk
    minimum must NOT shadow ``minNormalStopOrLimitDistance`` when both
    values are quoted."""
    broker, store, _ = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): {
            'dealingRules': {
                'minStepDistance': {'value': 0.01},
                'minDealSize': {'value': 0.01},
                'minNormalStopOrLimitDistance': {'value': 0.0001},
                'minControlledRiskStopDistance': {'value': 0.0050},
            },
            'instrument': {'lotSize': 0.01},
        },
    })
    rules = asyncio.run(broker._get_instrument_rules('EURUSD'))
    assert abs(rules.min_stop_or_limit_distance - 0.0001) < 1e-12
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


def __test_execute_cancel_filled_position_is_noop__(tmp_path):
    """Cancel after the entry has filled must NOT close the position.

    TV-verified ``strategy.cancel(entry_id)`` semantics: once the entry
    has filled, the cancel is a no-op. The plugin guards against a stale
    cancel intent slipping through and turning into a market close via
    ``DELETE /positions/{dealId}``.
    """
    broker, store, ctx = _make_broker(tmp_path)
    ctx.upsert_order('coid-p', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='deal-L',
                     extras={'kind': 'position'})
    env = DispatchEnvelope(
        intent=CancelIntent(pine_id='Long', symbol='EURUSD'),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    ok = asyncio.run(broker.execute_cancel(env))
    assert ok is True
    # No DELETE on /positions or /workingorders must be issued.
    assert not any(c[1] == 'delete' for c in broker._calls), (
        f"cancel of filled position must not DELETE, calls={broker._calls}"
    )
    store.close()


def __test_execute_cancel_bracket_leg_clears_via_put__(tmp_path):
    """Bracket leg cancel sends ``PUT /positions`` with one level cleared.

    The bracket TP/SL row carries no ``exchange_order_id`` (the level is
    a position attribute, not a separate order); a DELETE on the row id
    is meaningless. The plugin must instead clear the leg's level via
    PUT, leaving the sister leg untouched.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-L', 'put'): {'dealReference': 'cxl-ref'},
        ('confirms/cxl-ref', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    ctx.upsert_order('coid-tp', symbol='EURUSD', side='sell', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     from_entry='Long',
                     extras={'leg_kind': 'tp', 'parent_deal_id': 'deal-L'})
    env = DispatchEnvelope(
        intent=CancelIntent(pine_id='Long', symbol='EURUSD',
                            from_entry='Long'),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    ok = asyncio.run(broker.execute_cancel(env))
    assert ok is True
    put_calls = [c for c in broker._calls if c[1] == 'put']
    assert len(put_calls) == 1, f"expected one PUT, got {broker._calls}"
    endpoint, _, body = put_calls[0]
    assert endpoint == 'positions/deal-L'
    # TP cancel → only profitLevel cleared; stopLevel untouched (omitted).
    assert body == {'profitLevel': None}
    # Mirror the execute_exit/modify_exit minta: a confirm round-trip on
    # the returned dealReference must follow the bracket PUT (TTL-bounded
    # endpoint, also seeds activity stream).
    assert any(c[0] == 'confirms/cxl-ref' and c[1] == 'get'
               for c in broker._calls), (
        f"bracket cancel must confirm dealReference, calls={broker._calls}"
    )
    store.close()


def __test_execute_cancel_entry_does_not_clear_brackets__(tmp_path):
    """Entry-side ``cancel(entry_id)`` must NOT remove protective brackets.

    Bracket rows store ``pine_entry_id == entry_id`` (so subsequent
    ``execute_exit`` lookups find them). A ``CancelIntent`` with
    ``from_entry=None`` therefore matches the bracket row by
    ``pine_entry_id``. The plugin must skip those bracket rows on entry
    cancels — otherwise an intended-no-op cancel of a filled entry would
    clear TP/SL on a live position.
    """
    broker, store, ctx = _make_broker(tmp_path)
    # Filled entry plus its TP and SL bracket rows.
    ctx.upsert_order('coid-e', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='deal-L',
                     extras={'kind': 'position'})
    ctx.upsert_order('coid-tp', symbol='EURUSD', side='sell', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     from_entry='Long', tp_level=1.20,
                     extras={'leg_kind': 'tp', 'parent_deal_id': 'deal-L'})
    ctx.upsert_order('coid-sl', symbol='EURUSD', side='sell', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     from_entry='Long', sl_level=1.10,
                     extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L'})
    env = DispatchEnvelope(
        intent=CancelIntent(pine_id='Long', symbol='EURUSD'),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    ok = asyncio.run(broker.execute_cancel(env))
    assert ok is True
    # No PUT, no DELETE — the cancel of a filled entry is a complete no-op
    # on the broker side; brackets must remain.
    assert not any(c[1] in ('put', 'delete') for c in broker._calls), (
        f"entry cancel must not touch brackets, calls={broker._calls}"
    )
    # Bracket rows still live in the store.
    assert ctx.get_order('coid-tp') is not None
    assert ctx.get_order('coid-sl') is not None
    store.close()


def __test_execute_cancel_trailing_sl_disables_native_trailing__(tmp_path):
    """Trailing SL cancel must send ``trailingStop: False`` not just ``stopLevel: None``.

    A native trailing stop on Capital.com is gated by the ``trailingStop``
    flag plus ``stopDistance``. Clearing only ``stopLevel`` would close
    the local row but leave the broker still managing a trailing stop on
    the position.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-L', 'put'): {},
    })
    ctx.upsert_order('coid-sl', symbol='EURUSD', side='sell', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     from_entry='Long', trailing_distance=0.0050,
                     trailing_stop=True,
                     extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L'})
    env = DispatchEnvelope(
        intent=CancelIntent(pine_id='SL', symbol='EURUSD',
                            from_entry='Long'),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    ok = asyncio.run(broker.execute_cancel(env))
    assert ok is True
    put_calls = [c for c in broker._calls if c[1] == 'put']
    assert len(put_calls) == 1, f"expected one PUT, got {broker._calls}"
    _, _, body = put_calls[0]
    assert body.get('trailingStop') is False, (
        f"trailing SL cancel must disable trailingStop, body={body}"
    )
    assert body.get('stopLevel') is None
    store.close()


def __test_execute_cancel_does_not_sweep_unrelated_entries__(tmp_path):
    """``intent.from_entry=None`` must not sweep other entries with no from_entry.

    Two pending working orders sit on the same symbol with different
    ``pine_entry_id``s and no ``from_entry``. A cancel for one ID must
    DELETE only that order — the previous matching predicate
    ``row.from_entry == intent.from_entry`` falsely treated
    ``None == None`` as a match for every other entry.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('workingorders/w-L1', 'delete'): {},
        ('workingorders/w-L2', 'delete'): {},
    })
    ctx.upsert_order('coid-w1', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long1',
                     exchange_order_id='w-L1',
                     extras={'kind': 'working'})
    ctx.upsert_order('coid-w2', symbol='EURUSD', side='buy', qty=1.0,
                     state='confirmed', pine_entry_id='Long2',
                     exchange_order_id='w-L2',
                     extras={'kind': 'working'})
    env = DispatchEnvelope(
        intent=CancelIntent(pine_id='Long1', symbol='EURUSD'),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    ok = asyncio.run(broker.execute_cancel(env))
    assert ok is True
    deleted = [c[0] for c in broker._calls if c[1] == 'delete']
    assert deleted == ['workingorders/w-L1'], (
        f"only Long1 should be deleted, got {deleted}"
    )
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


def __test_trailing_monitor_pending_stays_pending_on_put_failure__(tmp_path):
    """Transient PUT failure must not advance the row past ``pending``.

    If the first activation PUT raises a ``BrokerError``, the row must
    stay in ``pending`` so the next tick re-enters the send path.
    Previously the row flipped to ``activating`` *before* the PUT, so a
    failed PUT left the protective trailing stop inactive forever.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('error', 'positions/deal-L', 'put'): BrokerError("transient"),
    })
    ctx.upsert_order('sl-coid', symbol='EURUSD', side='sell', qty=1.0,
                     state='confirmed', pine_entry_id='Long',
                     from_entry='Long', trailing_distance=0.005,
                     extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                             'trail_state': 'pending',
                             'trail_activation_price': 1.15})
    positions = {'deal-L': {
        'market': {'epic': 'EURUSD', 'bid': 1.1502, 'offer': 1.1504},
        'position': {'trailingStop': False},
    }}

    asyncio.run(broker._trailing_activation_monitor(positions))
    row = ctx.get_order('sl-coid')
    assert row.extras.get('trail_state') == 'pending', (
        f"failed PUT must leave trail_state pending, got "
        f"{row.extras.get('trail_state')!r}"
    )
    # The PUT was attempted exactly once this tick.
    put_calls = [c for c in broker._calls if c[1] == 'put']
    assert len(put_calls) == 1
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
    # stop policy (BrokerPlugin default) → UnexpectedCancelError bubbles up.
    broker, store, ctx = _make_broker(
        tmp_path,
        config=_make_config(
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
    broker, store, ctx = _make_broker(tmp_path)
    # The CLI normally injects the runtime policy from brokers.toml; in tests
    # we set it directly on the plugin instance, which is what reconcile reads.
    broker.on_unexpected_cancel = 'ignore'
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


def __test_map_exception_invalid_stop_maxvalue_carries_max_value__():
    """``error.invalid.stoploss.maxvalue: X`` — a fresh fill moved the
    spread past the requested SL level. Must map to
    :class:`InvalidStopMaxValueError` (NOT the distance variant — semantics
    differ: ``X`` here is an absolute price level boundary, not a
    distance) and preserve the boundary level.
    """
    broker = _FakeBroker(config=_make_config())
    err = CapitalComError(
        "API error occured: error.invalid.stoploss.maxvalue: 1.17517",
    )
    mapped = broker._map_exception(err)
    assert isinstance(mapped, InvalidStopMaxValueError)
    assert abs(mapped.max_value - 1.17517) < 1e-9
    assert not isinstance(mapped, InvalidStopDistanceError)


def __test_map_exception_invalid_takeprofit_maxvalue_carries_max_value__():
    """Twin of the stop-loss maxvalue test for the TP leg."""
    broker = _FakeBroker(config=_make_config())
    err = CapitalComError(
        "API error occured: error.invalid.takeprofit.maxvalue: 1.18000",
    )
    mapped = broker._map_exception(err)
    assert isinstance(mapped, InvalidTakeProfitMaxValueError)
    assert abs(mapped.max_value - 1.18000) < 1e-9
    assert not isinstance(mapped, InvalidTakeProfitDistanceError)


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


def __test_call_maps_httpx_connect_error_to_exchange_connection_error__(monkeypatch):
    from pynecore.core.broker.exceptions import ExchangeConnectionError

    broker = CapitalCom(config=_make_config())

    def fake_get(_url, **_kwargs):
        raise httpx.ConnectError(
            "[Errno 8] nodename nor servname provided, or not known",
        )

    monkeypatch.setattr(httpx, 'get', fake_get)

    with pytest.raises(ExchangeConnectionError):
        asyncio.run(broker._call('positions', method='get'))


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
        extras={
            'leg_kind': 'tp', 'parent_deal_id': deal_id,
            'parent_coid': entry_coid,
        },
    )
    ctx.upsert_order(
        sl_coid, symbol=symbol, side=exit_side, qty=qty,
        state='confirmed', pine_entry_id=pine_id, from_entry=pine_id,
        sl_level=sl_price,
        extras={
            'leg_kind': 'sl', 'parent_deal_id': deal_id,
            'parent_coid': entry_coid,
        },
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


# =======================================================================
# Bracket failure / modify_exit / manual-close regression tests:
#   * Bracket attach failure — synchronous reject must roll back the
#     persisted TP/SL leg rows; network timeout must flip them to
#     ``disposition_unknown`` and raise OrderDispositionUnknownError.
#   * modify_exit — the bracket leg rows' ``tp_level`` / ``sl_level``
#     must be mirrored alongside the entry row's ``set_risk``, otherwise
#     a Capital.com close activity with ``level=0`` would surface the
#     OLD level via the closing-leg fallback in ``_activity_to_event``.
#   * Manual-close activity (``USER`` / ``DEALER`` source on an entry
#     row whose entry-fill activity has already been processed) must
#     route as ``LegType.CLOSE`` with the side flipped — the previous
#     branch returned ``LegType.ENTRY`` and ``BrokerPosition.record_fill``
#     would ADD to the position instead of reducing it.
# =======================================================================


def __test_execute_exit_bracket_attach_rejected_rolls_back_legs__(tmp_path):
    """REJECTED confirm on the bracket PUT leaves no protective leg
    on the exchange. The TP/SL rows persisted before the PUT must be
    physically closed in BrokerStore (state='rejected', closed_ts_ms
    set), so a process restart's recovery path does not see stale
    'submitted' protective legs the exchange has no record of.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-L', 'put'): {'dealReference': 'attach-bad'},
        ('confirms/attach-bad', 'get'): {
            'dealStatus': 'REJECTED', 'reason': 'min stop distance violated',
        },
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.17900, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    tp_coid = env.client_order_id('t')
    sl_coid = env.client_order_id('s')

    with pytest.raises(ExchangeOrderRejectedError):
        asyncio.run(broker.execute_exit(env))

    # Both leg rows physically retired (NOT live anymore).
    live_coids = {r.client_order_id for r in ctx.iter_live_orders()}
    assert tp_coid not in live_coids, (
        "TP leg row must be closed after rejected attach"
    )
    assert sl_coid not in live_coids, (
        "SL leg row must be closed after rejected attach"
    )
    tp_row = ctx.get_order(tp_coid)
    sl_row = ctx.get_order(sl_coid)
    assert tp_row is not None and tp_row.closed_ts_ms is not None
    assert sl_row is not None and sl_row.closed_ts_ms is not None
    assert tp_row.state == 'rejected' and sl_row.state == 'rejected'
    store.close()


def __test_execute_exit_bracket_attach_mapped_reject_rolls_back_legs__(tmp_path):
    """In production ``_call`` runs ``_map_exception`` and re-raises 4xx
    rejects as mapped subclasses (``InvalidStopDistanceError`` etc.) — NOT
    raw ``CapitalComError``. The rollback handler must catch those too,
    otherwise the just-persisted TP/SL leg rows linger as ``submitted``
    even though the broker definitely did not attach the bracket.

    Once the rollback succeeds, the parent fill is open AND unprotected;
    the plugin must surface
    :class:`BracketAttachAfterFillRejectedError` (carrying the parent
    deal/coid/side/qty so the sync engine can issue a defensive close)
    rather than the raw mapped reject — halting on the mapped reject
    would leave the unprotected position exposed.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('error', 'positions/deal-L', 'put'):
            InvalidStopDistanceError(
                "error.invalid.stoploss.minvalue: 5", min_distance=5.0,
            ),
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.17900, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    tp_coid = env.client_order_id('t')
    sl_coid = env.client_order_id('s')

    with pytest.raises(BracketAttachAfterFillRejectedError) as exc:
        asyncio.run(broker.execute_exit(env))

    # Cause chain preserves the original mapped reject for log
    # correlation / risk-layer pattern matching.
    assert isinstance(exc.value.__cause__, InvalidStopDistanceError)

    # The error carries the parent-position context the sync engine
    # needs to build a defensive CloseIntent.
    assert exc.value.position_deal_id == 'deal-L'
    assert exc.value.position_coid == 'coid-entry'
    assert exc.value.symbol == 'EURUSD'
    assert exc.value.position_side == 'buy'
    assert exc.value.qty == 1.0
    assert exc.value.from_entry == 'Long'

    # Both leg rows physically retired (state='rejected', closed_ts_ms set).
    live_coids = {r.client_order_id for r in ctx.iter_live_orders()}
    assert tp_coid not in live_coids
    assert sl_coid not in live_coids
    tp_row = ctx.get_order(tp_coid)
    sl_row = ctx.get_order(sl_coid)
    assert tp_row is not None and tp_row.closed_ts_ms is not None
    assert sl_row is not None and sl_row.closed_ts_ms is not None
    assert tp_row.state == 'rejected' and sl_row.state == 'rejected'
    store.close()


def __test_execute_exit_bracket_put_timeout_flips_legs_to_unknown__(tmp_path):
    """A timeout on the bracket PUT leaves the bracket state opaque on
    the exchange — neither rolled back nor confirmed. The leg rows
    must stay live but flip to ``disposition_unknown`` so the next
    ``_reconcile_snapshot`` poll can promote them, and the caller
    receives :class:`OrderDispositionUnknownError` so the engine parks
    the intent rather than treating it as a hard failure.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('error', 'positions/deal-L', 'put'):
            httpx.TimeoutException("read timeout"),
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.17900, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    tp_coid = env.client_order_id('t')
    sl_coid = env.client_order_id('s')

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_exit(env))

    # Leg rows still LIVE (closed_ts_ms is None) but state flipped.
    tp_row = ctx.get_order(tp_coid)
    sl_row = ctx.get_order(sl_coid)
    assert tp_row is not None and tp_row.closed_ts_ms is None
    assert sl_row is not None and sl_row.closed_ts_ms is None
    assert tp_row.state == 'disposition_unknown'
    assert sl_row.state == 'disposition_unknown'
    store.close()


def __test_execute_exit_bracket_confirm_mapped_error_flips_legs_to_unknown__(tmp_path):
    """The bracket PUT succeeded (returned ``dealReference``) but the
    follow-up confirm GET hit a mapped broker error
    (e.g. ``ExchangeRateLimitError`` 429, ``OrderNotFoundError`` 404)
    instead of a network/timeout class. The bracket may already be
    attached on the exchange, so this is just as ambiguous as a
    network timeout — the leg rows must flip to
    ``disposition_unknown`` and the caller must receive
    :class:`OrderDispositionUnknownError` so the engine parks the
    intent and :meth:`_resolve_bracket_leg_disposition` gets a chance
    to promote-or-reject from the next ``/positions`` snapshot.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-L', 'put'): {'dealReference': 'attach-pending'},
        ('error', 'confirms/attach-pending', 'get'):
            ExchangeRateLimitError(
                "error.too-many.requests", retry_after=1.0,
            ),
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.17900, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    tp_coid = env.client_order_id('t')
    sl_coid = env.client_order_id('s')

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_exit(env))

    # Leg rows must NOT be rolled back (the PUT may already have
    # attached the bracket) — they must end up in disposition_unknown
    # so the next snapshot reconcile can resolve them deterministically.
    tp_row = ctx.get_order(tp_coid)
    sl_row = ctx.get_order(sl_coid)
    assert tp_row is not None and tp_row.closed_ts_ms is None
    assert sl_row is not None and sl_row.closed_ts_ms is None
    assert tp_row.state == 'disposition_unknown', (
        f"confirm GET mapped-broker-error must flip TP leg to "
        f"disposition_unknown, got {tp_row.state!r}; without this the "
        f"row stays in 'submitted' and the snapshot resolver never "
        f"sees it"
    )
    assert sl_row.state == 'disposition_unknown'
    store.close()


def __test_reconcile_snapshot_promotes_disposition_unknown_when_bracket_attached__(tmp_path):
    """When ``_mark_bracket_legs_disposition_unknown`` flips a TP/SL row
    after a bracket PUT/confirm timeout, the row has no
    ``exchange_order_id`` (Capital.com brackets share the parent
    position's deal_id). The standard snapshot loop keys off
    ``exchange_order_id`` and would skip it forever. This recovery
    consults the parent position's ``profitLevel`` / ``stopLevel``: if
    they match what the row asked for, the bracket landed despite the
    timeout and the row is promoted to ``confirmed``. The persisted
    park resolution (keyed on the *parent* entry COID — the same id the
    engine parked under) is set to ``'attached'`` so
    :meth:`OrderSyncEngine._verify_pending_dispatches` clears the parked
    envelope on the next sync without dropping the active ExitIntent.
    """
    broker, store, ctx = _make_broker(tmp_path)
    e_coid, tp_coid, sl_coid = _seed_bracket_setup(ctx)
    # Simulate the post-timeout state. Engine parks under the entry COID,
    # the same id ``execute_exit`` passed via ``OrderDispositionUnknownError``.
    ctx.set_order_state(tp_coid, 'disposition_unknown')
    ctx.set_order_state(sl_coid, 'disposition_unknown')
    ctx.record_park(e_coid, 'Bracket\x00Long')

    positions_by_deal = {
        'deal-L': {
            'position': {
                'dealId': 'deal-L',
                'profitLevel': 1.17699,  # matches tp_price seeded
                'stopLevel': 1.17636,    # matches sl_price seeded
                'size': 1.0,
                'level': 1.17667,
            },
        },
    }

    async def drain():
        out = []
        async for ev in broker._reconcile_snapshot(positions_by_deal, {}):
            out.append(ev)
        return out

    asyncio.run(drain())

    tp_row = ctx.get_order(tp_coid)
    sl_row = ctx.get_order(sl_coid)
    assert tp_row is not None and tp_row.state == 'confirmed', (
        "matching profitLevel must promote the leg row to confirmed"
    )
    assert sl_row is not None and sl_row.state == 'confirmed'
    # Rows stay live (still protect the position).
    assert tp_row.closed_ts_ms is None
    assert sl_row.closed_ts_ms is None
    # Parent COID's parked dispatch must be marked ``attached`` so the
    # engine clears the park but keeps the ExitIntent active.
    pending = ctx.iter_pending_resolutions()
    parent_resolutions = {p.coid: p.resolution for p in pending}
    assert parent_resolutions.get(e_coid) == 'attached', (
        f"expected parent COID {e_coid!r} to be resolved as 'attached', "
        f"got {parent_resolutions!r}"
    )
    store.close()


def __test_reconcile_snapshot_rejects_disposition_unknown_when_bracket_absent__(tmp_path):
    """If the parent position is open but does NOT carry the bracket
    levels we asked for, the original PUT did not take effect. The
    leg row is rolled back like a synchronous reject — state='rejected',
    ``closed_ts_ms`` set — and the parent entry COID's parked dispatch
    is marked ``'rejected'`` so the engine drops the active ExitIntent
    on the next sync, allowing the next ``_diff_and_dispatch`` to send
    the bracket out again instead of leaving the position unprotected.
    """
    broker, store, ctx = _make_broker(tmp_path)
    e_coid, tp_coid, sl_coid = _seed_bracket_setup(ctx)
    ctx.set_order_state(tp_coid, 'disposition_unknown')
    ctx.set_order_state(sl_coid, 'disposition_unknown')
    ctx.record_park(e_coid, 'Bracket\x00Long')

    positions_by_deal = {
        'deal-L': {
            'position': {
                'dealId': 'deal-L',
                'profitLevel': None,  # bracket NOT attached
                'stopLevel': None,
                'size': 1.0,
                'level': 1.17667,
            },
        },
    }

    async def drain():
        out = []
        async for ev in broker._reconcile_snapshot(positions_by_deal, {}):
            out.append(ev)
        return out

    asyncio.run(drain())

    tp_row = ctx.get_order(tp_coid)
    sl_row = ctx.get_order(sl_coid)
    assert tp_row is not None and tp_row.state == 'rejected'
    assert sl_row is not None and sl_row.state == 'rejected'
    assert tp_row.closed_ts_ms is not None
    assert sl_row.closed_ts_ms is not None
    pending = ctx.iter_pending_resolutions()
    parent_resolutions = {p.coid: p.resolution for p in pending}
    assert parent_resolutions.get(e_coid) == 'rejected', (
        f"expected parent COID {e_coid!r} to be resolved as 'rejected', "
        f"got {parent_resolutions!r}"
    )
    store.close()


def __test_reconcile_snapshot_closes_disposition_unknown_when_parent_gone__(tmp_path):
    """If the parent position has vanished from the snapshot, the
    bracket has nothing left to protect — close the leg row regardless
    of whether the original PUT ever attached, and resolve the parent
    COID's parked dispatch as ``'rejected'`` so the engine clears the
    stale ExitIntent (no parent → nothing to amend on the next sync).
    """
    broker, store, ctx = _make_broker(tmp_path)
    e_coid, tp_coid, sl_coid = _seed_bracket_setup(ctx)
    ctx.set_order_state(tp_coid, 'disposition_unknown')
    ctx.set_order_state(sl_coid, 'disposition_unknown')
    ctx.record_park(e_coid, 'Bracket\x00Long')

    async def drain():
        out = []
        async for ev in broker._reconcile_snapshot({}, {}):
            out.append(ev)
        return out

    asyncio.run(drain())

    tp_row = ctx.get_order(tp_coid)
    sl_row = ctx.get_order(sl_coid)
    assert tp_row is not None and tp_row.state == 'rejected'
    assert sl_row is not None and sl_row.state == 'rejected'
    assert tp_row.closed_ts_ms is not None
    assert sl_row.closed_ts_ms is not None
    pending = ctx.iter_pending_resolutions()
    parent_resolutions = {p.coid: p.resolution for p in pending}
    assert parent_resolutions.get(e_coid) == 'rejected'
    store.close()


def __test_reconcile_snapshot_resolves_trailing_stop_leg__(tmp_path):
    """Trailing-stop SL legs cannot be verified by ``stopLevel`` — the
    server-side level moves with price. The resolver checks
    ``trailingStop`` + ``stopDistance`` instead. When both flags match,
    the leg is ``confirmed`` and the parent COID's park is resolved
    ``'attached'``; when the position is open without trailing
    activated, the leg is ``'rejected'`` so the engine re-dispatches.
    """
    broker, store, ctx = _make_broker(tmp_path)

    e_coid = 'coid-entry'
    sl_coid = 'coid-sl'
    deal_id = 'deal-L'
    pine_id = 'Long'
    ctx.upsert_order(
        e_coid, symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id=pine_id,
        exchange_order_id=deal_id,
        extras={'kind': 'position', 'confirm_level': 1.17600},
    )
    ctx.add_ref(e_coid, 'deal_id', deal_id)
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0,
        state='disposition_unknown', pine_entry_id=pine_id,
        from_entry=pine_id,
        trailing_distance=0.0010,
        trailing_stop=True,
        extras={
            'leg_kind': 'sl',
            'parent_deal_id': deal_id,
            'parent_coid': e_coid,
            'trail_offset': 0.0010,
        },
    )
    ctx.record_park(e_coid, 'Bracket\x00Long')

    async def drain(positions):
        async for _ev in broker._reconcile_snapshot(positions, {}):
            pass

    # Case 1: trailing is active server-side and stopDistance matches → attached.
    asyncio.run(drain({
        'deal-L': {
            'position': {
                'dealId': 'deal-L',
                'trailingStop': True,
                'stopDistance': 0.0010,
                'size': 1.0,
                'level': 1.17600,
            },
        },
    }))
    sl_row = ctx.get_order(sl_coid)
    assert sl_row is not None and sl_row.state == 'confirmed'
    assert sl_row.closed_ts_ms is None
    pending = ctx.iter_pending_resolutions()
    assert {p.coid: p.resolution for p in pending}.get(e_coid) == 'attached'

    # Reset for case 2: position open but trailing NOT active → rejected.
    ctx.set_order_state(sl_coid, 'disposition_unknown')
    ctx.record_park(e_coid, 'Bracket\x00Long')
    asyncio.run(drain({
        'deal-L': {
            'position': {
                'dealId': 'deal-L',
                'trailingStop': False,
                'stopDistance': None,
                'size': 1.0,
                'level': 1.17600,
            },
        },
    }))
    sl_row = ctx.get_order(sl_coid)
    assert sl_row is not None and sl_row.state == 'rejected'
    assert sl_row.closed_ts_ms is not None
    pending = ctx.iter_pending_resolutions()
    assert {p.coid: p.resolution for p in pending}.get(e_coid) == 'rejected'
    store.close()


def __test_reconcile_snapshot_split_bracket_aggregates_to_single_rejected_write__(tmp_path):
    """Mixed bracket (TP attached, SL rejected) must produce ONE
    aggregated ``record_resolution`` call per parent COID — not one
    per leg.

    Per-leg writes race the engine's ``_consume_plugin_resolutions``:
    if the engine sync runs between the early ``'attached'`` write and
    the later ``'rejected'`` write, it deletes the
    ``pending_verifications`` row, and the late ``UPDATE ... WHERE
    coid = ?`` finds zero rows. The sticky-rejected SQL in
    :meth:`record_resolution` cannot help once the row is gone — the
    engine never learns SL is missing and the bracket stays half-open
    forever. The aggregator collapses both legs' results within the
    same poll cycle, then writes once after the loop.
    """
    broker, store, ctx = _make_broker(tmp_path)
    e_coid, tp_coid, sl_coid = _seed_bracket_setup(ctx)
    ctx.set_order_state(tp_coid, 'disposition_unknown')
    ctx.set_order_state(sl_coid, 'disposition_unknown')
    ctx.record_park(e_coid, 'Bracket\x00Long')

    # Spy: capture every record_resolution call so we can count writes
    # per parent COID. The race the aggregator closes is invisible to
    # the resolution column itself (sticky-rejected makes either order
    # converge to 'rejected'); the cardinality of writes is what matters.
    calls: list[tuple[str, str]] = []
    real = ctx.record_resolution

    def spy(coid, resolution):
        calls.append((coid, resolution))
        return real(coid, resolution)

    ctx.record_resolution = spy  # type: ignore[method-assign]

    # TP profitLevel matches seeded tp_price (1.17699) → leg attached.
    # SL stopLevel deliberately diverges from seeded sl_price (1.17636)
    # → leg rejected. Both leg rows are visible in the same
    # ``iter_live_orders`` pass.
    positions_by_deal = {
        'deal-L': {
            'position': {
                'dealId': 'deal-L',
                'profitLevel': 1.17699,
                'stopLevel': 1.50000,
                'size': 1.0,
                'level': 1.17667,
            },
        },
    }

    async def drain():
        async for _ev in broker._reconcile_snapshot(positions_by_deal, {}):
            pass

    asyncio.run(drain())

    parent_calls = [c for c in calls if c[0] == e_coid]
    assert len(parent_calls) == 1, (
        f"expected exactly ONE record_resolution per parent COID "
        f"(aggregator must collapse both leg outcomes into a single "
        f"write to close the engine-consumer race), got {parent_calls!r}"
    )
    assert parent_calls[0] == (e_coid, 'rejected'), (
        f"any rejected leg must win — aggregate must be 'rejected', "
        f"got {parent_calls[0]!r}"
    )
    # Leg-level state changes: SL is naturally rejected by the
    # level mismatch, and the aggregate-flush sibling retirement
    # also retires the TP that was promoted to ``confirmed`` earlier
    # in this batch — otherwise the engine's modify-rejected branch
    # below would drop the parent dispatch's mapping and re-dispatch
    # with fresh COIDs, leaving the confirmed TP row alongside the
    # new dispatch's TP row as a duplicate bracket leg under the
    # same parent_deal_id.
    tp_row = ctx.get_order(tp_coid)
    sl_row = ctx.get_order(sl_coid)
    assert tp_row is not None and tp_row.state == 'rejected', (
        f"mixed bracket rejection: the TP that was promoted to "
        f"'confirmed' in this batch must be retired by the aggregate "
        f"flush so the engine's re-dispatch (with new COIDs) does "
        f"not seed duplicate bracket legs under the same parent. "
        f"Got tp_row.state={tp_row.state!r}"
    )
    assert tp_row.closed_ts_ms is not None
    assert sl_row is not None and sl_row.state == 'rejected'
    assert sl_row.closed_ts_ms is not None
    # Final persisted resolution matches the single aggregate write.
    pending = ctx.iter_pending_resolutions()
    assert {p.coid: p.resolution for p in pending}.get(e_coid) == 'rejected'
    store.close()


def __test_reconcile_snapshot_both_attached_aggregates_to_single_attached_write__(tmp_path):
    """Symmetric counterpart: both legs attached → ONE aggregated
    ``'attached'`` write per parent COID. The aggregator must not
    fire twice when both legs land cleanly.
    """
    broker, store, ctx = _make_broker(tmp_path)
    e_coid, tp_coid, sl_coid = _seed_bracket_setup(ctx)
    ctx.set_order_state(tp_coid, 'disposition_unknown')
    ctx.set_order_state(sl_coid, 'disposition_unknown')
    ctx.record_park(e_coid, 'Bracket\x00Long')

    calls: list[tuple[str, str]] = []
    real = ctx.record_resolution

    def spy(coid, resolution):
        calls.append((coid, resolution))
        return real(coid, resolution)

    ctx.record_resolution = spy  # type: ignore[method-assign]

    positions_by_deal = {
        'deal-L': {
            'position': {
                'dealId': 'deal-L',
                'profitLevel': 1.17699,
                'stopLevel': 1.17636,
                'size': 1.0,
                'level': 1.17667,
            },
        },
    }

    async def drain():
        async for _ev in broker._reconcile_snapshot(positions_by_deal, {}):
            pass

    asyncio.run(drain())

    parent_calls = [c for c in calls if c[0] == e_coid]
    assert len(parent_calls) == 1, (
        f"both-attached must still produce a single aggregated write, "
        f"got {parent_calls!r}"
    )
    assert parent_calls[0] == (e_coid, 'attached')
    store.close()


def __test_modify_exit_updates_bracket_leg_levels__(tmp_path):
    """``modify_exit`` must mirror the new TP/SL onto the BRACKET LEG
    rows, not just call ``set_risk`` on the entry row. The closing-leg
    ``level<=0`` fallback in :meth:`_activity_to_event` reads
    ``leg_row.tp_level`` / ``leg_row.sl_level`` — without the mirror an
    amended TP that fires with ``activity.level=0`` would surface the
    OLD level into ``BrokerPosition.record_fill``.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-L', 'put'): {'dealReference': 'modify-ok'},
        ('confirms/modify-ok', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.17900, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    tp_coid = env.client_order_id('t')
    sl_coid = env.client_order_id('s')
    # Seed entry + bracket legs with the OLD levels, mirroring what
    # an earlier ``execute_exit`` would have written.
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.upsert_order(
        tp_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        tp_level=1.17900,
        extras={'leg_kind': 'tp', 'parent_deal_id': 'deal-L'},
    )
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        sl_level=1.17400,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L'},
    )

    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18250, sl_price=1.17150,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(env, new_env))

    tp_row = ctx.get_order(tp_coid)
    sl_row = ctx.get_order(sl_coid)
    assert tp_row is not None and tp_row.tp_level == pytest.approx(1.18250), (
        f"TP leg row tp_level must mirror the amended price; "
        f"got {tp_row.tp_level!r}"
    )
    assert sl_row is not None and sl_row.sl_level == pytest.approx(1.17150), (
        f"SL leg row sl_level must mirror the amended price; "
        f"got {sl_row.sl_level!r}"
    )
    store.close()


def __test_modify_exit_amend_fixed_to_trailing_updates_trailing_stop_flag__(tmp_path):
    """Amending an SL leg between fixed and native trailing must update
    the persisted ``trailing_stop`` flag on the leg row. The flag drives
    :meth:`_activity_to_event`'s ``LegType.STOP_LOSS`` vs
    ``LegType.TRAILING_STOP`` classification on the eventual fill, and
    leaving it stale on the existing-leg update path would mis-route
    the leg type when the trailing stop fires.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'modify-trail'},
        ('confirms/modify-trail', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    # Seed: existing fixed SL leg (trailing_stop=False).
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    sl_coid = env.client_order_id('s')
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        sl_level=1.17400, trailing_stop=False, trailing_distance=None,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    # Amend fixed → trailing.
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, trail_offset=0.00200,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(env, new_env))
    sl_row = ctx.get_order(sl_coid)
    assert sl_row is not None
    assert sl_row.trailing_stop is True, (
        "amend fixed→trailing must set trailing_stop=True on the leg "
        "row; otherwise _activity_to_event will classify the eventual "
        "trailing-stop fill as LegType.STOP_LOSS"
    )
    assert sl_row.trailing_distance == pytest.approx(0.00200)

    # And amend trailing → fixed in a second amend on the same leg.
    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'modify-fix'}
    broker._responses[('confirms/modify-fix', 'get')] = {'dealStatus': 'ACCEPTED'}
    fix_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, sl_price=1.17350,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    # Capture the PUT body for the trailing→fixed amend by clearing
    # ``_calls`` (the prior fixed→trailing PUT noise must not leak in)
    # and asserting the new body explicitly carries
    # ``trailingStop: False`` — Capital.com keeps the prior native
    # trailing flag active otherwise.
    broker._calls.clear()
    asyncio.run(broker.modify_exit(new_env, fix_env))
    sl_row2 = ctx.get_order(sl_coid)
    assert sl_row2 is not None
    assert sl_row2.trailing_stop is False, (
        "amend trailing→fixed must clear trailing_stop on the leg row; "
        "otherwise _activity_to_event will classify the eventual "
        "fixed-stop fill as LegType.TRAILING_STOP"
    )
    assert sl_row2.sl_level == pytest.approx(1.17350)
    put_calls = [c for c in broker._calls
                 if c[0] == 'positions/deal-L' and c[1] == 'put']
    assert len(put_calls) == 1, (
        f"expected exactly one PUT positions/deal-L call for the "
        f"trailing→fixed amend, got {len(put_calls)}: {put_calls!r}"
    )
    body = put_calls[0][2] or {}
    assert body.get('trailingStop') is False, (
        f"trailing→fixed amend body must include trailingStop=False to "
        f"clear Capital.com's prior trailing flag; got body={body!r}"
    )
    assert body.get('stopLevel') == pytest.approx(1.17350)
    store.close()


def __test_modify_exit_trailing_to_fixed_clears_entry_trailing_stop__(tmp_path):
    """The native-trailing → fixed-SL transition must clear
    ``trailing_stop`` on the *parent entry row*, not just the leg row.
    Capital.com fires SL fill activities against the entry's dealId
    (brackets are POSITION attributes), so :meth:`_activity_to_event`
    reads ``row.trailing_stop`` from the entry row to decide between
    :class:`LegType.STOP_LOSS` and :class:`LegType.TRAILING_STOP`.
    Leaving the entry row's flag as ``True`` after the amend would
    then mis-route the eventual fixed-stop fill as
    ``LegType.TRAILING_STOP``, desyncing fill classification — even
    though the leg-row flag is correct and the broker has been told
    ``trailingStop=False``.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'modify-fix'},
        ('confirms/modify-fix', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    # Seed: entry row with native trailing currently active.
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        trailing_stop=True, trailing_distance=0.00200,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    # Existing trailing leg row (matches what execute_exit would have left).
    trail_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, trail_offset=0.00200,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    sl_coid = trail_env.client_order_id('s')
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        trailing_stop=True, trailing_distance=0.00200,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    # Amend trailing → fixed.
    fix_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, sl_price=1.17350,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(trail_env, fix_env))

    entry_row = ctx.get_order('coid-entry')
    assert entry_row is not None
    assert entry_row.trailing_stop is False, (
        f"trailing→fixed amend must clear trailing_stop on the "
        f"PARENT ENTRY ROW (not just the leg row); otherwise "
        f"_activity_to_event reads the stale True off the entry's "
        f"dealId-matched row and routes the fixed-stop fill as "
        f"LegType.TRAILING_STOP. got entry_row.trailing_stop="
        f"{entry_row.trailing_stop!r}"
    )
    # Leg row is also fixed — parity with the existing
    # leg-update test, here we are guarding the entry-row mirror.
    sl_row = ctx.get_order(sl_coid)
    assert sl_row is not None
    assert sl_row.trailing_stop is False
    store.close()


def __test_modify_exit_creates_missing_leg_row_on_amend__(tmp_path):
    """Amending an SL-only bracket to add a TP must INSERT the new TP leg
    row, not silently skip it because ``get_order(tp_coid) is None``.

    Without the row, ``_resolve_bracket_leg_disposition``,
    ``_find_bracket_leg_row`` (the level=0 close-fill fallback), and any
    restart recovery would all be blind to a leg the broker is actively
    enforcing — so a later TP fill could mis-reconcile or appear to
    arrive without an owner.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'modify-add-tp'},
        ('confirms/modify-add-tp', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    tp_coid = env.client_order_id('t')
    sl_coid = env.client_order_id('s')
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    # Only SL leg seeded — TP leg row deliberately absent.
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        sl_level=1.17400,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    assert ctx.get_order(tp_coid) is None, (
        "precondition: TP leg row must be absent before the amend"
    )

    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, sl_price=1.17400, tp_price=1.18250,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(env, new_env))

    tp_row = ctx.get_order(tp_coid)
    assert tp_row is not None, (
        "modify_exit must INSERT the newly-attached TP leg row when it "
        "did not exist before the amend"
    )
    assert tp_row.tp_level == pytest.approx(1.18250)
    assert tp_row.symbol == 'EURUSD' and tp_row.side == 'sell'
    assert tp_row.from_entry == 'Long' and tp_row.pine_entry_id == 'Long'
    assert tp_row.state == 'confirmed', (
        "amend-created leg row must be persisted as 'confirmed' — by the "
        "time modify_exit reaches the upsert, the broker PUT + confirm "
        "have already succeeded; persisting 'submitted' would make "
        "_recover_in_flight_submissions treat it as an unresolved "
        "dispatch on restart"
    )
    extras = tp_row.extras or {}
    assert extras.get('leg_kind') == 'tp'
    assert extras.get('parent_deal_id') == 'deal-L'
    assert extras.get('parent_coid') == 'coid-entry', (
        "parent_coid must be set so _resolve_bracket_leg_disposition "
        "can route record_resolution to the engine's park key"
    )
    store.close()


def __test_modify_exit_rejected_confirm_does_not_persist_legs__(tmp_path):
    """``modify_exit`` must inspect the ``confirms/{ref}`` payload and bail
    before any state mutation when ``dealStatus='REJECTED'``. Capital.com
    returns the PUT with HTTP 200 + a ``dealReference`` even when the
    amend is rejected (e.g. attaching a fresh TP whose distance violates
    the live spread): the rejection only surfaces in the confirm payload.

    Without this guard the bracket-leg upsert path below would persist
    new/reopened leg rows as ``state='confirmed'`` while Capital.com kept
    the prior bracket state, leaving BrokerStore reporting a protective
    leg that does not exist on the exchange — a subsequent fill on the
    phantom leg would never arrive, and ``set_risk`` on the entry row
    would advertise risk levels the broker never accepted.

    The test seeds an SL-only bracket, amends to ADD a TP, and arranges
    the confirm to return REJECTED. After the call:
      * ``ExchangeOrderRejectedError`` must be raised.
      * The TP leg row must NOT exist.
      * The entry row's ``tp_price`` / ``sl_price`` / ``trailing_*`` must
        retain their pre-amend values (no ``set_risk`` mutation).
      * A ``modify_exit_rejected`` event row must be logged for audit.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'modify-rej'},
        ('confirms/modify-rej', 'get'): {
            'dealStatus': 'REJECTED',
            'reason': 'attach.constraint.violation',
        },
    })
    # Seed an entry with a fixed SL leg only (no TP yet) — mirroring the
    # post-execute_exit state that an SL-only Pine bracket leaves behind.
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        sl_level=1.17400,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')
    sl_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    sl_coid_seed = sl_env.client_order_id('s')
    ctx.upsert_order(
        sl_coid_seed, symbol='EURUSD', side='sell', qty=1.0,
        state='confirmed', pine_entry_id='Long', from_entry='Long',
        sl_level=1.17400,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )

    # Amend: add a TP that the broker will reject in confirm.
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.17900, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    tp_coid_amend = new_env.client_order_id('t')

    with pytest.raises(ExchangeOrderRejectedError) as excinfo:
        asyncio.run(broker.modify_exit(sl_env, new_env))
    assert 'attach.constraint.violation' in str(excinfo.value), (
        f"raised error must surface the reason from the confirm payload; "
        f"got {excinfo.value!s}"
    )

    # The TP leg row must not have been created — modify_exit must bail
    # BEFORE the upsert_order path that would otherwise persist the new
    # leg as ``state='confirmed'``.
    tp_row = ctx.get_order(tp_coid_amend)
    assert tp_row is None, (
        f"REJECTED confirm must NOT cause a TP leg row to be persisted; "
        f"BrokerStore would then advertise a protective leg that does "
        f"not exist on Capital.com. Got tp_row={tp_row!r}"
    )
    # The entry row's risk fields must remain at their pre-amend values
    # — set_risk must not have run.
    entry_row = ctx.get_order('coid-entry')
    assert entry_row is not None
    assert entry_row.tp_level is None, (
        f"REJECTED confirm must NOT advertise the new TP on the entry "
        f"row; got tp_level={entry_row.tp_level!r}"
    )
    assert entry_row.sl_level == pytest.approx(1.17400), (
        f"entry row's pre-amend sl_level must be preserved; "
        f"got {entry_row.sl_level!r}"
    )
    # Audit event must be logged so downstream debugging can correlate
    # the rejection with the dispatch.
    event_kinds = [
        r['kind'] for r in ctx._store._conn.execute(
            "SELECT kind FROM events WHERE run_instance_id = ?",
            (ctx.run_instance_id,),
        )
    ]
    assert 'modify_exit_rejected' in event_kinds, (
        f"a 'modify_exit_rejected' event row must be logged for "
        f"forensic correlation; got event kinds={event_kinds!r}"
    )
    store.close()


def __test_modify_exit_put_timeout_flips_legs_to_disposition_unknown__(tmp_path):
    """``modify_exit`` PUT timeout: Capital.com may have already applied
    the amend (e.g. cleared ``stopLevel`` / flipped ``trailingStop`` for
    a fixed → pending-trail transition) while BrokerStore still shows
    the old leg state. Without recovery the position would sit
    unprotected/unmanaged: the activation monitor would not own the new
    pending trail and the engine would never get a parked-resolution
    path, unlike :meth:`execute_exit`.

    Fix contract enforced by this test:
      * Existing TP/SL leg rows must flip to ``state='disposition_unknown'``
        (so :meth:`_resolve_bracket_leg_disposition` resolves them
        against the next ``/positions`` snapshot).
      * The leg rows must remain LIVE (``closed_ts_ms is None``).
      * :class:`OrderDispositionUnknownError` must be raised so the
        sync engine parks the dispatch for verification.
      * A ``bracket_attach_disposition_unknown`` audit event must be
        logged for forensic correlation.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('error', 'positions/deal-L', 'put'):
            httpx.TimeoutException("read timeout"),
    })
    # Entry + existing TP/SL bracket (the modify_exit will amend levels).
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        sl_level=1.17400, tp_level=1.17900,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')
    ctx.upsert_order(
        'coid-tp-old', symbol='EURUSD', side='sell', qty=1.0,
        state='confirmed', pine_entry_id='Long', from_entry='Long',
        tp_level=1.17900,
        extras={'leg_kind': 'tp', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    ctx.upsert_order(
        'coid-sl-old', symbol='EURUSD', side='sell', qty=1.0,
        state='confirmed', pine_entry_id='Long', from_entry='Long',
        sl_level=1.17400,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.17900, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000, sl_price=1.17300,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_exit(old_env, new_env))

    tp_row = ctx.get_order('coid-tp-old')
    sl_row = ctx.get_order('coid-sl-old')
    assert tp_row is not None and tp_row.closed_ts_ms is None, (
        "PUT timeout must NOT close the TP leg row — the broker may "
        "still be enforcing it; the snapshot reconcile decides"
    )
    assert sl_row is not None and sl_row.closed_ts_ms is None, (
        "PUT timeout must NOT close the SL leg row — the broker may "
        "still be enforcing it; the snapshot reconcile decides"
    )
    assert tp_row.state == 'disposition_unknown', (
        f"TP leg row must flip to disposition_unknown so "
        f"_resolve_bracket_leg_disposition can resolve it against the "
        f"next /positions snapshot; got state={tp_row.state!r}"
    )
    assert sl_row.state == 'disposition_unknown', (
        f"SL leg row must flip to disposition_unknown; got "
        f"state={sl_row.state!r}"
    )
    event_kinds = [
        r['kind'] for r in ctx._store._conn.execute(
            "SELECT kind FROM events WHERE run_instance_id = ?",
            (ctx.run_instance_id,),
        )
    ]
    assert 'bracket_attach_disposition_unknown' in event_kinds, (
        f"PUT-ambiguous path must log a "
        f"'bracket_attach_disposition_unknown' event for forensic "
        f"correlation with the parked dispatch; got {event_kinds!r}"
    )
    store.close()


def __test_modify_exit_confirm_timeout_flips_legs_to_disposition_unknown__(tmp_path):
    """``modify_exit`` confirm GET timeout: the PUT returned a
    ``dealReference`` (so Capital.com saw the amend) but the follow-up
    confirm read failed. The amend may already be live on the
    exchange — same ambiguity as :meth:`execute_exit`'s confirm timeout.

    Mirrors :meth:`execute_exit`'s confirm-error contract: leg rows must
    flip to ``disposition_unknown``, raise
    :class:`OrderDispositionUnknownError`, log a
    ``bracket_attach_disposition_unknown`` event with the
    ``deal_reference`` payload field for forensics.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'amend-pending'},
        ('error', 'confirms/amend-pending', 'get'):
            httpx.TimeoutException("read timeout"),
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        sl_level=1.17400, tp_level=1.17900,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')
    ctx.upsert_order(
        'coid-tp-old', symbol='EURUSD', side='sell', qty=1.0,
        state='confirmed', pine_entry_id='Long', from_entry='Long',
        tp_level=1.17900,
        extras={'leg_kind': 'tp', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    ctx.upsert_order(
        'coid-sl-old', symbol='EURUSD', side='sell', qty=1.0,
        state='confirmed', pine_entry_id='Long', from_entry='Long',
        sl_level=1.17400,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.17900, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000, sl_price=1.17300,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_exit(old_env, new_env))

    tp_row = ctx.get_order('coid-tp-old')
    sl_row = ctx.get_order('coid-sl-old')
    assert tp_row is not None and tp_row.state == 'disposition_unknown'
    assert sl_row is not None and sl_row.state == 'disposition_unknown'
    assert tp_row.closed_ts_ms is None and sl_row.closed_ts_ms is None
    # Audit payload must carry the deal_reference for the ambiguous PUT.
    payload_rows = list(ctx._store._conn.execute(
        "SELECT payload FROM events WHERE run_instance_id = ? "
        "AND kind = 'bracket_attach_disposition_unknown'",
        (ctx.run_instance_id,),
    ))
    assert len(payload_rows) == 1, (
        f"exactly one disposition_unknown event must be logged; got "
        f"{len(payload_rows)} rows"
    )
    payload = json.loads(payload_rows[0]['payload'])
    assert payload.get('stage') == 'confirm', (
        f"audit payload must mark the failure stage as 'confirm'; "
        f"got {payload!r}"
    )
    assert payload.get('deal_reference') == 'amend-pending', (
        f"audit payload must carry the dealReference from the PUT "
        f"response; got {payload!r}"
    )
    store.close()


def __test_modify_exit_fixed_to_pending_trail_put_timeout_flips_existing_sl__(tmp_path):
    """Fixed SL → pending-trail amend: the PUT body actively clears the
    broker-side ``stopLevel`` / ``trailingStop``. If that PUT times out
    after Capital.com applied the clear, the existing SL leg row must
    flip to ``disposition_unknown`` so the snapshot reconcile resolves
    it. Without this the row stays ``confirmed`` in BrokerStore — the
    reconciler skips it (only visits ``disposition_unknown`` rows) — and
    the local state silently keeps showing a protective leg the broker
    has dropped, leaving the position unprotected.

    The pending-trail leg itself is purely local (the activation monitor
    will PUT it later), so it must NOT also be flipped — that would
    confuse the reconcile (parent has ``trailingStop=False``, which is
    correct for "not yet activated", but the reconciler would treat that
    as "rejected" and kill the pending-trail intent).
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('error', 'positions/deal-L', 'put'):
            httpx.TimeoutException("read timeout"),
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        sl_level=1.17400,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')
    # Existing FIXED SL leg row — broker-active.
    ctx.upsert_order(
        'coid-sl-old', symbol='EURUSD', side='sell', qty=1.0,
        state='confirmed', pine_entry_id='Long', from_entry='Long',
        sl_level=1.17400, trailing_distance=None, trailing_stop=False,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0,
            trail_offset=0.00500, trail_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_exit(old_env, new_env))

    sl_row = ctx.get_order('coid-sl-old')
    assert sl_row is not None and sl_row.closed_ts_ms is None, (
        "PUT timeout must NOT close the existing SL leg row — the "
        "broker may still be enforcing it; the snapshot reconcile decides"
    )
    assert sl_row.state == 'disposition_unknown', (
        f"fixed→pending-trail PUT timeout must flip the existing SL leg "
        f"to disposition_unknown so _resolve_bracket_leg_disposition can "
        f"resolve it against the broker's actual stopLevel/trailingStop "
        f"on the next /positions snapshot; without this the leg stays "
        f"'confirmed' forever and local state silently shows a "
        f"protective stop the broker has cleared; got state={sl_row.state!r}"
    )
    # Audit event must carry the SL coid so forensics can correlate.
    payload_rows = list(ctx._store._conn.execute(
        "SELECT payload FROM events WHERE run_instance_id = ? "
        "AND kind = 'bracket_attach_disposition_unknown'",
        (ctx.run_instance_id,),
    ))
    assert len(payload_rows) == 1
    payload = json.loads(payload_rows[0]['payload'])
    assert payload.get('sl_coid') == 'coid-sl-old', (
        f"audit payload must carry the existing SL leg coid that was "
        f"flipped to disposition_unknown; got {payload!r}"
    )
    store.close()


def __test_modify_exit_native_trail_to_pending_put_timeout_flips_existing_sl__(tmp_path):
    """Same disposition-recovery contract as the fixed→pending-trail
    case, but starting from an immediate native trailing stop. The PUT
    body clears ``trailingStop`` (and Capital.com clears ``stopDistance``
    with it). If the PUT times out post-application, the existing
    trailing SL row must flip to ``disposition_unknown`` so the snapshot
    reconcile can resolve it via the trailing-leg branch
    (``trailingStop`` flag + ``stopDistance`` match on the parent).
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('error', 'positions/deal-L', 'put'):
            httpx.TimeoutException("read timeout"),
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')
    # Existing NATIVE-TRAILING SL leg row — broker-active.
    ctx.upsert_order(
        'coid-sl-old', symbol='EURUSD', side='sell', qty=1.0,
        state='confirmed', pine_entry_id='Long', from_entry='Long',
        sl_level=None, trailing_distance=0.00800, trailing_stop=True,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, trail_offset=0.00800,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0,
            trail_offset=0.00500, trail_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_exit(old_env, new_env))

    sl_row = ctx.get_order('coid-sl-old')
    assert sl_row is not None and sl_row.closed_ts_ms is None
    assert sl_row.state == 'disposition_unknown', (
        f"native-trail→pending PUT timeout must flip the existing "
        f"trailing SL leg to disposition_unknown; got state={sl_row.state!r}"
    )
    store.close()


def __test_modify_exit_fixed_to_pending_trail_confirm_timeout_flips_existing_sl__(tmp_path):
    """Same as the PUT-timeout regression but for the confirm-GET
    failure path: the PUT returned a ``dealReference`` (so Capital.com
    saw and likely applied the clear) but the confirm read failed. The
    existing SL leg row must still flip to ``disposition_unknown``.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'amend-clear-pending'},
        ('error', 'confirms/amend-clear-pending', 'get'):
            httpx.TimeoutException("read timeout"),
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        sl_level=1.17400,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')
    ctx.upsert_order(
        'coid-sl-old', symbol='EURUSD', side='sell', qty=1.0,
        state='confirmed', pine_entry_id='Long', from_entry='Long',
        sl_level=1.17400, trailing_distance=None, trailing_stop=False,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0,
            trail_offset=0.00500, trail_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_exit(old_env, new_env))

    sl_row = ctx.get_order('coid-sl-old')
    assert sl_row is not None and sl_row.state == 'disposition_unknown', (
        f"fixed→pending-trail confirm timeout must flip the existing SL "
        f"leg row; got state={sl_row.state if sl_row else None!r}"
    )
    payload_rows = list(ctx._store._conn.execute(
        "SELECT payload FROM events WHERE run_instance_id = ? "
        "AND kind = 'bracket_attach_disposition_unknown'",
        (ctx.run_instance_id,),
    ))
    assert len(payload_rows) == 1
    payload = json.loads(payload_rows[0]['payload'])
    assert payload.get('stage') == 'confirm'
    assert payload.get('deal_reference') == 'amend-clear-pending'
    assert payload.get('sl_coid') == 'coid-sl-old'
    store.close()


def __test_modify_exit_add_pending_trail_with_tp_put_timeout_persists_pending_row__(tmp_path):
    """Pending-trail leg + non-trail body content (e.g. amended TP):
    when the PUT/confirm becomes ambiguous, the pending-trail SL row
    must be persisted in ``state='confirmed'`` with ``trail_state='pending'``
    extras BEFORE the disposition error unwinds. Without this, the
    sync engine eventually adopts the new exit intent (after the TP
    leg resolves as ``attached``) but ``_trailing_activation_monitor``
    has no row to activate — the requested trailing protection
    silently disappears.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('error', 'positions/deal-L', 'put'):
            httpx.TimeoutException("read timeout"),
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        tp_level=1.17900,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')
    # Existing TP leg only (no SL). The amend keeps TP (modified) and
    # ADDS a pending-trail SL.
    ctx.upsert_order(
        'coid-tp-old', symbol='EURUSD', side='sell', qty=1.0,
        state='confirmed', pine_entry_id='Long', from_entry='Long',
        tp_level=1.17900,
        extras={'leg_kind': 'tp', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.17900,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000,
            trail_offset=0.00500, trail_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    sl_coid = new_env.client_order_id('s')

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_exit(old_env, new_env))

    sl_row = ctx.get_order(sl_coid)
    assert sl_row is not None, (
        f"the pending-trail SL row must be pre-seeded on the ambiguous "
        f"PUT path so _trailing_activation_monitor has something to "
        f"activate when the engine adopts the new intent after TP "
        f"resolution; without seeding the trailing protection silently "
        f"disappears. Looked for coid={sl_coid!r}"
    )
    assert sl_row.state == 'confirmed', (
        f"pending-trail row must be 'confirmed' (purely local — broker "
        f"has no leg to resolve), not 'disposition_unknown' (would let "
        f"the resolver wrongly mark it rejected against the parent's "
        f"trailingStop=False); got state={sl_row.state!r}"
    )
    assert sl_row.closed_ts_ms is None
    extras = sl_row.extras or {}
    assert extras.get('trail_state') == 'pending', (
        f"trail_state must be 'pending' so _trailing_activation_monitor "
        f"picks the row up when the threshold is crossed; got "
        f"trail_state={extras.get('trail_state')!r}"
    )
    assert extras.get('trail_activation_price') == pytest.approx(1.18000)
    assert extras.get('trail_offset') == pytest.approx(0.00500)
    assert sl_row.trailing_distance == pytest.approx(0.00500)
    assert sl_row.trailing_stop is False
    store.close()


def __test_modify_exit_add_pending_trail_with_tp_confirm_timeout_persists_pending_row__(tmp_path):
    """Same contract as the PUT-timeout regression but for the confirm-GET
    failure path: the PUT returned a ``dealReference`` (Capital.com
    saw the amend) but the confirm read failed. The pending-trail SL
    row must still be persisted with ``trail_state='pending'``.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'add-pending-trail'},
        ('error', 'confirms/add-pending-trail', 'get'):
            httpx.TimeoutException("read timeout"),
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        tp_level=1.17900,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')
    ctx.upsert_order(
        'coid-tp-old', symbol='EURUSD', side='sell', qty=1.0,
        state='confirmed', pine_entry_id='Long', from_entry='Long',
        tp_level=1.17900,
        extras={'leg_kind': 'tp', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.17900,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000,
            trail_offset=0.00500, trail_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    sl_coid = new_env.client_order_id('s')

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_exit(old_env, new_env))

    sl_row = ctx.get_order(sl_coid)
    assert sl_row is not None and sl_row.state == 'confirmed'
    extras = sl_row.extras or {}
    assert extras.get('trail_state') == 'pending'
    assert extras.get('trail_offset') == pytest.approx(0.00500)
    store.close()


def __test_modify_exit_existing_sl_to_pending_trail_force_disposition_rejected__(tmp_path):
    """Existing fixed-SL → pending-trail transition with PUT timeout:
    the existing SL leg row must be flipped to ``disposition_unknown``
    AND stamped with ``extras['force_disposition_rejected']=True``.

    Without the marker, ``_resolve_bracket_leg_disposition`` compares
    the row's old ``sl_level`` against the parent's actual ``stopLevel``
    — if the broker did NOT apply the PUT (timeout before reaching it),
    the old stop is still live and the comparison says ``attached`` →
    resolution ``'attached'`` → engine clears the modify park while
    keeping the new pending-trail intent active. Local intent diverges
    from broker risk: BrokerStore says pending-trail, broker has the
    old fixed stop, and the engine never retries the amend.

    The marker forces the resolver to record ``'rejected'`` regardless
    of broker state. The engine's modify-rejected branch then restores
    the pre-modify ``_active_intents`` snapshot so the next sync
    re-runs ``modify_exit``, whose retry explicitly clears the
    broker-side stop and persists the pending-trail row.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('error', 'positions/deal-L', 'put'):
            httpx.TimeoutException("read timeout"),
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        sl_level=1.17400,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')
    ctx.upsert_order(
        'coid-sl-old', symbol='EURUSD', side='sell', qty=1.0,
        state='confirmed', pine_entry_id='Long', from_entry='Long',
        sl_level=1.17400, trailing_distance=None, trailing_stop=False,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0,
            trail_offset=0.00500, trail_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_exit(old_env, new_env))

    sl_row = ctx.get_order('coid-sl-old')
    assert sl_row is not None
    assert sl_row.state == 'disposition_unknown'
    assert (sl_row.extras or {}).get('force_disposition_rejected') is True, (
        f"existing SL row must be stamped with "
        f"force_disposition_rejected so the snapshot resolver records "
        f"'rejected' regardless of broker state — without it, a PUT "
        f"that did not reach the broker leaves the old stop live and "
        f"the comparison wrongly resolves 'attached', desyncing local "
        f"intent from broker risk; got extras={sl_row.extras!r}"
    )

    # Drive the resolver: simulate the broker still showing the old
    # fixed stop (the case where the PUT did NOT reach the broker).
    # Without the marker, the level comparison says 'attached'; with
    # the marker, the resolver must force 'rejected'.
    positions_by_deal = {'deal-L': {
        'position': {'stopLevel': 1.17400, 'trailingStop': False},
        'market': {'epic': 'EURUSD'},
    }}
    bracket_resolutions: dict[str, bool] = {}
    broker._resolve_bracket_leg_disposition(
        sl_row, 'sl', positions_by_deal,
        bracket_resolutions=bracket_resolutions,
    )
    assert bracket_resolutions.get('coid-entry') is False, (
        f"resolver must aggregate False for the parent COID even "
        f"though the broker still shows the old stopLevel "
        f"matching the row — the marker forces rejection so the "
        f"engine retries; got {bracket_resolutions!r}"
    )
    closed_row = ctx.get_order('coid-sl-old')
    assert closed_row is not None
    assert closed_row.state == 'rejected'
    assert closed_row.closed_ts_ms is not None, (
        "force-rejected resolution must close the existing SL row so "
        "the modify_exit retry's _find_bracket_leg_row sees no live "
        "leg and routes through the explicit-clear body branch"
    )
    store.close()


def __test_modify_exit_existing_tp_level_change_put_timeout_persists_attempted_target__(tmp_path):
    """Existing TP level change with PUT timeout: the leg row's
    ``tp_level`` must be updated to the attempted NEW target before
    flipping to ``disposition_unknown``.

    Without the persist, ``_resolve_bracket_leg_disposition`` would
    compare the row's STALE OLD ``tp_level`` against the broker's
    ``profitLevel``. If the broker did NOT apply the PUT (timeout
    before reaching it), the broker still has the old level — and
    old-row matches old-broker → resolver records ``'attached'`` →
    engine clears the modify park while ``_active_intents`` already
    advanced to the NEW intent. Local-vs-broker desync: BrokerStore
    says new TP, broker has old TP, engine never retries.

    With the persist, the row carries the NEW level when the resolver
    runs: broker still has OLD → mismatch → ``'rejected'`` → engine
    restores pre-modify intent and re-dispatches.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('error', 'positions/deal-L', 'put'):
            httpx.TimeoutException("read timeout"),
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        sl_level=1.17400, tp_level=1.17900,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')
    ctx.upsert_order(
        'coid-tp-old', symbol='EURUSD', side='sell', qty=1.0,
        state='confirmed', pine_entry_id='Long', from_entry='Long',
        tp_level=1.17900,
        extras={'leg_kind': 'tp', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    ctx.upsert_order(
        'coid-sl-old', symbol='EURUSD', side='sell', qty=1.0,
        state='confirmed', pine_entry_id='Long', from_entry='Long',
        sl_level=1.17400,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.17900, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_exit(old_env, new_env))

    tp_row = ctx.get_order('coid-tp-old')
    assert tp_row is not None
    assert tp_row.state == 'disposition_unknown'
    assert tp_row.tp_level == pytest.approx(1.18000), (
        f"existing TP row's tp_level must be updated to the attempted "
        f"NEW target (1.18000) before disposition_unknown so the "
        f"resolver compares against the new intent — without this, "
        f"the broker-not-applied case wrongly resolves 'attached' "
        f"because old-row-level matches old-broker-level; got "
        f"tp_level={tp_row.tp_level!r}"
    )

    # Drive the resolver: broker did NOT apply the PUT, so it still
    # shows the OLD profitLevel. Without the persist this is the
    # silent-desync case (old==old → 'attached'). With the persist,
    # row.tp_level=1.18000 vs broker.profitLevel=1.17900 → mismatch →
    # 'rejected'.
    positions_by_deal = {'deal-L': {
        'position': {'profitLevel': 1.17900, 'stopLevel': 1.17400,
                     'trailingStop': False},
        'market': {'epic': 'EURUSD'},
    }}
    bracket_resolutions: dict[str, bool] = {}
    broker._resolve_bracket_leg_disposition(
        tp_row, 'tp', positions_by_deal,
        bracket_resolutions=bracket_resolutions,
    )
    assert bracket_resolutions.get('coid-entry') is False, (
        f"resolver must record 'rejected' for the parent COID when the "
        f"broker still has the OLD profitLevel — the persisted attempted "
        f"target makes the comparison say not_attached. Without the "
        f"persist, this would wrongly resolve 'attached' and desync "
        f"local intent from broker risk; got {bracket_resolutions!r}"
    )
    closed_row = ctx.get_order('coid-tp-old')
    assert closed_row is not None
    assert closed_row.state == 'rejected'
    assert closed_row.closed_ts_ms is not None
    store.close()


def __test_modify_exit_existing_tp_clear_put_timeout_persists_attempted_target__(tmp_path):
    """Existing TP clear (level → None) with PUT timeout: the leg row's
    ``tp_level`` must be set to ``None`` before ``disposition_unknown``.

    Without the persist, the row keeps its OLD ``tp_level`` and the
    resolver compares it against broker. If broker did NOT apply the
    clear, broker still has the old level → match → ``'attached'`` →
    engine thinks the clear succeeded while broker still has the TP.

    With the persist (``tp_level=None``), ``_levels_match(None, x)``
    returns False for any x → resolver records ``'rejected'`` → engine
    re-dispatches the clear. (For broker-applied clears this means one
    wasted retry round-trip; the alternative — special-casing None ==
    None in the resolver — is more code for marginal efficiency.)
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('error', 'positions/deal-L', 'put'):
            httpx.TimeoutException("read timeout"),
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        sl_level=1.17400, tp_level=1.17900,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')
    ctx.upsert_order(
        'coid-tp-old', symbol='EURUSD', side='sell', qty=1.0,
        state='confirmed', pine_entry_id='Long', from_entry='Long',
        tp_level=1.17900,
        extras={'leg_kind': 'tp', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    ctx.upsert_order(
        'coid-sl-old', symbol='EURUSD', side='sell', qty=1.0,
        state='confirmed', pine_entry_id='Long', from_entry='Long',
        sl_level=1.17400,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.17900, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_exit(old_env, new_env))

    tp_row = ctx.get_order('coid-tp-old')
    assert tp_row is not None
    assert tp_row.state == 'disposition_unknown'
    assert tp_row.tp_level is None, (
        f"existing TP row's tp_level must be cleared to None before "
        f"disposition_unknown so the resolver does not compare against "
        f"a stale level. Without this, broker-not-applied keeps the "
        f"old level matching the row → 'attached' → engine wrongly "
        f"thinks the clear succeeded; got tp_level={tp_row.tp_level!r}"
    )

    # Drive the resolver: broker did NOT apply the clear → still has
    # old profitLevel.
    positions_by_deal = {'deal-L': {
        'position': {'profitLevel': 1.17900, 'stopLevel': 1.17400,
                     'trailingStop': False},
        'market': {'epic': 'EURUSD'},
    }}
    bracket_resolutions: dict[str, bool] = {}
    broker._resolve_bracket_leg_disposition(
        tp_row, 'tp', positions_by_deal,
        bracket_resolutions=bracket_resolutions,
    )
    assert bracket_resolutions.get('coid-entry') is False, (
        f"clear attempt with broker-not-applied must resolve 'rejected' "
        f"so the engine re-dispatches the clear. Without the persist, "
        f"row keeps old level matching broker → wrongly 'attached' → "
        f"silent desync; got {bracket_resolutions!r}"
    )
    closed_row = ctx.get_order('coid-tp-old')
    assert closed_row is not None
    assert closed_row.state == 'rejected'
    assert closed_row.closed_ts_ms is not None
    store.close()


def __test_modify_exit_existing_trail_distance_change_confirm_timeout_persists_attempted_target__(tmp_path):
    """Existing active-trail distance change with confirm GET timeout:
    the row's ``trailing_distance`` must be updated to the attempted
    NEW offset before ``disposition_unknown``.

    Without the persist, the resolver's trailing comparison
    (``expected_distance = row.trailing_distance``) uses the OLD
    distance. Broker-not-applied still has the OLD distance → match →
    ``'attached'`` → engine thinks the new distance landed.

    With the persist, row.trailing_distance reflects the NEW offset →
    broker still on OLD → mismatch → ``'rejected'`` → engine retries.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'amend-pending'},
        ('error', 'confirms/amend-pending', 'get'):
            httpx.TimeoutException("read timeout"),
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        trailing_distance=0.00500, trailing_stop=True,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')
    ctx.upsert_order(
        'coid-sl-old', symbol='EURUSD', side='sell', qty=1.0,
        state='confirmed', pine_entry_id='Long', from_entry='Long',
        sl_level=None, trailing_distance=0.00500, trailing_stop=True,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, trail_offset=0.00500,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, trail_offset=0.00800,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_exit(old_env, new_env))

    sl_row = ctx.get_order('coid-sl-old')
    assert sl_row is not None
    assert sl_row.state == 'disposition_unknown'
    assert sl_row.trailing_distance == pytest.approx(0.00800), (
        f"existing SL row's trailing_distance must be updated to the "
        f"attempted NEW offset (0.00800) before disposition_unknown — "
        f"otherwise the resolver's trailing comparison uses the OLD "
        f"distance and broker-not-applied wrongly resolves 'attached'; "
        f"got trailing_distance={sl_row.trailing_distance!r}"
    )
    assert sl_row.trailing_stop is True, (
        f"trailing_stop must remain True for an active-trail target; "
        f"got {sl_row.trailing_stop!r}"
    )

    # Drive the resolver: broker did NOT apply the new distance →
    # still shows OLD distance 0.00500 with trailingStop=True.
    positions_by_deal = {'deal-L': {
        'position': {'stopLevel': None, 'profitLevel': None,
                     'trailingStop': True, 'stopDistance': 0.00500},
        'market': {'epic': 'EURUSD'},
    }}
    bracket_resolutions: dict[str, bool] = {}
    broker._resolve_bracket_leg_disposition(
        sl_row, 'sl', positions_by_deal,
        bracket_resolutions=bracket_resolutions,
    )
    assert bracket_resolutions.get('coid-entry') is False, (
        f"trailing distance change with broker-not-applied must resolve "
        f"'rejected' so the engine retries the amend. Without the "
        f"persist, row keeps old distance matching broker → 'attached' "
        f"→ silent desync; got {bracket_resolutions!r}"
    )
    closed_row = ctx.get_order('coid-sl-old')
    assert closed_row is not None
    assert closed_row.state == 'rejected'
    assert closed_row.closed_ts_ms is not None
    store.close()


def __test_seed_added_legs_disposition_unknown_reopens_closed_row__(tmp_path):
    """Pre-seeded leg row must reopen a CLOSED row at the same COID.

    Failure mode without reopen: a previous attach for the same
    envelope can have closed the row (e.g. ``_rollback_bracket_legs``
    on a prior synchronous REJECTED, or a ``not_attached`` resolution
    from an earlier ambiguous round). ``upsert_order`` only updates
    fields and leaves ``closed_ts_ms`` set, so ``iter_live_orders``
    keeps skipping the row and ``_reconcile_snapshot`` never resolves
    the parked dispatch — the engine stays parked indefinitely.

    Mirrors the post-PUT mirror's closed-row guard
    (:meth:`__test_modify_exit_reopens_closed_leg_row_on_amend__`)
    but for the ambiguous-PUT seeding path.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('error', 'positions/deal-L', 'put'):
            httpx.TimeoutException("read timeout"),
    })
    # Entry without any TP/SL — the amend will ADD a TP, which routes
    # through the "no existing leg" seed branch.
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')

    # Compute the new envelope's TP coid and pre-seed it as CLOSED to
    # simulate a row that a prior cycle had rolled back.
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18250,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    tp_coid = new_env.client_order_id('t')
    ctx.upsert_order(
        tp_coid, symbol='EURUSD', side='sell', qty=1.0,
        state='rejected', pine_entry_id='Long', from_entry='Long',
        tp_level=1.17900,
        extras={'leg_kind': 'tp', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    ctx.close_order(tp_coid)
    pre = ctx.get_order(tp_coid)
    assert pre is not None and pre.closed_ts_ms is not None

    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_exit(old_env, new_env))

    seeded = ctx.get_order(tp_coid)
    assert seeded is not None
    assert seeded.closed_ts_ms is None, (
        f"seeded TP row must be REOPENED (closed_ts_ms cleared) so "
        f"iter_live_orders sees it; otherwise _reconcile_snapshot "
        f"never resolves the parked dispatch and the engine sits "
        f"parked forever; got closed_ts_ms={seeded.closed_ts_ms!r}"
    )
    assert seeded.state == 'disposition_unknown'
    assert seeded.tp_level == pytest.approx(1.18250)
    # Visibility check: the reopened seed must show up in
    # iter_live_orders so the snapshot reconcile picks it up.
    assert any(r.client_order_id == tp_coid
               for r in ctx.iter_live_orders(symbol='EURUSD'))
    store.close()


def __test_seed_added_legs_pending_trail_reopens_closed_row__(tmp_path):
    """Same closed-row reopen contract for the pending-trail seed
    branch: when the catch path seeds a NEW pending-trail SL row at a
    coid where a previously-closed row exists, the row must be
    reopened so ``_trailing_activation_monitor`` can pick it up.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('error', 'positions/deal-L', 'put'):
            httpx.TimeoutException("read timeout"),
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        tp_level=1.17900,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')
    # Seed a TP leg as live so _find_bracket_leg_row('tp') returns it
    # (otherwise the TP seed branch fires too).
    ctx.upsert_order(
        'coid-tp-old', symbol='EURUSD', side='sell', qty=1.0,
        state='confirmed', pine_entry_id='Long', from_entry='Long',
        tp_level=1.17900,
        extras={'leg_kind': 'tp', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000,
            trail_offset=0.00500, trail_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    sl_coid = new_env.client_order_id('s')
    # Pre-seed sl_coid as CLOSED to simulate a prior failed cycle.
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0,
        state='rejected', pine_entry_id='Long', from_entry='Long',
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    ctx.close_order(sl_coid)
    pre_sl = ctx.get_order(sl_coid)
    assert pre_sl is not None and pre_sl.closed_ts_ms is not None

    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.17900,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_exit(old_env, new_env))

    seeded = ctx.get_order(sl_coid)
    assert seeded is not None
    assert seeded.closed_ts_ms is None, (
        f"pending-trail seed must REOPEN the closed row so "
        f"_trailing_activation_monitor's iter_live_orders pass picks "
        f"it up; got closed_ts_ms={seeded.closed_ts_ms!r}"
    )
    assert seeded.state == 'confirmed'
    extras = seeded.extras or {}
    assert extras.get('trail_state') == 'pending'
    store.close()


def __test_listen_loop_sentinel_targets_local_queue_not_successor__(tmp_path):
    """Listener's exit sentinel must land on the queue THIS listener
    was started with, not on a successor connection's queue established
    by a fast disconnect/reconnect cycle.

    Failure mode without local-queue capture: ``disconnect()`` cancels
    the listener and clears ``self._update_queue``; a fresh
    ``connect()`` then creates a NEW queue. If the cancelled listener
    task hasn't reached its ``finally`` yet (cancellation is not
    synchronous), reading ``self._update_queue`` in ``finally`` picks
    up the NEW connection's queue and the sentinel ``None`` lands
    there — ``watch_ohlcv`` would raise ``ConnectionError`` on an
    otherwise healthy reconnect.
    """
    broker = _FakeBroker(config=_make_config())

    async def runner():
        # Stage one listener with its own queue, then start the task.
        old_queue: asyncio.Queue = asyncio.Queue()
        broker._update_queue = old_queue

        class _DummyWS:
            close_code = None

            def __aiter__(self):
                return self

            async def __anext__(self):
                # Block forever — the listener stays at the iterator
                # await until cancelled.
                await asyncio.sleep(3600)
                raise StopAsyncIteration

        broker._ws = _DummyWS()  # type: ignore[assignment]
        listener = asyncio.create_task(broker._listen_loop())
        await asyncio.sleep(0)  # let the listener reach the await

        # Simulate the disconnect/reconnect race: replace the broker's
        # queue with a NEW one BEFORE the cancelled listener gets to run
        # its finally. With local-queue capture, the listener still
        # writes to ``old_queue``; without it, the sentinel lands on
        # ``new_queue`` and breaks the next consumer.
        new_queue: asyncio.Queue = asyncio.Queue()
        broker._update_queue = new_queue
        listener.cancel()
        try:
            await listener
        except asyncio.CancelledError:
            pass
        return old_queue, new_queue

    old_queue, new_queue = asyncio.run(runner())
    assert old_queue.qsize() == 1, (
        f"listener's exit sentinel must land on the OLD queue (the "
        f"one this listener was started with), not on the successor "
        f"queue established mid-cancellation; got "
        f"old_queue.qsize()={old_queue.qsize()}, "
        f"new_queue.qsize()={new_queue.qsize()}"
    )
    assert new_queue.qsize() == 0, (
        f"successor queue must NOT receive a stale None — that would "
        f"make watch_ohlcv raise ConnectionError on a healthy "
        f"reconnect; got new_queue.qsize()={new_queue.qsize()}"
    )
    sentinel = old_queue.get_nowait()
    assert sentinel is None


def __test_resolve_bracket_leg_disposition_force_rejected_marker_overrides_native_trail_match__(tmp_path):
    """The force-rejected marker must override the native-trailing
    branch too: existing native-trail SL → pending-trail transition
    with the broker still showing trailingStop=True+matching distance
    would normally resolve as ``attached``. The marker forces
    ``rejected`` so the engine retries.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={})
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.upsert_order(
        'coid-sl-trail', symbol='EURUSD', side='sell', qty=1.0,
        state='disposition_unknown',
        pine_entry_id='Long', from_entry='Long',
        sl_level=None, trailing_distance=0.00800, trailing_stop=True,
        extras={
            'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
            'parent_coid': 'coid-entry',
            'force_disposition_rejected': True,
        },
    )
    sl_row = ctx.get_order('coid-sl-trail')
    assert sl_row is not None

    # Broker still has trailingStop=True with matching distance —
    # without the marker the resolver would say 'attached'.
    positions_by_deal = {'deal-L': {
        'position': {'trailingStop': True, 'stopDistance': 0.00800},
        'market': {'epic': 'EURUSD'},
    }}
    bracket_resolutions: dict[str, bool] = {}
    broker._resolve_bracket_leg_disposition(
        sl_row, 'sl', positions_by_deal,
        bracket_resolutions=bracket_resolutions,
    )
    assert bracket_resolutions.get('coid-entry') is False
    rejected_row = ctx.get_order('coid-sl-trail')
    assert rejected_row is not None
    assert rejected_row.state == 'rejected'
    store.close()


def __test_reconcile_clears_stale_close_event_yielded_breadcrumb__(tmp_path):
    """The ``close_event_yielded_at`` breadcrumb is a one-poll race
    signal: ``_process_activity`` stamps it when a close fill yielded
    while ``/positions`` had not yet reflected the close. The breadcrumb
    must NOT survive a subsequent poll where the deal is observed STILL
    PRESENT.

    Failure mode if the breadcrumb is left in place:
      * Activity 1 (partial USER close): yields the partial fill,
        stamps the breadcrumb (the deal is still present at the
        residual size).
      * Many polls pass — the deal is visible at the residual size,
        the breadcrumb is intentionally not consumed by the
        ``pos is None and work is None`` race-resolution branch.
      * Eventually the residual is closed by a manual action whose
        activity rolls out of Capital.com's ~60s ``/history/activity``
        window before our next poll — so the close fill is never
        emitted.
      * Reconcile finds ``pos is None and work is None`` AND a
        ``close_event_yielded_at`` extras key still set →  the
        race-resolution branch fires and stamps ``natural_close_at``,
        suppressing the missing-pending grace tracker. The strategy
        is silently desynced (its ``BrokerPosition`` retains the
        residual; the broker is flat).

    Fix contract enforced by this test: any reconcile pass that observes
    the deal still present clears the stale breadcrumb. A subsequent
    disappearance then routes through the normal missing-pending grace
    window (``UnexpectedCancelError`` after the grace expires), letting
    the strategy re-sync rather than silently truncating.
    """
    broker, store, ctx = _make_broker(tmp_path)
    e_coid, tp_coid, sl_coid = _seed_bracket_setup(ctx, qty=1.0)
    entry_row = ctx.get_order(e_coid)
    assert entry_row is not None
    base_extras = dict(entry_row.extras or {})
    base_extras['entry_filled_at'] = 1700000000.0
    # Pretend a prior poll's _process_activity stamped the breadcrumb
    # because /positions still showed the deal when the activity yielded
    # a CLOSE fill (could be a genuine partial close OR a race that has
    # since NOT resolved as a full close). The breadcrumb's poll-id
    # reflects the cycle that wrote it; this test exercises the
    # subsequent-poll branch where the deal is still observed alive
    # against a FRESH snapshot — so the clear must run.
    base_extras['close_event_yielded_at'] = 1700000005.0
    base_extras['close_event_yielded_at_poll_id'] = 1
    ctx.upsert_order(e_coid, filled_qty=1.0, extras=base_extras)

    # Reconcile pass 1: a LATER poll cycle observes the deal still
    # present in /positions at the residual size — the breadcrumb's
    # poll-id is stale so the same-poll guard does not fire and the
    # clear must execute.
    broker._current_poll_id = 2
    positions_by_deal = {
        'deal-L': {'position': {'dealId': 'deal-L', 'size': 0.6,
                                'level': 1.17500,
                                'direction': 'BUY'}},
    }

    async def run_reconcile(pos, work):
        out = []
        async for ev in broker._reconcile_snapshot(pos, work):
            out.append(ev)
        return out

    asyncio.run(run_reconcile(positions_by_deal, {}))

    refreshed_entry = ctx.get_order(e_coid)
    assert refreshed_entry is not None
    assert (refreshed_entry.extras or {}).get('close_event_yielded_at') is None, (
        f"reconcile must clear close_event_yielded_at when the deal is "
        f"still present — leaving it in place causes a later silent "
        f"teardown if the residual disappears without a priced "
        f"activity. Got extras={refreshed_entry.extras!r}"
    )
    # The bracket rows must remain untouched (no teardown stamp, no
    # missing-pending stamp — the deal is alive).
    for coid in (e_coid, tp_coid, sl_coid):
        row = ctx.get_order(coid)
        assert row is not None
        assert (row.extras or {}).get('natural_close_at') is None, (
            f"row {coid!r} must not be torn down — the deal is alive"
        )
        assert (row.extras or {}).get('missing_pending_since') is None, (
            f"row {coid!r} must not carry missing_pending_since — "
            f"the deal is present in this reconcile pass"
        )

    # Reconcile pass 2: simulate the residual disappearing later
    # (e.g. manual close whose activity rolled out of the 60s window).
    # The previously-cleared breadcrumb means reconcile MUST go through
    # the missing-pending grace tracker rather than silently tearing
    # down — the missing_pending_since stamp lets _missing_pending_tracker
    # eventually raise UnexpectedCancelError so the strategy can re-sync.
    asyncio.run(run_reconcile({}, {}))

    final_entry = ctx.get_order(e_coid)
    assert final_entry is not None
    assert (final_entry.extras or {}).get('natural_close_at') is None, (
        f"after the breadcrumb is cleared, a later disappearance must "
        f"NOT stamp natural_close_at via the race-resolution shortcut. "
        f"Got extras={final_entry.extras!r}"
    )
    assert (final_entry.extras or {}).get('missing_pending_since') is not None, (
        f"after the breadcrumb is cleared, a later disappearance must "
        f"route through the missing-pending grace tracker (stamp "
        f"missing_pending_since); otherwise the strategy gets no "
        f"recovery signal. Got extras={final_entry.extras!r}"
    )
    store.close()


def __test_close_event_yielded_breadcrumb_survives_same_poll_reconcile__(tmp_path):
    """The breadcrumb-clear in :meth:`_reconcile_snapshot` must NOT fire
    against the same-poll snapshot that caused :meth:`_process_activity`
    to defer in the first place.

    Race window covered:
      * Poll N opens, snapshots ``/positions`` (deal still alive).
      * Between that GET and the later ``/history/activity`` GET, the
        deal closes on the broker side.
      * ``_process_activity`` reads the close fill, looks up the
        same-poll positions snapshot, sees ``size > 0``, and *defers*
        the natural-close teardown by stamping
        ``close_event_yielded_at`` (and the new
        ``close_event_yielded_at_poll_id``).
      * ``_reconcile_snapshot`` runs immediately after, against the
        SAME stale snapshot. It must NOT clear the breadcrumb here —
        the next poll's fresh snapshot will rightfully observe the
        deal as gone, and the breadcrumb is what tells reconcile to
        promote the natural close instead of routing it through the
        ``missing_pending_since`` grace tracker (which would raise a
        false :class:`UnexpectedCancelError`).
      * A later poll observing the deal still alive against a FRESH
        snapshot is the legitimate clear case — covered by the
        adjacent ``__test_reconcile_clears_stale_close_event_yielded_breadcrumb__``.
    """
    broker, store, ctx = _make_broker(tmp_path)
    e_coid, tp_coid, sl_coid = _seed_bracket_setup(ctx, qty=1.0)
    entry_row = ctx.get_order(e_coid)
    assert entry_row is not None
    base_extras = dict(entry_row.extras or {})
    base_extras['entry_filled_at'] = 1700000000.0
    # Stamp written *during this same cycle* — emulates _process_activity
    # writing the breadcrumb against the snapshot that _reconcile_snapshot
    # is about to consume.
    broker._current_poll_id = 7
    base_extras['close_event_yielded_at'] = 1700000005.0
    base_extras['close_event_yielded_at_poll_id'] = 7
    ctx.upsert_order(e_coid, filled_qty=1.0, extras=base_extras)

    positions_by_deal = {
        'deal-L': {'position': {'dealId': 'deal-L', 'size': 0.6,
                                'level': 1.17500,
                                'direction': 'BUY'}},
    }

    async def run_reconcile(pos, work):
        out = []
        async for ev in broker._reconcile_snapshot(pos, work):
            out.append(ev)
        return out

    asyncio.run(run_reconcile(positions_by_deal, {}))

    same_poll_entry = ctx.get_order(e_coid)
    assert same_poll_entry is not None
    same_poll_extras = same_poll_entry.extras or {}
    assert same_poll_extras.get('close_event_yielded_at') == 1700000005.0, (
        f"breadcrumb must SURVIVE the same-poll reconcile pass — clearing "
        f"it here would route a next-poll deal disappearance through the "
        f"missing_pending grace tracker and raise a false "
        f"UnexpectedCancelError instead of the natural-close teardown. "
        f"Got extras={same_poll_extras!r}"
    )
    assert same_poll_extras.get('close_event_yielded_at_poll_id') == 7, (
        f"breadcrumb poll-id must SURVIVE the same-poll reconcile pass. "
        f"Got extras={same_poll_extras!r}"
    )

    # Next poll: a FRESH snapshot still shows the deal alive (genuine
    # partial close, not a race resolving as full). Bumping the poll
    # counter satisfies the gate — clear must now run, restoring the
    # original race-resolution semantics.
    broker._current_poll_id = 8
    asyncio.run(run_reconcile(positions_by_deal, {}))

    next_poll_entry = ctx.get_order(e_coid)
    assert next_poll_entry is not None
    next_poll_extras = next_poll_entry.extras or {}
    assert next_poll_extras.get('close_event_yielded_at') is None, (
        f"breadcrumb must clear once a fresh snapshot in a LATER poll "
        f"observes the deal still alive — leaving it would mask a "
        f"manual-close residual disappearance whose activity rolls out "
        f"of the 60s history window. Got extras={next_poll_extras!r}"
    )
    assert next_poll_extras.get('close_event_yielded_at_poll_id') is None, (
        f"breadcrumb poll-id must clear together with the timestamp. "
        f"Got extras={next_poll_extras!r}"
    )
    store.close()


def __test_execute_exit_retry_after_rejected_legs_reopens_rows__(tmp_path):
    """A bracket leg row that was flipped to ``rejected`` and physically
    closed (``closed_ts_ms`` set by :meth:`_record_bracket_resolution`'s
    not-attached branch) must NOT stay invisible after the engine
    re-dispatches the same :class:`ExitIntent` and ``execute_exit`` is
    invoked with an envelope that resolves to the same client-order-id.

    Failure mode if the row is not reopened:
      * ``DispatchEnvelope.client_order_id`` is a deterministic
        function of ``(run_tag, pine_id, bar_ts_ms, kind, retry_seq)``,
        so a same-bar retry computes IDENTICAL ``tp_coid`` /
        ``sl_coid`` values.
      * The retry's ``upsert_order`` lands on the existing row but
        does NOT touch ``closed_ts_ms`` — the SQL UPSERT only writes
        the columns it was passed.
      * ``iter_live_orders`` filters out closed rows, so the
        reattached protective leg is invisible to reconcile, the
        bracket-deal-id ref lookup, and the fill-price fallback chain.
        The strategy thinks it has SL/TP attached, the broker store
        cannot see it, and the next reconcile silently treats the leg
        as missing.

    Fix contract enforced here: ``execute_exit`` checks the existing
    row before the upsert and reopens it (clearing ``closed_ts_ms``)
    when a prior cycle closed the same COID. Mirrors the fix already
    present in :meth:`modify_exit`.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-L', 'put'): {'dealReference': 'attach-retry'},
        ('confirms/attach-retry', 'get'): {'dealStatus': 'ACCEPTED'},
    })
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

    # First dispatch — leg rows are seeded against the envelope's
    # deterministic COIDs.
    legs_first = asyncio.run(broker.execute_exit(env))
    assert len(legs_first) == 2
    tp_coid = legs_first[0].client_order_id
    sl_coid = legs_first[1].client_order_id

    # Simulate _record_bracket_resolution's not-attached branch: row
    # state flipped to 'rejected', physical close stamps closed_ts_ms.
    for coid in (tp_coid, sl_coid):
        ctx.set_order_state(coid, 'rejected')
        ctx.close_order(coid)
        closed = ctx.get_order(coid)
        assert closed is not None
        assert closed.closed_ts_ms is not None, (
            "test setup: leg row must be physically closed "
            "before the retry"
        )

    # Engine re-dispatches the same ExitIntent on the next sync. With
    # an unchanged bar, the envelope's COIDs collapse onto the same
    # tp_coid / sl_coid as the first dispatch — that's the path the
    # exchange-side dedup relies on. Retry must reopen those rows.
    legs_retry = asyncio.run(broker.execute_exit(env))
    assert len(legs_retry) == 2
    assert legs_retry[0].client_order_id == tp_coid, (
        "deterministic COID assumption broken — retry produced a "
        "different tp_coid; the test no longer exercises the "
        "row-collision path it intends to."
    )
    assert legs_retry[1].client_order_id == sl_coid, (
        "deterministic COID assumption broken — retry produced a "
        "different sl_coid; the test no longer exercises the "
        "row-collision path it intends to."
    )

    for coid in (tp_coid, sl_coid):
        row = ctx.get_order(coid)
        assert row is not None
        assert row.closed_ts_ms is None, (
            f"row {coid!r} must have been reopened on retry — leaving "
            f"closed_ts_ms set hides the live protective leg from "
            f"iter_live_orders, reconcile, and fill-price fallback. "
            f"Got closed_ts_ms={row.closed_ts_ms!r}, state={row.state!r}"
        )
        assert row.state == 'confirmed', (
            f"row {coid!r} must be re-confirmed after the retry's "
            f"successful PUT + confirm — got state={row.state!r}"
        )

    # The retry's leg rows must be visible to iter_live_orders again.
    live_coids = {r.client_order_id for r in ctx.iter_live_orders()}
    assert tp_coid in live_coids, (
        f"reopened tp_coid must show up in iter_live_orders — "
        f"got {live_coids!r}"
    )
    assert sl_coid in live_coids, (
        f"reopened sl_coid must show up in iter_live_orders — "
        f"got {live_coids!r}"
    )
    store.close()


def __test_modify_exit_reopens_closed_leg_row_on_amend__(tmp_path):
    """A previously rolled-back leg row (``state='rejected'``,
    ``closed_ts_ms`` stamped by :meth:`_rollback_bracket_legs`) must
    NOT make modify_exit's "leg already exists" branch run on it —
    the broker just attached a fresh leg, but plain ``upsert_order``
    in the UPDATE path cannot clear ``closed_ts_ms``, so
    ``iter_live_orders`` would keep skipping the row and the live
    protective leg would be invisible to recovery / fill fallback.
    The plugin must detect closed rows, reopen them via
    :meth:`RunContext.reopen_order`, and re-persist the leg fields.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'modify-reopen'},
        ('confirms/modify-reopen', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    tp_coid = env.client_order_id('t')
    sl_coid = env.client_order_id('s')
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    # Seed the SL leg as live so the TP path can be tested cleanly.
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        sl_level=1.17400,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    # Seed the TP leg as ROLLED-BACK from a previous synchronous
    # bracket-attach reject — state='rejected', closed_ts_ms stamped.
    ctx.upsert_order(
        tp_coid, symbol='EURUSD', side='sell', qty=1.0,
        state='rejected', pine_entry_id='Long', from_entry='Long',
        tp_level=1.17900,
        extras={'leg_kind': 'tp', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    ctx.close_order(tp_coid)
    pre = ctx.get_order(tp_coid)
    assert pre is not None and pre.closed_ts_ms is not None, (
        "precondition: TP leg must be CLOSED before the amend"
    )

    # Amend that adds the TP leg (broker just re-attached it).
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, sl_price=1.17400, tp_price=1.18250,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(env, new_env))

    tp_row = ctx.get_order(tp_coid)
    assert tp_row is not None
    assert tp_row.closed_ts_ms is None, (
        f"TP leg row must be REOPENED after the amend (closed_ts_ms "
        f"cleared); got closed_ts_ms={tp_row.closed_ts_ms!r}. Without "
        f"the reopen, iter_live_orders skips the row and recovery is "
        f"blind to a leg the broker is actively enforcing."
    )
    assert tp_row.state == 'confirmed', (
        f"reopened TP leg must be re-persisted as 'confirmed' (the "
        f"PUT + confirm have already succeeded); got state={tp_row.state!r}"
    )
    assert tp_row.tp_level == pytest.approx(1.18250)
    # Leg must be visible to live-iteration after reopen.
    assert any(r.client_order_id == tp_coid
               for r in ctx.iter_live_orders(symbol='EURUSD')), (
        "reopened TP leg must appear in iter_live_orders('EURUSD'); "
        "otherwise reconcile / fill-fallback can't see it"
    )
    store.close()


def __test_modify_exit_pending_trail_sl_leg_carries_pending_state__(tmp_path):
    """Pine ``trail_price`` + ``trail_offset`` on an amend that adds a
    new SL leg must persist the row in pending-trail state — the PUT
    body intentionally omits ``trailingStop`` because the activation
    monitor issues its own PUT once the threshold is crossed. Without
    ``trail_state='pending'`` + ``trail_activation_price`` extras the
    monitor would skip the row and the protective trailing stop would
    never attach, even though the broker side already exists.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'modify-pending'},
        ('confirms/modify-pending', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18250,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    tp_coid = env.client_order_id('t')
    sl_coid = env.client_order_id('s')
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )

    # Amend: add TP + pending-trail SL (trail_price + trail_offset, no fixed sl).
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18250,
            trail_offset=0.00500, trail_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(env, new_env))

    # The PUT body must NOT enable native trailing — that is the
    # activation monitor's job once the threshold is crossed.
    put_calls = [c for c in broker._calls
                 if c[0] == 'positions/deal-L' and c[1] == 'put']
    assert len(put_calls) == 1, (
        f"expected exactly one PUT for the amend, got {len(put_calls)}"
    )
    put_body = put_calls[0][2] or {}
    assert put_body.get('profitLevel') == pytest.approx(1.18250)
    assert 'trailingStop' not in put_body, (
        f"pending-trail amend must defer trailingStop until activation; "
        f"PUT body leaked trailingStop={put_body.get('trailingStop')!r}"
    )
    assert 'stopDistance' not in put_body, (
        f"pending-trail amend must not pre-arm stopDistance; "
        f"PUT body has stopDistance={put_body.get('stopDistance')!r}"
    )

    sl_row = ctx.get_order(sl_coid)
    assert sl_row is not None, (
        "pending-trail amend must persist the SL leg row even though the "
        "PUT body did not include the trailing fields"
    )
    assert sl_row.state == 'confirmed'
    assert sl_row.trailing_distance == pytest.approx(0.00500)
    assert sl_row.trailing_stop is False, (
        f"pending trail must NOT mark trailing_stop=True locally — the "
        f"broker is not yet trailing; got trailing_stop={sl_row.trailing_stop!r}"
    )
    extras = sl_row.extras or {}
    assert extras.get('trail_state') == 'pending', (
        f"_trailing_activation_monitor reads trail_state to decide whether "
        f"the row needs activation; got trail_state={extras.get('trail_state')!r}"
    )
    assert extras.get('trail_activation_price') == pytest.approx(1.18000), (
        f"trail_activation_price must mirror the Pine trail_price so the "
        f"monitor knows when to flip to native trailing; "
        f"got trail_activation_price={extras.get('trail_activation_price')!r}"
    )
    assert extras.get('trail_offset') == pytest.approx(0.00500), (
        f"trail_offset must be stashed in extras so the monitor can build "
        f"the activation PUT body; got trail_offset={extras.get('trail_offset')!r}"
    )
    assert extras.get('parent_deal_id') == 'deal-L'
    assert extras.get('leg_kind') == 'sl'

    # End-to-end: monitor must pick up the row when mid crosses the threshold.
    broker._calls.clear()
    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'activate'}
    positions = {'deal-L': {
        'market': {'epic': 'EURUSD', 'bid': 1.18002, 'offer': 1.18004},
        'position': {'trailingStop': False},
    }}
    asyncio.run(broker._trailing_activation_monitor(positions))
    activated = ctx.get_order(sl_coid)
    assert activated is not None
    assert (activated.extras or {}).get('trail_state') == 'activating', (
        "after the threshold is crossed the monitor must transition the "
        "row to 'activating' and issue the broker PUT — pending-trail "
        "regression would leave it stuck in 'pending'"
    )
    monitor_puts = [c for c in broker._calls
                    if c[0] == 'positions/deal-L' and c[1] == 'put']
    assert len(monitor_puts) == 1
    assert monitor_puts[0][2] == {
        'trailingStop': True, 'stopDistance': pytest.approx(0.00500),
    }
    store.close()


def __test_modify_exit_pending_trail_only_persists_without_put__(tmp_path):
    """Amending a position with ONLY pending trailing (no fixed SL, no
    TP) yields an empty PUT body; ``modify_exit`` must still INSERT the
    SL leg row in pending-trail state. Otherwise the activation monitor
    has nothing to find and the protective trailing stop never attaches.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
    })
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    sl_coid = env.client_order_id('s')
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )

    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0,
            trail_offset=0.00500, trail_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(env, new_env))

    # No PUT issued — the activation monitor will issue its own PUT later.
    put_calls = [c for c in broker._calls
                 if c[0] == 'positions/deal-L' and c[1] == 'put']
    assert not put_calls, (
        f"pending-trail-only amend must NOT issue a PUT (body empty); "
        f"saw {put_calls!r}"
    )

    # But the SL leg row MUST be persisted, otherwise the monitor can't see it.
    sl_row = ctx.get_order(sl_coid)
    assert sl_row is not None, (
        "pending-trail-only amend must INSERT the SL leg row even though "
        "the PUT body was empty — early-returning before persistence "
        "would leave the position unprotected"
    )
    extras = sl_row.extras or {}
    assert extras.get('trail_state') == 'pending'
    assert extras.get('trail_activation_price') == pytest.approx(1.18000)
    assert sl_row.trailing_stop is False
    store.close()


def __test_modify_exit_existing_sl_amend_to_pending_trail_writes_extras__(tmp_path):
    """Amending an EXISTING fixed SL leg to Pine ``trail_price`` +
    ``trail_offset`` must write ``trail_state='pending'`` /
    ``trail_activation_price`` / ``trail_offset`` extras and clear
    ``trailing_stop`` on the row. Without these,
    :meth:`_trailing_activation_monitor` skips the row (its only gate
    is ``extras['trail_state'] in ('pending', 'activating')``) and the
    protective trailing stop never attaches — the PUT body intentionally
    omits the trailing fields, so the broker has nothing either.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'modify-pending-existing'},
        ('confirms/modify-pending-existing', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, sl_price=1.17400, tp_price=1.18250,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    sl_coid = env.client_order_id('s')
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    # Seed: existing FIXED SL leg (no trail_state).
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        sl_level=1.17400, trailing_distance=None, trailing_stop=False,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )

    # Amend: keep TP, switch SL from fixed to pending-trail.
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18250,
            trail_offset=0.00500, trail_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(env, new_env))

    # PUT body must NOT pre-arm native trailing (no ``stopDistance``,
    # no ``trailingStop=True``) — activation is deferred to the monitor.
    # But it MUST explicitly clear the prior fixed SL: Capital.com keeps
    # any field absent from the body unchanged, so without
    # ``stopLevel=None`` the old fixed stop would stay live on the
    # broker side while we mark the row pending — the position could
    # close at the OLD level before Pine's activation price is reached.
    put_calls = [c for c in broker._calls
                 if c[0] == 'positions/deal-L' and c[1] == 'put']
    assert len(put_calls) == 1
    put_body = put_calls[0][2] or {}
    assert 'stopDistance' not in put_body
    assert put_body.get('trailingStop') is False, (
        f"fixed→pending-trail amend must clear any prior trailing flag "
        f"with trailingStop=False; got body={put_body!r}"
    )
    assert 'stopLevel' in put_body and put_body['stopLevel'] is None, (
        f"fixed→pending-trail amend must clear the prior fixed stopLevel "
        f"so the broker drops it; without this clear the old SL stays "
        f"live and the position can close before Pine's activation "
        f"price; got body={put_body!r}"
    )

    sl_row = ctx.get_order(sl_coid)
    assert sl_row is not None
    assert sl_row.trailing_distance == pytest.approx(0.00500)
    assert sl_row.trailing_stop is False, (
        f"pending-trail must not flag trailing_stop locally — broker is "
        f"not yet trailing; got trailing_stop={sl_row.trailing_stop!r}"
    )
    extras = sl_row.extras or {}
    assert extras.get('trail_state') == 'pending', (
        f"existing-row UPDATE path must write trail_state='pending' so "
        f"_trailing_activation_monitor picks the row up; got "
        f"trail_state={extras.get('trail_state')!r}"
    )
    assert extras.get('trail_activation_price') == pytest.approx(1.18000)
    assert extras.get('trail_offset') == pytest.approx(0.00500)

    # End-to-end: monitor activates when mid crosses the threshold.
    broker._calls.clear()
    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'activate'}
    positions = {'deal-L': {
        'market': {'epic': 'EURUSD', 'bid': 1.18002, 'offer': 1.18004},
        'position': {'trailingStop': False},
    }}
    asyncio.run(broker._trailing_activation_monitor(positions))
    activated = ctx.get_order(sl_coid)
    assert activated is not None
    assert (activated.extras or {}).get('trail_state') == 'activating'
    store.close()


def __test_modify_exit_existing_pending_to_fixed_clears_stale_extras__(tmp_path):
    """Amending an EXISTING pending-trail SL back to a fixed stop must
    drop ``trail_state`` / ``trail_activation_price`` from extras.
    Otherwise :meth:`_trailing_activation_monitor` would still see the
    row as pending and race a future PUT against the fixed stop the
    user just amended in.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'modify-pending-clear'},
        ('confirms/modify-pending-clear', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0,
            trail_offset=0.00500, trail_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    sl_coid = env.client_order_id('s')
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    # Seed: existing pending-trail SL leg (extras carry pending state).
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        sl_level=None, trailing_distance=0.00500, trailing_stop=False,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry',
                'trail_state': 'pending',
                'trail_activation_price': 1.18000,
                'trail_offset': 0.00500},
    )

    # Amend: revert to a plain fixed SL.
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(env, new_env))

    sl_row = ctx.get_order(sl_coid)
    assert sl_row is not None
    assert sl_row.sl_level == pytest.approx(1.17400)
    assert sl_row.trailing_distance is None
    assert sl_row.trailing_stop is False
    extras = sl_row.extras or {}
    assert 'trail_state' not in extras, (
        f"transitioning out of pending must drop trail_state; got "
        f"trail_state={extras.get('trail_state')!r}"
    )
    assert 'trail_activation_price' not in extras, (
        f"transitioning out of pending must drop trail_activation_price; "
        f"got trail_activation_price={extras.get('trail_activation_price')!r}"
    )

    # Monitor must NOT find this row as pending anymore.
    positions = {'deal-L': {
        'market': {'epic': 'EURUSD', 'bid': 1.18002, 'offer': 1.18004},
        'position': {'trailingStop': False},
    }}
    broker._calls.clear()
    asyncio.run(broker._trailing_activation_monitor(positions))
    monitor_puts = [c for c in broker._calls
                    if c[0] == 'positions/deal-L' and c[1] == 'put']
    assert not monitor_puts, (
        f"after pending→fixed amend the monitor must NOT issue an "
        f"activation PUT (the pending state has been cleared); "
        f"saw {monitor_puts!r}"
    )
    store.close()


def __test_execute_exit_pending_trail_put_timeout_does_not_park_sl_leg__(tmp_path):
    """A timeout on the bracket PUT must NOT flip a pending-trail SL leg
    to ``disposition_unknown``. The pending-trail leg was deliberately
    excluded from the PUT body (the activation monitor sends it later)
    so the broker has no trail to confirm; routing the row through
    :meth:`_resolve_bracket_leg_disposition` would observe
    ``trailingStop`` absent on the parent and wrongly mark the row
    rejected — closing it and leaving the position with no protective
    trailing stop on top of the original ambiguity.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('error', 'positions/deal-L', 'put'):
            httpx.TimeoutException("read timeout"),
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18250,
            trail_offset=0.00500, trail_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    tp_coid = env.client_order_id('t')
    sl_coid = env.client_order_id('s')

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_exit(env))

    tp_row = ctx.get_order(tp_coid)
    assert tp_row is not None
    assert tp_row.state == 'disposition_unknown', (
        "TP leg WAS in the PUT body — the timeout leaves it ambiguous, "
        "so flipping to disposition_unknown is correct"
    )
    sl_row = ctx.get_order(sl_coid)
    assert sl_row is not None and sl_row.closed_ts_ms is None
    assert sl_row.state == 'confirmed', (
        f"pending-trail SL leg was NOT in the PUT body — the broker "
        f"never saw it, so its disposition is unambiguous: it is "
        f"purely local. The post-PUT block that normally promotes it "
        f"from the pre-PUT 'submitted' to 'confirmed' is skipped on "
        f"the timeout path, so we must promote it explicitly before "
        f"raising — otherwise (a) recovery treats the row as an "
        f"in-flight broker submission on restart, and (b) the "
        f"activation monitor's lifecycle race against a row stuck in "
        f"'submitted'. Got state={sl_row.state!r}."
    )
    extras = sl_row.extras or {}
    assert extras.get('trail_state') == 'pending', (
        f"pending-trail SL must remain in 'pending' so the activation "
        f"monitor can still pick it up later; got "
        f"trail_state={extras.get('trail_state')!r}"
    )
    store.close()


def __test_execute_exit_pending_trail_confirm_timeout_promotes_sl_to_confirmed__(tmp_path):
    """Same defensive promote on the *confirm* timeout path: the PUT
    landed (we got a dealReference) but reading ``confirms/{ref}`` timed
    out. TP is ambiguous and gets ``disposition_unknown``; pending-trail
    SL was never in the PUT, so it remains purely local and must be
    promoted to ``confirmed`` rather than being left at the pre-PUT
    ``submitted`` placeholder.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('positions/deal-L', 'put'): {'dealReference': 'amb-confirm'},
        ('error', 'confirms/amb-confirm', 'get'):
            httpx.TimeoutException("read timeout"),
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18250,
            trail_offset=0.00500, trail_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    tp_coid = env.client_order_id('t')
    sl_coid = env.client_order_id('s')

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_exit(env))

    tp_row = ctx.get_order(tp_coid)
    assert tp_row is not None
    assert tp_row.state == 'disposition_unknown'
    sl_row = ctx.get_order(sl_coid)
    assert sl_row is not None and sl_row.closed_ts_ms is None
    assert sl_row.state == 'confirmed', (
        f"confirm-timeout path must promote pending-trail SL from "
        f"'submitted' to 'confirmed' before raising; got "
        f"state={sl_row.state!r}"
    )
    assert (sl_row.extras or {}).get('trail_state') == 'pending'
    store.close()


def __test_modify_exit_native_trail_to_pending_clears_trailing_flag__(tmp_path):
    """Amending an EXISTING native (immediate) trailing SL to a Pine
    pending-trail must clear the broker-side ``trailingStop`` flag.
    Capital.com keeps any field absent from the PUT body unchanged, so
    the prior ``trailingStop=True`` would stay live while BrokerStore
    is rewritten as ``trail_state='pending'`` — the position would
    keep trailing immediately even though Pine semantics say it must
    not move until activation.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'native-to-pending'},
        ('confirms/native-to-pending', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    # Old: native immediate trailing (trail_offset set, trail_price None).
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, trail_offset=0.00800,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    sl_coid = env.client_order_id('s')
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        sl_level=None, trailing_distance=0.00800, trailing_stop=True,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )

    # Amend: switch to pending-trail.
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0,
            trail_offset=0.00500, trail_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(env, new_env))

    put_calls = [c for c in broker._calls
                 if c[0] == 'positions/deal-L' and c[1] == 'put']
    assert len(put_calls) == 1
    put_body = put_calls[0][2] or {}
    assert put_body.get('trailingStop') is False, (
        f"native→pending amend must explicitly clear trailingStop on "
        f"the broker side; without this the old immediate trailing "
        f"stays active and Pine's activation gate is bypassed; got "
        f"body={put_body!r}"
    )
    assert 'stopDistance' not in put_body, (
        f"native→pending must not pre-arm a new stopDistance — that "
        f"would re-enable trailing on the broker; got body={put_body!r}"
    )

    sl_row = ctx.get_order(sl_coid)
    assert sl_row is not None
    assert sl_row.trailing_stop is False
    extras = sl_row.extras or {}
    assert extras.get('trail_state') == 'pending'
    store.close()


def __test_modify_exit_no_clear_when_no_prior_broker_stop__(tmp_path):
    """When there is no prior broker-side stop (old intent had no
    ``sl_price`` / no ``trail_offset``), the pending-trail amend must
    NOT include a defensive ``stopLevel=None`` / ``trailingStop=False``
    in the body — those clears would be churn-only API calls that the
    broker handles fine but the test guards against unnecessary API
    surface in the PUT body.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'no-prior'},
        ('confirms/no-prior', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    # Old: NO bracket at all (TP-only). New: add pending-trail SL.
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18250,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )

    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18250,
            trail_offset=0.00500, trail_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(env, new_env))

    put_calls = [c for c in broker._calls
                 if c[0] == 'positions/deal-L' and c[1] == 'put']
    assert len(put_calls) == 1
    put_body = put_calls[0][2] or {}
    assert put_body.get('profitLevel') == pytest.approx(1.18250)
    assert 'stopLevel' not in put_body, (
        f"no prior broker-side stop existed, so the body must not "
        f"include a defensive stopLevel=None clear; got body={put_body!r}"
    )
    assert 'trailingStop' not in put_body, (
        f"no prior broker-side trailing flag existed, so the body must "
        f"not include trailingStop=False; got body={put_body!r}"
    )
    store.close()


def __test_reconcile_snapshot_working_to_position_stamps_kind_and_entry_filled__(tmp_path):
    """When ``_reconcile_snapshot`` promotes a working order to a
    filled position (entry fill observed via ``/positions`` snapshot
    rather than the activity stream), the row's ``kind`` must flip
    from ``'working'`` to ``'position'`` AND ``extras['entry_filled_at']``
    must be stamped. Several downstream paths gate on these fields:
    :meth:`_find_active_entry_row` (modify_exit lookup),
    partial-fill detection, and manual ``USER`` / ``DEALER`` close
    detection. Without the stamps, a manual close activity for a
    limit-fill entry would route as ``LegType.ENTRY`` and
    ``BrokerPosition.record_fill`` would ADD to the local position
    instead of closing it.
    """
    broker, store, ctx = _make_broker(tmp_path)
    # Seed: a working LIMIT entry that's already been ack'd by the
    # broker (state='server_ref_seen', kind='working').
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='server_ref_seen', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'working', 'order_type': OrderType.LIMIT.value},
    )
    # ``_process_activity`` resolves activities to rows via ``deal_id``
    # ref lookup — without this link the close activity below would be
    # treated as external and dropped.
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')
    positions = {'deal-L': {
        'market': {'epic': 'EURUSD'},
        'position': {'dealId': 'deal-L', 'direction': 'BUY',
                     'size': 1.0, 'level': 1.17500},
    }}

    async def drain():
        out = []
        async for ev in broker._reconcile_snapshot(positions, {}):
            out.append(ev)
        return out

    events = asyncio.run(drain())
    assert len(events) == 1
    assert events[0].leg_type == LegType.ENTRY
    assert events[0].fill_price == pytest.approx(1.17500)

    row = ctx.get_order('coid-entry')
    assert row is not None
    assert row.state == 'confirmed'
    assert row.filled_qty == pytest.approx(1.0)
    extras = row.extras or {}
    assert extras.get('kind') == 'position', (
        f"working→position promote must flip kind so subsequent paths "
        f"(modify_exit lookup, partial-fill detection, manual-close "
        f"detection) recognise the row as a position; got "
        f"kind={extras.get('kind')!r}"
    )
    assert extras.get('entry_filled_at') is not None, (
        "snapshot-confirmed entry must stamp entry_filled_at so a later "
        "USER/DEALER close activity is recognised as a manual close "
        "rather than mis-routed as another ENTRY add"
    )

    # End-to-end proof: a USER close activity now routes as LegType.CLOSE.
    close_activity = {
        'dateUTC': '2026-04-21T11:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'USER',
        'epic': 'EURUSD', 'direction': 'BUY',
        'size': 1.0, 'level': 1.17820,
    }

    async def drain_close():
        out = []
        async for ev in broker._process_activity([close_activity]):
            out.append(ev)
        return out

    close_events = asyncio.run(drain_close())
    assert len(close_events) == 1
    assert close_events[0].leg_type == LegType.CLOSE, (
        f"with kind='position' + entry_filled_at stamped, the manual "
        f"USER close must classify as CLOSE; got "
        f"leg_type={close_events[0].leg_type!r}"
    )
    assert close_events[0].order.side == 'sell'
    store.close()


def __test_process_activity_first_entry_fill_stamps_row__(tmp_path):
    """The first POSITION/EXECUTED activity for an entry row must
    classify as ``LegType.ENTRY`` and stamp ``extras['entry_filled_at']``.
    The stamp is the discriminator that lets a SUBSEQUENT manual-close
    activity for the same dealId route as ``LegType.CLOSE``.
    """
    broker, store, ctx = _make_broker(tmp_path)
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0, filled_qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')

    activity = {
        'dateUTC': '2026-04-21T09:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'USER',
        'epic': 'EURUSD', 'direction': 'BUY',
        'size': 1.0, 'level': 1.17500,
    }

    async def drain():
        out = []
        async for ev in broker._process_activity([activity]):
            out.append(ev)
        return out

    events = asyncio.run(drain())
    assert len(events) == 1
    assert events[0].leg_type == LegType.ENTRY, (
        "first POSITION/EXECUTED for an entry row routes as ENTRY"
    )
    assert events[0].order.side == 'buy'

    row = ctx.get_order('coid-entry')
    assert row is not None
    assert (row.extras or {}).get('entry_filled_at') is not None, (
        "entry row must be stamped after first entry-fill activity so "
        "subsequent USER/DEALER activities classify as manual close"
    )
    store.close()


def __test_process_activity_manual_close_routes_as_close_when_entry_activity_rolled_out__(tmp_path):
    """Defensive fallback for the "activity rolled out before stamp"
    race: ``execute_entry`` confirms a market entry (``state='confirmed'``,
    ``filled_qty>0``, ``kind='position'``) but the process crashes before
    the activity poll stamps ``entry_filled_at``. By the time the process
    restarts and resumes polling, Capital.com's 60s activity window has
    rolled past the entry, so the entry-fill activity never replays —
    the breadcrumb stays absent forever.

    Without the fallback, a later ``USER`` / ``DEALER`` manual close on
    the same dealId would route as ``LegType.ENTRY`` and ADD to the local
    position instead of reducing it. The guard recognises the row's
    ``state``/``filled_qty``/``kind`` triplet as evidence of a filled
    entry, then uses ``row.created_ts_ms`` vs the activity's ``dateUTC``
    to confirm the activity is too old to be the entry's own first fill
    (well beyond the 60s window).
    """
    import datetime as _dt
    broker, store, ctx = _make_broker(tmp_path)
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0, filled_qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        # Notable absence: no ``entry_filled_at`` — modelling the
        # crash-between-confirm-and-stamp scenario the guard exists for.
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')
    # Pin ``created_ts_ms`` so the activity's ``dateUTC`` is provably
    # more than 60s after row creation (the discriminator the guard
    # uses to decide the activity cannot be the entry's first fill).
    row_created_ts_ms = int(_dt.datetime(2026, 4, 21, 10, 0, 0,
                                         tzinfo=_dt.UTC).timestamp() * 1000)
    ctx._store._conn.execute(
        "UPDATE orders SET created_ts_ms = ? WHERE client_order_id = ?",
        (row_created_ts_ms, 'coid-entry'),
    )

    activity = {
        # 1 hour after row creation — far beyond Capital.com's 60s
        # activity window, so this activity is necessarily a separate
        # trade (manual close), not the entry's own fill.
        'dateUTC': '2026-04-21T11:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'USER',
        'epic': 'EURUSD', 'direction': 'BUY',
        'size': 1.0, 'level': 1.17820,
    }

    async def drain():
        out = []
        async for ev in broker._process_activity([activity]):
            out.append(ev)
        return out

    events = asyncio.run(drain())
    assert len(events) == 1
    ev = events[0]
    assert ev.leg_type == LegType.CLOSE, (
        f"USER POSITION/EXECUTED on a confirmed+filled entry row whose "
        f"``entry_filled_at`` was lost in a restart-with-rolled-out-window "
        f"race must still route as CLOSE — defensive fallback uses "
        f"row.state/filled_qty/kind + activity-age vs created_ts_ms; "
        f"got {ev.leg_type!r}"
    )
    assert ev.order.side == 'sell', (
        f"close-leg side must flip vs entry side (buy→sell); got "
        f"{ev.order.side!r}"
    )
    store.close()


def __test_process_activity_first_entry_fill_within_window_still_routes_as_entry__(tmp_path):
    """Inverse of the rolled-out-window guard: a fresh entry whose
    activity arrives within Capital.com's 60s activity window must
    still route as ``LegType.ENTRY`` even though ``entry_filled_at``
    is absent (the breadcrumb is stamped *after* this routing).

    Pins the new defensive fallback at its temporal boundary —
    activities younger than the row's creation + 60s do not trip the
    fallback, preserving the legacy first-activity-stamps-the-row
    contract that
    :func:`__test_process_activity_first_entry_fill_stamps_row__`
    tests against on the happy path.
    """
    import datetime as _dt
    broker, store, ctx = _make_broker(tmp_path)
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0, filled_qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')
    row_created_ts_ms = int(_dt.datetime(2026, 4, 21, 10, 0, 0,
                                         tzinfo=_dt.UTC).timestamp() * 1000)
    ctx._store._conn.execute(
        "UPDATE orders SET created_ts_ms = ? WHERE client_order_id = ?",
        (row_created_ts_ms, 'coid-entry'),
    )

    activity = {
        # 30s after row creation — well inside Capital.com's 60s
        # activity window; this is the entry's own fill activity.
        'dateUTC': '2026-04-21T10:00:30.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'USER',
        'epic': 'EURUSD', 'direction': 'BUY',
        'size': 1.0, 'level': 1.17500,
    }

    async def drain():
        out = []
        async for ev in broker._process_activity([activity]):
            out.append(ev)
        return out

    events = asyncio.run(drain())
    assert len(events) == 1
    assert events[0].leg_type == LegType.ENTRY, (
        f"activity within 60s of row creation must route as ENTRY — "
        f"the rolled-out-window fallback only triggers beyond the "
        f"activity window; got {events[0].leg_type!r}"
    )
    store.close()


def __test_process_activity_manual_close_routes_as_close__(tmp_path):
    """USER source on an entry row that already has
    ``extras['entry_filled_at']`` set is a manual close (web UI /
    mobile / dealer intervention). The emitted event must carry
    ``leg_type=LegType.CLOSE``, the side flipped against the entry's
    open side, and the entry+bracket rows must be flagged with
    ``natural_close_at`` so the missing-pending tracker doesn't fire
    a false UnexpectedCancelError.
    """
    broker, store, ctx = _make_broker(tmp_path)
    e_coid, tp_coid, sl_coid = _seed_bracket_setup(ctx)
    # Promote the entry row past its first entry-fill activity (the
    # discriminator the plugin uses to recognise subsequent activities
    # as closes).
    entry_row = ctx.get_order(e_coid)
    assert entry_row is not None
    base_extras = dict(entry_row.extras or {})
    base_extras['entry_filled_at'] = 1700000000.0
    ctx.upsert_order(e_coid, filled_qty=1.0, extras=base_extras)

    activity = {
        'dateUTC': '2026-04-21T11:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'USER',
        # Capital reports POSITION direction on close (BUY for a long
        # being closed) — record_fill must NOT trust this for ``side``.
        'epic': 'EURUSD', 'direction': 'BUY',
        'size': 1.0, 'level': 1.17820,
    }

    async def drain():
        out = []
        async for ev in broker._process_activity([activity]):
            out.append(ev)
        return out

    events = asyncio.run(drain())
    assert len(events) == 1
    ev = events[0]
    assert ev.leg_type == LegType.CLOSE, (
        f"USER on filled entry row must route as CLOSE, got {ev.leg_type!r}"
    )
    assert ev.event_type == 'filled'
    assert ev.order.side == 'sell', (
        f"close-leg side must be the OPPOSITE of the entry side "
        f"(buy→sell); got {ev.order.side!r}"
    )
    # Bracket teardown mirrors the TP/SL natural-close path.
    for coid in (e_coid, tp_coid, sl_coid):
        row = ctx.get_order(coid)
        assert row is not None
        assert (row.extras or {}).get('natural_close_at') is not None, (
            f"row {coid!r} must carry natural_close_at after manual close"
        )
    store.close()


def __test_process_activity_manual_close_zero_level_defers__(tmp_path):
    """Manual close (``USER`` source) on a stamped entry row executes at the
    prevailing market price — NOT at the still-attached bracket SL level.
    When Capital.com omits ``level`` (``0``) on the close activity, the
    bracket-leg fallback chain is gated so it only fires for actual TP/SL
    fills, leaving ``level=0``. The plugin must then DEFER the activity
    rather than yielding a zero-priced close: ``BrokerPosition.record_fill``
    rejects ``fill_price <= 0`` and the post-yield ``natural_close_at``
    stamp would suppress reconcile recovery — silently desyncing the
    strategy from the broker. Defer = no event, no dedup commit, no
    natural-close stamp; the next poll re-evaluates with a fresh
    snapshot, and if the activity rolls out of Capital.com's 60s
    history window first ``_reconcile_snapshot`` still detects the
    vanished deal via the missing-pending grace window.
    """
    broker, store, ctx = _make_broker(tmp_path)
    sl_price = 1.17636  # the bracket SL level a buggy fallback would surface
    e_coid, tp_coid, sl_coid = _seed_bracket_setup(ctx, sl_price=sl_price)
    entry_row = ctx.get_order(e_coid)
    assert entry_row is not None
    base_extras = dict(entry_row.extras or {})
    base_extras['entry_filled_at'] = 1700000000.0
    ctx.upsert_order(e_coid, filled_qty=1.0, extras=base_extras)

    activity = {
        'dateUTC': '2026-04-21T11:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'USER',
        'epic': 'EURUSD', 'direction': 'BUY',
        'size': 1.0, 'level': 0.0,  # Capital omitted the close price
    }
    fingerprint = _activity_fingerprint(activity)

    async def drain():
        out = []
        async for ev in broker._process_activity([activity]):
            out.append(ev)
        return out

    events = asyncio.run(drain())
    assert len(events) == 0, (
        f"zero-priced generic close must DEFER, got {len(events)} events"
    )
    cursor = broker._activity_cursor
    assert fingerprint not in cursor.seen_fingerprints, (
        "deferred activity must remain retryable on the next poll "
        "(fingerprint must NOT be added to seen_fingerprints)"
    )
    assert cursor.last_date_utc != activity['dateUTC'], (
        "deferred activity must NOT advance last_date_utc"
    )
    for coid in (e_coid, tp_coid, sl_coid):
        row = ctx.get_order(coid)
        assert row is not None
        assert (row.extras or {}).get('natural_close_at') is None, (
            f"row {coid!r} must NOT carry natural_close_at after a "
            f"deferred close — the stamp would suppress reconcile recovery"
        )
    store.close()


def __test_process_activity_manual_close_with_snapshot_still_defers__(tmp_path):
    """A ``positions_by_deal`` snapshot is NOT a valid fallback price
    source for a closing-leg activity. The snapshot's ``level`` field is
    the position's OPEN price (since the position has to be still in
    ``/positions`` to land in the snapshot at all); using it as the
    close fill price would silently corrupt P&L. The fallback must be
    gated to non-closing activities — when a manual close arrives with
    ``level=0`` and a stale-but-still-open snapshot, the plugin must
    defer rather than emit at the entry price.
    """
    broker, store, ctx = _make_broker(tmp_path)
    e_coid, tp_coid, sl_coid = _seed_bracket_setup(ctx)
    entry_row = ctx.get_order(e_coid)
    assert entry_row is not None
    base_extras = dict(entry_row.extras or {})
    base_extras['entry_filled_at'] = 1700000000.0
    ctx.upsert_order(e_coid, filled_qty=1.0, extras=base_extras)
    open_price = 1.17500  # the position's OPEN price (snapshot.level)
    activity = {
        'dateUTC': '2026-04-21T11:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'USER',
        'epic': 'EURUSD', 'direction': 'BUY',
        'size': 1.0, 'level': 0.0,  # close price omitted by Capital
    }
    # Race scenario: ``/positions`` was fetched BEFORE the close
    # finalised, so the deal is still in the snapshot at the open price.
    snap = {'deal-L': {'position': {'dealId': 'deal-L', 'level': open_price}}}
    fingerprint = _activity_fingerprint(activity)

    async def drain():
        out = []
        async for ev in broker._process_activity([activity], snap):
            out.append(ev)
        return out

    events = asyncio.run(drain())
    assert events == [], (
        "snapshot fallback must NOT surface a close event at the OPEN "
        f"price ({open_price}); generic / manual closes with level=0 "
        "must defer regardless of whether the position is still in the "
        "snapshot"
    )
    cursor = broker._activity_cursor
    assert fingerprint not in cursor.seen_fingerprints, (
        "deferred activity must remain retryable on the next poll"
    )
    for coid in (e_coid, tp_coid, sl_coid):
        row = ctx.get_order(coid)
        assert row is not None
        assert (row.extras or {}).get('natural_close_at') is None, (
            f"row {coid!r} must NOT carry natural_close_at after a "
            f"deferred close"
        )
    store.close()


def __test_process_activity_manual_partial_close_zero_level_emits_with_proxy_price__(
        tmp_path):
    """Manual partial close (``USER`` source) with ``level=0`` while the
    deal is still alive in the same-poll snapshot must emit with a live-
    mid proxy price — NOT defer.

    The original defer guard was symmetrical for all
    ``LegType.CLOSE`` / TP / SL / TRAILING_STOP zero-priced fills, but
    Capital.com's vanished-deal recovery (via
    :meth:`_reconcile_snapshot`'s missing-pending grace window) only
    handles deals that have left ``/positions``. A partial manual close
    leaves the deal alive with a residual size, so vanished-deal
    recovery never reaches it. The snapshot reconcile's partial-fill
    detection is also unreachable for this case: its check
    ``row.filled_qty + 1e-9 < cumulative`` is false once the entry is
    fully filled (``row.filled_qty == row.qty``). Without a forced
    emit, once the activity rolls out of Capital.com's 60s history
    window the local position would silently stay larger than the
    broker forever.

    The plugin therefore falls through with the live mid as a
    fill_price proxy (not the actual close mid — Capital.com offers
    no REST endpoint for that — but accurate enough that
    ``BrokerPosition.record_fill`` can apply the reduction).
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
    })
    e_coid, tp_coid, sl_coid = _seed_bracket_setup(ctx, qty=1.0)
    entry_row = ctx.get_order(e_coid)
    assert entry_row is not None
    base_extras = dict(entry_row.extras or {})
    base_extras['entry_filled_at'] = 1700000000.0
    ctx.upsert_order(e_coid, filled_qty=1.0, extras=base_extras)

    activity = {
        'dateUTC': '2026-04-21T11:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'USER',
        'epic': 'EURUSD', 'direction': 'BUY',
        # Capital.com omitted the close price; size = 0.4 of a 1.0 long.
        'size': 0.4, 'level': 0.0,
    }
    # Same-poll snapshot still carries the residual deal — vanished-
    # deal recovery cannot reach this event.
    snap = {'deal-L': {'position': {'dealId': 'deal-L', 'size': 0.6,
                                    'level': 1.17500}}}
    fingerprint = _activity_fingerprint(activity)

    async def drain():
        out = []
        async for ev in broker._process_activity([activity], snap):
            out.append(ev)
        return out

    events = asyncio.run(drain())
    assert len(events) == 1, (
        f"manual partial close with level=0 + deal still alive must "
        f"emit (with a proxy price) instead of deferring; otherwise "
        f"the activity rolls out of the 60s window and the local "
        f"position silently stays larger than the broker. Got "
        f"{len(events)} events"
    )
    ev = events[0]
    assert ev.leg_type == LegType.CLOSE, (
        f"USER on filled entry row must route as CLOSE, got {ev.leg_type!r}"
    )
    assert ev.fill_price == pytest.approx(1.17501), (
        f"event must carry the live mid as proxy fill_price "
        f"((bid+offer)/2 = (1.17500+1.17502)/2 = 1.17501), got "
        f"{ev.fill_price!r}"
    )
    assert ev.fill_qty == 0.4, (
        f"event must carry the activity-reported partial close size "
        f"(0.4), got {ev.fill_qty!r}"
    )
    cursor = broker._activity_cursor
    assert fingerprint in cursor.seen_fingerprints, (
        "proxy-priced emit must commit the fingerprint so the next "
        "poll does not re-emit the same partial close"
    )
    store.close()


def __test_process_activity_manual_partial_close_zero_level_defers_when_proxy_unavailable__(
        tmp_path):
    """When the live-mid fetch fails (network / rate-limit) for a manual
    partial close, fall back to the original defer behavior so the next
    poll retries the proxy. Without this fallback a transient
    ``/markets/{epic}`` failure would force a yield with ``fill_price=0``
    that ``BrokerPosition.record_fill`` would reject anyway, AND it
    would commit the fingerprint — silently dropping the reduction.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('error', 'markets/EURUSD', 'get'):
            httpx.TimeoutException("read timeout"),
    })
    e_coid, _tp_coid, _sl_coid = _seed_bracket_setup(ctx, qty=1.0)
    entry_row = ctx.get_order(e_coid)
    assert entry_row is not None
    base_extras = dict(entry_row.extras or {})
    base_extras['entry_filled_at'] = 1700000000.0
    ctx.upsert_order(e_coid, filled_qty=1.0, extras=base_extras)

    activity = {
        'dateUTC': '2026-04-21T11:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'USER',
        'epic': 'EURUSD', 'direction': 'BUY',
        'size': 0.4, 'level': 0.0,
    }
    snap = {'deal-L': {'position': {'dealId': 'deal-L', 'size': 0.6,
                                    'level': 1.17500}}}
    fingerprint = _activity_fingerprint(activity)

    async def drain():
        out = []
        async for ev in broker._process_activity([activity], snap):
            out.append(ev)
        return out

    events = asyncio.run(drain())
    assert events == [], (
        f"proxy fetch failed → defer (no event); got {len(events)}"
    )
    cursor = broker._activity_cursor
    assert fingerprint not in cursor.seen_fingerprints, (
        "deferred activity (proxy unavailable) must remain retryable "
        "on the next poll — fingerprint must NOT be committed"
    )
    store.close()


def __test_process_activity_partial_close_does_not_teardown__(tmp_path):
    """A ``USER`` close that only REDUCES the deal (partial close via
    ``DELETE /positions/{dealId}`` with ``size`` < remaining) leaves the
    position live with the OCA bracket still attached to the residual
    exposure. Stamping ``natural_close_at`` on the entry + bracket rows
    here would orphan the residual: ``execute_exit`` / ``modify_exit``
    would skip the row by its own teardown guard, and
    ``_reconcile_snapshot`` would no longer track the surviving deal.

    The plugin must therefore route partial closes as follows:
      * The ``LegType.CLOSE`` event still yields (so the strategy's
        :class:`BrokerPosition` reduces by the closed amount).
      * No teardown — entry + brackets keep ``natural_close_at`` unset.
      * A ``close_event_yielded_at`` breadcrumb on the entry row signals
        a subsequent reconcile poll: if the deal then vanishes the row
        is upgraded to a teardown via the race-resolution path
        (covered by the companion reconcile test below).
    """
    broker, store, ctx = _make_broker(tmp_path)
    e_coid, tp_coid, sl_coid = _seed_bracket_setup(ctx, qty=1.0)
    entry_row = ctx.get_order(e_coid)
    assert entry_row is not None
    base_extras = dict(entry_row.extras or {})
    base_extras['entry_filled_at'] = 1700000000.0
    ctx.upsert_order(e_coid, filled_qty=1.0, extras=base_extras)

    activity = {
        'dateUTC': '2026-04-21T11:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'USER',
        'epic': 'EURUSD', 'direction': 'BUY',
        # Partial close: 0.4 units of a 1.0 long. Residual = 0.6.
        'size': 0.4, 'level': 1.17820,
    }
    # Same-poll snapshot reflects the post-close residual size.
    snap = {'deal-L': {'position': {'dealId': 'deal-L', 'size': 0.6,
                                    'level': 1.17500}}}

    async def drain():
        out = []
        async for ev in broker._process_activity([activity], snap):
            out.append(ev)
        return out

    events = asyncio.run(drain())
    assert len(events) == 1, (
        f"partial close must still yield the CLOSE fill event so "
        f"BrokerPosition reduces by the closed amount; got {len(events)}"
    )
    ev = events[0]
    assert ev.leg_type == LegType.CLOSE, (
        f"USER on filled entry row must route as CLOSE, got {ev.leg_type!r}"
    )
    assert ev.fill_qty == 0.4, (
        f"event must carry the activity-reported close size (0.4), "
        f"got {ev.fill_qty!r}"
    )
    for coid in (e_coid, tp_coid, sl_coid):
        row = ctx.get_order(coid)
        assert row is not None
        assert (row.extras or {}).get('natural_close_at') is None, (
            f"row {coid!r} must NOT carry natural_close_at after a "
            f"partial close — residual exposure ({0.6}) is still live "
            f"and the OCA bracket stays attached to it. Stamping the "
            f"flag would orphan the residual from execute_exit / "
            f"modify_exit / reconcile."
        )
    refreshed_entry = ctx.get_order(e_coid)
    assert refreshed_entry is not None
    assert (refreshed_entry.extras or {}).get('close_event_yielded_at') is not None, (
        "entry row must carry close_event_yielded_at after a partial "
        "close so a subsequent reconcile poll can promote the row to "
        "a teardown if the deal then vanishes (race-window resolution)"
    )
    store.close()


def __test_reconcile_promotes_close_event_yielded_after_race__(tmp_path):
    """Race-window resolution: a close event was yielded one poll while
    ``/positions`` still carried the deal (so the eager teardown was
    deferred via the ``close_event_yielded_at`` breadcrumb). On a
    subsequent poll the deal has vanished from BOTH ``positions`` and
    ``workingorders``. ``_reconcile_snapshot`` must then promote the
    row to a teardown — stamp the entry + brackets with
    ``natural_close_at`` rather than ``missing_pending_since`` —
    otherwise the missing-pending grace tracker raises a false
    :class:`UnexpectedCancelError` once the grace window expires.
    """
    broker, store, ctx = _make_broker(tmp_path)
    e_coid, tp_coid, sl_coid = _seed_bracket_setup(ctx)
    # Simulate the prior poll's outcome: a close event yielded while the
    # deal was still in /positions, so the entry row carries the
    # breadcrumb stamp but no teardown flag yet.
    entry_row = ctx.get_order(e_coid)
    assert entry_row is not None
    base_extras = dict(entry_row.extras or {})
    base_extras['entry_filled_at'] = 1700000000.0
    base_extras['close_event_yielded_at'] = 1700000005.0
    ctx.upsert_order(e_coid, filled_qty=1.0, extras=base_extras)

    async def run_reconcile():
        out = []
        async for ev in broker._reconcile_snapshot({}, {}):
            out.append(ev)
        return out

    asyncio.run(run_reconcile())

    for coid in (e_coid, tp_coid, sl_coid):
        row = ctx.get_order(coid)
        assert row is not None
        assert (row.extras or {}).get('natural_close_at') is not None, (
            f"row {coid!r} must be stamped natural_close_at after the "
            f"race resolves (deal gone + close_event_yielded_at present)"
        )
        assert 'missing_pending_since' not in (row.extras or {}), (
            f"row {coid!r} must NOT carry missing_pending_since — the "
            f"breadcrumb path bypasses the grace tracker so no false "
            f"UnexpectedCancelError can fire"
        )
    store.close()


def __test_load_activity_cursor_clamps_below_unresolved_deferred_close__(tmp_path):
    """Across-restart cursor clamp: when a previous incarnation logged
    an ``activity_close_deferred_no_price`` event AND a later
    ``activity_processed`` event in the same batch, the rebuilt
    ``cursor.last_date_utc`` must NOT advance past the deferred row's
    ``dateUTC``. Otherwise the next poll's
    ``date_utc < cursor.last_date_utc`` guard permanently drops the
    deferred close, silently desyncing strategy and broker.
    """
    broker, store, ctx = _make_broker(tmp_path)
    deferred_date = '2026-04-21T11:00:00.000'
    later_date = '2026-04-21T11:00:05.000'
    ctx.log_event(
        'activity_close_deferred_no_price',
        exchange_order_id='deal-L',
        payload={'fingerprint': 'fp-deferred', 'dateUTC': deferred_date,
                 'deal_id': 'deal-L',
                 'type': 'POSITION', 'status': 'EXECUTED', 'source': 'USER'},
    )
    ctx.log_event(
        'activity_processed',
        exchange_order_id='deal-O',
        payload={'fingerprint': 'fp-later', 'dateUTC': later_date,
                 'deal_id': 'deal-O',
                 'type': 'POSITION', 'status': 'EXECUTED', 'source': 'DEALER'},
    )

    asyncio.run(broker._load_activity_cursor_from_events())
    cursor = broker._activity_cursor

    assert (cursor.last_date_utc is None
            or cursor.last_date_utc < deferred_date), (
        f"rebuilt cursor.last_date_utc must stay below the unresolved "
        f"deferred dateUTC ({deferred_date!r}); got "
        f"{cursor.last_date_utc!r}. Without the cross-restart clamp, "
        "the next poll would skip the deferred close via the date_utc "
        "< cursor guard."
    )
    # The later activity's fingerprint must STILL be in the seen set
    # (so the next poll doesn't re-emit it via the dedup path).
    assert 'fp-later' in cursor.seen_fingerprints
    store.close()


def __test_load_activity_cursor_advances_past_resolved_deferred_close__(tmp_path):
    """Inverse: if a deferred close has been resolved (the same
    ``(dateUTC, deal_id)`` later appears in an ``activity_processed``
    event — Capital re-emitted the row with a populated ``level`` on
    a subsequent poll), the rebuilt cursor must be free to advance to
    later activities. Otherwise the cursor is permanently stuck at
    the resolved close's timestamp.
    """
    broker, store, ctx = _make_broker(tmp_path)
    deferred_date = '2026-04-21T11:00:00.000'
    later_date = '2026-04-21T11:00:05.000'
    ctx.log_event(
        'activity_close_deferred_no_price',
        exchange_order_id='deal-L',
        payload={'fingerprint': 'fp-deferred', 'dateUTC': deferred_date,
                 'deal_id': 'deal-L',
                 'type': 'POSITION', 'status': 'EXECUTED', 'source': 'USER'},
    )
    # Resolution: same (dateUTC, deal_id) reappears in
    # activity_processed (the activity was re-emitted by Capital with
    # a populated level on a later poll, producing a fresh fingerprint
    # but preserving the dealId and dateUTC).
    ctx.log_event(
        'activity_processed',
        exchange_order_id='deal-L',
        payload={'fingerprint': 'fp-resolved', 'dateUTC': deferred_date,
                 'deal_id': 'deal-L',
                 'type': 'POSITION', 'status': 'EXECUTED', 'source': 'USER'},
    )
    ctx.log_event(
        'activity_processed',
        exchange_order_id='deal-O',
        payload={'fingerprint': 'fp-later', 'dateUTC': later_date,
                 'deal_id': 'deal-O',
                 'type': 'POSITION', 'status': 'EXECUTED', 'source': 'DEALER'},
    )

    asyncio.run(broker._load_activity_cursor_from_events())
    cursor = broker._activity_cursor

    assert cursor.last_date_utc == later_date, (
        f"resolved deferred close must NOT clamp the rebuilt cursor; "
        f"expected last_date_utc={later_date!r}, got "
        f"{cursor.last_date_utc!r}"
    )
    store.close()


def __test_load_activity_cursor_clamps_when_sibling_shares_dateutc__(tmp_path):
    """Same-``dateUTC`` collision: when an unresolved deferred close
    for ``deal-L`` shares its ``dateUTC`` with a *processed* activity
    for an unrelated ``deal-O``, the resolution probe must NOT treat
    the deferred row as resolved.

    Matching on ``dateUTC`` alone would let the cursor advance to the
    sibling's timestamp, and the next poll's
    ``date_utc < cursor.last_date_utc`` guard would silently drop the
    deferred close — desyncing the strategy from the broker. The
    ``(dateUTC, deal_id)`` identity probe keeps the deferred row
    unresolved until *its own* dealId reappears in ``activity_processed``.
    """
    broker, store, ctx = _make_broker(tmp_path)
    shared_date = '2026-04-21T11:00:00.000'
    later_date = '2026-04-21T11:00:05.000'
    # Deferred close on deal-L (no level yet — resolution still pending).
    ctx.log_event(
        'activity_close_deferred_no_price',
        exchange_order_id='deal-L',
        payload={'fingerprint': 'fp-deferred', 'dateUTC': shared_date,
                 'deal_id': 'deal-L',
                 'type': 'POSITION', 'status': 'EXECUTED', 'source': 'USER'},
    )
    # Sibling: a different deal's activity at the SAME dateUTC.
    ctx.log_event(
        'activity_processed',
        exchange_order_id='deal-O',
        payload={'fingerprint': 'fp-sibling', 'dateUTC': shared_date,
                 'deal_id': 'deal-O',
                 'type': 'POSITION', 'status': 'EXECUTED', 'source': 'DEALER'},
    )
    # And a strictly later, unrelated processed row.
    ctx.log_event(
        'activity_processed',
        exchange_order_id='deal-O',
        payload={'fingerprint': 'fp-later', 'dateUTC': later_date,
                 'deal_id': 'deal-O',
                 'type': 'POSITION', 'status': 'EXECUTED', 'source': 'DEALER'},
    )

    asyncio.run(broker._load_activity_cursor_from_events())
    cursor = broker._activity_cursor

    assert (cursor.last_date_utc is None
            or cursor.last_date_utc < shared_date), (
        f"sibling deal's processed activity at the same dateUTC must "
        f"not mask the unresolved deferred close on deal-L; expected "
        f"cursor below {shared_date!r}, got {cursor.last_date_utc!r}. "
        "Without the (dateUTC, deal_id) identity probe, the next poll "
        "would skip deal-L's deferred close permanently."
    )
    # Sibling's fingerprint must still be tracked for dedup on next poll.
    assert 'fp-sibling' in cursor.seen_fingerprints
    assert 'fp-later' in cursor.seen_fingerprints
    store.close()


def __test_process_activity_defer_does_not_advance_cursor_past_later_rows__(tmp_path):
    """A deferred close MUST clamp the cursor watermark for the rest of
    the batch. Activities are sorted by ``dateUTC`` ascending; if a
    later (newer) row in the SAME batch advances ``cursor.last_date_utc``
    past the deferred row's timestamp, the next poll's
    ``date_utc < cursor.last_date_utc`` guard permanently drops the
    deferred row — silently desyncing the strategy from the broker.
    """
    broker, store, ctx = _make_broker(tmp_path)
    # Setup A: a stamped entry that will receive a zero-priced manual close.
    e_coid, _tp_coid, _sl_coid = _seed_bracket_setup(ctx)
    entry_row = ctx.get_order(e_coid)
    assert entry_row is not None
    base_extras = dict(entry_row.extras or {})
    base_extras['entry_filled_at'] = 1700000000.0
    ctx.upsert_order(e_coid, filled_qty=1.0, extras=base_extras)
    # Setup B: a SECOND, independent entry row whose first POSITION/EXECUTED
    # arrives in the same activity batch — this one is a normal entry fill
    # (level present), processed after the deferred close in the sorted batch.
    ctx.upsert_order(
        'coid-other', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Other',
        exchange_order_id='deal-O', extras={'kind': 'position'},
    )
    ctx.add_ref('coid-other', 'deal_id', 'deal-O')

    deferred = {
        'dateUTC': '2026-04-21T11:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'USER',
        'epic': 'EURUSD', 'direction': 'BUY',
        'size': 1.0, 'level': 0.0,  # → zero-priced close → defer
    }
    later_normal = {
        'dateUTC': '2026-04-21T11:00:05.000', 'dealId': 'deal-O',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'DEALER',
        'epic': 'EURUSD', 'direction': 'BUY',
        'size': 1.0, 'level': 1.17500,  # normal entry fill
    }

    async def drain():
        out = []
        async for ev in broker._process_activity([deferred, later_normal]):
            out.append(ev)
        return out

    events = asyncio.run(drain())
    assert len(events) == 1, (
        "later normal activity must still be yielded — defer of an "
        "earlier row in the batch must not block other rows from emitting"
    )
    assert events[0].order.client_order_id == 'coid-other'

    cursor = broker._activity_cursor
    assert (cursor.last_date_utc is None
            or cursor.last_date_utc < deferred['dateUTC']), (
        f"cursor.last_date_utc must NOT advance past the deferred row's "
        f"timestamp ({deferred['dateUTC']!r}); got {cursor.last_date_utc!r}. "
        "If it did, the next poll would skip the deferred row via the "
        "date_utc < cursor guard, permanently dropping the close event."
    )
    assert _activity_fingerprint(deferred) not in cursor.seen_fingerprints
    assert _activity_fingerprint(later_normal) in cursor.seen_fingerprints, (
        "the later normal activity is fully processed — its fingerprint "
        "MUST be added to seen_fingerprints to avoid re-emitting on the "
        "next poll (the cursor clamp alone would not dedup it)"
    )
    store.close()


def __test_process_activity_close_out_source_routes_as_close__(tmp_path):
    """Unambiguous close sources (CLOSE_OUT / CLOSE / MARGIN /
    STOP_OUT) classify as CLOSE regardless of whether the entry row
    has been stamped with ``entry_filled_at`` — these can never be
    entry fills.
    """
    broker, store, ctx = _make_broker(tmp_path)
    _seed_bracket_setup(ctx)
    activity = {
        'dateUTC': '2026-04-21T11:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'CLOSE_OUT',
        'epic': 'EURUSD', 'direction': 'BUY',
        'size': 1.0, 'level': 1.17820,
    }

    async def drain():
        out = []
        async for ev in broker._process_activity([activity]):
            out.append(ev)
        return out

    events = asyncio.run(drain())
    assert len(events) == 1
    assert events[0].leg_type == LegType.CLOSE
    assert events[0].order.side == 'sell'
    store.close()


# --- opening-hours parser ---------------------------------------------------

from datetime import time as _time  # noqa: E402

from pynecore_capitalcom.plugin import _parse_opening_hours_segment  # noqa: E402


def __test_parse_opening_hours_full_day__():
    """``00:00 - 00:00`` denotes a 24h session (both sides midnight)."""
    assert _parse_opening_hours_segment("00:00 - 00:00") == (_time(0, 0), _time(0, 0))


def __test_parse_opening_hours_open_at_midnight__():
    """``00:00 - HH:MM`` denotes a session that closes at HH:MM."""
    assert _parse_opening_hours_segment("00:00 - 17:00") == (_time(0, 0), _time(17, 0))


def __test_parse_opening_hours_close_at_midnight__():
    """``HH:MM - 00:00`` denotes a session that opens at HH:MM."""
    assert _parse_opening_hours_segment("22:00 - 00:00") == (_time(22, 0), _time(0, 0))


def __test_parse_opening_hours_daytime_only__():
    """A bounded daytime session like NYSE 09:30-16:00 must round-trip cleanly.

    The previous parser silently dropped these because they did not start or
    end with a dash after the ``00:00`` strip; the new helper handles them
    natively.
    """
    assert _parse_opening_hours_segment("09:30 - 16:00") == (_time(9, 30), _time(16, 0))


def __test_parse_opening_hours_no_spaces_around_dash__():
    """Whitespace around the dash is optional — robustness against payload variants."""
    assert _parse_opening_hours_segment("09:30-16:00") == (_time(9, 30), _time(16, 0))
    assert _parse_opening_hours_segment("00:00-22:00") == (_time(0, 0), _time(22, 0))


def __test_parse_opening_hours_extra_whitespace__():
    """Leading/trailing/internal whitespace is tolerated."""
    assert _parse_opening_hours_segment("  09:30  -  16:00  ") == (_time(9, 30), _time(16, 0))


def __test_parse_opening_hours_empty_returns_none__():
    """Empty / whitespace-only payload (e.g. closed-day marker) → ``None``."""
    assert _parse_opening_hours_segment("") is None
    assert _parse_opening_hours_segment("   ") is None


def __test_parse_opening_hours_multi_dash_returns_none__():
    """A segment with more than one dash is rejected rather than silently misparsed."""
    assert _parse_opening_hours_segment("08:00 - 12:00 - 16:00") is None


def __test_parse_opening_hours_invalid_time_returns_none__():
    """Non-time tokens on either side return ``None`` instead of raising."""
    assert _parse_opening_hours_segment("not-a-time") is None
    assert _parse_opening_hours_segment("09:99 - 16:00") is None
    assert _parse_opening_hours_segment("09:30 - banana") is None


def __test_parse_opening_hours_bare_dash_returns_none__():
    """A bare ``"-"`` (or whitespace around a bare dash, or a dash with
    only one populated side) is a closed-day marker, NOT a 24h session.
    Defaulting an empty side to ``time(0, 0)`` would round-trip to
    ``(00:00, 00:00)``, which the caller treats as a 24h shape — silently
    flipping the market open all day.
    """
    assert _parse_opening_hours_segment("-") is None
    assert _parse_opening_hours_segment(" - ") is None
    assert _parse_opening_hours_segment("  -  ") is None
    assert _parse_opening_hours_segment("09:00 -") is None
    assert _parse_opening_hours_segment("- 17:00") is None


def __test_update_symbol_info_dash_closed_marker_does_not_open_24h__(monkeypatch):
    """Integration: ``openingHours`` weekday entries that contain only a
    bare ``"-"`` (Capital.com closed-day marker on some symbols) must
    NOT promote the day to a 24h interval. ``_check_session`` would
    otherwise treat the symbol as open all day, executing trades on
    days the venue is closed.
    """
    from pynecore.types.weekdays import Weekdays

    broker = _FakeBroker(
        symbol="LSE_TEST",
        timeframe="60",
        config=_make_config(),
    )
    broker.timezone = "UTC"

    def fake_market_details():
        return {
            'instrument': {
                'name': "LSE Test",
                'epic': "LSE_TEST",
                'currency': "GBP",
                'symbol': "LSE_TEST",
                'type': "SHARES",
                'lotSize': 1.0,
                'openingHours': {
                    'zone': "UTC",
                    'mon': ["09:00 - 17:00"],
                    'tue': ["09:00 - 17:00"],
                    'wed': ["09:00 - 17:00"],
                    'thu': ["09:00 - 17:00"],
                    'fri': ["09:00 - 17:00"],
                    'sat': ["-"],  # closed-day markers
                    'sun': ["-"],
                },
            },
            'dealingRules': {'minStepDistance': {'value': 0.01}},
        }

    def fake_historical_prices():
        return {'prices': [{'closePrice': {'bid': 100.0, 'ask': 100.02}}]}

    monkeypatch.setattr(broker, 'get_single_market_details', fake_market_details)
    monkeypatch.setattr(broker, 'get_historical_prices', fake_historical_prices)

    info = broker.update_symbol_info()
    sat = Weekdays.Sat.value
    sun = Weekdays.Sun.value
    weekend_intervals = [iv for iv in info.opening_hours
                         if iv.day in (sat, sun)]
    assert not weekend_intervals, (
        f"closed-day '-' markers must not produce intervals; got "
        f"{weekend_intervals!r}"
    )


def __test_update_symbol_info_emits_daytime_session_markers__(monkeypatch):
    """Integration: a daytime-only session (e.g. NYSE 09:30-16:00) must emit
    BOTH a session_start AND a session_end entry — the previous parser dropped
    these segments silently. 24h-bounded segments must continue to behave as
    before (no markers, single full-day interval).
    """
    broker = _FakeBroker(
        symbol="AAPL",
        timeframe="60",
        config=_make_config(),
    )

    def fake_market_details():
        return {
            'instrument': {
                'name': "Apple Inc",
                'epic': "AAPL",
                'currency': "USD",
                'symbol': "AAPL",
                'type': "SHARES",
                'lotSize': 1.0,
                'openingHours': {
                    'zone': "US/Eastern",
                    'mon': ["09:30 - 16:00"],
                    'tue': ["09:30 - 16:00"],
                    'wed': ["09:30 - 16:00"],
                    'thu': ["09:30 - 16:00"],
                    'fri': ["09:30 - 16:00"],
                    'sat': [],
                    'sun': [],
                },
            },
            'dealingRules': {
                'minStepDistance': {'value': 0.01},
            },
        }

    def fake_historical_prices():
        return {'prices': [{'closePrice': {'bid': 100.0, 'ask': 100.05}}]}

    monkeypatch.setattr(broker, 'get_single_market_details', fake_market_details)
    monkeypatch.setattr(broker, 'get_historical_prices', fake_historical_prices)

    info = broker.update_symbol_info()

    # Five trading days × (one start + one end) = 10 markers total.
    assert len(info.session_starts) == 5
    assert len(info.session_ends) == 5
    assert all(s.time == _time(9, 30) for s in info.session_starts)
    assert all(e.time == _time(16, 0) for e in info.session_ends)
    # Five intervals — one per weekday — none for sat/sun.
    assert len(info.opening_hours) == 5


def __test_update_symbol_info_marker_uses_source_midnight__(monkeypatch):
    """Markers are decided by the SOURCE-side midnight, not the converted
    local time. A non-midnight source value can convert to local midnight
    by tz coincidence (e.g. UTC+5 ``05:00`` → UTC ``00:00``); the session
    marker must still fire because midnight in the source zone is the only
    real day-boundary sentinel.
    """
    broker = _FakeBroker(
        symbol="X",
        timeframe="60",
        config=_make_config(),
    )
    # Override the conversion target so the test is independent of the
    # plugin's default ``US/Eastern`` constant.
    broker.timezone = "UTC"

    def fake_market_details():
        return {
            'instrument': {
                'name': "Tz Edge",
                'epic': "X",
                'currency': "USD",
                'symbol': "X",
                'type': "CURRENCIES",
                'lotSize': 1.0,
                'openingHours': {
                    # Fixed-offset zone (no DST) so the assertion is
                    # deterministic regardless of when the test runs.
                    # ``Etc/GMT-5`` is UTC+5 by POSIX sign convention.
                    'zone': "Etc/GMT-5",
                    'mon': ["05:00 - 13:00"],
                    'tue': [], 'wed': [], 'thu': [], 'fri': [],
                    'sat': [], 'sun': [],
                },
            },
            'dealingRules': {
                'minStepDistance': {'value': 0.0001},
            },
        }

    def fake_historical_prices():
        return {'prices': [{'closePrice': {'bid': 1.0, 'ask': 1.0001}}]}

    monkeypatch.setattr(broker, 'get_single_market_details', fake_market_details)
    monkeypatch.setattr(broker, 'get_historical_prices', fake_historical_prices)

    info = broker.update_symbol_info()

    # 05:00 in UTC+5 → 00:00 UTC. Marker must STILL be emitted because the
    # source side was non-midnight (real session start, just happened to
    # land on local midnight after conversion).
    assert len(info.session_starts) == 1
    assert info.session_starts[0].time == _time(0, 0)
    # 13:00 in UTC+5 → 08:00 UTC. Standard non-midnight conversion.
    assert len(info.session_ends) == 1
    assert info.session_ends[0].time == _time(8, 0)


def __test_update_symbol_info_24h_emits_no_session_markers__(monkeypatch):
    """A 24h instrument (``00:00 - 00:00`` every day) emits the daily interval
    but no session_start / session_end markers — midnight is the day boundary,
    not a real transition.
    """
    broker = _FakeBroker(
        symbol="EURUSD",
        timeframe="60",
        config=_make_config(),
    )

    def fake_market_details():
        return {
            'instrument': {
                'name': "EUR/USD",
                'epic': "EURUSD",
                'currency': "USD",
                'symbol': "EUR/USD",
                'type': "CURRENCIES",
                'lotSize': 1.0,
                'openingHours': {
                    'zone': "US/Eastern",
                    'mon': ["00:00 - 00:00"],
                    'tue': ["00:00 - 00:00"],
                    'wed': ["00:00 - 00:00"],
                    'thu': ["00:00 - 00:00"],
                    'fri': ["00:00 - 00:00"],
                    'sat': [],
                    'sun': ["17:00 - 00:00"],  # mixed: Sunday opens at 17:00 ET
                },
            },
            'dealingRules': {
                'minStepDistance': {'value': 0.0001},
            },
        }

    def fake_historical_prices():
        return {'prices': [{'closePrice': {'bid': 1.10, 'ask': 1.1001}}]}

    monkeypatch.setattr(broker, 'get_single_market_details', fake_market_details)
    monkeypatch.setattr(broker, 'get_historical_prices', fake_historical_prices)

    info = broker.update_symbol_info()

    # Only the Sunday 17:00 entry is a real session start; weekday 24h sessions
    # are continuous, no markers emitted.
    assert len(info.session_starts) == 1
    assert info.session_starts[0].time == _time(17, 0)
    assert info.session_ends == []
    # Six intervals total: five weekday 24h + one Sunday 17:00→midnight.
    assert len(info.opening_hours) == 6
    # 24h sessions must be represented as full-day intervals (start 00:00,
    # end 23:59:59) — NOT zero-length ``start == end == midnight``, which
    # ``pynecore.lib.session._check_session`` would treat as closed all day.
    weekday_intervals = [iv for iv in info.opening_hours
                         if iv.start == _time(0, 0) and iv.end == _time(23, 59, 59)]
    assert len(weekday_intervals) == 5, (
        "24h sessions must use full-day shape, not start==end==midnight"
    )


def __test_update_symbol_info_tz_shift_splits_local_midnight__(monkeypatch):
    """A source session whose local-tz conversion crosses local midnight
    must be split into two daily intervals — one ending at 23:59:59 of
    the start local-day, one starting at 00:00 of the end local-day —
    and the day field must use the LOCAL weekday, not the source one.

    ``pynecore.lib.session._check_session`` matches candles by exact
    weekday, then extends overnight. Recording the source weekday with a
    converted local time would fire the session on the wrong local day
    entirely; not splitting would silently miss every post-midnight
    candle on the next local day.
    """
    from pynecore.types.weekdays import Weekdays

    broker = _FakeBroker(
        symbol="USDJPY",
        timeframe="60",
        config=_make_config(),
    )
    # Fixed-offset zones eliminate DST ambiguity from the assertion —
    # ``Etc/GMT-9`` is UTC+9 and ``Etc/GMT+5`` is UTC-5 by POSIX sign.
    broker.timezone = "Etc/GMT+5"

    def fake_market_details():
        return {
            'instrument': {
                'name': "USD/JPY",
                'epic': "USDJPY",
                'currency': "JPY",
                'symbol': "USD/JPY",
                'type': "CURRENCIES",
                'lotSize': 1.0,
                'openingHours': {
                    # Tokyo Mon 09:00-17:00 (UTC+9) → Sun 19:00 - Mon 03:00
                    # at UTC-5, crossing local midnight.
                    'zone': "Etc/GMT-9",
                    'mon': ["09:00 - 17:00"],
                    'tue': [], 'wed': [], 'thu': [], 'fri': [],
                    'sat': [], 'sun': [],
                },
            },
            'dealingRules': {
                'minStepDistance': {'value': 0.01},
            },
        }

    def fake_historical_prices():
        return {'prices': [{'closePrice': {'bid': 150.0, 'ask': 150.02}}]}

    monkeypatch.setattr(broker, 'get_single_market_details', fake_market_details)
    monkeypatch.setattr(broker, 'get_historical_prices', fake_historical_prices)

    info = broker.update_symbol_info()

    sun = Weekdays.Sun.value
    mon = Weekdays.Mon.value

    intervals_by_day: dict[int, list] = {}
    for iv in info.opening_hours:
        intervals_by_day.setdefault(iv.day, []).append(iv)
    assert set(intervals_by_day.keys()) == {sun, mon}, (
        f"local interval must cover Sun + Mon after TZ split, got "
        f"{sorted(intervals_by_day.keys())}"
    )
    sun_iv = intervals_by_day[sun][0]
    mon_iv = intervals_by_day[mon][0]
    assert sun_iv.start == _time(19, 0) and sun_iv.end == _time(23, 59, 59)
    assert mon_iv.start == _time(0, 0) and mon_iv.end == _time(3, 0)

    # Markers track the local-tz boundary, not the source weekday.
    assert len(info.session_starts) == 1
    assert info.session_starts[0].day == sun
    assert info.session_starts[0].time == _time(19, 0)
    assert len(info.session_ends) == 1
    assert info.session_ends[0].day == mon
    assert info.session_ends[0].time == _time(3, 0)


def __test_modify_exit_remove_native_trail_to_tp_only_clears_trailingStop__(tmp_path):
    """Amending ``TP+native-trail`` → ``TP-only`` must explicitly send
    ``trailingStop: False`` in the PUT body. Capital.com preserves
    unspecified fields, so without the clear the broker keeps the
    trailing stop live while the local store records ``trailing_stop=False``
    on the entry / no SL leg row — leaving broker / engine risk state
    divergent and misclassifying the eventual stop fill.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'modify-tp-only'},
        ('confirms/modify-tp-only', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    # Entry row with native trailing currently active (the broker
    # accepted ``trailingStop=True`` on a prior amend and we mirrored
    # it locally).
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        trailing_stop=True, trailing_distance=0.00200,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000, trail_offset=0.00200,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    sl_coid = old_env.client_order_id('s')
    # Existing native-trailing SL leg row seeded.
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        trailing_stop=True, trailing_distance=0.00200,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    # Amend to keep the TP only, dropping the trailing stop entirely
    # (no fixed SL replacement). Pine allows this — the position runs
    # with TP and no stop.
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(old_env, new_env))
    put_calls = [c for c in broker._calls
                 if c[0] == 'positions/deal-L' and c[1] == 'put']
    assert len(put_calls) == 1, (
        f"expected exactly one PUT positions/deal-L call for the "
        f"trail-removal amend, got {len(put_calls)}: {put_calls!r}"
    )
    body = put_calls[0][2] or {}
    assert body.get('trailingStop') is False, (
        f"removing native trailing without replacing it (TP+trail → "
        f"TP-only) must include trailingStop=False to clear Capital.com's "
        f"prior trailing flag; otherwise the broker keeps trailing while "
        f"local state shows no SL leg, leaving live risk active that the "
        f"engine no longer tracks. body={body!r}"
    )
    store.close()


def __test_reconcile_snapshot_mixed_bracket_retires_confirmed_sibling__(tmp_path):
    """Mixed bracket disposition (one leg attached, sibling rejected) must
    physically retire the leg that ``_record_bracket_resolution`` already
    promoted to ``'confirmed'`` in this batch — not just record a parent
    ``'rejected'`` resolution.

    Without sibling retirement: the engine's modify-rejected /
    entry-rejected branch (driven by the parent ``'rejected'`` resolution)
    drops the parent dispatch's mapping and re-dispatches the exit with
    fresh TP/SL COIDs. The freshly re-dispatched envelope inserts a NEW
    set of bracket leg rows alongside the still-``confirmed`` sibling
    from the prior batch — duplicate bracket rows under the same
    ``parent_deal_id``. Later fill-resolution and cancellation lookups
    that walk ``parent_deal_id`` could then read the stale row's level
    instead of the freshly attached one, mispricing closes and missing
    cancellations.

    The aggregate flush must therefore stamp the confirmed sibling as
    ``state='rejected'`` + ``close_order(...)`` and emit a
    ``bracket_sibling_retired_on_mixed_rejection`` audit event before
    handing the parent ``'rejected'`` resolution to the engine.
    """
    broker, store, ctx = _make_broker(tmp_path)
    e_coid, tp_coid, sl_coid = _seed_bracket_setup(ctx)
    ctx.set_order_state(tp_coid, 'disposition_unknown')
    ctx.set_order_state(sl_coid, 'disposition_unknown')
    ctx.record_park(e_coid, 'Bracket\x00Long')

    # TP profitLevel matches seeded tp_price (1.17699) → leg attached.
    # SL stopLevel diverges from seeded sl_price (1.17636) → leg rejected.
    positions_by_deal = {
        'deal-L': {
            'position': {
                'dealId': 'deal-L',
                'profitLevel': 1.17699,
                'stopLevel': 1.50000,
                'size': 1.0,
                'level': 1.17667,
            },
        },
    }

    async def drain():
        async for _ev in broker._reconcile_snapshot(positions_by_deal, {}):
            pass

    asyncio.run(drain())

    tp_row = ctx.get_order(tp_coid)
    sl_row = ctx.get_order(sl_coid)
    # SL — naturally rejected by the level mismatch.
    assert sl_row is not None and sl_row.state == 'rejected'
    assert sl_row.closed_ts_ms is not None
    # TP — promoted to 'confirmed' early in the batch (level matches),
    # then retired by the aggregate flush because the parent went mixed
    # → 'rejected'. Without the retirement the row would survive as a
    # phantom alongside the engine's re-dispatch.
    assert tp_row is not None
    assert tp_row.state == 'rejected', (
        f"mixed bracket rejection: confirmed TP sibling must be "
        f"retired by the aggregate flush so the engine's re-dispatch "
        f"does not seed duplicate bracket leg rows under "
        f"parent_deal_id={tp_row.extras and tp_row.extras.get('parent_deal_id')!r}. "
        f"Got tp_row.state={tp_row.state!r}"
    )
    assert tp_row.closed_ts_ms is not None, (
        "retired sibling must be physically closed (closed_ts_ms set) "
        "so iter_live_orders skips it on subsequent polls"
    )
    # Audit trail — the retirement must be loggable so post-hoc
    # debugging can distinguish a sibling-retire from a leg-rejection.
    audit_events = list(ctx.iter_events_by_kind_since(
        'bracket_sibling_retired_on_mixed_rejection', since_ts_ms=0,
    ))
    retire_coids = {ev.get('sibling_coid') for ev in audit_events}
    assert tp_coid in retire_coids, (
        f"expected bracket_sibling_retired_on_mixed_rejection audit "
        f"event for the retired TP coid; got events={list(retire_coids)!r}"
    )
    store.close()


def __test_reconcile_snapshot_pending_trail_sibling_retired_on_rejection__(tmp_path):
    """Pending-trail SL sibling must be retired when the parent bracket
    resolves ``rejected``.

    Pine ``trail_offset`` + ``trail_price`` defers native trailing until
    the activation threshold crosses, so :meth:`attach_bracket` /
    :meth:`modify_exit` carry only the TP in the PUT body and seed the
    SL row locally as ``state='confirmed'`` with
    ``extras['trail_state'] = 'pending'``. That row never enters
    :meth:`_record_bracket_resolution` (its leg is purely local) and
    therefore never enters ``bracket_attached_coids``.

    If the parent's TP attach later resolves ``rejected`` (e.g. PUT
    timed out, next snapshot shows the broker did not attach the TP),
    the engine drops the parent dispatch's mapping and re-dispatches
    with fresh COIDs. Without retirement, the old pending-trail SL row
    keeps the same ``parent_deal_id`` and ``trail_state='pending'`` —
    when the market crosses ``trail_activation_price``,
    :meth:`_trailing_activation_monitor` PUTs ``trailingStop=true``
    against the stale row, racing the freshly re-dispatched leg.

    The aggregate flush therefore must walk live pending-trail SL rows
    by ``parent_coid`` and retire any whose parent goes ``rejected`` in
    the same poll, mirroring the confirmed-sibling path.
    """
    broker, store, ctx = _make_broker(tmp_path)
    e_coid, tp_coid, sl_coid = _seed_bracket_setup(ctx)
    # TP is in flight (PUT ambiguous → marked disposition_unknown).
    ctx.set_order_state(tp_coid, 'disposition_unknown')
    # SL is the pending-trail leg: confirmed locally (PUT body did not
    # carry it), with the trail-state extras the activation monitor
    # reads.
    ctx.upsert_order(
        sl_coid,
        extras={
            'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
            'parent_coid': e_coid,
            'trail_state': 'pending',
            'trail_activation_price': 1.18000,
            'trail_offset': 0.00200,
        },
        trailing_distance=0.00200,
    )
    ctx.record_park(e_coid, 'Bracket\x00Long')

    # Snapshot: parent position exists but has NO TP attached
    # (profitLevel diverges from the seeded tp_price 1.17699). The
    # missing TP forces a parent rejection; the SL row is not in the
    # snapshot's bracket fields (pending-trail is purely local) so the
    # rejection drives the pending-trail retirement path, not the
    # confirmed-sibling path.
    positions_by_deal = {
        'deal-L': {
            'position': {
                'dealId': 'deal-L',
                'profitLevel': 1.50000,
                'stopLevel': None,
                'size': 1.0,
                'level': 1.17667,
            },
        },
    }

    async def drain():
        async for _ev in broker._reconcile_snapshot(positions_by_deal, {}):
            pass

    asyncio.run(drain())

    sl_row = ctx.get_order(sl_coid)
    assert sl_row is not None, "SL row must remain in the store (audit trail)"
    assert sl_row.state == 'rejected', (
        f"pending-trail SL sibling of a rejected parent must be retired "
        f"to ``rejected`` so the engine's re-dispatch does not coexist "
        f"with the stale pending row. Got sl_row.state={sl_row.state!r} "
        f"— without the fix _trailing_activation_monitor would PUT "
        f"trailingStop=true against this row when the threshold crosses, "
        f"racing the freshly re-dispatched SL leg."
    )
    assert sl_row.closed_ts_ms is not None, (
        "retired pending-trail row must be physically closed so "
        "iter_live_orders / activation monitor skip it on subsequent "
        "polls"
    )

    # Audit event documents the retirement path. ``reason`` distinguishes
    # the pending-trail cleanup from the confirmed-sibling cleanup so
    # post-hoc debugging can trace which retirement path fired.
    audit_events = list(ctx.iter_events_by_kind_since(
        'bracket_sibling_retired_on_mixed_rejection', since_ts_ms=0,
    ))
    pending_trail_audit = [
        ev for ev in audit_events
        if ev.get('reason') == 'pending_trail_parent_rejected'
        and ev.get('sibling_coid') == sl_coid
    ]
    assert pending_trail_audit, (
        f"expected bracket_sibling_retired_on_mixed_rejection audit "
        f"event with reason='pending_trail_parent_rejected' for the "
        f"retired pending-trail SL coid; got events={audit_events!r}"
    )
    store.close()


def __test_modify_exit_tp_to_pending_trail_only_clears_profitLevel__(tmp_path):
    """An amend transitioning ``TP-only`` → ``pending-trail-only``
    must send ``profitLevel: None`` so Capital.com clears the broker-
    side TP.

    Capital.com preserves any field NOT mentioned in the PUT body, so
    leaving ``profitLevel`` out (the natural shape since the new
    intent carries no TP) keeps the OLD ``profitLevel`` live on the
    position. ``set_risk`` and the local TP leg state would record
    "no TP" while the broker still has the prior TP attached — the
    position can later close at a stale level the engine believes was
    removed, and the engine will not re-amend (Pine intent matches
    the local active intent).

    Additionally, the previously ``confirmed`` TP leg row must be
    retired (``set_order_state('rejected') + close_order(...)``). A
    ``confirmed`` leg row carries no ``exchange_order_id``, so
    :meth:`_reconcile_snapshot` cannot retire it via deal-id matching
    and :meth:`_resolve_bracket_leg_disposition` only revisits
    ``disposition_unknown`` rows. Without the retire BrokerStore
    advertises a phantom TP leg that no longer exists on the broker.
    Mirror of the SL retirement for ``trail_offset`` removal.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'modify-pending-trail'},
        ('confirms/modify-pending-trail', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        tp_level=1.18000,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    tp_coid = old_env.client_order_id('t')
    ctx.upsert_order(
        tp_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        tp_level=1.18000,
        extras={'leg_kind': 'tp', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    pre_tp = ctx.get_order(tp_coid)
    assert pre_tp is not None and pre_tp.closed_ts_ms is None, (
        "precondition: TP leg row must be live before the amend"
    )

    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0,
            trail_price=1.17800, trail_offset=0.00200,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(old_env, new_env))

    put_calls = [
        c for c in broker._calls
        if c[0] == 'positions/deal-L' and c[1] == 'put'
    ]
    assert len(put_calls) == 1, (
        f"expected exactly one PUT to the position so the broker "
        f"clears profitLevel; got calls={broker._calls!r}"
    )
    body = put_calls[0][2]
    assert body.get('profitLevel') is None and 'profitLevel' in body, (
        f"TP-removal amend must send ``profitLevel: None`` to clear "
        f"the broker-side TP — Capital.com preserves omitted fields, "
        f"so without the explicit null the old profitLevel stays "
        f"attached. Got body={body!r}"
    )

    tp_after = ctx.get_order(tp_coid)
    assert tp_after is not None, (
        "leg row must remain in the store (audit trail) — only its "
        "lifecycle fields are retired"
    )
    assert tp_after.closed_ts_ms is not None, (
        "removing the TP must close the prior TP leg row; otherwise "
        "BrokerStore reports a phantom TP leg the broker no longer "
        "enforces, misleading later _find_bracket_leg_row lookups, "
        "restart recovery, and natural-close bookkeeping that walks "
        "parent_deal_id"
    )
    assert tp_after.state == 'rejected', (
        f"retired TP leg state should be 'rejected' (matches the "
        f"existing _rollback_bracket_legs pattern); got "
        f"{tp_after.state!r}"
    )
    store.close()


def __test_modify_exit_trail_removal_retires_phantom_sl_leg_row__(tmp_path):
    """The trail-removal amend (``TP+native-trail`` → ``TP-only``)
    sends ``trailingStop: false`` to clear the broker-side stop, but
    the previously ``confirmed`` SL leg row would otherwise linger
    live in BrokerStore.

    ``confirmed`` bracket legs carry no ``exchange_order_id``, so
    :meth:`_reconcile_snapshot` cannot retire them via deal-id
    matching, and :meth:`_resolve_bracket_leg_disposition` only
    revisits ``disposition_unknown`` rows. Without an explicit
    teardown the leg sits in the store forever — a phantom protective
    leg the broker no longer enforces, available to mislead later
    leg lookups (``_find_bracket_leg_row``, restart recovery, and any
    natural-close bookkeeping that walks ``parent_deal_id``).
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'modify-tp-only'},
        ('confirms/modify-tp-only', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        trailing_stop=True, trailing_distance=0.00200,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000, trail_offset=0.00200,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    sl_coid = old_env.client_order_id('s')
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        trailing_stop=True, trailing_distance=0.00200,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    pre_sl = ctx.get_order(sl_coid)
    assert pre_sl is not None and pre_sl.closed_ts_ms is None, (
        "precondition: SL leg row must be live before the amend"
    )

    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(old_env, new_env))

    sl_after = ctx.get_order(sl_coid)
    assert sl_after is not None, (
        "leg row must remain in the store (audit trail) — only its "
        "lifecycle fields are retired"
    )
    assert sl_after.closed_ts_ms is not None, (
        "removing native trailing without replacement (TP+trail → "
        "TP-only) must close the prior SL leg row; otherwise "
        "BrokerStore reports a phantom protective leg the broker "
        "stopped enforcing when the PUT cleared trailingStop"
    )
    assert sl_after.state == 'rejected', (
        f"retired leg state should be 'rejected' (matches the existing "
        f"_rollback_bracket_legs pattern); got {sl_after.state!r}"
    )
    store.close()


def __test_modify_exit_tp_removal_clears_parent_tp_level__(tmp_path):
    """After a ``TP+SL`` → ``SL-only`` amend (TP removal), the parent
    entry row's durable ``tp_level`` MUST be ``None``.

    ``RunContext.set_risk`` is documented (storage.py docstring) to
    treat ``None`` as "leave unchanged" — passing ``tp=None`` is a
    no-op, so the parent entry row keeps the OLD ``tp_level`` even
    after the broker-side ``profitLevel: None`` PUT clears it. The
    leg row gets retired (separate fix), but BrokerStore's durable
    risk snapshot on the entry row diverges from Capital.com's
    actual state — misleading debug / analytics readers and any
    future diff/recovery code that walks the entry row's risk
    columns.

    Fix is an explicit ``upsert_order(coid, tp_level=None)`` after
    ``set_risk``, which writes SQL ``NULL`` (the ``None``-filter that
    blocks the equivalent ``set_risk(tp=None)`` is the only reason a
    direct call is needed).
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'modify-sl-only'},
        ('confirms/modify-sl-only', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        tp_level=1.18000, sl_level=1.17000,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0,
            tp_price=1.18000, sl_price=1.17000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    tp_coid = old_env.client_order_id('t')
    sl_coid = old_env.client_order_id('s')
    ctx.upsert_order(
        tp_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        tp_level=1.18000,
        extras={'leg_kind': 'tp', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        sl_level=1.17000,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )

    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, sl_price=1.17000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(old_env, new_env))

    parent_after = ctx.get_order('coid-entry')
    assert parent_after is not None
    assert parent_after.tp_level is None, (
        f"after a TP-removal amend the parent entry row's ``tp_level`` "
        f"must be NULL — set_risk treats ``tp=None`` as no-op, so an "
        f"explicit upsert is needed to clear the stale value left over "
        f"from the prior bracket; got {parent_after.tp_level!r}"
    )
    # Untouched fields (sl_level still set) survive the amend.
    assert parent_after.sl_level == pytest.approx(1.17000), (
        f"sl_level was not part of the removal — must be preserved; "
        f"got {parent_after.sl_level!r}"
    )
    store.close()


def __test_modify_exit_trail_removal_clears_parent_trailing_distance__(tmp_path):
    """After a ``TP+native-trail`` → ``TP-only`` amend (trail removal),
    the parent entry row's durable ``trailing_distance`` MUST be
    ``None``.

    Same root cause as the TP-removal sibling test: ``set_risk`` drops
    ``trailing_distance=None`` as "leave unchanged", so the parent
    entry row keeps the OLD trailing distance even after the broker
    side cleared ``trailingStop`` and the SL leg row was retired.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'modify-tp-only'},
        ('confirms/modify-tp-only', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        tp_level=1.18000,
        trailing_stop=True, trailing_distance=0.00200,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000, trail_offset=0.00200,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    sl_coid = old_env.client_order_id('s')
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        trailing_stop=True, trailing_distance=0.00200,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )

    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(old_env, new_env))

    parent_after = ctx.get_order('coid-entry')
    assert parent_after is not None
    assert parent_after.trailing_distance is None, (
        f"after a trail-removal amend the parent entry row's "
        f"``trailing_distance`` must be NULL — set_risk treats "
        f"``trailing_distance=None`` as no-op, so an explicit upsert "
        f"is needed to clear the stale value; got "
        f"{parent_after.trailing_distance!r}"
    )
    # ``trailing_stop`` boolean: set_risk receives ``False`` here (not
    # ``None``) and writes it normally — covered by the existing
    # __test_modify_exit_native_trail_to_pending_clears_trailing_flag__
    # / __test_modify_exit_trailing_to_fixed_clears_entry_trailing_stop__
    # tests; this test focuses on the level/distance gap.
    assert parent_after.trailing_stop is False, (
        f"trailing_stop must be cleared too (already exercised by the "
        f"existing trailing-flag tests, asserted here for breadth); "
        f"got {parent_after.trailing_stop!r}"
    )
    store.close()


def __test_modify_exit_fixed_sl_removal_clears_broker_stopLevel__(tmp_path):
    """A ``TP+fixed-SL`` → ``TP-only`` amend must send
    ``stopLevel: None`` so Capital.com clears the broker-side fixed
    stop.

    Capital.com preserves any field NOT mentioned in the PUT body, so
    leaving ``stopLevel`` out (the natural shape since the new intent
    carries no SL) keeps the OLD ``stopLevel`` live on the position.
    The leg row gets retired (separate fix), but BrokerStore would
    record "no SL" while the broker still has the prior fixed stop
    attached — the position can later close at a stale level the
    engine believes was removed, and the engine will not re-amend
    (Pine intent matches the local active intent). Mirror of the
    existing TP-removal explicit clear.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'modify-tp-only'},
        ('confirms/modify-tp-only', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        sl_level=1.17400, tp_level=1.18000,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    sl_coid = old_env.client_order_id('s')
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        sl_level=1.17400,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(old_env, new_env))

    put_calls = [
        c for c in broker._calls
        if c[0] == 'positions/deal-L' and c[1] == 'put'
    ]
    assert len(put_calls) == 1, (
        f"expected exactly one PUT to the position so the broker clears "
        f"stopLevel; got calls={broker._calls!r}"
    )
    body = put_calls[0][2]
    assert body.get('stopLevel') is None and 'stopLevel' in body, (
        f"fixed-SL removal amend must send ``stopLevel: None`` to clear "
        f"the broker-side stop — Capital.com preserves omitted fields, "
        f"so without the explicit null the old stopLevel stays attached. "
        f"Got body={body!r}"
    )
    store.close()


def __test_modify_exit_fixed_sl_removal_retires_phantom_sl_leg_row__(tmp_path):
    """A ``TP+fixed-SL`` → ``TP-only`` amend must retire the prior SL
    leg row.

    The PUT body's ``stopLevel: None`` clear (separate fix) drops the
    broker-side stop, but the previously ``confirmed`` SL leg row would
    otherwise linger live: confirmed bracket legs carry no
    ``exchange_order_id``, so :meth:`_reconcile_snapshot` cannot retire
    them via deal-id matching, and
    :meth:`_resolve_bracket_leg_disposition` only revisits
    ``disposition_unknown`` rows. Without an explicit teardown the leg
    sits in BrokerStore forever — a phantom protective leg the broker
    no longer enforces, available to mislead later
    ``_find_bracket_leg_row`` lookups, restart recovery, and any
    natural-close bookkeeping that walks ``parent_deal_id``. Mirror of
    the native-trailing removal retirement.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'modify-tp-only'},
        ('confirms/modify-tp-only', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        sl_level=1.17400, tp_level=1.18000,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    sl_coid = old_env.client_order_id('s')
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        sl_level=1.17400,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    pre_sl = ctx.get_order(sl_coid)
    assert pre_sl is not None and pre_sl.closed_ts_ms is None, (
        "precondition: SL leg row must be live before the amend"
    )

    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(old_env, new_env))

    sl_after = ctx.get_order(sl_coid)
    assert sl_after is not None, (
        "leg row must remain in the store (audit trail) — only its "
        "lifecycle fields are retired"
    )
    assert sl_after.closed_ts_ms is not None, (
        "removing the fixed SL without replacement (TP+SL → TP-only) "
        "must close the prior SL leg row; otherwise BrokerStore "
        "reports a phantom protective leg the broker stopped enforcing "
        "when the PUT cleared stopLevel"
    )
    assert sl_after.state == 'rejected', (
        f"retired leg state should be 'rejected' (matches the existing "
        f"_rollback_bracket_legs pattern); got {sl_after.state!r}"
    )
    store.close()


def __test_modify_exit_pending_trail_removal_retires_phantom_sl_leg_row__(tmp_path):
    """A ``TP+pending-trail`` → ``TP-only`` amend must retire the
    prior pending-trail SL leg row.

    Pending trailing is purely local: the leg row is persisted with
    ``state='confirmed'`` and ``extras['trail_state']='pending'``,
    waiting for :meth:`_trailing_activation_monitor` to PUT
    ``trailingStop=true`` once the activation threshold is crossed.
    There is no broker-side leg, so the PUT body carries nothing on
    the stop side.

    Without the explicit retire, the SL row sits forever:
      * :meth:`_reconcile_snapshot` cannot retire it (no
        ``exchange_order_id``).
      * :meth:`_resolve_bracket_leg_disposition` only revisits
        ``disposition_unknown`` rows.
      * :meth:`_trailing_activation_monitor` would later activate the
        stale ``trail_state='pending'`` row and PUT a trailing stop
        Pine has already removed — the position would be silently
        protected with a stop the engine no longer tracks.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('positions/deal-L', 'put'): {'dealReference': 'modify-tp-only'},
        ('confirms/modify-tp-only', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        tp_level=1.18000,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000,
            trail_price=1.17800, trail_offset=0.00200,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    sl_coid = old_env.client_order_id('s')
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        trailing_distance=0.00200,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry',
                'trail_state': 'pending',
                'trail_activation_price': 1.17800,
                'trail_offset': 0.00200},
    )
    pre_sl = ctx.get_order(sl_coid)
    assert pre_sl is not None and pre_sl.closed_ts_ms is None, (
        "precondition: pending-trail SL leg row must be live"
    )

    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.18000,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(old_env, new_env))

    sl_after = ctx.get_order(sl_coid)
    assert sl_after is not None, (
        "leg row must remain in the store (audit trail) — only its "
        "lifecycle fields are retired"
    )
    assert sl_after.closed_ts_ms is not None, (
        "removing pending-trail without replacement (TP+pending-trail "
        "→ TP-only) must close the prior SL leg row; otherwise the "
        "trailing-activation monitor would later PUT a trailingStop "
        "Pine has already removed, leaving the position protected by "
        "a stop the engine no longer tracks"
    )
    assert sl_after.state == 'rejected', (
        f"retired leg state should be 'rejected'; got {sl_after.state!r}"
    )
    store.close()


def __test_modify_exit_pending_trail_only_to_empty_retires_sl_leg_row__(tmp_path):
    """A pending-trail-only exit removed entirely (no TP, no fixed SL,
    no replacement trail) must retire the prior SL leg row.

    The PUT body in this transition is empty: the prior pending-trail
    leg was purely local (the activation monitor had not yet PUT
    ``trailingStop=true``), so the broker has no stop to clear in the
    body, and the new intent has no leg to seed. Without an explicit
    early-return widening, the body-empty + needs_persist=False guard
    returns before the SL retirement branch runs — leaving the live
    SL row with ``trail_state='pending'`` in extras.

    :meth:`_trailing_activation_monitor` would then PUT
    ``trailingStop=true`` against that stale row when its activation
    threshold is crossed, attaching a stop Pine has already removed.

    Distinct from
    ``__test_modify_exit_pending_trail_removal_retires_phantom_sl_leg_row__``
    where the new intent keeps a TP — there the body still carries
    ``profitLevel`` and the early return does not fire.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0,
            trail_price=1.17800, trail_offset=0.00200,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    sl_coid = old_env.client_order_id('s')
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        trailing_distance=0.00200,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry',
                'trail_state': 'pending',
                'trail_activation_price': 1.17800,
                'trail_offset': 0.00200},
    )
    pre_sl = ctx.get_order(sl_coid)
    assert pre_sl is not None and pre_sl.closed_ts_ms is None, (
        "precondition: pending-trail SL leg row must be live"
    )

    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.modify_exit(old_env, new_env))

    sl_after = ctx.get_order(sl_coid)
    assert sl_after is not None, (
        "leg row must remain in the store (audit trail) — only its "
        "lifecycle fields are retired"
    )
    assert sl_after.closed_ts_ms is not None, (
        "removing pending-trail-only with no replacement (empty new "
        "exit) must close the prior SL leg row; otherwise the early "
        "return on body-empty + needs_persist=False would leave "
        "trail_state='pending' on the row and the activation monitor "
        "would later attach a trailing stop Pine has already removed"
    )
    assert sl_after.state == 'rejected', (
        f"retired leg state should be 'rejected'; got {sl_after.state!r}"
    )
    store.close()


def __test_modify_exit_tp_removal_put_timeout_flips_existing_tp__(tmp_path):
    """``TP+SL`` → ``SL-only`` amend with PUT timeout: the body's
    ``profitLevel: None`` clear may have landed on Capital.com before
    the timeout, so the existing TP leg row must flip to
    ``disposition_unknown`` for the snapshot reconciler to resolve.

    Without this the row stays ``confirmed`` with the OLD ``tp_level``
    forever — :meth:`_resolve_bracket_leg_disposition` only revisits
    ``disposition_unknown`` rows — leaving a phantom TP visible to
    ``_find_bracket_leg_row`` and natural-close bookkeeping. If the
    broker did apply the clear, the phantom is plain wrong; if it did
    not, the leg is still attached and the row should be promoted back
    to ``confirmed``. The reconciler decides by comparing
    ``row.tp_level`` against the parent's live ``profitLevel``.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('error', 'positions/deal-L', 'put'):
            httpx.TimeoutException("read timeout"),
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        sl_level=1.17400, tp_level=1.17900,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.17900, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    tp_coid = old_env.client_order_id('t')
    sl_coid = old_env.client_order_id('s')
    ctx.upsert_order(
        tp_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        tp_level=1.17900,
        extras={'leg_kind': 'tp', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        sl_level=1.17400,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_exit(old_env, new_env))

    tp_row = ctx.get_order(tp_coid)
    assert tp_row is not None and tp_row.closed_ts_ms is None, (
        "PUT timeout must NOT close the TP leg row — the broker may "
        "still be enforcing it; the snapshot reconcile decides"
    )
    assert tp_row.state == 'disposition_unknown', (
        f"TP-removal ambiguous PUT must flip the existing TP leg row "
        f"to disposition_unknown so _resolve_bracket_leg_disposition "
        f"can reconcile it against the parent's actual profitLevel; "
        f"got state={tp_row.state!r}"
    )
    store.close()


def __test_modify_exit_fixed_sl_removal_put_timeout_flips_existing_sl__(tmp_path):
    """``TP+SL`` → ``TP-only`` amend with PUT timeout: the body's
    ``stopLevel: None`` clear may have landed before the timeout, so
    the existing SL leg row must flip to ``disposition_unknown`` for
    the snapshot reconciler to resolve. Mirror of the TP-removal
    ambiguous-PUT contract.
    """
    broker, store, ctx = _make_broker(tmp_path, responses={
        ('markets/EURUSD', 'get'): _rules_resp_with_snapshot(1.17500, 1.17502),
        ('error', 'positions/deal-L', 'put'):
            httpx.TimeoutException("read timeout"),
    })
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        sl_level=1.17400, tp_level=1.17900,
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')
    old_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.17900, sl_price=1.17400,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    tp_coid = old_env.client_order_id('t')
    sl_coid = old_env.client_order_id('s')
    ctx.upsert_order(
        tp_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        tp_level=1.17900,
        extras={'leg_kind': 'tp', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    ctx.upsert_order(
        sl_coid, symbol='EURUSD', side='sell', qty=1.0, state='confirmed',
        pine_entry_id='Long', from_entry='Long',
        sl_level=1.17400,
        extras={'leg_kind': 'sl', 'parent_deal_id': 'deal-L',
                'parent_coid': 'coid-entry'},
    )
    new_env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Bracket', from_entry='Long', symbol='EURUSD',
            side='sell', qty=1.0, tp_price=1.17900,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_exit(old_env, new_env))

    sl_row = ctx.get_order(sl_coid)
    assert sl_row is not None and sl_row.closed_ts_ms is None, (
        "PUT timeout must NOT close the SL leg row — the broker may "
        "still be enforcing it; the snapshot reconcile decides"
    )
    assert sl_row.state == 'disposition_unknown', (
        f"fixed-SL-removal ambiguous PUT must flip the existing SL leg "
        f"row to disposition_unknown so _resolve_bracket_leg_disposition "
        f"can reconcile it against the parent's actual stopLevel; "
        f"got state={sl_row.state!r}"
    )
    store.close()


def __test_process_activity_entry_fill_stamp_persists_before_yield__(tmp_path):
    """``entry_filled_at`` must be stamped BEFORE the entry-fill event
    is yielded.

    The ``activity_processed`` event log + cursor advance are durable
    by the time we reach the yield, so a crash or cancellation between
    the yield and the post-yield stamp would leave the activity deduped
    on restart but the row unmarked. A later ``USER`` / ``DEALER`` close
    for the same dealId would then fail the manual-close guard
    (:meth:`_activity_to_event`'s ``entry_already_filled`` check) and
    route as ``LegType.ENTRY`` — ``BrokerPosition.record_fill`` would
    ADD to the position instead of reducing it.

    Verification: inspect the row INSIDE the consumer's loop body for
    the entry event. With the stamp moved ahead of ``yield event``, the
    consumer sees ``entry_filled_at`` set on the same iteration; the
    legacy post-yield order would surface ``None`` here.
    """
    broker, store, ctx = _make_broker(tmp_path)
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0, filled_qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')

    activity = {
        'dateUTC': '2026-04-21T09:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'USER',
        'epic': 'EURUSD', 'direction': 'BUY',
        'size': 1.0, 'level': 1.17500,
    }

    async def drain_capturing_stamp():
        stamp_at_yield: list = []
        async for ev in broker._process_activity([activity]):
            if ev.leg_type == LegType.ENTRY:
                row = ctx.get_order('coid-entry')
                stamp_at_yield.append((row.extras or {}).get('entry_filled_at'))
        return stamp_at_yield

    stamps = asyncio.run(drain_capturing_stamp())
    assert len(stamps) == 1, (
        f"expected exactly one ENTRY event for the seeded activity, "
        f"got {len(stamps)}: {stamps!r}"
    )
    assert stamps[0] is not None, (
        "entry_filled_at must be persisted BEFORE _process_activity "
        "yields the entry-fill event; the activity_processed log is "
        "already durable at the yield point, so a crash/cancel between "
        "yield and the post-yield stamp would dedupe on restart but "
        "leave the row unmarked, mis-routing later USER/DEALER closes "
        "as LegType.ENTRY"
    )
    store.close()


def __test_process_activity_close_breadcrumb_persists_before_yield__(tmp_path):
    """The close-event breadcrumb (``close_event_yielded_at`` for the
    partial / race case, ``natural_close_at`` for the full close)
    must be stamped BEFORE :meth:`_process_activity` yields the event.

    The ``activity_processed`` event log + cursor advance run before
    ``yield event``, so a crash or cancellation between the yield and
    the legacy post-yield stamp would leave the activity deduped on
    restart but the breadcrumb missing. The next poll's
    :meth:`_reconcile_snapshot` would then see the entry row's
    ``dealId`` missing from ``/positions``, stamp
    ``missing_pending_since`` on it, and
    :meth:`_missing_pending_tracker` eventually raises a false
    :class:`UnexpectedCancelError` — the bot halts on what should
    have been a clean natural close.

    Verification: inspect the row INSIDE the consumer's loop body for
    the close event. With the stamp moved ahead of ``yield event``,
    the consumer sees ``close_event_yielded_at`` set on the same
    iteration; the legacy post-yield order would surface ``None``.
    """
    broker, store, ctx = _make_broker(tmp_path)
    # Pre-stamped entry row: subsequent USER activity classifies as a
    # manual close (LegType.CLOSE) rather than another entry.
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=2.0, filled_qty=2.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500,
                'entry_filled_at': 1700000000.0},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')

    # Partial close: USER reduced the deal from 2.0 to 1.0. The deal
    # is still present in the snapshot at size > 0, so the close
    # branch must stamp ``close_event_yielded_at`` (NOT
    # ``natural_close_at``) — the OCA bracket stays attached to the
    # remaining 1.0 exposure.
    activity = {
        'dateUTC': '2026-04-21T10:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'USER',
        'epic': 'EURUSD', 'direction': 'SELL',
        'size': 1.0, 'level': 1.17600,
    }
    positions_by_deal = {
        'deal-L': {
            'position': {
                'dealId': 'deal-L', 'size': 1.0, 'level': 1.17500,
            },
        },
    }

    async def drain_capturing_breadcrumb():
        breadcrumbs: list = []
        async for ev in broker._process_activity(
                [activity], positions_by_deal,
        ):
            if ev.leg_type == LegType.CLOSE:
                row = ctx.get_order('coid-entry')
                bx = row.extras or {}
                breadcrumbs.append({
                    'close_event_yielded_at': bx.get('close_event_yielded_at'),
                    'natural_close_at': bx.get('natural_close_at'),
                })
        return breadcrumbs

    bcs = asyncio.run(drain_capturing_breadcrumb())
    assert len(bcs) == 1, (
        f"expected exactly one CLOSE event for the seeded partial-close "
        f"activity, got {len(bcs)}: {bcs!r}"
    )
    assert bcs[0]['close_event_yielded_at'] is not None, (
        "close_event_yielded_at must be persisted BEFORE "
        "_process_activity yields the close event; the activity_processed "
        "log is already durable at the yield point, so a crash/cancel "
        "between yield and the post-yield stamp would dedupe on restart "
        "but leave the breadcrumb missing — _reconcile_snapshot would "
        "then stamp missing_pending_since on the now-vanished dealId "
        "and _missing_pending_tracker eventually raises a false "
        "UnexpectedCancelError"
    )
    # Defensive: partial close (deal still alive) must NOT stamp
    # natural_close_at — that would orphan the remaining exposure.
    assert bcs[0]['natural_close_at'] is None, (
        f"partial close (deal still present at size > 0) must use the "
        f"close_event_yielded_at breadcrumb, NOT natural_close_at; got "
        f"natural_close_at={bcs[0]['natural_close_at']!r}"
    )
    store.close()


def __test_process_activity_natural_close_stamp_persists_before_yield__(tmp_path):
    """``_close_bracket_after_natural_close`` (full natural close
    teardown) must run BEFORE :meth:`_process_activity` yields, by the
    same reasoning as the partial-close breadcrumb: the
    ``activity_processed`` log + cursor advance are durable at the
    yield point, so a crash between yield and the legacy post-yield
    stamp leaves the activity deduped on restart but the entry's
    ``natural_close_at`` unmarked, routing the now-vanished dealId
    into ``missing_pending_since`` / :class:`UnexpectedCancelError`.
    """
    broker, store, ctx = _make_broker(tmp_path)
    ctx.upsert_order(
        'coid-entry', symbol='EURUSD', side='buy', qty=1.0, filled_qty=1.0,
        state='confirmed', pine_entry_id='Long',
        exchange_order_id='deal-L',
        extras={'kind': 'position', 'confirm_level': 1.17500,
                'entry_filled_at': 1700000000.0},
    )
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')

    # TP fill: deal is fully gone from the snapshot — must trigger the
    # natural-close teardown branch.
    activity = {
        'dateUTC': '2026-04-21T10:00:00.000', 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': 'TP',
        'epic': 'EURUSD', 'direction': 'SELL',
        'size': 1.0, 'level': 1.17800,
    }
    positions_by_deal: dict[str, dict] = {}

    async def drain_capturing_stamp():
        stamps: list = []
        async for ev in broker._process_activity(
                [activity], positions_by_deal,
        ):
            if ev.leg_type == LegType.TAKE_PROFIT:
                row = ctx.get_order('coid-entry')
                stamps.append((row.extras or {}).get('natural_close_at'))
        return stamps

    stamps = asyncio.run(drain_capturing_stamp())
    assert len(stamps) == 1, (
        f"expected exactly one TP event for the seeded activity, "
        f"got {len(stamps)}: {stamps!r}"
    )
    assert stamps[0] is not None, (
        "natural_close_at must be persisted BEFORE _process_activity "
        "yields the TP event; the activity_processed log is already "
        "durable at the yield point, so a crash/cancel between yield "
        "and the post-yield _close_bracket_after_natural_close call "
        "would dedupe on restart but leave the entry row unmarked, "
        "letting _reconcile_snapshot stamp missing_pending_since on "
        "the now-vanished dealId — _missing_pending_tracker would "
        "eventually raise a false UnexpectedCancelError"
    )
    store.close()


# ---------------------------------------------------------------------------
# Session refresh — proactive + reactive coverage of the ~1h Capital.com
# session cap (undocumented hard expiry; the 10-min "inactivity TTL" docs
# don't surface it. Observed in live trading after the bot ran ~1h05m and
# every subsequent reconnect failed with error.invalid.session.token).
# ---------------------------------------------------------------------------


class _MockHttpResponse:
    """Minimal stand-in for ``httpx.Response`` for ``__call__`` unit tests."""

    def __init__(self, status_code: int, json_body: dict,
                 headers: dict | None = None):
        self.status_code = status_code
        self._json = json_body
        self.headers = headers or {}
        self.text = json.dumps(json_body)

    @property
    def is_error(self) -> bool:
        return self.status_code >= 400

    def json(self) -> dict:
        return self._json


def _make_jwt(payload: dict) -> str:
    """Build an unsigned JWT-shaped string for testing the JWT parser."""
    from base64 import urlsafe_b64encode
    header = urlsafe_b64encode(b'{"alg":"none","typ":"JWT"}').rstrip(b'=').decode()
    body = urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b'=').decode()
    return f"{header}.{body}.signature"


def __test_extract_jwt_expiry_returns_exp_claim__():
    from pynecore_capitalcom.helpers import _extract_jwt_expiry
    token = _make_jwt({'exp': 1234567890, 'sub': 'user'})
    assert _extract_jwt_expiry(token) == 1234567890.0


def __test_extract_jwt_expiry_returns_none_for_non_jwt__():
    from pynecore_capitalcom.helpers import _extract_jwt_expiry
    assert _extract_jwt_expiry('opaque-session-id-12345') is None
    assert _extract_jwt_expiry('') is None
    assert _extract_jwt_expiry(None) is None
    assert _extract_jwt_expiry('not.valid.jwt') is None
    token_no_exp = _make_jwt({'sub': 'user'})
    assert _extract_jwt_expiry(token_no_exp) is None


# noinspection PyProtectedMember
def __test_compute_token_expiry_falls_back_to_hardcoded_when_opaque__():
    from time import time as epoch_time

    from pynecore_capitalcom.rest import _SESSION_REFRESH_FALLBACK_S
    broker = _FakeBroker(config=_make_config())
    now = epoch_time()
    broker._security_token_deadline = broker._token_deadline(
        'opaque-x-security-token', now,
    )
    broker._cst_token_deadline = broker._token_deadline(
        'opaque-cst-token', now,
    )
    expiry = broker._compute_token_expiry()
    assert expiry == now + _SESSION_REFRESH_FALLBACK_S


# noinspection PyProtectedMember
def __test_compute_token_expiry_uses_min_jwt_exp__():
    """The session is only as fresh as the earlier-expiring token."""
    broker = _FakeBroker(config=_make_config())
    broker._security_token_deadline = broker._token_deadline(
        _make_jwt({'exp': 2000000000}), 0.0,
    )
    broker._cst_token_deadline = broker._token_deadline(
        _make_jwt({'exp': 1900000000}), 0.0,
    )
    assert broker._compute_token_expiry() == 1900000000.0


# noinspection PyProtectedMember
def __test_compute_token_expiry_mixed_jwt_and_opaque_uses_opaque_cap__():
    """A far-future JWT must not mask an opaque token's hard cap."""
    from time import time as epoch_time

    from pynecore_capitalcom.rest import _SESSION_REFRESH_FALLBACK_S
    broker = _FakeBroker(config=_make_config())
    now = epoch_time()
    broker._security_token_deadline = broker._token_deadline(
        _make_jwt({'exp': 2000000000}), now,
    )
    broker._cst_token_deadline = broker._token_deadline(
        'opaque-cst-token', now,
    )
    expiry = broker._compute_token_expiry()
    assert expiry == now + _SESSION_REFRESH_FALLBACK_S


# noinspection PyProtectedMember
def __test_partial_opaque_rotation_preserves_unchanged_deadline__(monkeypatch):
    """Rotating only X-SECURITY-TOKEN must not slide the unchanged opaque CST forward.

    Regression: ``_compute_token_expiry`` used to recompute both opaque
    deadlines from ``epoch_time()`` on every rotation, so a partial
    header rotation at t=30min would push the unchanged CST's deadline
    from t=50min to t=80min, defeating proactive refresh.
    """
    import httpx as _httpx

    broker = _FakeBroker(config=_make_config())

    class _StubResponse:
        is_error = False

        def __init__(self, headers: dict):
            self.headers = headers

        def json(self):
            return {'ok': True}

    # Establish initial session at t=t0: both opaque tokens issued together.
    t0 = 1_000_000.0
    monkeypatch.setattr('pynecore_capitalcom.rest.epoch_time', lambda: t0)
    monkeypatch.setattr(_httpx, 'get', lambda url, **_kw: _StubResponse(
        {'X-SECURITY-TOKEN': 'sec-A', 'CST': 'cst-B'},
    ))
    broker('any/endpoint', method='get')
    cst_deadline_after_login = broker._cst_token_deadline

    # At t=t0+30min, only X-SECURITY-TOKEN rotates. CST is the same.
    monkeypatch.setattr('pynecore_capitalcom.rest.epoch_time',
                        lambda: t0 + 30 * 60)
    monkeypatch.setattr(_httpx, 'get', lambda url, **_kw: _StubResponse(
        {'X-SECURITY-TOKEN': 'sec-A-prime', 'CST': 'cst-B'},
    ))
    broker('any/endpoint', method='get')

    assert broker._cst_token_deadline == cst_deadline_after_login
    # And the session expiry reflects the earlier (unchanged CST) deadline.
    assert broker._session_token_expiry_ts == cst_deadline_after_login


def __test_concurrent_refresh_response_does_not_roll_back_fresh_tokens__(monkeypatch):
    """A slow response that returns AFTER another worker has refreshed
    the session must not overwrite the freshly issued tokens, even when
    the response carries server-rotated headers of its own.

    Regression: the rotation handler used to compare against the live
    attribute (always "current") instead of the request-time snapshot
    and apply rotation unconditionally — so a stale response could
    silently roll the session back to expiring tokens.
    """
    import httpx as _httpx
    from time import time as _time

    broker = _FakeBroker(config=_make_config())
    broker.security_token = 'sec-old'
    broker.cst_token = 'cst-old'
    fresh = _time() + 3600
    broker._security_token_deadline = fresh
    broker._cst_token_deadline = fresh
    broker._session_token_expiry_ts = fresh

    class _StubResponse:
        is_error = False
        headers = {
            'X-SECURITY-TOKEN': 'sec-server-rotated',
            'CST': 'cst-server-rotated',
        }

        def json(self):
            return {'ok': True}

    advanced = fresh + 7200

    def _stub(url, **_kw):
        # Concurrent refresh advances the session while this request is
        # in flight: another worker called create_session and installed
        # fresh tokens before our response arrives. ``create_session``
        # increments ``_session_generation`` as part of its bootstrap
        # rotation handler, so the simulation must mirror that — without
        # the bump the discard would (correctly, in the new generation
        # model) treat the response as a peer rotation and apply it.
        broker.security_token = 'sec-NEW'
        broker.cst_token = 'cst-NEW'
        broker._security_token_deadline = advanced
        broker._cst_token_deadline = advanced
        broker._session_token_expiry_ts = advanced
        broker._session_generation += 1
        return _StubResponse()

    monkeypatch.setattr(_httpx, 'get', _stub)
    broker('any/endpoint', method='get')

    assert broker.security_token == 'sec-NEW'
    assert broker.cst_token == 'cst-NEW'
    assert broker._security_token_deadline == advanced
    assert broker._cst_token_deadline == advanced


def __test_concurrent_normal_request_rotation_is_not_discarded__(monkeypatch):
    """Two concurrent normal requests both rotating tokens must NOT lose
    the second response's rotation — they share a session generation,
    so last-writer-wins is the correct rule.

    Regression: an earlier fix used a snapshot-token guard
    (``self.security_token != req_security_token`` → discard) which
    couldn't tell apart "another worker created a fresh session" (drop
    the late response) from "another worker rotated via a peer normal
    response" (keep both rotations) — and dropped the second rotation,
    silently keeping the broker on a stale-but-newer token.
    """
    import httpx as _httpx
    from time import time as _time

    broker = _FakeBroker(config=_make_config())
    broker.security_token = 'sec-X'
    broker.cst_token = 'cst-X'
    fresh = _time() + 3600
    broker._security_token_deadline = fresh
    broker._cst_token_deadline = fresh
    broker._session_token_expiry_ts = fresh

    class _StubResponse:
        is_error = False
        headers = {
            'X-SECURITY-TOKEN': 'sec-Z',
            'CST': 'cst-Z',
        }

        def json(self):
            return {'ok': True}

    def _stub(url, **_kw):
        # Simulate a peer normal request that already rotated the
        # session before our response arrives. Crucially this is NOT a
        # bootstrap login, so ``_session_generation`` stays put.
        broker.security_token = 'sec-Y'
        broker.cst_token = 'cst-Y'
        return _StubResponse()

    monkeypatch.setattr(_httpx, 'get', _stub)
    broker('any/endpoint', method='get')

    # Our own rotation header (Z) is the latest valid one — the snapshot
    # guard used to throw it away on a token-mismatch, leaving the
    # broker on Y.
    assert broker.security_token == 'sec-Z'
    assert broker.cst_token == 'cst-Z'


def __test_create_session_keeps_old_tokens_visible_during_login__(monkeypatch):
    """While ``create_session`` runs, the existing CST / X-SECURITY-TOKEN
    must remain readable by concurrent observers (e.g. the WebSocket
    ping loop in ``streaming._send``). Bootstrap requests skip auth
    headers in ``__call__``, so the class state must not be wiped.

    Regression: an earlier fix nulled the tokens at the top of
    ``create_session``, opening a window where ``_send`` would push
    pings with ``cst=None`` / ``securityToken=None`` and get the live
    stream rejected.
    """
    import httpx as _httpx
    from time import time as _time

    broker = _FakeBroker(config=_make_config())
    broker.security_token = 'sec-old'
    broker.cst_token = 'cst-old'
    fresh = _time() + 3600
    broker._security_token_deadline = fresh
    broker._cst_token_deadline = fresh
    broker._session_token_expiry_ts = fresh

    observed: list[tuple] = []

    class _EncryptionKeyResponse:
        is_error = False
        headers: dict = {}

        def json(self):
            return {'encryptionKey': 'fake-key', 'timeStamp': 12345}

    class _SessionResponse:
        is_error = False
        headers = {'X-SECURITY-TOKEN': 'sec-NEW', 'CST': 'cst-NEW'}

        def json(self):
            return {}

    def _stub_get(url, **_kw):
        observed.append(('get', broker.security_token, broker.cst_token))
        return _EncryptionKeyResponse()

    def _stub_post(url, **_kw):
        observed.append(('post', broker.security_token, broker.cst_token))
        return _SessionResponse()

    monkeypatch.setattr(_httpx, 'get', _stub_get)
    monkeypatch.setattr(_httpx, 'post', _stub_post)
    monkeypatch.setattr('pynecore_capitalcom.rest.encrypt_password',
                        lambda *_a, **_kw: 'fake-encrypted')

    broker.create_session()

    assert observed == [
        ('get', 'sec-old', 'cst-old'),
        ('post', 'sec-old', 'cst-old'),
    ]
    assert broker.security_token == 'sec-NEW'
    assert broker.cst_token == 'cst-NEW'


def __test_call_does_not_recurse_when_bootstrap_returns_session_token_error__(monkeypatch):
    """A bootstrap endpoint returning ``error.invalid.session.token`` must
    surface the error directly, never recurse via ``create_session``.

    Regression: the recovery branch used to retry every endpoint, including
    ``session/encryptionKey`` and ``session``, which ``create_session``
    itself calls with ``_level=0`` — so a stale-token failure on bootstrap
    triggered ``create_session`` → bootstrap → recovery → ``create_session``
    until ``RecursionError``.
    """
    import httpx as _httpx

    broker = _FakeBroker(config=_make_config())

    class _StubResponse:
        is_error = True
        headers: dict = {}

        def json(self):
            return {'errorCode': 'error.invalid.session.token'}

    call_count = [0]

    def _stub(url, **_kw):
        call_count[0] += 1
        return _StubResponse()

    monkeypatch.setattr(_httpx, 'get', _stub)
    monkeypatch.setattr(_httpx, 'post', _stub)

    with pytest.raises(CapitalComError):
        broker.create_session()
    # First bootstrap call fails → CapitalComError. No retry, no recursion.
    assert call_count[0] == 1


# noinspection PyProtectedMember
def __test_refresh_session_if_stale_skips_when_fresh__(monkeypatch):
    from time import time as epoch_time
    broker = _FakeBroker(config=_make_config())
    broker._session_token_expiry_ts = epoch_time() + 3600
    refresh_called: list = []
    monkeypatch.setattr(broker, 'create_session',
                        lambda: refresh_called.append(True))
    broker._refresh_session_if_stale()
    assert refresh_called == []


# noinspection PyProtectedMember
def __test_refresh_session_if_stale_triggers_inside_safety_window__(monkeypatch):
    """Within 5 min of expiry the refresh fires."""
    from time import time as epoch_time
    broker = _FakeBroker(config=_make_config())
    broker._session_token_expiry_ts = epoch_time() + 60
    refresh_called: list = []
    monkeypatch.setattr(broker, 'create_session',
                        lambda: refresh_called.append(True))
    broker._refresh_session_if_stale()
    assert refresh_called == [True]


# noinspection PyProtectedMember
def __test_refresh_session_if_stale_skips_when_no_session__(monkeypatch):
    """expiry==0.0 means no session yet — caller will create it on demand."""
    broker = _FakeBroker(config=_make_config())
    broker._session_token_expiry_ts = 0.0
    refresh_called: list = []
    monkeypatch.setattr(broker, 'create_session',
                        lambda: refresh_called.append(True))
    broker._refresh_session_if_stale()
    assert refresh_called == []


# noinspection PyProtectedMember
def __test_call_reactive_retry_on_invalid_session_token__(monkeypatch):
    """error.invalid.session.token triggers create_session + retry with fresh tokens."""
    broker = _FakeBroker(config=_make_config())
    broker.security_token = 'old-x-sec'
    broker.cst_token = 'old-cst'
    broker._session_token_expiry_ts = 0.0  # no proactive refresh path

    def fake_create_session():
        broker.security_token = 'new-x-sec'
        broker.cst_token = 'new-cst'
    monkeypatch.setattr(broker, 'create_session', fake_create_session)

    responses = [
        _MockHttpResponse(401, {'errorCode': 'error.invalid.session.token'}),
        _MockHttpResponse(200, {'positions': []}, {}),
    ]
    captured_csts: list[str] = []

    def fake_get(url, **kwargs):
        captured_csts.append(kwargs['headers'].get('CST', ''))
        return responses.pop(0)
    monkeypatch.setattr(httpx, 'get', fake_get)

    result = broker('positions', method='get')
    assert result == {'positions': []}
    assert captured_csts == ['old-cst', 'new-cst']


# noinspection PyProtectedMember
def __test_call_proactive_refresh_runs_before_request__(monkeypatch):
    """Proactive refresh fires when token is inside the safety window."""
    from time import time as epoch_time
    broker = _FakeBroker(config=_make_config())
    broker.security_token = 'tok-v1'
    broker.cst_token = 'cst-v1'
    broker._session_token_expiry_ts = epoch_time() + 60

    def fake_create_session():
        broker.security_token = 'tok-v2'
        broker.cst_token = 'cst-v2'
        broker._session_token_expiry_ts = epoch_time() + 3600
    monkeypatch.setattr(broker, 'create_session', fake_create_session)

    captured_csts: list[str] = []

    def fake_get(url, **kwargs):
        captured_csts.append(kwargs['headers'].get('CST', ''))
        return _MockHttpResponse(200, {'positions': []}, {})
    monkeypatch.setattr(httpx, 'get', fake_get)

    broker('positions', method='get')
    assert captured_csts == ['cst-v2'], (
        "proactive refresh must run BEFORE the HTTP request — the call "
        "should land at Capital.com with the freshly minted token"
    )


# noinspection PyProtectedMember
def __test_call_proactive_refresh_skipped_for_bootstrap_endpoints__(monkeypatch):
    """create_session() reentry guard: session/* endpoints don't re-refresh."""
    from time import time as epoch_time
    broker = _FakeBroker(config=_make_config())
    broker.security_token = 'tok'
    broker.cst_token = 'cst'
    broker._session_token_expiry_ts = epoch_time() + 60  # stale
    refresh_called: list = []
    monkeypatch.setattr(broker, 'create_session',
                        lambda: refresh_called.append(True))
    monkeypatch.setattr(
        httpx, 'get',
        lambda url, **kwargs: _MockHttpResponse(200, {'k': 'v'}, {}),
    )
    monkeypatch.setattr(
        httpx, 'post',
        lambda url, **kwargs: _MockHttpResponse(200, {'k': 'v'}, {}),
    )
    broker('session/encryptionKey', method='get')
    broker('session', data={'identifier': 'x'}, method='post')
    assert refresh_called == []


# noinspection PyProtectedMember
def __test_call_authenticated_session_method_is_not_treated_as_bootstrap__(monkeypatch):
    """``PUT session`` (Capital.com account-switch) must keep the auth
    headers and the reactive retry path — only the unauthenticated
    bootstrap pair (``GET session/encryptionKey`` + ``POST session``) is
    exempt.

    Regression: the bootstrap check used to key on endpoint alone, so
    any future ``PUT session`` caller would silently lose
    ``CST``/``X-SECURITY-TOKEN`` on the wire and fail as anonymous.
    """
    broker = _FakeBroker(config=_make_config())
    broker.security_token = 'sec-tok'
    broker.cst_token = 'cst-tok'
    broker._session_token_expiry_ts = 0.0  # skip proactive refresh
    captured_headers: list[dict] = []

    def fake_put(url, **kwargs):
        captured_headers.append(dict(kwargs['headers']))
        return _MockHttpResponse(200, {'ok': True}, {})

    monkeypatch.setattr(httpx, 'put', fake_put)
    broker('session', data={'accountId': 'X'}, method='put')

    assert captured_headers == [{
        'X-CAP-API-KEY': 'test-key',
        'X-SECURITY-TOKEN': 'sec-tok',
        'CST': 'cst-tok',
    }]


# noinspection PyProtectedMember
def __test_call_token_rotation_recomputes_expiry_from_jwt__(monkeypatch):
    """A header-rotated CST that is a JWT updates _session_token_expiry_ts."""
    from time import time as epoch_time
    broker = _FakeBroker(config=_make_config())
    broker.security_token = 'old-opaque-x-sec'
    broker.cst_token = 'old-opaque-cst'
    broker._session_token_expiry_ts = epoch_time() + 7200  # well clear of safety

    new_exp = float(int(epoch_time()) + 1800)  # 30 min from now
    new_cst_jwt = _make_jwt({'exp': new_exp})

    monkeypatch.setattr(
        httpx, 'get',
        lambda url, **kwargs: _MockHttpResponse(
            200, {'positions': []},
            {'CST': new_cst_jwt, 'X-SECURITY-TOKEN': 'new-opaque-x-sec'},
        ),
    )

    broker('positions', method='get')
    # X-SECURITY-TOKEN remains opaque → fallback (now+50min); CST is a
    # JWT exp=now+30min. min() of the two pins to the JWT's exp.
    assert broker._session_token_expiry_ts == new_exp


# === Startup orphan retire =================================================

def __test_recover_retires_confirmed_row_with_no_exchange_counterpart__(
        tmp_path, caplog,
):
    """A ``confirmed`` row whose ``dealId`` is gone gets retired silently.

    Reproduces the user-reported case: the bot was stopped, the
    operator closed the position manually on the exchange UI, then
    started the bot again. The BrokerStore still carries the entry row
    plus its TP/SL bracket legs, but ``/positions`` and
    ``/workingorders`` return empty. Without the retire pass the runtime
    reconcile would later raise ``UnexpectedCancelError`` and halt.
    """
    import logging as _logging

    broker = _FakeBroker(config=_make_config(), responses={
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
    })
    store, ctx = _open_store_ctx(tmp_path, broker)
    try:
        ctx.upsert_order(
            'entry-coid', symbol='EURUSD', side='buy', qty=1.0,
            state='confirmed',
            intent_key='Long', pine_entry_id='Long',
            extras={'kind': 'position', 'order_type': 'market',
                    'entry_filled_at': 1.0},
        )
        ctx.set_exchange_id('entry-coid', 'deal-gone-1')
        ctx.add_ref('entry-coid', 'deal_id', 'deal-gone-1')

        ctx.upsert_order(
            'tp-coid', symbol='EURUSD', side='sell', qty=1.0,
            state='confirmed',
            intent_key='Long-TP', pine_entry_id='Long',
            extras={'leg_kind': 'tp', 'parent_coid': 'entry-coid',
                    'parent_deal_id': 'deal-gone-1'},
        )
        ctx.upsert_order(
            'sl-coid', symbol='EURUSD', side='sell', qty=1.0,
            state='confirmed',
            intent_key='Long-SL', pine_entry_id='Long',
            extras={'leg_kind': 'sl', 'parent_coid': 'entry-coid',
                    'parent_deal_id': 'deal-gone-1'},
        )

        caplog.set_level(_logging.INFO, logger='pynecore')

        asyncio.run(broker._recover_in_flight_submissions())

        # All three rows have been closed.
        assert ctx.get_order('entry-coid').closed_ts_ms is not None
        assert ctx.get_order('tp-coid').closed_ts_ms is not None
        assert ctx.get_order('sl-coid').closed_ts_ms is not None

        # Audit events: one per retired row.
        retired = store._conn.execute(
            "SELECT client_order_id FROM events "
            "WHERE kind = 'startup_orphan_retired' "
            "ORDER BY client_order_id",
        ).fetchall()
        assert {r['client_order_id'] for r in retired} == {
            'entry-coid', 'tp-coid', 'sl-coid',
        }

        # One [BROKER] INFO summary line.
        summary = [
            rec for rec in caplog.records
            if rec.levelno == _logging.INFO
            and 'retired' in rec.getMessage()
            and 'orphan' in rec.getMessage()
        ]
        assert len(summary) == 1
        assert '3 orphan order row(s)' in summary[0].getMessage()
    finally:
        ctx.close()
        store.close()


def __test_recover_does_not_retire_when_position_still_present__(tmp_path):
    """A ``confirmed`` row whose ``dealId`` IS in /positions stays live.

    Counter-test for the retire pass — guards against accidentally
    retiring rows the bot legitimately needs to re-adopt across a
    restart.
    """
    broker = _FakeBroker(config=_make_config(), responses={
        ('positions', 'get'): {
            'positions': [{
                'position': {
                    'dealId': 'deal-still-here',
                    'dealReference': 'ref-still-here',
                    'size': 1.0,
                },
            }],
        },
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
    })
    store, ctx = _open_store_ctx(tmp_path, broker)
    try:
        ctx.upsert_order(
            'entry-coid', symbol='EURUSD', side='buy', qty=1.0,
            state='confirmed',
            intent_key='Long', pine_entry_id='Long',
            extras={'kind': 'position', 'order_type': 'market',
                    'entry_filled_at': 1.0},
        )
        ctx.set_exchange_id('entry-coid', 'deal-still-here')
        ctx.add_ref('entry-coid', 'deal_id', 'deal-still-here')

        asyncio.run(broker._recover_in_flight_submissions())

        row = ctx.get_order('entry-coid')
        assert row is not None
        assert row.closed_ts_ms is None  # still live

        n_retired = store._conn.execute(
            "SELECT COUNT(*) AS n FROM events "
            "WHERE kind = 'startup_orphan_retired'",
        ).fetchone()['n']
        assert n_retired == 0
    finally:
        ctx.close()
        store.close()
