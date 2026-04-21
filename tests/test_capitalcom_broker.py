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
    assert caps.watch_orders is False
    assert caps.fetch_position is True
    assert caps.client_id_echo is False
    assert caps.idempotency_native is False


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


def __test_execute_stubs_raise_not_implemented__():
    """Stubs must refuse silent awaits — they point at the §9 blockers."""
    broker = _FakeBroker(config=_make_config())
    envelope = DispatchEnvelope(
        intent=EntryIntent(
            pine_id="Long", symbol="EURUSD", side="buy", qty=1.0,
            order_type=OrderType.MARKET,
        ),
        run_tag="test", bar_ts_ms=1700000000000,
    )
    with pytest.raises(NotImplementedError, match="§9"):
        asyncio.run(broker.execute_entry(envelope))
    with pytest.raises(NotImplementedError, match="§9"):
        asyncio.run(broker.execute_exit(envelope))
    with pytest.raises(NotImplementedError, match="§9"):
        asyncio.run(broker.execute_close(envelope))
    with pytest.raises(NotImplementedError):
        asyncio.run(broker.execute_cancel(envelope))


def __test_watch_orders_signals_polling_fallback__():
    broker = _FakeBroker(config=_make_config())
    with pytest.raises(NotImplementedError, match="no WS order channel"):
        broker.watch_orders()
