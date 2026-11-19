"""Deal identity of own entries: a confirmed fill holds its deal until the venue shows it closed.

Regression for capitalcom cycle 442 (pyramid, BTCUSD, 2026-11-19). The third
short pyramid leg's market entry confirmed with ``dealId …4bbe``, but the venue
had run the POST through its working-order machinery: ``…4bbe`` was the executed
working order, and the position opened as ``…4bc1`` (activity: ``POSITION
ACCEPTED …4bc1`` + ``WORKING_ORDER EXECUTED …4bbe``), which the bulk
``/positions`` read did not list for over 30 s. The bracket PUT on ``…4bbe``
answered not-found and its verify read did not list ``…4bbe``, so a defensive
close went out; its DELETE on ``…4bbe`` answered not-found and was booked as
"already gone"; 30 s later the defensive-close settlement trusted a position
read that omitted the deal and booked a close that never happened. The venue
kept ``…4bc1`` open and unprotected (venue -0.015, journal -0.01).
"""
import asyncio

import pytest

from pynecore.core.broker.disappearance import MissingResolution
from pynecore.core.broker.exceptions import (
    BracketAttachAfterFillRejectedError,
    OrderDispositionUnknownError,
    OrderSkippedByPlugin,
)
from pynecore.core.broker.models import (
    CloseIntent,
    DispatchEnvelope,
    EntryIntent,
    ExitIntent,
    OrderType,
)
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore, RunContext

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
    """Broker whose REST calls answer from canned responses per endpoint."""

    def __init__(self, *, responses: dict):
        super().__init__(config=CapitalComConfig(
            demo=True, user_email="test@example.com", api_key="test-key",
            api_password="test-password",
        ))
        self._responses = responses
        self._calls: list[tuple[str, str, dict | None]] = []

    def _fake_response(self, endpoint, *, data=None, method='post'):
        self._calls.append((endpoint, method, data))
        err = self._responses.get(('error', endpoint, method))
        if err is not None:
            raise err
        return self._responses.get((endpoint, method), {})

    def _call_serialized_write(self, endpoint, *, data=None, method='post'):
        return self._fake_response(endpoint, data=data, method=method)

    async def _call(self, endpoint, *, data=None, method='post'):
        return self._fake_response(endpoint, data=data, method=method)


def _not_found() -> OrderNotFoundError:
    return OrderNotFoundError("API error occured: error.not-found.dealId", ref_type='deal_id')


def _listed(deal_id: str) -> dict:
    """A ``/positions`` row of one of the earlier pyramid legs."""
    return {'position': {'dealId': deal_id, 'direction': 'SELL', 'size': 1.0, 'level': 1.1},
            'market': {'epic': 'EURUSD'}}


#: The two earlier pyramid legs the venue lists all along.
_POSITIONS = {'positions': [_listed('deal-S1'), _listed('deal-S2')]}

#: What the venue published about the third leg: the position under its fresh
#: ``dealId``, back-linking the working order the confirm named.
_ROLLOVER_ACTIVITIES = {'activities': [
    {'dateUTC': '2026-11-19T10:16:10.394', 'dealId': 'pos-3', 'epic': 'EURUSD',
     'type': 'POSITION', 'status': 'ACCEPTED', 'source': 'USER',
     'details': {'dealReference': 'o-3', 'workingOrderId': 'wo-3',
                 'direction': 'SELL', 'size': 1.0, 'level': 1.1}},
    {'dateUTC': '2026-11-19T10:16:10.394', 'dealId': 'wo-3', 'epic': 'EURUSD',
     'type': 'WORKING_ORDER', 'status': 'EXECUTED', 'source': 'USER',
     'details': {'dealReference': 'o-3', 'direction': 'SELL', 'size': 1.0, 'level': 0}},
]}


def _make_broker(tmp_path, responses: dict) -> tuple[_FakeBroker, BrokerStore, RunContext]:
    """A broker on an open journal holding the two earlier legs, ready for the third.

    The third leg's market entry confirms with the executed working order's
    ``dealId`` (``wo-3``) and no ``affectedDeals``; both ``positions/wo-3``
    writes answer not-found.
    """
    broker = _FakeBroker(responses={
        ('markets/EURUSD', 'get'): _RULES_RESP,
        ('positions', 'post'): {'dealReference': 'o-3'},
        ('confirms/o-3', 'get'): {'dealStatus': 'ACCEPTED', 'status': 'OPEN',
                                  'dealId': 'wo-3', 'level': 1.1, 'size': 1.0},
        ('positions', 'get'): _POSITIONS,
        ('error', 'positions/wo-3', 'put'): _not_found(),
        ('error', 'positions/wo-3', 'delete'): _not_found(),
        ('error', 'positions/wo-3', 'get'): _not_found(),
        **responses,
    })
    broker._account_id = 'capitalcom-demo-test-account'
    store = BrokerStore(tmp_path / "broker.sqlite", plugin_name=broker.plugin_name)
    ctx = store.open_run(RunIdentity(strategy_id="test", symbol="EURUSD", timeframe="1",
                                     account_id="test-account", label=None),
                         script_source="// test")
    broker.store_ctx = ctx
    for leg in ('S1', 'S2'):
        coid = f'coid-{leg}'
        ctx.upsert_order(coid, symbol='EURUSD', side='sell', qty=1.0, filled_qty=1.0,
                         state='confirmed', pine_entry_id=leg,
                         exchange_order_id=f'deal-{leg}',
                         extras={'kind': 'position', 'confirm_level': 1.1,
                                 'entry_filled_at': 1.0})
        ctx.add_ref(coid, 'deal_id', f'deal-{leg}')
    return broker, store, ctx


def _enter_third_leg(broker: _FakeBroker) -> str:
    """Send the third leg's market entry; return its client order id."""
    orders = asyncio.run(broker.execute_entry(DispatchEnvelope(
        intent=EntryIntent(pine_id='S3', symbol='EURUSD', side='sell', qty=1.0,
                           order_type=OrderType.MARKET),
        run_tag='test', bar_ts_ms=1700000000000,
    )))
    assert orders[0].id == 'wo-3' and orders[0].filled_qty == 1.0
    return orders[0].client_order_id or ''


def _exit_envelope(retry_seq: int = 0) -> DispatchEnvelope:
    return DispatchEnvelope(
        intent=ExitIntent(pine_id='S3-X', from_entry='S3', symbol='EURUSD',
                          side='buy', qty=1.0, tp_price=1.09, sl_price=1.11),
        run_tag='test', bar_ts_ms=1700000000000, retry_seq=retry_seq,
    )


def _defensive_close(coid: str, deal_id: str) -> DispatchEnvelope:
    return DispatchEnvelope(
        intent=CloseIntent(pine_id=f'__pyne_defensive_close__{coid}', symbol='EURUSD',
                           side='buy', qty=1.0, immediately=True,
                           synthetic_kind='defensive_close',
                           target_position_coid=coid, target_exchange_id=deal_id),
        run_tag='test', bar_ts_ms=1700000000000,
    )


def _event_kinds(ctx: RunContext) -> list[str]:
    return [row['kind'] for row in ctx._store._conn.execute(
        "SELECT kind FROM events WHERE run_instance_id = ?", (ctx.run_instance_id,))]


def _short_size(broker: _FakeBroker) -> float:
    position = asyncio.run(broker.get_position('EURUSD'))
    assert position is not None and position.side == 'short'
    return position.size


def __test_working_order_deal_id_rekeys_and_the_exit_protects_the_position__(tmp_path):
    """The cycle-442 rollover: the bracket goes onto the position's own deal.

    The exit's PUT on the confirm's ``dealId`` answers not-found and the bulk
    read lists neither id; the recent activities back-link the position. The
    row moves onto that deal and the exit waits one sync instead of closing
    the entry defensively; the re-sent exit attaches the bracket there.
    """
    broker, store, ctx = _make_broker(tmp_path, {
        ('history/activity', 'get'): _ROLLOVER_ACTIVITIES,
        ('positions/pos-3', 'put'): {'dealReference': 'att-3'},
        ('confirms/att-3', 'get'): {'dealStatus': 'ACCEPTED', 'status': 'OPEN',
                                    'dealId': 'pos-3'},
    })
    coid = _enter_third_leg(broker)
    # The run holds three legs while the read lists two.
    assert _short_size(broker) == 3.0

    with pytest.raises(OrderSkippedByPlugin) as skipped:
        asyncio.run(broker.execute_exit(_exit_envelope()))
    assert skipped.value.reason == 'position_deal_pending'
    row = ctx.get_order(coid)
    assert row is not None and row.exchange_order_id == 'pos-3'
    assert row.state == 'confirmed' and (row.extras or {}).get('natural_close_at') is None

    legs = asyncio.run(broker.execute_exit(_exit_envelope(retry_seq=1)))
    assert {leg.id for leg in legs} == {'pos-3:tp', 'pos-3:sl'}
    assert ('positions/pos-3', 'put') in {(c[0], c[1]) for c in broker._calls}
    assert not any(c[0].startswith('positions/') and c[1] == 'delete' for c in broker._calls)
    store.close()


def __test_a_materialising_deal_is_never_booked_closed__(tmp_path):
    """The cycle-442 lag: nothing shows the position yet, and nothing books it gone.

    No read lists the deal and nothing back-links it. The exit waits instead
    of closing the entry defensively; a defensive close that goes out anyway
    parks instead of counting the target as swept; the run's position keeps
    the deal (the engine's stale defensive-close settlement read exactly this
    position and booked the close); the disappearance tracker defers.
    """
    broker, store, ctx = _make_broker(tmp_path, {})
    coid = _enter_third_leg(broker)

    with pytest.raises(OrderSkippedByPlugin) as skipped:
        asyncio.run(broker.execute_exit(_exit_envelope()))
    assert skipped.value.reason == 'position_deal_pending'

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_close(_defensive_close(coid, 'wo-3')))
    kinds = _event_kinds(ctx)
    assert 'close_already_gone' not in kinds
    assert 'close_target_not_addressable' in kinds
    row = ctx.get_order(coid)
    assert row is not None and row.state == 'confirmed'
    assert (row.extras or {}).get('natural_close_at') is None

    assert _short_size(broker) == 3.0
    verdict = asyncio.run(broker._confirm_missing_cancelled(row))
    assert verdict.resolution is MissingResolution.INCONCLUSIVE
    store.close()


def __test_a_listed_deal_ends_materialising__(tmp_path):
    """Once a read listed the deal, its later absence is a close again."""
    broker, store, ctx = _make_broker(tmp_path, {})
    coid = _enter_third_leg(broker)
    broker._responses[('positions', 'get')] = {
        'positions': [*_POSITIONS['positions'], _listed('wo-3')]}
    assert _short_size(broker) == 3.0
    broker._responses[('positions', 'get')] = _POSITIONS
    assert _short_size(broker) == 2.0
    with pytest.raises(BracketAttachAfterFillRejectedError):
        asyncio.run(broker.execute_exit(_exit_envelope()))
    row = ctx.get_order(coid)
    assert row is not None
    assert not broker._materialising(row)
    store.close()


def __test_defensive_close_on_the_working_order_id_closes_the_position__(tmp_path):
    """A DELETE on the confirm's ``dealId`` follows the back-link to the position."""
    broker, store, ctx = _make_broker(tmp_path, {
        ('history/activity', 'get'): _ROLLOVER_ACTIVITIES,
        ('positions/pos-3', 'delete'): {'dealReference': 'p_pos-3'},
    })
    coid = _enter_third_leg(broker)
    asyncio.run(broker.execute_close(_defensive_close(coid, 'wo-3')))
    assert ('positions/pos-3', 'delete') in {(c[0], c[1]) for c in broker._calls}
    assert 'close_already_gone' not in _event_kinds(ctx)
    row = ctx.get_order(coid)
    assert row is not None and row.exchange_order_id == 'pos-3' and row.state == 'closing'
    store.close()


def __test_the_position_activity_rekeys_a_market_entry_row__(tmp_path):
    """The activity poll moves the row onto the position its ``POSITION`` activity names.

    A defensive close cut for the confirm's ``dealId`` before the move still
    finds its target and closes the position's deal.
    """
    broker, store, ctx = _make_broker(tmp_path, {
        ('positions/pos-3', 'delete'): {'dealReference': 'p_pos-3'},
    })
    coid = _enter_third_leg(broker)

    async def poll():
        async for _ in broker._process_activity(list(_ROLLOVER_ACTIVITIES['activities'])):
            pass

    asyncio.run(poll())
    row = ctx.get_order(coid)
    assert row is not None and row.exchange_order_id == 'pos-3'

    asyncio.run(broker.execute_close(_defensive_close(coid, 'wo-3')))
    assert ('positions/pos-3', 'delete') in {(c[0], c[1]) for c in broker._calls}
    assert 'orphan_close_exact_target_missing' not in _event_kinds(ctx)
    store.close()
