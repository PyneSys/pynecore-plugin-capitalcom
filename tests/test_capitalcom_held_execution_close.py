"""
@pyne

``held_execution_close`` contract tests.

When an entry's execution evidence never arrives (the engine's fill-evidence
hold expired) and the strategy asks to close or reverse the exposure over it,
the engine reads the venue position and sends a symbol-wide
``CloseIntent(synthetic_kind='held_execution_close', qty=|venue position|)``.
The plugin declares ``supports_held_execution_close``:

* **Ownership.** A deal whose ``dealId`` or ``workingOrderId`` is an order of an
  entry the engine holds (its stored fill-evidence hold or held-execution
  close) is the run's own, also without a live journal row: ``get_position``
  counts it, ``fetch_raw_positions`` lists it on a hedging account. A held
  take-over's deal is owned too, but no close reaches it before the bot has
  proven its fill; a foreign deal stays foreign.
* **Netting execution.** The close DELETEs whole own deals of the reducing
  side, oldest first, summing to at most ``intent.qty``; a deal already
  closing counts against it and is not sent again; one that does not fit
  stays open. Nothing is sent when no deal fits. A foreign deal is never
  touched. A held deal no live row tracks gets a seeded row (no booked
  exposure) before its DELETE.
* **Fills.** The close's fills carry the close's client order id and Pine id
  on the activity path and on the disappearance path, after a restart too,
  and wait behind a fill of the deal still owed.
* **Hedging.** A core leg close of a held deal no row tracks seeds the row
  before its DELETE, so its fill reports the leg close's ``coid``.
"""
import asyncio
from datetime import datetime, timezone

import pytest

from pynecore.core.broker.exceptions import (
    ExchangeConnectionError,
    OrderDispositionUnknownError,
    OrderSkippedByPlugin,
)
from pynecore.core.broker.idempotency import KIND_CLOSE
from pynecore.core.broker.models import CloseIntent, DispatchEnvelope, LegType
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import (
    ENTRY_FILL_EVIDENCE_HOLD_EVENT_KIND,
    HELD_EXECUTION_CLOSE_EVENT_KIND,
    BrokerStore,
)
from pynecore.core.broker.store_helpers import KIND_FULL_CLOSE, STATE_DISPOSITION_UNKNOWN

from pynecore_capitalcom import CapitalCom, CapitalComConfig
from pynecore_capitalcom.exceptions import OrderNotFoundError
from pynecore_capitalcom.helpers import (
    EXTRAS_KEY_CLOSE_LEG_DISPATCHED,
    EXTRAS_KEY_ENTRY_PRICE_PENDING,
    EXTRAS_KEY_HELD_CLOSE,
    EXTRAS_KEY_HELD_CLOSE_SEEDED,
    EXTRAS_KEY_TAKE_OVER_UNPROVEN,
)


def main():
    pass


__test_helper_RULES = {
    'dealingRules': {
        'minStepDistance': {'value': 0.01},
        'minSizeIncrement': {'value': 0.01},
        'minDealSize': {'value': 0.01},
        'minNormalStopOrLimitDistance': {'value': 0.0001},
    },
    'instrument': {'lotSize': 0.01},
    'snapshot': {'bid': 1.2000, 'offer': 1.2002},
}

__test_helper_PINE_ID = '__pyne_held_execution_close__0'


class __test_helper_Broker(CapitalCom):
    """A Capital.com plugin answering REST calls from canned responses."""

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
        if isinstance(value, list):
            if not value:
                return {}
            return value.pop(0)
        return value

    def _call_serialized_write(self, endpoint, *, data=None, method='post'):
        return self._fake_response(endpoint, data=data, method=method)

    async def _call(self, endpoint, *, data=None, method='post'):
        return self._fake_response(endpoint, data=data, method=method)


def __test_helper_config():
    return CapitalComConfig(demo=True, user_email='test@example.com',
                            api_key='k', api_password='p')


def __test_helper_make(tmp_path, *, responses=None, hedged=False):
    resp: dict = {('markets/EURUSD', 'get'): __test_helper_RULES}
    resp.update(responses or {})
    broker = __test_helper_Broker(config=__test_helper_config(), responses=resp)
    broker.symbol = 'EURUSD'
    broker._account_id = 'capitalcom-demo-test-account'
    if hedged:
        broker._hedging_enabled = True
        broker.position_port = broker
    store = BrokerStore(tmp_path / 'broker.sqlite', plugin_name=broker.plugin_name)
    ctx = store.open_run(
        RunIdentity(strategy_id='test', symbol='EURUSD', timeframe='60',
                    account_id='acc', label=None),
        script_source='// test',
    )
    broker.store_ctx = ctx
    return broker, store, ctx


def __test_helper_position(deal_id, size, *, direction='BUY', created='2026-11-26T10:00:00.000',
                           working_order_id=None):
    position = {'dealId': deal_id, 'direction': direction, 'size': size,
                'level': 1.1, 'upl': 0.0, 'createdDateUTC': created}
    if working_order_id is not None:
        position['workingOrderId'] = working_order_id
    return {'market': {'epic': 'EURUSD'}, 'position': position}


def __test_helper_hold(ctx, *order_ids, key='HeldLong\x00'):
    ctx.replace_keyed_events(
        ENTRY_FILL_EVIDENCE_HOLD_EVENT_KIND,
        {key: {'order_ids': list(order_ids), 'since_ms': 1, 'expired': True}},
    )


def __test_helper_own_row(ctx, coid, deal_id, size, *, state='confirmed', pine_id='Long',
                          extras=None):
    ctx.upsert_order(coid, symbol='EURUSD', side='buy', qty=size, filled_qty=size,
                     state=state, pine_entry_id=pine_id, from_entry=pine_id,
                     exchange_order_id=deal_id,
                     extras={'kind': 'position', 'entry_filled_at': 1.0, **(extras or {})})
    ctx.add_ref(coid, 'deal_id', deal_id)


def __test_helper_envelope(qty, *, side='sell'):
    return DispatchEnvelope(
        intent=CloseIntent(
            pine_id=__test_helper_PINE_ID, symbol='EURUSD', side=side, qty=qty,
            immediately=True, synthetic_kind='held_execution_close',
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )


def __test_helper_writes(broker):
    return [call for call in broker._calls if call[1] in ('delete', 'post', 'put')]


def __test_helper_close_activity(deal_id, size):
    return {
        'dateUTC': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3],
        'dealId': deal_id, 'type': 'POSITION', 'status': 'EXECUTED', 'source': 'USER',
        'epic': 'EURUSD',
        'details': {'direction': 'SELL', 'size': size, 'level': 1.2},
    }


async def __test_helper_drain(agen):
    return [event async for event in agen]


def __test_helper_event_kinds(ctx, coid):
    rows = ctx._store._conn.execute(
        "SELECT kind FROM events WHERE run_instance_id = ? AND client_order_id = ? "
        "ORDER BY id", (ctx.run_instance_id, coid),
    )
    return [row['kind'] for row in rows]


# --- Capability ------------------------------------------------------------


def __test_plugin_declares_held_execution_close__():
    assert CapitalCom.supports_held_execution_close is True


# --- Ownership of the held order's deal --------------------------------------


def __test_get_position_counts_the_held_orders_deal_without_a_live_row__(tmp_path):
    """The held entry's working order executed into ``deal-H`` and no live
    row tracks it: once the engine's stored hold names the order, the netting
    position counts the deal; the foreign deal stays out."""
    broker, store, ctx = __test_helper_make(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            __test_helper_position('deal-H', 0.5, working_order_id='wo-H'),
            __test_helper_position('deal-F', 2.0),
        ]},
    })
    __test_helper_own_row(ctx, 'coid-a', 'deal-A', 1.0)
    assert asyncio.run(broker.get_position('EURUSD')) is None
    __test_helper_hold(ctx, 'wo-H')
    position = asyncio.run(broker.get_position('EURUSD'))
    assert position is not None
    assert position.side == 'long' and abs(position.size - 0.5) < 1e-12
    store.close()


def __test_held_close_record_and_deal_id_both_identify_the_deal__(tmp_path):
    """The stored held-execution close names the held orders too, and an
    order id that is the deal's own ``dealId`` (the confirm named it)
    identifies it as well."""
    broker, store, ctx = __test_helper_make(tmp_path)
    ctx.replace_keyed_events(HELD_EXECUTION_CLOSE_EVENT_KIND, {'close': {
        'pine_ids': [__test_helper_PINE_ID], 'order_ids': ['deal-H'], 'hold_keys': ['H'],
        'request': 'close_all', 'venue_signed': 0.5, 'qty': 0.5, 'sent_ms': 1,
    }})
    rows = [__test_helper_position('deal-H', 0.5), __test_helper_position('deal-F', 1.0)]
    assert broker._owned_deal_entries(rows) == {'deal-H': None}
    store.close()


def __test_held_order_does_not_override_a_live_rows_entry__(tmp_path):
    """A deal a live row already ties to its entry keeps that entry when a
    hold names its working order too."""
    broker, store, ctx = __test_helper_make(tmp_path)
    __test_helper_own_row(ctx, 'coid-a', 'deal-A', 1.0, pine_id='A')
    __test_helper_hold(ctx, 'wo-A')
    rows = [__test_helper_position('deal-A', 1.0, working_order_id='wo-A')]
    assert broker._owned_deal_entries(rows) == {'deal-A': 'A'}
    store.close()


def __test_held_take_over_deal_is_owned_but_no_close_reaches_it_before_its_proof__(tmp_path):
    """A take-over held because its fill could not be proven came from the
    bot's own order: its deal is owned (unattributed), also when a hold names
    it. Until the bot has booked its fill from the venue's history, neither a
    ``held_execution_close`` nor a core leg close sends anything to it."""
    position = __test_helper_position('deal-T', 0.5, working_order_id='wo-T')
    broker, store, ctx = __test_helper_make(tmp_path, responses={
        ('positions', 'get'): {'positions': [position]},
    })
    ctx.upsert_order('coid-t', symbol='EURUSD', side='buy', qty=1.0, state='confirmed',
                     pine_entry_id='T', exchange_order_id='wo-T',
                     extras={'kind': 'working',
                             EXTRAS_KEY_TAKE_OVER_UNPROVEN: {'deal_id': 'deal-T', 'size': 0.5}})
    __test_helper_hold(ctx, 'wo-T', 'deal-T')
    assert broker._owned_deal_entries([position]) == {'deal-T': None}
    with pytest.raises(OrderSkippedByPlugin) as skipped:
        asyncio.run(broker.execute_close(__test_helper_envelope(0.5)))
    assert skipped.value.reason == 'nothing_to_close'
    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.close_leg('EURUSD', 'deal-T', 50, 'leg-coid'))
    assert __test_helper_writes(broker) == []
    store.close()


def __test_hedging_fetch_raw_positions_lists_the_held_deal__(tmp_path):
    """On a hedging account the core emulator closes the legs this read
    lists: the held deal is one, unattributed; the foreign one is not."""
    broker, store, ctx = __test_helper_make(tmp_path, hedged=True, responses={
        ('positions', 'get'): {'positions': [
            __test_helper_position('deal-H', 0.5, working_order_id='wo-H'),
            __test_helper_position('deal-F', 2.0, direction='SELL'),
        ]},
    })
    __test_helper_hold(ctx, 'wo-H')
    legs = asyncio.run(broker.fetch_raw_positions('EURUSD'))
    assert [(leg.leg_id, leg.entry_id) for leg in legs] == [('deal-H', None)]
    store.close()


# --- Netting execution -------------------------------------------------------


def __test_held_close_deletes_own_deals_with_the_held_one_and_never_a_foreign__(tmp_path):
    """The engine read 1.5 (own ``deal-A`` 1.0 plus the held ``deal-H`` 0.5):
    both are DELETEd whole, the foreign BUY deal is not touched, the held deal
    gets a seeded row that books no exposure, and every target carries the
    close's label before the DELETE."""
    broker, store, ctx = __test_helper_make(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            __test_helper_position('deal-F', 2.0, created='2026-11-26T09:00:00.000'),
            __test_helper_position('deal-A', 1.0, created='2026-11-26T10:00:00.000'),
            __test_helper_position('deal-H', 0.5, created='2026-11-26T10:05:00.000',
                                   working_order_id='wo-H'),
        ]},
        ('positions/deal-A', 'delete'): {},
        ('positions/deal-H', 'delete'): {},
    })
    __test_helper_own_row(ctx, 'coid-a', 'deal-A', 1.0)
    __test_helper_hold(ctx, 'wo-H')
    env = __test_helper_envelope(1.5)
    result = asyncio.run(broker.execute_close(env))
    coid = env.client_order_id(KIND_CLOSE)

    assert __test_helper_writes(broker) == [
        ('positions/deal-A', 'delete', None),
        ('positions/deal-H', 'delete', None),
    ]
    assert result.id == 'deal-A' and abs(result.filled_qty - 1.5) < 1e-12
    command = ctx.get_order(coid)
    assert command is not None
    assert (command.extras or {}).get('kind') == KIND_FULL_CLOSE
    assert (command.extras or {}).get('targets') == ['deal-A', 'deal-H']
    seeded = ctx.find_by_ref('deal_id', 'deal-H')
    assert seeded is not None and seeded.state == 'closing'
    assert seeded.filled_qty == 0.0 and abs(seeded.qty - 0.5) < 1e-12
    assert (seeded.extras or {}).get(EXTRAS_KEY_HELD_CLOSE_SEEDED) is not None
    for row_coid, size in (('coid-a', 1.0), (seeded.client_order_id, 0.5)):
        row = ctx.get_order(row_coid)
        assert row is not None and row.state == 'closing'
        assert (row.extras or {}).get(EXTRAS_KEY_HELD_CLOSE) == {
            'coid': coid, 'pine_id': __test_helper_PINE_ID, 'size': size}
        assert (row.extras or {}).get(EXTRAS_KEY_CLOSE_LEG_DISPATCHED) == coid
    store.close()


def __test_held_close_never_deletes_more_than_its_quantity__(tmp_path):
    """The venue holds more own exposure than the engine read (a deal joined
    after the read): the oldest deals that fit go, the rest stays open."""
    broker, store, ctx = __test_helper_make(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            __test_helper_position('deal-A', 1.0, created='2026-11-26T10:00:00.000'),
            __test_helper_position('deal-B', 1.0, created='2026-11-26T10:01:00.000'),
        ]},
        ('positions/deal-A', 'delete'): {},
    })
    __test_helper_own_row(ctx, 'coid-a', 'deal-A', 1.0)
    __test_helper_own_row(ctx, 'coid-b', 'deal-B', 1.0)
    asyncio.run(broker.execute_close(__test_helper_envelope(1.0)))
    assert __test_helper_writes(broker) == [('positions/deal-A', 'delete', None)]
    row = ctx.get_order('coid-b')
    assert row is not None and row.state == 'confirmed'
    store.close()


def __test_held_close_sends_nothing_when_no_deal_fits_whole__(tmp_path):
    """A 0.5 close over a single 1.0 deal: a whole-deal DELETE would close
    more than the engine read, and a reduce POST could net someone else's
    deal or open the other side. Nothing is sent."""
    broker, store, ctx = __test_helper_make(tmp_path, responses={
        ('positions', 'get'): {'positions': [__test_helper_position('deal-A', 1.0)]},
    })
    __test_helper_own_row(ctx, 'coid-a', 'deal-A', 1.0)
    with pytest.raises(OrderSkippedByPlugin) as skipped:
        asyncio.run(broker.execute_close(__test_helper_envelope(0.5)))
    assert skipped.value.reason == 'held_close_exceeds_qty'
    assert __test_helper_writes(broker) == []
    store.close()


def __test_held_close_counts_an_in_flight_close_and_does_not_resend_it__(tmp_path):
    """``deal-A`` is already closing: it counts against the quantity and is
    not sent again; alone it leaves nothing to send."""
    broker, store, ctx = __test_helper_make(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            __test_helper_position('deal-A', 1.0, created='2026-11-26T10:00:00.000'),
            __test_helper_position('deal-B', 1.0, created='2026-11-26T10:01:00.000'),
        ]},
        ('positions/deal-B', 'delete'): {},
    })
    __test_helper_own_row(ctx, 'coid-a', 'deal-A', 1.0, state='closing')
    __test_helper_own_row(ctx, 'coid-b', 'deal-B', 1.0)
    with pytest.raises(OrderSkippedByPlugin) as skipped:
        asyncio.run(broker.execute_close(__test_helper_envelope(1.0)))
    assert skipped.value.reason == 'held_close_exceeds_qty'
    assert __test_helper_writes(broker) == []
    asyncio.run(broker.execute_close(__test_helper_envelope(2.0)))
    assert __test_helper_writes(broker) == [('positions/deal-B', 'delete', None)]
    store.close()


def __test_held_close_with_every_deal_closing_is_already_in_flight__(tmp_path):
    broker, store, ctx = __test_helper_make(tmp_path, responses={
        ('positions', 'get'): {'positions': [__test_helper_position('deal-A', 1.0)]},
    })
    __test_helper_own_row(ctx, 'coid-a', 'deal-A', 1.0, state='closing')
    with pytest.raises(OrderSkippedByPlugin) as skipped:
        asyncio.run(broker.execute_close(__test_helper_envelope(1.0)))
    assert skipped.value.reason == 'close_already_in_flight'
    store.close()


def __test_held_close_without_own_deals_sends_nothing__(tmp_path):
    """Only a foreign deal is on the reducing side: nothing is closed."""
    broker, store, ctx = __test_helper_make(tmp_path, responses={
        ('positions', 'get'): {'positions': [__test_helper_position('deal-F', 1.0)]},
    })
    with pytest.raises(OrderSkippedByPlugin) as skipped:
        asyncio.run(broker.execute_close(__test_helper_envelope(1.0)))
    assert skipped.value.reason == 'nothing_to_close'
    assert __test_helper_writes(broker) == []
    store.close()


def __test_held_close_targets_the_working_row_the_deal_back_links__(tmp_path):
    """The held entry's working row executed (its fill is queued for its
    price) and the position back-links it: that row is the target, no row is
    seeded, and the not-found DELETE on the order id moves the row onto the
    position before the DELETE goes there."""
    snapshot = {'positions': [
        __test_helper_position('deal-W', 0.5, working_order_id='wo-W'),
    ]}
    broker, store, ctx = __test_helper_make(tmp_path, responses={
        ('positions', 'get'): snapshot,
        ('error', 'positions/wo-W', 'delete'): OrderNotFoundError(
            'error.not-found.dealId', ref_type='deal_id'),
        ('positions/deal-W', 'delete'): {},
        ('history/activity', 'get'): {'activities': []},
    })
    ctx.upsert_order('coid-w', symbol='EURUSD', side='buy', qty=0.5, state='confirmed',
                     pine_entry_id='W', from_entry='W', exchange_order_id='wo-W',
                     extras={'kind': 'working',
                             EXTRAS_KEY_ENTRY_PRICE_PENDING: '2026-11-26T10:00:00.000'})
    ctx.add_ref('coid-w', 'deal_id', 'wo-W')
    __test_helper_hold(ctx, 'wo-W')
    asyncio.run(broker.execute_close(__test_helper_envelope(0.5)))
    assert __test_helper_writes(broker) == [
        ('positions/wo-W', 'delete', None),
        ('positions/deal-W', 'delete', None),
    ]
    row = ctx.get_order('coid-w')
    assert row is not None and row.exchange_order_id == 'deal-W' and row.state == 'closing'
    owner = ctx.find_by_ref('deal_id', 'deal-W')
    assert owner is not None and owner.client_order_id == 'coid-w'
    assert ctx.get_order('__pyne_held_close__EURUSD__deal-W') is None
    store.close()


# --- Fills of the close ------------------------------------------------------


def __test_helper_closed_held_and_own(tmp_path):
    broker, store, ctx = __test_helper_make(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            __test_helper_position('deal-A', 1.0, created='2026-11-26T10:00:00.000'),
            __test_helper_position('deal-H', 0.5, created='2026-11-26T10:05:00.000',
                                   working_order_id='wo-H'),
        ]},
        ('positions/deal-A', 'delete'): {},
        ('positions/deal-H', 'delete'): {},
    })
    __test_helper_own_row(ctx, 'coid-a', 'deal-A', 1.0)
    __test_helper_hold(ctx, 'wo-H')
    env = __test_helper_envelope(1.5)
    asyncio.run(broker.execute_close(env))
    return broker, store, ctx, env.client_order_id(KIND_CLOSE)


def __test_held_close_activity_fills_carry_the_close_coid_and_pine_id__(tmp_path):
    broker, store, ctx, coid = __test_helper_closed_held_and_own(tmp_path)
    events = asyncio.run(__test_helper_drain(broker._process_activity([
        __test_helper_close_activity('deal-A', 1.0),
        __test_helper_close_activity('deal-H', 0.5),
    ])))
    assert [(ev.order.id, ev.leg_type, ev.event_type) for ev in events] == [
        ('deal-A', LegType.CLOSE, 'filled'), ('deal-H', LegType.CLOSE, 'filled')]
    for event, size in zip(events, (1.0, 0.5)):
        assert event.order.client_order_id == coid
        assert event.pine_id == __test_helper_PINE_ID
        assert abs((event.fill_qty or 0.0) - size) < 1e-12
        assert abs((event.journal_fill_qty or 0.0) - size) < 1e-12
    seeded = ctx.get_order('__pyne_held_close__EURUSD__deal-H')
    assert seeded is not None
    for row_coid in ('coid-a', seeded.client_order_id):
        kinds = __test_helper_event_kinds(ctx, row_coid)
        assert 'own_close_activity_attributed' in kinds
        assert 'manual_close_report_pending' not in kinds
    store.close()


def __test_held_close_fill_after_a_restart_keeps_its_labels__(tmp_path):
    """A later process, with nothing of the send in memory, reads the labels
    from the journal."""
    _, store, ctx, coid = __test_helper_closed_held_and_own(tmp_path)
    restarted = __test_helper_Broker(config=__test_helper_config(), responses={
        ('markets/EURUSD', 'get'): __test_helper_RULES,
    })
    restarted.symbol = 'EURUSD'
    restarted.store_ctx = ctx
    events = asyncio.run(__test_helper_drain(restarted._process_activity([
        __test_helper_close_activity('deal-H', 0.5)])))
    assert [(ev.order.client_order_id, ev.pine_id) for ev in events] == [
        (coid, __test_helper_PINE_ID)]
    store.close()


def __test_held_close_with_a_lost_answer_parks_and_keeps_its_labels__(tmp_path):
    """The DELETE's answer is lost: the close parks with the targets in its
    command row for the restart recovery, and the labels written before the
    DELETE left still name the close's fill."""
    broker, store, ctx = __test_helper_make(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            __test_helper_position('deal-H', 0.5, working_order_id='wo-H')]},
        ('error', 'positions/deal-H', 'delete'): ExchangeConnectionError('drop'),
    })
    __test_helper_hold(ctx, 'wo-H')
    env = __test_helper_envelope(0.5)
    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_close(env))
    coid = env.client_order_id(KIND_CLOSE)
    command = ctx.get_order(coid)
    assert command is not None and command.state == STATE_DISPOSITION_UNKNOWN
    assert (command.extras or {}).get('targets') == ['deal-H']
    events = asyncio.run(__test_helper_drain(broker._process_activity([
        __test_helper_close_activity('deal-H', 0.5)])))
    assert [(ev.order.client_order_id, ev.pine_id) for ev in events] == [
        (coid, __test_helper_PINE_ID)]
    store.close()


def __test_restart_recovers_a_parked_held_close_like_any_full_close__(tmp_path):
    """The process stops after the lost answer; the deal is gone at the
    restart. The recovery settles the parked close against the positions
    snapshot like any full close, and the seeded row is retired with the
    startup retirement of a vanished deal: a fresh process adopts the venue's
    book, so no close fill is synthesized for it (the engine reads the venue
    again before it sends anything over the held entry)."""
    broker, store, ctx = __test_helper_make(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            __test_helper_position('deal-H', 0.5, working_order_id='wo-H')]},
        ('error', 'positions/deal-H', 'delete'): ExchangeConnectionError('drop'),
    })
    __test_helper_hold(ctx, 'wo-H')
    env = __test_helper_envelope(0.5)
    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_close(env))
    coid = env.client_order_id(KIND_CLOSE)
    restarted = __test_helper_Broker(config=__test_helper_config(), responses={
        ('markets/EURUSD', 'get'): __test_helper_RULES,
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
    })
    restarted.symbol = 'EURUSD'
    restarted._account_id = 'capitalcom-demo-test-account'
    restarted.store_ctx = ctx
    asyncio.run(restarted._recover_in_flight_submissions())
    command = ctx.get_order(coid)
    assert command is not None and command.state == 'closing'
    assert (command.extras or {}).get('recovery_path') == 'full_close_targets_vanished'
    seeded = ctx.get_order('__pyne_held_close__EURUSD__deal-H')
    assert seeded is not None and seeded.closed_ts_ms is not None
    assert 'startup_orphan_retired' in __test_helper_event_kinds(ctx, seeded.client_order_id)
    events = asyncio.run(__test_helper_drain(restarted._reconcile_snapshot({}, {})))
    assert [ev for ev in events if ev.leg_type == LegType.CLOSE] == []
    store.close()


def __test_vanished_held_close_fill_carries_its_labels_and_venue_size__(tmp_path):
    """No activity is published for the DELETEs: the close fills booked from
    the deals' disappearance carry the close's coid and Pine id, and the
    seeded row's fill is the deal's venue size, not its zero journal size."""
    broker, store, ctx, coid = __test_helper_closed_held_and_own(tmp_path)
    events = asyncio.run(__test_helper_drain(broker._reconcile_snapshot({}, {})))
    fills = sorted((ev.order.id, ev.order.client_order_id, ev.pine_id, ev.fill_qty)
                   for ev in events if ev.leg_type == LegType.CLOSE)
    assert fills == [
        ('deal-A', coid, __test_helper_PINE_ID, 1.0),
        ('deal-H', coid, __test_helper_PINE_ID, 0.5),
    ]
    store.close()


def __test_other_close_of_a_labelled_row_keeps_the_rows_labels__(tmp_path):
    """A close the venue made itself (a stop-loss) of a row the held close
    labelled is not that close's fill."""
    broker, store, ctx, coid = __test_helper_closed_held_and_own(tmp_path)
    stop = __test_helper_close_activity('deal-A', 1.0)
    stop['source'] = 'SL'
    events = asyncio.run(__test_helper_drain(broker._process_activity([stop])))
    assert events and all(ev.order.client_order_id != coid for ev in events)
    assert all(ev.pine_id == 'Long' for ev in events)
    store.close()


def __test_held_close_fill_waits_behind_the_deals_queued_entry__(tmp_path):
    """The held deal's entry is queued for its price: the close of the deal is
    not delivered before it, so the engine never books the close ahead of
    the entry it closes."""
    broker, store, ctx = __test_helper_make(tmp_path, responses={
        ('positions', 'get'): {'positions': [__test_helper_position('deal-H', 0.5)]},
        ('positions/deal-H', 'delete'): {},
    })
    ctx.upsert_order('coid-h', symbol='EURUSD', side='buy', qty=0.5, state='confirmed',
                     pine_entry_id='H', from_entry='H', exchange_order_id='deal-H',
                     extras={'kind': 'position',
                             EXTRAS_KEY_ENTRY_PRICE_PENDING: '2026-11-26T10:00:00.000'})
    ctx.add_ref('coid-h', 'deal_id', 'deal-H')
    __test_helper_hold(ctx, 'deal-H')
    asyncio.run(broker.execute_close(__test_helper_envelope(0.5)))
    events = asyncio.run(__test_helper_drain(broker._process_activity([
        __test_helper_close_activity('deal-H', 0.5)])))
    assert events == []
    assert 'activity_held_entry_price_pending' in __test_helper_event_kinds(ctx, 'coid-h')
    store.close()


# --- Hedging: the core leg close of a held deal ------------------------------


def __test_close_leg_seeds_the_row_of_a_held_deal_and_reports_its_coid__(tmp_path):
    broker, store, ctx = __test_helper_make(tmp_path, hedged=True, responses={
        ('positions', 'get'): {'positions': [
            __test_helper_position('deal-H', 0.5, working_order_id='wo-H')]},
        ('positions/deal-H', 'delete'): {},
    })
    __test_helper_hold(ctx, 'wo-H')
    asyncio.run(broker.get_volume_quantizer('EURUSD'))
    asyncio.run(broker.close_leg('EURUSD', 'deal-H', 50, 'pcl:held:deal-H'))
    assert __test_helper_writes(broker) == [('positions/deal-H', 'delete', None)]
    seeded = ctx.find_by_ref('deal_id', 'deal-H')
    assert seeded is not None and seeded.state == 'closing' and seeded.filled_qty == 0.0
    events = asyncio.run(__test_helper_drain(broker._process_activity([
        __test_helper_close_activity('deal-H', 0.5)])))
    assert [(ev.leg_type, ev.order.client_order_id) for ev in events] == [
        (LegType.CLOSE, 'pcl:held:deal-H')]
    store.close()


def __test_close_leg_of_an_untracked_deal_no_hold_names_seeds_nothing__(tmp_path):
    """Without a hold the untracked leg stays as it was: no read, no row."""
    broker, store, ctx = __test_helper_make(tmp_path, hedged=True, responses={
        ('positions/deal-X', 'delete'): {},
    })
    asyncio.run(broker.close_leg('EURUSD', 'deal-X', 50, 'pcl:x:deal-X'))
    assert ('positions', 'get', None) not in broker._calls
    assert ctx.find_by_ref('deal_id', 'deal-X') is None
    store.close()
