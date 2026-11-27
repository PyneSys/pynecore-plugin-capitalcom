"""
@pyne

The bot gets out of its own trouble: order-history reads and self-repairs.

* ``fetch_order_execution`` answers from ``GET /history/activity`` (24 h,
  ``detailed=true``): an entry's opening activity is its execution, returned
  as the stream's fill under the stream's fill id; a working order the venue
  ended without an execution is a known zero; a partial close's execution is
  the close its own-close record explains. The venue reports no commission
  (``fee_reported=False``) and nothing ever proves an order absent
  (``order=None`` is never answered).
* A stream copy of an entry the engine already booked from that read is not
  yielded again, but still settles the row.
* A take-over held for its unproven fill is proven from the deal's opening
  activity (or, past the history, from a stable ``/positions`` size), booked,
  and the deal's later closes are queued for the activity poll.
* The ``ENTRY_BLOCK`` causes the plugin latches end by themselves: the
  residual deal is closed by the bot, the netting of a foreign deal ends with
  the order's accounted execution.
"""
import asyncio
import time
from datetime import datetime, timedelta, timezone

from pynecore.core.broker.models import LegType, OrderStatus, QuarantineKind
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore, FillReceipt
from pynecore.core.broker.store_helpers import KIND_PARTIAL_CLOSE

from pynecore_capitalcom import CapitalCom, CapitalComConfig
from pynecore_capitalcom.delivery import queued_fills
from pynecore_capitalcom.helpers import (
    EXTRAS_KEY_BOOKED_CLOSES,
    EXTRAS_KEY_ENTRY_PRICE_PENDING,
    EXTRAS_KEY_OWN_CLOSES,
    EXTRAS_KEY_TAKE_OVER_UNPROVEN,
    EXTRAS_KEY_TAKEN_OVER_AT,
    _netted_foreign_deal_cause,
    _residual_deal_cause,
)
from pynecore_capitalcom.models import _activity_fingerprint


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


def __test_helper_make(tmp_path, *, responses=None):
    resp: dict = {('markets/EURUSD', 'get'): __test_helper_RULES}
    resp.update(responses or {})
    broker = __test_helper_Broker(
        config=CapitalComConfig(demo=True, user_email='test@example.com',
                                api_key='k', api_password='p'),
        responses=resp,
    )
    broker.symbol = 'EURUSD'
    broker._account_id = 'capitalcom-demo-test-account'
    store = BrokerStore(tmp_path / 'broker.sqlite', plugin_name=broker.plugin_name)
    ctx = store.open_run(
        RunIdentity(strategy_id='test', symbol='EURUSD', timeframe='60',
                    account_id='acc', label=None),
        script_source='// test',
    )
    broker.store_ctx = ctx
    latched: list = []
    released: list = []
    broker.quarantine_sink = (lambda reason, context, *, kind, cause=None:
                              latched.append((reason, kind, cause)))
    broker.quarantine_release_sink = (lambda cause, resolution:
                                      released.append(cause) is None)
    return broker, store, ctx, latched, released


def __test_helper_utc(offset_s=0.0):
    """A venue ``dateUTC`` ``offset_s`` seconds from now."""
    at = datetime.now(timezone.utc) + timedelta(seconds=offset_s)
    return at.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]


def __test_helper_activity(deal_id, *, direction='BUY', size=1.0, level=1.1, offset_s=-60.0,
                           activity_type='POSITION', status='ACCEPTED', source='USER',
                           working_order_id=None):
    details: dict = {'direction': direction, 'size': size, 'level': level}
    if working_order_id is not None:
        details['workingOrderId'] = working_order_id
    return {'dateUTC': __test_helper_utc(offset_s), 'dealId': deal_id, 'epic': 'EURUSD',
            'type': activity_type, 'status': status, 'source': source, 'details': details}


def __test_helper_position(deal_id, size, *, direction='BUY', level=1.1, created=None,
                           working_order_id=None):
    position = {'dealId': deal_id, 'direction': direction, 'size': size, 'level': level,
                'upl': 0.0, 'createdDateUTC': created or __test_helper_utc(-120.0)}
    if working_order_id is not None:
        position['workingOrderId'] = working_order_id
    return {'market': {'epic': 'EURUSD'}, 'position': position}


def __test_helper_history(broker, *activities):
    broker._responses[('history/activity', 'get')] = {'activities': list(activities)}


def __test_helper_history_reads(broker):
    return [call for call in broker._calls if call[:2] == ('history/activity', 'get')]


def __test_helper_writes(broker):
    return [call for call in broker._calls if call[1] in ('delete', 'post', 'put')]


def __test_helper_entry_row(ctx, coid='coid-l', deal_id='deal-L', *, qty=1.0, state='confirmed',
                            extras=None, ref=True):
    ctx.upsert_order(coid, symbol='EURUSD', side='buy', qty=qty, filled_qty=0.0,
                     state=state, pine_entry_id='L', from_entry=None,
                     exchange_order_id=deal_id,
                     extras={'kind': 'position', **(extras or {})})
    if ref and deal_id is not None:
        ctx.add_ref(coid, 'deal_id', deal_id)


async def __test_helper_drain(agen):
    return [event async for event in agen]


def __test_helper_record_resolutions(ctx):
    """Record every park resolution the plugin writes, keeping the write."""
    recorded: list = []
    original = ctx.record_resolution

    def record(coid, resolution):
        recorded.append((coid, resolution))
        original(coid, resolution)

    ctx.record_resolution = record
    return recorded


def __test_helper_events(ctx, kind):
    return [payload for _, _, _, payload in ctx.iter_events_by_kind_for_run_id(kind)]


# --- fetch_order_execution: entries ------------------------------------------


def __test_market_entry_execution_is_its_opening_activity_with_the_streams_fill_id__(
        tmp_path):
    """The deal's opening activity is the order's execution: ``FILLED`` with the
    opened size at its level, the fill under the activity fingerprint the stream
    books it with, no commission reported."""
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_entry_row(ctx)
    opening = __test_helper_activity('deal-L', size=1.0, level=1.1234)
    close = __test_helper_activity('deal-L', direction='SELL', size=1.0, level=1.2,
                                   offset_s=-30.0)
    __test_helper_history(broker, opening, close)

    execution = asyncio.run(broker.fetch_order_execution('deal-L', None))

    assert execution is not None and execution.fee_reported is False
    order = execution.order
    assert order is not None and order.status is OrderStatus.FILLED
    assert order.filled_qty == 1.0 and order.filled_qty_known
    assert order.average_fill_price == 1.1234 and order.client_order_id == 'coid-l'
    assert execution.fills is not None and len(execution.fills) == 1
    fill = execution.fills[0]
    assert fill.fill_id == _activity_fingerprint(opening)
    assert fill.leg_type is LegType.ENTRY and fill.fill_qty == 1.0
    assert fill.journal_fill_qty == 1.0 and fill.fill_price == 1.1234
    assert __test_helper_history_reads(broker)[0][2] == {'lastPeriod': 86400,
                                                         'detailed': 'true'}
    assert __test_helper_writes(broker) == []
    store.close()


def __test_client_order_id_alone_finds_the_entry__(tmp_path):
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_entry_row(ctx)
    __test_helper_history(broker, __test_helper_activity('deal-L'))
    execution = asyncio.run(broker.fetch_order_execution(None, 'coid-l'))
    assert execution is not None and execution.order is not None
    assert execution.order.status is OrderStatus.FILLED
    store.close()


def __test_working_order_execution_is_the_fresh_deal_that_back_links_it__(tmp_path):
    """A working order's fill opens a fresh deal whose ``workingOrderId`` names
    the order; a deal the venue lists in the order's direction is its own."""
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_entry_row(ctx, 'coid-w', 'wo-1',
                            extras={'kind': 'working', 'order_type': 'limit'})
    broker._remember_positions_snapshot(
        {'deal-P': __test_helper_position('deal-P', 1.0, created=__test_helper_utc(-60.0),
                                          working_order_id='wo-1')},
        time.time())
    opening = __test_helper_activity('deal-P', working_order_id='wo-1', level=1.15)
    __test_helper_history(broker, opening)

    execution = asyncio.run(broker.fetch_order_execution('wo-1', None))

    assert execution is not None and execution.order is not None
    assert execution.order.id == 'wo-1' and execution.order.status is OrderStatus.FILLED
    assert execution.order.average_fill_price == 1.15
    assert execution.fills is not None
    assert execution.fills[0].fill_id == _activity_fingerprint(opening)
    store.close()


def __test_a_back_linked_deal_nothing_shows_the_order_opened_is_never_its_execution__(
        tmp_path):
    """On a netting account the order may only have netted an existing deal that
    the activity names too: a deal neither listed nor remembered, or one that
    existed before the execution, proves nothing (asked again)."""
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_entry_row(ctx, 'coid-w', 'wo-1', extras={'kind': 'working'})
    __test_helper_history(broker, __test_helper_activity('deal-X', working_order_id='wo-1'))
    assert asyncio.run(broker.fetch_order_execution('wo-1', None)) is None

    broker._remember_positions_snapshot(
        {'deal-X': __test_helper_position('deal-X', 3.0, created=__test_helper_utc(-7200.0))},
        time.time())
    assert asyncio.run(broker.fetch_order_execution('wo-1', None)) is None
    store.close()


def __test_an_opening_of_another_size_answers_nothing__(tmp_path):
    """A partial fill or a partly netted execution is the activity poll's to
    settle: the read does not guess the order's executed quantity."""
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_entry_row(ctx, qty=2.0)
    __test_helper_history(broker, __test_helper_activity('deal-L', size=1.0))
    assert asyncio.run(broker.fetch_order_execution('deal-L', None)) is None
    store.close()


def __test_a_working_order_the_venue_ended_unexecuted_is_a_known_zero__(tmp_path):
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_entry_row(ctx, 'coid-w', 'wo-1', extras={'kind': 'working'})
    __test_helper_history(broker, __test_helper_activity(
        'wo-1', activity_type='WORKING_ORDER', status='DELETED'))

    execution = asyncio.run(broker.fetch_order_execution('wo-1', None))

    assert execution is not None and execution.fills is None and not execution.fee_reported
    order = execution.order
    assert order is not None and order.status is OrderStatus.CANCELLED
    assert order.filled_qty == 0.0 and order.proven_unexecuted
    store.close()


def __test_an_ended_order_the_history_shows_executed_is_not_a_zero__(tmp_path):
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_entry_row(ctx, 'coid-w', 'wo-1', extras={'kind': 'working'})
    __test_helper_history(
        broker,
        __test_helper_activity('wo-1', activity_type='WORKING_ORDER', status='EXECUTED'),
        __test_helper_activity('wo-1', activity_type='WORKING_ORDER', status='DELETED',
                               offset_s=-10.0))
    assert asyncio.run(broker.fetch_order_execution('wo-1', None)) is None
    store.close()


def __test_no_record_answers_none_and_never_proves_an_order_absent__(tmp_path):
    """An empty history (24 h, completeness undocumented) and an order the
    journal does not know both answer ``None``: asked again, never released as
    unexecuted."""
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_entry_row(ctx)
    __test_helper_history(broker)
    assert asyncio.run(broker.fetch_order_execution('deal-L', None)) is None
    assert asyncio.run(broker.fetch_order_execution('deal-unknown', 'coid-unknown')) is None
    store.close()


def __test_an_entry_booked_in_the_journal_whose_opening_left_the_history_answers_none__(
        tmp_path):
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_entry_row(ctx, extras={'entry_filled_at': 1.0})
    __test_helper_history(broker)
    assert asyncio.run(broker.fetch_order_execution('deal-L', None)) is None
    store.close()


# --- fetch_order_execution: partial closes -----------------------------------


def __test_helper_partial_close(ctx, *, matched):
    __test_helper_entry_row(ctx, qty=2.0, extras={
        'entry_filled_at': 1.0,
        EXTRAS_KEY_OWN_CLOSES: [{'full': False, 'size': 1.0, 'sent_at': time.time() - 30.0,
                                 'done_at': time.time() - 29.0,
                                 'matched': [__test_helper_utc(-29.5)] if matched else []}],
    })
    ctx.upsert_order('coid-pc', symbol='EURUSD', side='sell', qty=1.0, state='closing',
                     pine_entry_id='L', extras={
                         'kind': KIND_PARTIAL_CLOSE, 'target_coid': 'coid-l',
                         'target_deal_id': 'deal-L',
                         'own_close_sent_at': ctx.get_order('coid-l').extras[
                             EXTRAS_KEY_OWN_CLOSES][0]['sent_at'],
                     })


def __test_a_partial_close_whose_record_matched_executed_in_full__(tmp_path):
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_partial_close(ctx, matched=True)

    execution = asyncio.run(broker.fetch_order_execution(None, 'coid-pc'))

    assert execution is not None and execution.order is not None
    assert execution.order.status is OrderStatus.FILLED and execution.order.filled_qty == 1.0
    assert execution.fills is None and not execution.fee_reported
    assert __test_helper_history_reads(broker) == []
    store.close()


def __test_a_partial_close_explained_by_a_close_activity_in_the_history__(tmp_path):
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_partial_close(ctx, matched=False)
    __test_helper_history(broker, __test_helper_activity(
        'deal-L', direction='SELL', size=1.0, level=1.2, offset_s=-29.5))

    execution = asyncio.run(broker.fetch_order_execution(None, 'coid-pc'))

    assert execution is not None and execution.order is not None
    assert execution.order.status is OrderStatus.FILLED and execution.order.filled_qty == 1.0
    store.close()


def __test_a_partial_close_no_activity_explains_answers_none__(tmp_path):
    """No close of the target fits the record: an absence proves no refusal."""
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_partial_close(ctx, matched=False)
    __test_helper_history(broker, __test_helper_activity(
        'deal-L', direction='SELL', size=0.5, level=1.2, offset_s=-29.5))
    assert asyncio.run(broker.fetch_order_execution(None, 'coid-pc')) is None
    store.close()


# --- The stream after the engine's own recovery -------------------------------


def __test_helper_engine_receipt(ctx, coid='coid-l', qty=1.0):
    ctx.record_fill_receipts([FillReceipt(
        fill_id='deal-L:venue-total', booked=True, fee=0.0, client_order_id=coid,
        order_id='deal-L', entry_id='L', qty=qty)])


def __test_a_stream_copy_of_an_entry_the_engine_recovered_settles_the_row_unyielded__(
        tmp_path):
    """The engine booked the entry from the venue's history first: the
    stream's own copy stamps the row booked and moves its exposure cursor, but
    is not yielded a second time."""
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_entry_row(ctx)
    __test_helper_engine_receipt(ctx)
    opening = __test_helper_activity('deal-L')

    events = asyncio.run(__test_helper_drain(broker._process_activity(
        [opening], {'deal-L': __test_helper_position('deal-L', 1.0)})))

    assert events == []
    row = ctx.get_order('coid-l')
    assert row is not None and row.filled_qty == 1.0
    assert (row.extras or {}).get('entry_filled_at') is not None
    assert len(__test_helper_events(ctx, 'entry_fill_recovered_by_engine')) == 1
    store.close()


def __test_without_an_engine_receipt_the_stream_yields_the_entry__(tmp_path):
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_entry_row(ctx)
    events = asyncio.run(__test_helper_drain(broker._process_activity(
        [__test_helper_activity('deal-L')], {'deal-L': __test_helper_position('deal-L', 1.0)})))
    assert [(ev.event_type, ev.leg_type) for ev in events] == [('filled', LegType.ENTRY)]
    store.close()


def __test_an_entry_queued_for_its_price_is_released_by_the_engines_booking__(tmp_path):
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_entry_row(ctx, extras={EXTRAS_KEY_ENTRY_PRICE_PENDING: __test_helper_utc(-60)})
    __test_helper_engine_receipt(ctx)

    asyncio.run(broker._settle_own_troubles({}))

    row = ctx.get_order('coid-l')
    assert row is not None and row.filled_qty == 1.0
    extras = row.extras or {}
    assert extras.get('entry_filled_at') is not None
    assert extras.get(EXTRAS_KEY_ENTRY_PRICE_PENDING) is None
    store.close()


# --- Held take-overs ---------------------------------------------------------


def __test_helper_market_take_over(ctx, *, deal_size=0.5):
    ctx.upsert_order('coid-t', symbol='EURUSD', side='buy', qty=1.0, filled_qty=0.0,
                     state='submitted', pine_entry_id='L', from_entry=None,
                     exchange_order_id=None, extras={
                         'kind': 'position',
                         EXTRAS_KEY_TAKE_OVER_UNPROVEN: {'deal_id': 'deal-T', 'size': deal_size,
                                                         'at': time.time() - 5.0},
                     })


def __test_held_market_take_over_is_proven_by_its_opening_and_its_closes_queued__(tmp_path):
    """The deal's opening states the submitted size: the fill is booked under the
    opening's fingerprint, the row confirmed on the deal, the engine's hold
    settled, and the deal's later close queued for the activity poll."""
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_market_take_over(ctx)
    resolutions = __test_helper_record_resolutions(ctx)
    opening = __test_helper_activity('deal-T', size=1.0, level=1.1, offset_s=-90.0)
    reduction = __test_helper_activity('deal-T', direction='SELL', size=0.5, level=1.2,
                                       offset_s=-45.0)
    __test_helper_history(broker, opening, reduction)
    positions = {'deal-T': __test_helper_position('deal-T', 0.5)}

    events = asyncio.run(__test_helper_drain(broker._resolve_unproven_take_overs(positions)))

    assert [(ev.event_type, ev.leg_type, ev.fill_qty, ev.fill_price, ev.fill_id)
            for ev in events] == [('filled', LegType.ENTRY, 1.0, 1.1,
                                   _activity_fingerprint(opening))]
    row = ctx.get_order('coid-t')
    assert row is not None and row.state == 'confirmed' and row.exchange_order_id == 'deal-T'
    assert row.filled_qty == 1.0
    extras = row.extras or {}
    assert extras.get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is None
    assert extras.get('entry_filled_at') is not None
    assert len(queued_fills(extras)) == 2, "the reduction, under its sized and sizeless identity"
    assert ctx.find_by_ref('deal_id', 'deal-T') is not None
    assert resolutions == [('coid-t', 'attached')]
    assert __test_helper_events(ctx, 'take_over_fill_proven')[0]['proof'] == 'opening_activity'
    assert broker._held_take_over_refs() == set()
    store.close()


def __test_the_queued_close_of_a_proven_take_over_is_booked_by_the_activity_poll__(tmp_path):
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_market_take_over(ctx)
    opening = __test_helper_activity('deal-T', size=1.0, offset_s=-90.0)
    reduction = __test_helper_activity('deal-T', direction='SELL', size=0.5, level=1.2,
                                       offset_s=-45.0)
    __test_helper_history(broker, opening, reduction)
    positions = {'deal-T': __test_helper_position('deal-T', 0.5)}
    asyncio.run(__test_helper_drain(broker._resolve_unproven_take_overs(positions)))

    events = asyncio.run(__test_helper_drain(broker._process_activity(
        [opening, reduction], positions)))

    assert [(ev.event_type, ev.leg_type, ev.fill_qty) for ev in events] == [
        ('filled', LegType.CLOSE, 0.5)]
    row = ctx.get_order('coid-t')
    assert row is not None and queued_fills(row.extras or {}) == {}
    store.close()


def __test_held_working_take_over_moves_onto_its_proven_deal__(tmp_path):
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    ctx.upsert_order('coid-w', symbol='EURUSD', side='buy', qty=1.0, filled_qty=0.0,
                     state='confirmed', pine_entry_id='L', exchange_order_id='wo-1',
                     extras={'kind': 'working', EXTRAS_KEY_TAKEN_OVER_AT: time.time() - 60.0,
                             EXTRAS_KEY_TAKE_OVER_UNPROVEN: {'deal_id': 'deal-P', 'size': 0.5}})
    ctx.add_ref('coid-w', 'deal_id', 'wo-1')
    resolutions = __test_helper_record_resolutions(ctx)
    opening = __test_helper_activity('deal-P', size=1.0, level=1.15, working_order_id='wo-1')
    __test_helper_history(broker, opening)
    positions = {'deal-P': __test_helper_position('deal-P', 0.5, working_order_id='wo-1')}

    events = asyncio.run(__test_helper_drain(broker._resolve_unproven_take_overs(positions)))

    assert [(ev.fill_qty, ev.fill_price) for ev in events] == [(1.0, 1.15)]
    row = ctx.get_order('coid-w')
    assert row is not None and row.exchange_order_id == 'deal-P' and row.filled_qty == 1.0
    extras = row.extras or {}
    assert extras.get('kind') == 'position' and extras.get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is None
    assert resolutions == [('coid-w', 'attached')]
    store.close()


def __test_a_take_over_whose_opening_is_of_another_size_stays_held__(tmp_path):
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_market_take_over(ctx)
    __test_helper_history(broker, __test_helper_activity('deal-T', size=0.5))
    positions = {'deal-T': __test_helper_position('deal-T', 0.5)}

    assert asyncio.run(__test_helper_drain(broker._resolve_unproven_take_overs(positions))) == []
    row = ctx.get_order('coid-t')
    assert row is not None and (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None
    store.close()


def __test_the_take_over_proof_reads_the_history_at_most_every_30_seconds__(tmp_path):
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_market_take_over(ctx)
    __test_helper_history(broker)
    positions = {'deal-T': __test_helper_position('deal-T', 0.5)}
    for _ in range(3):
        asyncio.run(__test_helper_drain(broker._resolve_unproven_take_overs(positions)))
    assert len(__test_helper_history_reads(broker)) == 1
    broker._take_over_proof_read_at -= 31.0
    asyncio.run(__test_helper_drain(broker._resolve_unproven_take_overs(positions)))
    assert len(__test_helper_history_reads(broker)) == 2
    store.close()


def __test_without_a_held_take_over_nothing_is_read__(tmp_path):
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_entry_row(ctx)
    asyncio.run(__test_helper_drain(broker._resolve_unproven_take_overs({})))
    assert broker._calls == []
    store.close()


def __test_a_take_over_older_than_the_history_is_booked_from_a_stable_venue_size__(tmp_path):
    """The deal opened more than 24 hours ago: its opening cannot be read. Once
    its listed size stayed unchanged for twice the publication lag, that size is
    booked at the deal's open level, and the closes the history still shows are
    settled as delivered (the size already holds them)."""
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_market_take_over(ctx)
    reduction = __test_helper_activity('deal-T', direction='SELL', size=0.5, level=1.2,
                                       offset_s=-3600.0)
    __test_helper_history(broker, reduction)
    positions = {'deal-T': __test_helper_position(
        'deal-T', 0.5, level=1.05, created=__test_helper_utc(-90000.0))}

    assert asyncio.run(__test_helper_drain(broker._resolve_unproven_take_overs(positions))) == []
    broker._take_over_proof_read_at -= 31.0
    broker._take_over_size_seen['coid-t'] = (0.5, time.time() - 121.0)
    events = asyncio.run(__test_helper_drain(broker._resolve_unproven_take_overs(positions)))

    assert [(ev.fill_qty, ev.fill_price, ev.fill_id) for ev in events] == [(0.5, 1.05, None)]
    row = ctx.get_order('coid-t')
    assert row is not None and row.filled_qty == 0.5
    extras = row.extras or {}
    assert extras.get(EXTRAS_KEY_BOOKED_CLOSES) and queued_fills(extras) == {}
    proven = __test_helper_events(ctx, 'take_over_fill_proven')[0]
    assert proven['proof'] == 'position_beyond_history' and proven['settled_closes'] == 1
    replay = asyncio.run(__test_helper_drain(broker._process_activity([reduction], positions)))
    assert replay == [], "the settled close is a replay"
    store.close()


def __test_a_take_over_the_engine_booked_is_settled_without_a_second_fill__(tmp_path):
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    __test_helper_market_take_over(ctx)
    ctx.record_fill_receipts([FillReceipt(
        fill_id='x', booked=True, fee=0.0, client_order_id='coid-t', order_id='deal-T',
        entry_id='L', qty=1.0)])
    __test_helper_history(broker, __test_helper_activity('deal-T', size=1.0))
    positions = {'deal-T': __test_helper_position('deal-T', 1.0)}

    assert asyncio.run(__test_helper_drain(broker._resolve_unproven_take_overs(positions))) == []
    row = ctx.get_order('coid-t')
    assert row is not None and row.state == 'confirmed'
    assert (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is None
    store.close()


# --- ENTRY_BLOCK causes ------------------------------------------------------


def __test_the_bot_closes_an_unbooked_residual_deal_and_releases_its_cause__(tmp_path):
    broker, store, ctx, latched, released = __test_helper_make(tmp_path)
    cause = _residual_deal_cause('deal-N')
    broker._residual_deals_reported.add('deal-N')
    broker._latch_own_trouble('residual', {}, cause)
    assert latched == [('residual', QuarantineKind.ENTRY_BLOCK, cause)]
    positions = {'deal-N': __test_helper_position('deal-N', 1.0)}

    asyncio.run(broker._settle_own_troubles(positions))
    asyncio.run(broker._settle_own_troubles(positions))

    assert __test_helper_writes(broker) == [('positions/deal-N', 'delete', None)], \
        "closed once, not again within the retry interval"
    assert released == []
    assert len(__test_helper_events(ctx, 'own_working_order_residual_closed')) == 1

    asyncio.run(broker._settle_own_troubles({}))
    assert released == [cause] and broker._own_trouble_causes == {}
    store.close()


def __test_a_residual_deal_still_open_after_a_restart_is_latched_and_closed_again__(
        tmp_path):
    """The entry block ended with the restart; the deal reported by the earlier
    process is still listed: latched again under its cause and closed."""
    broker, store, ctx, latched, _ = __test_helper_make(tmp_path)
    ctx.log_event('own_working_order_residual_unbooked', client_order_id='coid-wo',
                  exchange_order_id='deal-N', payload={})
    positions = {'deal-N': __test_helper_position('deal-N', 1.0)}

    asyncio.run(broker._settle_own_troubles(positions))

    assert [(kind, cause) for _, kind, cause in latched] == [
        (QuarantineKind.ENTRY_BLOCK, _residual_deal_cause('deal-N'))]
    assert __test_helper_writes(broker) == [('positions/deal-N', 'delete', None)]
    store.close()


def __test_a_stored_intervention_latch_forbids_the_residual_close__(tmp_path):
    broker, store, ctx, _, _ = __test_helper_make(tmp_path)
    ctx.record_quarantine('q-test', kind=str(QuarantineKind.EXTERNAL_INTERVENTION),
                          reason='test', intent_key=None, context={},
                          entered_ts_ms=int(time.time() * 1000))
    broker._residual_deals_reported.add('deal-N')
    asyncio.run(broker._settle_own_troubles({'deal-N': __test_helper_position('deal-N', 1.0)}))
    assert __test_helper_writes(broker) == []
    assert _residual_deal_cause('deal-N') in broker._own_trouble_causes
    store.close()


def __test_the_netted_foreign_deal_cause_ends_with_the_orders_retirement__(tmp_path):
    broker, store, ctx, _, released = __test_helper_make(tmp_path)
    __test_helper_entry_row(ctx, 'coid-w', 'wo-1', extras={'kind': 'working'})
    cause = _netted_foreign_deal_cause('coid-w')
    broker._latch_own_trouble('netted', {}, cause)

    asyncio.run(broker._settle_own_troubles({}))
    assert released == [], "the order's execution is not accounted yet"

    ctx.close_order('coid-w')
    asyncio.run(broker._settle_own_troubles({}))
    assert released == [cause]
    store.close()


def __test_a_cause_this_process_did_not_latch_releases_nothing__(tmp_path):
    broker, store, ctx, _, released = __test_helper_make(tmp_path)
    broker._release_own_trouble('own_working_order_residual_unbooked:deal-Z', 'gone')
    assert released == []
    assert __test_helper_events(ctx, 'entry_block_cause_resolved') == []
    store.close()
