"""In-run resolution of entry submissions an earlier process left pending.

Capital.com carries no client order id, so the engine holds an intent's entry
while an inherited dispatch of it is unresolved. The plugin feeds the core
resolver with the activity history as evidence; these tests pin each verdict
and what it does to the row, the park and the events.
"""
import asyncio
import logging
from datetime import UTC, datetime
from time import time as epoch_time

import pytest

from pynecore.core.broker.exceptions import ExchangeConnectionError
from pynecore.core.broker.journal import DispatchJournal
from pynecore.core.broker.models import (
    CancelDispositionOutcome,
    CancelIntent,
    DispatchEnvelope,
    OrderType,
)
from pynecore.core.broker.parked_dispatch import (
    inherited_dispatch_in_flight,
    park_inherited_dispatches,
)
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore, RunContext
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    STATE_DISPOSITION_UNKNOWN,
    STATE_SERVER_REF_SEEN,
    create_entry_order_row,
)

from pynecore_capitalcom.exceptions import OrderNotFoundError
from pynecore_capitalcom.helpers import (
    EXTRAS_KEY_CANCEL_REQUESTED,
    EXTRAS_KEY_TAKE_OVER_UNPROVEN,
    EXTRAS_KEY_TAKEN_OVER_AT,
)

from . import test_capitalcom_broker as base

__test_helper_COID = 'coid-inherited'
__test_helper_AGE_S = 200.0


def __test_helper_restart(store: BrokerStore, ctx: RunContext, broker) -> RunContext:
    """End the process that wrote the row and open the run again, like a restart.

    The engine parks every inherited entry row at startup on this venue; the
    new context gets those parks too.
    """
    ctx.close()
    identity = RunIdentity(
        strategy_id="test", symbol="EURUSD", timeframe="60",
        account_id="test-account", label=None,
    )
    restarted = store.open_run(identity, script_source="// test")
    broker.store_ctx = restarted
    park_inherited_dispatches(restarted)
    return restarted


def __test_helper_crashed_entry(
        tmp_path, *, responses: dict, kind: str = ENTRY_KIND_POSITION,
        age_s: float = __test_helper_AGE_S, state: str | None = None,
        deal_reference: str | None = None,
):
    """A process POSTed entry ``L`` and ended before it learned the outcome.

    :return: ``(broker, store, ctx, created_s)`` after the restart.
    """
    broker, store, ctx = base._make_broker(tmp_path, responses=responses)
    create_entry_order_row(
        ctx, coid=__test_helper_COID, symbol='EURUSD', side='buy', qty=1.0,
        intent_key='L', pine_entry_id='L', kind=kind,
        order_type='market' if kind == ENTRY_KIND_POSITION else 'limit',
    )
    created_s = epoch_time() - age_s
    ctx._store._conn.execute(
        "UPDATE orders SET created_ts_ms = ? WHERE client_order_id = ?",
        (int(created_s * 1000), __test_helper_COID),
    )
    if deal_reference is not None:
        ctx.add_ref(__test_helper_COID, 'deal_reference', deal_reference)
        row = ctx.get_order(__test_helper_COID)
        assert row is not None
        ctx.upsert_order(__test_helper_COID,
                         extras={**(row.extras or {}), 'deal_reference': deal_reference})
    if state is not None:
        ctx.set_order_state(__test_helper_COID, state)
    ctx = __test_helper_restart(store, ctx, broker)
    return broker, store, ctx, created_s


def __test_helper_activity(when_s: float, *, deal_id: str = 'deal-L', size: float = 1.0,
                           activity_type: str = 'POSITION') -> dict:
    stamp = datetime.fromtimestamp(when_s, tz=UTC)
    return {
        'epic': 'EURUSD', 'details': {'direction': 'BUY', 'size': size},
        'dateUTC': stamp.strftime('%Y-%m-%dT%H:%M:%S.') + f"{stamp.microsecond // 1000:03d}",
        'dealId': deal_id, 'type': activity_type, 'status': 'ACCEPTED',
    }


def __test_helper_clocked(broker) -> list[float]:
    """Drive the resolver's clock by hand; returns the mutable clock cell."""
    now = [epoch_time()]
    broker._inherited_resolver._clock = lambda: now[0]
    return now


def __test_helper_activity_reads(broker) -> list[dict]:
    return [data for endpoint, method, data in broker._calls
            if endpoint == 'history/activity' and method == 'get']


def __test_helper_pass(broker, positions_by_deal: dict | None = None) -> list:
    return asyncio.run(base._drain_agen(broker._reconcile_snapshot(positions_by_deal or {}, {})))


def __test_an_inherited_submission_without_a_trace_is_rejected_and_released__(tmp_path):
    # No activity of the POST in a read that covers the row's whole life:
    # the POST opened nothing. After three such reads the row is rejected and
    # its park carries the 'rejected' resolution the engine needs to send
    # the entry again under a fresh id.
    broker, store, ctx, _created = __test_helper_crashed_entry(tmp_path, responses={
        ('history/activity', 'get'): {'activities': []},
    })
    now = __test_helper_clocked(broker)

    for _ in range(2):
        assert __test_helper_pass(broker) == []
        row = ctx.get_order(__test_helper_COID)
        assert row is not None and row.state == 'submitted' and row.closed_ts_ms is None
        assert ctx.replay()[1][__test_helper_COID].resolution is None
        now[0] += 15.0
    assert __test_helper_pass(broker) == []

    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'rejected' and row.closed_ts_ms is not None
    assert ctx.replay()[1][__test_helper_COID].resolution == 'rejected'
    reads = __test_helper_activity_reads(broker)
    assert len(reads) == 3
    # Each read reaches back past the row's creation by the publication lag.
    assert all(read['lastPeriod'] >= __test_helper_AGE_S + 60 for read in reads)
    store.close()


def __test_an_inherited_submission_is_looked_up_no_more_often_than_its_interval__(tmp_path):
    broker, store, ctx, _created = __test_helper_crashed_entry(tmp_path, responses={
        ('history/activity', 'get'): {'activities': []},
    })
    __test_helper_clocked(broker)

    for _ in range(5):
        __test_helper_pass(broker)

    assert len(__test_helper_activity_reads(broker)) == 1
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'submitted'
    store.close()


def __test_a_young_inherited_submission_is_not_rejected_before_its_trace_can_show__(tmp_path):
    # The venue has published an activity 61 s after its deal: three empty
    # reads of a row younger than fill_report_grace_s prove nothing yet.
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, age_s=30.0, responses={('history/activity', 'get'): {'activities': []}},
    )
    now = __test_helper_clocked(broker)

    for _ in range(3):
        __test_helper_pass(broker)
        now[0] += 15.0

    assert len(__test_helper_activity_reads(broker)) == 3
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'submitted' and row.closed_ts_ms is None
    assert ctx.replay()[1][__test_helper_COID].resolution is None
    store.close()


def __test_an_inherited_market_submission_that_landed_books_its_fill_once__(tmp_path):
    # The POST's POSITION activity surfaced after the startup read: the deal
    # is the row's, its fill reaches the engine from the snapshot, the park
    # is attached, and the activity poll does not book the same deal again.
    created_s = epoch_time() - __test_helper_AGE_S
    activity = __test_helper_activity(created_s + 0.4)
    broker, store, ctx, _created = __test_helper_crashed_entry(tmp_path, responses={
        ('history/activity', 'get'): {'activities': [activity]},
    })
    position = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-L', 'direction': 'BUY',
                             'size': 1.0, 'level': 1.1}}

    events = __test_helper_pass(broker, {'deal-L': position})

    fills = [ev for ev in events if ev.event_type == 'filled']
    assert len(fills) == 1
    assert fills[0].order.client_order_id == __test_helper_COID
    assert fills[0].fill_qty == 1.0 and fills[0].fill_price == 1.1
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed'
    assert row.exchange_order_id == 'deal-L' and row.filled_qty == 1.0
    assert ctx.replay()[1][__test_helper_COID].resolution == 'attached'

    replayed = asyncio.run(base._drain_agen(
        broker._process_activity([activity], {'deal-L': position})))
    assert [ev for ev in replayed if ev.event_type == 'filled'] == []
    store.close()


def __test_an_inherited_working_submission_that_landed_gets_its_deal_id__(tmp_path):
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, kind=ENTRY_KIND_WORKING, responses={
            ('history/activity', 'get'): {'activities': [
                __test_helper_activity(created_s + 0.4, deal_id='deal-W',
                                       activity_type='WORKING_ORDER'),
            ]},
        })

    assert __test_helper_pass(broker) == []

    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed'
    assert row.exchange_order_id == 'deal-W'
    mapped = ctx.find_by_ref('deal_id', 'deal-W')
    assert mapped is not None and mapped.client_order_id == __test_helper_COID
    store.close()


def __test_an_inherited_working_submission_that_already_executed_books_its_fill_once__(tmp_path):
    # The working order executed into a fresh deal before its id was known:
    # the execution's activity was read while no row owned the order, so the
    # take-over books the fill from the position that back-links the order.
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, kind=ENTRY_KIND_WORKING, responses={
            ('history/activity', 'get'): {'activities': [
                __test_helper_activity(created_s + 0.4, deal_id='deal-W',
                                       activity_type='WORKING_ORDER'),
            ]},
        })
    position = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-P', 'workingOrderId': 'deal-W',
                             'direction': 'BUY', 'size': 1.0, 'level': 1.2}}

    events = __test_helper_pass(broker, {'deal-P': position})

    fills = [ev for ev in events if ev.event_type == 'filled']
    assert len(fills) == 1
    assert fills[0].order.client_order_id == __test_helper_COID
    assert fills[0].order.id == 'deal-P'
    assert fills[0].fill_qty == 1.0 and fills[0].fill_price == 1.2
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed'
    assert row.exchange_order_id == 'deal-P' and row.filled_qty == 1.0
    assert (row.extras or {}).get('kind') == ENTRY_KIND_POSITION
    assert ctx.replay()[1][__test_helper_COID].resolution == 'attached'

    executed = {**__test_helper_activity(created_s + 30.0, deal_id='deal-W',
                                         activity_type='WORKING_ORDER'),
                'status': 'EXECUTED'}
    opened = __test_helper_activity(created_s + 30.0, deal_id='deal-P')
    opened['details'] = {**opened['details'], 'workingOrderId': 'deal-W'}
    replayed = asyncio.run(base._drain_agen(
        broker._process_activity([executed, opened], {'deal-P': position})))
    assert [ev for ev in replayed if ev.event_type == 'filled'] == []
    store.close()


def __test_an_inherited_working_submission_whose_position_surfaces_late_books_its_fill_once__(
        tmp_path):
    # The working order executed before its id was known, but the position
    # that back-links it is not in the take-over's snapshot yet. A later poll
    # whose snapshot shows it books the fill, once.
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, kind=ENTRY_KIND_WORKING, responses={
            ('history/activity', 'get'): {'activities': [
                __test_helper_activity(created_s + 0.4, deal_id='deal-W',
                                       activity_type='WORKING_ORDER'),
            ]},
        })
    position = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-P', 'workingOrderId': 'deal-W',
                             'direction': 'BUY', 'size': 1.0, 'level': 1.2}}

    assert __test_helper_pass(broker) == []
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.exchange_order_id == 'deal-W' and row.filled_qty == 0.0

    events = __test_helper_pass(broker, {'deal-P': position})

    fills = [ev for ev in events if ev.event_type == 'filled']
    assert len(fills) == 1
    assert fills[0].order.client_order_id == __test_helper_COID
    assert fills[0].order.id == 'deal-P'
    assert fills[0].fill_qty == 1.0 and fills[0].fill_price == 1.2
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed' and row.closed_ts_ms is None
    assert row.exchange_order_id == 'deal-P' and row.filled_qty == 1.0
    assert (row.extras or {}).get('kind') == ENTRY_KIND_POSITION
    assert ctx.replay()[1][__test_helper_COID].resolution == 'attached'

    assert [ev for ev in __test_helper_pass(broker, {'deal-P': position})
            if ev.event_type == 'filled'] == []
    executed = {**__test_helper_activity(created_s + 30.0, deal_id='deal-W',
                                         activity_type='WORKING_ORDER'),
                'status': 'EXECUTED'}
    opened = __test_helper_activity(created_s + 30.0, deal_id='deal-P')
    opened['details'] = {**opened['details'], 'workingOrderId': 'deal-W'}
    replayed = asyncio.run(base._drain_agen(
        broker._process_activity([executed, opened], {'deal-P': position})))
    assert [ev for ev in replayed if ev.event_type == 'filled'] == []
    store.close()


def __test_an_interrupted_take_over_booking_leaves_the_fill_to_the_next_poll__(
        tmp_path, monkeypatch):
    # The fill write of a taken-over working order fails after the row was
    # moved onto its position: the whole booking rolls back, so the next
    # poll still sees the unbooked working order and books its fill, once.
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, kind=ENTRY_KIND_WORKING, responses={
            ('history/activity', 'get'): {'activities': [
                __test_helper_activity(created_s + 0.4, deal_id='deal-W',
                                       activity_type='WORKING_ORDER'),
            ]},
        })
    position = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-P', 'workingOrderId': 'deal-W',
                             'direction': 'BUY', 'size': 1.0, 'level': 1.2}}

    def fail_fill_write(*_args, **_kwargs):
        raise RuntimeError("journal write failed")

    with monkeypatch.context() as patched:
        patched.setattr(DispatchJournal, 'apply_reconcile_outcome', fail_fill_write)
        with pytest.raises(RuntimeError):
            __test_helper_pass(broker, {'deal-P': position})

    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.exchange_order_id == 'deal-W' and row.filled_qty == 0.0
    assert (row.extras or {}).get('kind') == ENTRY_KIND_WORKING
    assert (row.extras or {}).get('entry_filled_at') is None
    assert (row.extras or {}).get(EXTRAS_KEY_TAKEN_OVER_AT) is not None
    assert ctx.find_by_ref('deal_id', 'deal-P') is None
    assert ctx.replay()[1][__test_helper_COID].resolution is None

    events = __test_helper_pass(broker, {'deal-P': position})

    fills = [ev for ev in events if ev.event_type == 'filled']
    assert len(fills) == 1 and fills[0].fill_qty == 1.0
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.exchange_order_id == 'deal-P' and row.filled_qty == 1.0
    assert ctx.replay()[1][__test_helper_COID].resolution == 'attached'
    assert [ev for ev in __test_helper_pass(broker, {'deal-P': position})
            if ev.event_type == 'filled'] == []
    store.close()


def __test_helper_close_activity(when_s: float, deal_id: str, size: float) -> dict:
    """The dealing desk's partial close of the long ``deal_id``."""
    activity = __test_helper_activity(when_s, deal_id=deal_id, size=size)
    activity['details'] = {'direction': 'SELL', 'size': size, 'level': 1.25}
    return {**activity, 'status': 'EXECUTED', 'source': 'DEALER'}


def __test_helper_unproven_warnings(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records
            if 'cannot be proven' in r.getMessage()]


def __test_a_working_take_over_whose_position_was_reduced_stays_held__(tmp_path, caplog):
    # The working order executed 1.0 before its id was known and half of it
    # was closed before the take-over. Which closes the snapshot's 0.5
    # already shows cannot be proven, so nothing is booked: the order keeps
    # its park, its absence is no cancel (a cancel would release the entry for
    # a second send), the closes reach no row, and the operator is told once.
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, kind=ENTRY_KIND_WORKING, responses={
            ('history/activity', 'get'): {'activities': [
                __test_helper_activity(created_s + 0.4, deal_id='deal-W',
                                       activity_type='WORKING_ORDER'),
            ]},
        })
    position = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-P', 'workingOrderId': 'deal-W',
                             'direction': 'BUY', 'size': 0.5, 'level': 1.2}}

    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            assert [ev for ev in __test_helper_pass(broker, {'deal-P': position})
                    if ev.event_type == 'filled'] == []

    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed' and row.closed_ts_ms is None
    assert row.exchange_order_id == 'deal-W' and row.filled_qty == 0.0
    assert (row.extras or {}).get('kind') == ENTRY_KIND_WORKING
    assert (row.extras or {}).get('entry_filled_at') is None
    assert (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None
    assert ctx.find_by_ref('deal_id', 'deal-P') is None
    assert ctx.replay()[1][__test_helper_COID].resolution is None
    assert broker._is_disappearance_exempt(row)
    assert len(__test_helper_unproven_warnings(caplog)) == 1

    closes = [__test_helper_close_activity(created_s + 60.0, 'deal-P', 0.5),
              __test_helper_close_activity(epoch_time() + 1.0, 'deal-P', 0.25)]
    replayed = asyncio.run(base._drain_agen(
        broker._process_activity(closes, {'deal-P': position})))
    assert [ev for ev in replayed if ev.event_type == 'filled'] == []
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.filled_qty == 0.0 and row.closed_ts_ms is None

    # The same activities as the venue may also publish them: the closes and
    # the opening back-linking the order, and the order's own execution and
    # deletion. None of them promotes the row or reaches the engine.
    linked = [{**activity, 'details': {**activity['details'], 'workingOrderId': 'deal-W'}}
              for activity in closes]
    opened = __test_helper_activity(created_s + 30.0, deal_id='deal-P')
    opened['details'] = {**opened['details'], 'workingOrderId': 'deal-W'}
    executed = {**__test_helper_activity(created_s + 30.0, deal_id='deal-W',
                                         activity_type='WORKING_ORDER'),
                'status': 'EXECUTED'}
    deleted = {**__test_helper_activity(epoch_time() + 2.0, deal_id='deal-W',
                                        activity_type='WORKING_ORDER'),
               'status': 'DELETED', 'source': 'USER'}
    replayed = asyncio.run(base._drain_agen(broker._process_activity(
        [opened, executed, *linked, deleted], {'deal-P': position})))
    assert replayed == []
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed' and row.closed_ts_ms is None
    assert row.exchange_order_id == 'deal-W' and row.filled_qty == 0.0
    assert (row.extras or {}).get('kind') == ENTRY_KIND_WORKING
    assert (row.extras or {}).get('entry_filled_at') is None
    assert ctx.find_by_ref('deal_id', 'deal-P') is None
    assert ctx.replay()[1][__test_helper_COID].resolution is None
    store.close()


def __test_a_market_take_over_whose_position_was_reduced_stays_held__(tmp_path, caplog):
    # The inherited MARKET entry landed, but its deal shows 0.5 of the 1.0
    # sent: the row stays pending, so the engine keeps the entry held, and
    # the operator is told once.
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_crashed_entry(tmp_path, responses={
        ('history/activity', 'get'): {'activities': [
            __test_helper_activity(created_s + 0.4)]},
    })
    position = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-L', 'direction': 'BUY',
                             'size': 0.5, 'level': 1.1}}
    now = __test_helper_clocked(broker)

    with caplog.at_level(logging.WARNING):
        for _ in range(5):
            assert [ev for ev in __test_helper_pass(broker, {'deal-L': position})
                    if ev.event_type == 'filled'] == []
            now[0] += 15.0

    assert len(__test_helper_activity_reads(broker)) == 5
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'submitted' and row.closed_ts_ms is None
    assert row.exchange_order_id is None and row.filled_qty == 0.0
    assert (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None
    assert ctx.find_by_ref('deal_id', 'deal-L') is None
    assert ctx.replay()[1][__test_helper_COID].resolution is None
    assert inherited_dispatch_in_flight(ctx, 'L') == __test_helper_COID
    assert len(__test_helper_unproven_warnings(caplog)) == 1
    store.close()


def __test_a_held_market_take_over_is_not_booked_by_a_later_full_size_snapshot__(
        tmp_path, caplog):
    # The first snapshot shows the landed deal without a size, so the fill is
    # held. A later snapshot at the submitted 1.0 proves nothing more: the
    # deal may have been reduced and topped up meanwhile. The row stays
    # pending and unbooked, and a genuine close of the deal reaches no row.
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_crashed_entry(tmp_path, responses={
        ('history/activity', 'get'): {'activities': [
            __test_helper_activity(created_s + 0.4)]},
    })
    sizeless = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-L', 'direction': 'BUY', 'level': 1.1}}
    full = {'market': {'epic': 'EURUSD'},
            'position': {'dealId': 'deal-L', 'direction': 'BUY',
                         'size': 1.0, 'level': 1.1}}
    now = __test_helper_clocked(broker)

    with caplog.at_level(logging.WARNING):
        assert [ev for ev in __test_helper_pass(broker, {'deal-L': sizeless})
                if ev.event_type == 'filled'] == []
        held = ctx.get_order(__test_helper_COID)
        assert held is not None and (held.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN)
        for _ in range(3):
            now[0] += 15.0
            assert [ev for ev in __test_helper_pass(broker, {'deal-L': full})
                    if ev.event_type == 'filled'] == []

    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'submitted' and row.closed_ts_ms is None
    assert row.exchange_order_id is None and row.filled_qty == 0.0
    assert ctx.find_by_ref('deal_id', 'deal-L') is None
    assert ctx.replay()[1][__test_helper_COID].resolution is None
    assert inherited_dispatch_in_flight(ctx, 'L') == __test_helper_COID
    assert len(__test_helper_unproven_warnings(caplog)) == 1

    close = __test_helper_close_activity(epoch_time() + 1.0, 'deal-L', 0.5)
    replayed = asyncio.run(base._drain_agen(
        broker._process_activity([close], {'deal-L': full})))
    assert [ev for ev in replayed if ev.event_type == 'filled'] == []
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'submitted' and row.filled_qty == 0.0
    store.close()


def __test_helper_held_at_recovery_events(ctx: RunContext) -> list[dict]:
    return [payload for _, _, _, payload
            in ctx.iter_events_by_kind_for_run_id('take_over_unproven_held_at_recovery')]


def __test_a_held_working_take_over_stays_held_across_a_restart__(tmp_path):
    # The held working order of the reduced-position case meets a fresh
    # process. Its startup pass sees the order gone and the position that
    # back-links it: moving the row onto that position would book the
    # unproven 1.0, and retiring it as an orphan would release the entry for
    # a second send. The row stays exactly as the earlier process left it.
    created_s = epoch_time() - __test_helper_AGE_S
    creation = __test_helper_activity(created_s + 0.4, deal_id='deal-W',
                                      activity_type='WORKING_ORDER')
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, kind=ENTRY_KIND_WORKING, responses={
            ('history/activity', 'get'): {'activities': [creation]},
        })
    position = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-P', 'workingOrderId': 'deal-W',
                             'direction': 'BUY', 'size': 0.5, 'level': 1.2,
                             'createdDateUTC': ''}}
    assert [ev for ev in __test_helper_pass(broker, {'deal-P': position})
            if ev.event_type == 'filled'] == []
    held = ctx.get_order(__test_helper_COID)
    assert held is not None and (held.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN)

    ctx = __test_helper_restart(store, ctx, broker)
    executed = {**__test_helper_activity(created_s + 30.0, deal_id='deal-W',
                                         activity_type='WORKING_ORDER'),
                'status': 'EXECUTED'}
    opened = __test_helper_activity(created_s + 30.0, deal_id='deal-P')
    opened['details'] = {**opened['details'], 'workingOrderId': 'deal-W'}
    broker._responses.update({
        ('positions', 'get'): {'positions': [position]},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': [
            creation, executed, opened,
            __test_helper_close_activity(created_s + 60.0, 'deal-P', 0.5)]},
    })

    asyncio.run(broker._recover_in_flight_submissions())

    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed' and row.closed_ts_ms is None
    assert row.exchange_order_id == 'deal-W' and row.filled_qty == 0.0
    assert (row.extras or {}).get('kind') == ENTRY_KIND_WORKING
    assert (row.extras or {}).get('entry_filled_at') is None
    assert (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None
    assert ctx.find_by_ref('deal_id', 'deal-P') is None
    assert ctx.replay()[1][__test_helper_COID].resolution is None
    assert len(__test_helper_held_at_recovery_events(ctx)) == 1
    store.close()


def __test_a_held_working_take_over_survives_a_strategy_cancel_and_amend__(tmp_path, caplog):
    # Pine cancels, then amends, the held working entry. Its order already
    # executed, so a DELETE would answer 404, and the already-gone path would
    # close the row and let the engine treat the cancel as settled; an amend
    # of another size would cancel and send the entry a second time. Nothing
    # is sent, the cancel reports not landed, the row stays held, and the
    # operator is told once per action, however often the engine retries.
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, kind=ENTRY_KIND_WORKING, responses={
            ('history/activity', 'get'): {'activities': [
                __test_helper_activity(created_s + 0.4, deal_id='deal-W',
                                       activity_type='WORKING_ORDER'),
            ]},
            ('error', 'workingorders/deal-W', 'delete'):
                OrderNotFoundError('gone', ref_type='deal_id'),
            ('error', 'workingorders/deal-W', 'put'):
                OrderNotFoundError('gone', ref_type='deal_id'),
        })
    position = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-P', 'workingOrderId': 'deal-W',
                             'direction': 'BUY', 'size': 0.5, 'level': 1.2}}
    __test_helper_pass(broker, {'deal-P': position})
    held = ctx.get_order(__test_helper_COID)
    assert held is not None and (held.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN)
    calls_before = len(broker._calls)

    cancel = DispatchEnvelope(intent=CancelIntent(pine_id='L', symbol='EURUSD'),
                              run_tag='test', bar_ts_ms=1700000000000)
    old = base._entry_envelope(qty=1.0, order_type=OrderType.LIMIT, limit=1.2, pine_id='L')
    with caplog.at_level(logging.WARNING):
        for _ in range(2):
            assert asyncio.run(broker.execute_cancel(cancel)) is False
            assert asyncio.run(broker.execute_cancel_with_outcome(cancel)) \
                == CancelDispositionOutcome.UNKNOWN
        for qty, limit in ((2.0, 1.2), (1.0, 1.1)):
            new = base._entry_envelope(qty=qty, order_type=OrderType.LIMIT, limit=limit,
                                       pine_id='L')
            orders = asyncio.run(broker.modify_entry(old, new))
            assert [(o.id, o.qty, o.client_order_id) for o in orders] \
                == [('deal-W', 1.0, __test_helper_COID)]

    assert broker._calls[calls_before:] == []
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed' and row.closed_ts_ms is None
    assert row.exchange_order_id == 'deal-W' and row.filled_qty == 0.0
    assert (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None
    assert ctx.replay()[1][__test_helper_COID].resolution is None
    held_events = [payload['action'] for _, _, _, payload
                   in ctx.iter_events_by_kind_for_run_id('take_over_unproven_dispatch_held')]
    assert sorted(held_events) == ['cancel', 'modify']
    assert len([r for r in caplog.records if 'not sent' in r.getMessage()]) == 2
    store.close()


@pytest.mark.parametrize('with_outcome', [False, True])
def __test_a_take_over_held_while_its_cancel_is_in_flight_stays_held__(
        tmp_path, caplog, with_outcome):
    # Pine cancels the taken-over working entry while it still looks
    # unbooked. While the DELETE is in flight the poll sees the order's
    # position at half the size and holds the row; the DELETE then answers
    # 404. The row read before the request is stale: the cancel must not
    # close the held row or report itself settled.
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, kind=ENTRY_KIND_WORKING, responses={
            ('history/activity', 'get'): {'activities': [
                __test_helper_activity(created_s + 0.4, deal_id='deal-W',
                                       activity_type='WORKING_ORDER'),
            ]},
        })
    assert __test_helper_pass(broker) == []
    taken_over = ctx.get_order(__test_helper_COID)
    assert taken_over is not None and taken_over.exchange_order_id == 'deal-W'
    assert (taken_over.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is None
    position = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-P', 'workingOrderId': 'deal-W',
                             'direction': 'BUY', 'size': 0.5, 'level': 1.2}}
    plain_call = broker._call
    deletes: list[str] = []

    async def racing_call(endpoint, *, data=None, method='post'):
        if endpoint == 'workingorders/deal-W' and method == 'delete':
            deletes.append(endpoint)
            await base._drain_agen(broker._reconcile_snapshot({'deal-P': position}, {}))
            raise OrderNotFoundError('gone', ref_type='deal_id')
        return await plain_call(endpoint, data=data, method=method)

    broker._call = racing_call
    cancel = DispatchEnvelope(intent=CancelIntent(pine_id='L', symbol='EURUSD'),
                              run_tag='test', bar_ts_ms=1700000000000)
    with caplog.at_level(logging.WARNING):
        if with_outcome:
            assert asyncio.run(broker.execute_cancel_with_outcome(cancel)) \
                == CancelDispositionOutcome.UNKNOWN
        else:
            assert asyncio.run(broker.execute_cancel(cancel)) is False

    assert deletes == ['workingorders/deal-W']
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed' and row.closed_ts_ms is None
    assert row.exchange_order_id == 'deal-W' and row.filled_qty == 0.0
    assert (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None
    assert ctx.replay()[1][__test_helper_COID].resolution is None
    assert list(ctx.iter_events_by_kind_for_run_id('cancelled')) == []
    held_events = [payload['action'] for _, _, _, payload
                   in ctx.iter_events_by_kind_for_run_id('take_over_unproven_dispatch_held')]
    assert held_events == ['cancel']
    assert len([r for r in caplog.records if 'not sent' in r.getMessage()]) == 1
    store.close()


@pytest.mark.parametrize('venue', ['no_answer', 'booked_meanwhile', 'deleted'])
def __test_a_taken_over_order_gone_on_cancel_is_retired_only_on_proof_of_no_fill__(
        tmp_path, venue):
    # Pine cancels the taken-over working entry before any poll showed its
    # position. The DELETE answers 404, which an executed order answers too:
    # closing the row would leave the position no row books, and let a
    # cancel+create open the entry a second time. The row is retired only
    # when the venue's activity shows the order deleted; while it says
    # nothing the row stays live and the cancel unsettled, so the position
    # that surfaces later is booked onto it once. A fill booked while the
    # DELETE was in flight leaves the row a live position.
    created_s = epoch_time() - __test_helper_AGE_S
    creation = __test_helper_activity(created_s + 0.4, deal_id='deal-W',
                                      activity_type='WORKING_ORDER')
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, kind=ENTRY_KIND_WORKING, responses={
            ('history/activity', 'get'): {'activities': [creation]},
        })
    assert __test_helper_pass(broker) == []
    taken_over = ctx.get_order(__test_helper_COID)
    assert taken_over is not None and taken_over.exchange_order_id == 'deal-W'
    assert (taken_over.extras or {}).get(EXTRAS_KEY_TAKEN_OVER_AT) is not None
    if venue == 'deleted':
        deleted = {**__test_helper_activity(epoch_time() - 1.0, deal_id='deal-W',
                                            activity_type='WORKING_ORDER'),
                   'status': 'DELETED'}
        broker._responses[('history/activity', 'get')] = {'activities': [creation, deleted]}
    position = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-P', 'workingOrderId': 'deal-W',
                             'direction': 'BUY', 'size': 1.0, 'level': 1.2}}
    plain_call = broker._call
    booked: list = []

    async def gone_call(endpoint, *, data=None, method='post'):
        if endpoint == 'workingorders/deal-W' and method == 'delete':
            if venue == 'booked_meanwhile':
                booked.extend(await base._drain_agen(
                    broker._reconcile_snapshot({'deal-P': position}, {})))
            raise OrderNotFoundError('gone', ref_type='deal_id')
        return await plain_call(endpoint, data=data, method=method)

    broker._call = gone_call
    cancel = DispatchEnvelope(intent=CancelIntent(pine_id='L', symbol='EURUSD'),
                              run_tag='test', bar_ts_ms=1700000000000)
    landed = asyncio.run(broker.execute_cancel(cancel))

    row = ctx.get_order(__test_helper_COID)
    assert row is not None
    if venue == 'deleted':
        assert landed is True and row.closed_ts_ms is not None
        assert len(list(ctx.iter_events_by_kind_for_run_id('cancelled'))) == 1
        store.close()
        return
    assert list(ctx.iter_events_by_kind_for_run_id('cancelled')) == []
    assert row.state == 'confirmed' and row.closed_ts_ms is None
    if venue == 'no_answer':
        assert landed is False
        assert row.exchange_order_id == 'deal-W' and row.filled_qty == 0.0
        booked.extend(__test_helper_pass(broker, {'deal-P': position}))
    else:
        assert landed is True
    assert [(ev.event_type, ev.fill_qty) for ev in booked] == [('filled', 1.0)]
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.closed_ts_ms is None
    assert row.exchange_order_id == 'deal-P' and row.filled_qty == 1.0
    assert (row.extras or {}).get('kind') == ENTRY_KIND_POSITION
    assert asyncio.run(broker.execute_cancel(cancel)) is True
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.closed_ts_ms is None
    assert [ev for ev in __test_helper_pass(broker, {'deal-P': position})
            if ev.event_type == 'filled'] == []
    store.close()


@pytest.mark.parametrize('venue', ['clean', 'deleted'])
def __test_a_fill_booked_while_the_outcome_cancel_is_in_flight_is_not_retired__(
        tmp_path, venue):
    # The cancel-tentative loop cancels the taken-over working entry. While
    # its DELETE is in flight the poll books the order's position onto the
    # row; the DELETE then lands, or answers 404 with an activity that says
    # the order was deleted. The row read before the request is stale: the
    # booked position must stay live and the cancel must report the fill,
    # whatever the venue's answer to the DELETE was.
    created_s = epoch_time() - __test_helper_AGE_S
    creation = __test_helper_activity(created_s + 0.4, deal_id='deal-W',
                                      activity_type='WORKING_ORDER')
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, kind=ENTRY_KIND_WORKING, responses={
            ('history/activity', 'get'): {'activities': [creation]},
        })
    assert __test_helper_pass(broker) == []
    taken_over = ctx.get_order(__test_helper_COID)
    assert taken_over is not None and taken_over.exchange_order_id == 'deal-W'
    assert (taken_over.extras or {}).get(EXTRAS_KEY_TAKEN_OVER_AT) is not None
    if venue == 'deleted':
        deleted = {**__test_helper_activity(epoch_time() - 1.0, deal_id='deal-W',
                                            activity_type='WORKING_ORDER'),
                   'status': 'DELETED'}
        broker._responses[('history/activity', 'get')] = {'activities': [creation, deleted]}
    position = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-P', 'workingOrderId': 'deal-W',
                             'direction': 'BUY', 'size': 1.0, 'level': 1.2}}
    plain_call = broker._call
    booked: list = []

    async def racing_call(endpoint, *, data=None, method='post'):
        if endpoint == 'workingorders/deal-W' and method == 'delete':
            booked.extend(await base._drain_agen(
                broker._reconcile_snapshot({'deal-P': position}, {})))
            if venue == 'deleted':
                raise OrderNotFoundError('gone', ref_type='deal_id')
            return {}
        return await plain_call(endpoint, data=data, method=method)

    broker._call = racing_call
    cancel = DispatchEnvelope(intent=CancelIntent(pine_id='L', symbol='EURUSD'),
                              run_tag='test', bar_ts_ms=1700000000000)
    assert asyncio.run(broker.execute_cancel_with_outcome(cancel)) \
        == CancelDispositionOutcome.ALREADY_FILLED

    assert [(ev.event_type, ev.fill_qty) for ev in booked] == [('filled', 1.0)]
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed' and row.closed_ts_ms is None
    assert row.exchange_order_id == 'deal-P' and row.filled_qty == 1.0
    assert (row.extras or {}).get('kind') == ENTRY_KIND_POSITION
    assert list(ctx.iter_events_by_kind_for_run_id('cancelled')) == []
    assert [payload['reason'] for _, _, _, payload
            in ctx.iter_events_by_kind_for_run_id('cancel_noop')] == ['already_filled']
    assert [ev for ev in __test_helper_pass(broker, {'deal-P': position})
            if ev.event_type == 'filled'] == []
    store.close()


def __test_a_take_over_held_while_its_replacing_amend_cancels_is_not_sent_again__(
        tmp_path, caplog):
    # Pine changes the size of the taken-over working entry while it still
    # looks unbooked, so the amend cancels the order and sends a new one.
    # While the cancel's DELETE is in flight the poll sees the order's position
    # at half the size and holds the row; the DELETE then answers 404. The
    # cancel did not land: the replacement must not go out, or the entry
    # would be opened a second time next to the live position.
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, kind=ENTRY_KIND_WORKING, responses={
            ('history/activity', 'get'): {'activities': [
                __test_helper_activity(created_s + 0.4, deal_id='deal-W',
                                       activity_type='WORKING_ORDER'),
            ]},
        })
    assert __test_helper_pass(broker) == []
    taken_over = ctx.get_order(__test_helper_COID)
    assert taken_over is not None and taken_over.exchange_order_id == 'deal-W'
    assert (taken_over.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is None
    position = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-P', 'workingOrderId': 'deal-W',
                             'direction': 'BUY', 'size': 0.5, 'level': 1.2}}
    plain_call = broker._call

    async def racing_call(endpoint, *, data=None, method='post'):
        if endpoint == 'workingorders/deal-W' and method == 'delete':
            await base._drain_agen(broker._reconcile_snapshot({'deal-P': position}, {}))
            raise OrderNotFoundError('gone', ref_type='deal_id')
        return await plain_call(endpoint, data=data, method=method)

    broker._call = racing_call
    calls_before = len(broker._calls)
    old = base._entry_envelope(qty=1.0, order_type=OrderType.LIMIT, limit=1.2, pine_id='L')
    new = base._entry_envelope(qty=2.0, order_type=OrderType.LIMIT, limit=1.2, pine_id='L')
    with caplog.at_level(logging.WARNING):
        orders = asyncio.run(broker.modify_entry(old, new))

    assert [(o.id, o.qty, o.client_order_id) for o in orders] \
        == [('deal-W', 1.0, __test_helper_COID)]
    assert [(endpoint, method) for endpoint, method, _data in broker._calls[calls_before:]
            if method in ('post', 'put')] == []
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed' and row.closed_ts_ms is None
    assert row.exchange_order_id == 'deal-W' and row.filled_qty == 0.0
    assert (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None
    assert [r.client_order_id for r in ctx.iter_live_orders()] == [__test_helper_COID]
    held_events = [payload['action'] for _, _, _, payload
                   in ctx.iter_events_by_kind_for_run_id('take_over_unproven_dispatch_held')]
    assert sorted(held_events) == ['cancel', 'modify']
    store.close()


def __test_a_take_over_held_while_reconnect_recovery_reads_its_confirm_stays_held__(tmp_path):
    # A reconnect's recovery pass reads the confirm of a MARKET entry left in
    # server_ref_seen. While that read is in flight the poll takes the same
    # submission over, sees its deal at half the size and holds the row. The
    # recovery's verdict was built from its own earlier reads: confirming the
    # row from it would book the very fill the poll held back.
    reduced = {'market': {'epic': 'EURUSD'},
               'position': {'dealId': 'deal-L', 'dealReference': 'o-ref-L',
                            'direction': 'BUY', 'size': 0.5, 'level': 1.1}}
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, state=STATE_SERVER_REF_SEEN, deal_reference='o-ref-L', responses={
            ('confirms/o-ref-L', 'get'): {'dealStatus': 'ACCEPTED', 'dealId': 'deal-L',
                                          'affectedDeals': [{'dealId': 'deal-L'}],
                                          'level': 1.1, 'size': 1.0},
            ('positions', 'get'): {'positions': [reduced]},
            ('workingorders', 'get'): {'workingOrders': []},
            ('history/activity', 'get'): {'activities': []},
        })
    broker._connection_generation = 1
    plain_call = broker._call
    raced: list[str] = []

    async def racing_call(endpoint, *, data=None, method='post'):
        if endpoint == 'confirms/o-ref-L' and not raced:
            raced.append(endpoint)
            await base._drain_agen(broker._reconcile_snapshot({'deal-L': reduced}, {}))
        return await plain_call(endpoint, data=data, method=method)

    broker._call = racing_call
    asyncio.run(broker._recover_in_flight_submissions())

    assert raced == ['confirms/o-ref-L']
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == STATE_SERVER_REF_SEEN and row.closed_ts_ms is None
    assert row.exchange_order_id is None and row.filled_qty == 0.0
    assert (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None
    assert ctx.find_by_ref('deal_id', 'deal-L') is None
    assert ctx.replay()[1][__test_helper_COID].resolution is None
    assert list(ctx.iter_events_by_kind_for_run_id('recovered_confirmed')) == []
    assert len(__test_helper_held_at_recovery_events(ctx)) == 1
    store.close()


def __test_a_held_market_take_over_stays_held_across_a_restart__(tmp_path):
    # The held MARKET entry of the reduced-position case meets a fresh
    # process. Its startup recovery matches the POST's trace and would confirm
    # the row with the snapshot's 0.5 as its fill, which is exactly what the
    # earlier process could not prove. The row stays pending and held.
    created_s = epoch_time() - __test_helper_AGE_S
    opened = __test_helper_activity(created_s + 0.4)
    broker, store, ctx, _created = __test_helper_crashed_entry(tmp_path, responses={
        ('history/activity', 'get'): {'activities': [opened]},
    })
    position = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-L', 'direction': 'BUY',
                             'size': 0.5, 'level': 1.1, 'createdDateUTC': ''}}
    assert [ev for ev in __test_helper_pass(broker, {'deal-L': position})
            if ev.event_type == 'filled'] == []
    held = ctx.get_order(__test_helper_COID)
    assert held is not None and (held.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN)

    ctx = __test_helper_restart(store, ctx, broker)
    broker._responses.update({
        ('positions', 'get'): {'positions': [position]},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': [
            opened, __test_helper_close_activity(created_s + 60.0, 'deal-L', 0.5)]},
    })

    asyncio.run(broker._recover_in_flight_submissions())

    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'submitted' and row.closed_ts_ms is None
    assert row.exchange_order_id is None and row.filled_qty == 0.0
    assert (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None
    assert ctx.find_by_ref('deal_id', 'deal-L') is None
    assert ctx.replay()[1][__test_helper_COID].resolution is None
    assert inherited_dispatch_in_flight(ctx, 'L') == __test_helper_COID
    assert len(__test_helper_held_at_recovery_events(ctx)) == 1
    store.close()


def __test_a_held_take_over_deal_is_kept_out_of_the_bots_exposure__(tmp_path):
    # The held working order executed into deal-P, which back-links it. The
    # one-way emulator plans its closes and reversals over the legs
    # fetch_raw_positions returns, and the startup reconcile adopts
    # get_position: counting deal-P there would close or net it, or book the
    # unproven 0.5 after all. It is not someone else's deal either: it is
    # never reported as foreign, and every netting write it could reach is
    # refused like one against an unconfirmed own entry's deal.
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, kind=ENTRY_KIND_WORKING, responses={
            ('history/activity', 'get'): {'activities': [
                __test_helper_activity(created_s + 0.4, deal_id='deal-W',
                                       activity_type='WORKING_ORDER'),
            ]},
        })
    position = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-P', 'workingOrderId': 'deal-W',
                             'direction': 'BUY', 'size': 0.5, 'level': 1.2,
                             'createdDateUTC': ''}}
    __test_helper_pass(broker, {'deal-P': position})
    held = ctx.get_order(__test_helper_COID)
    assert held is not None and (held.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN)
    broker._responses[('positions', 'get')] = {'positions': [position]}

    assert asyncio.run(broker.fetch_raw_positions('EURUSD')) == []
    assert asyncio.run(broker.get_position('EURUSD')) is None
    foreign = broker._foreign_deals([position], 'EURUSD', direction='BUY')
    assert [(deal['deal_id'], deal.get('ownership_pending')) for deal in foreign] \
        == [('deal-P', True)]
    assert list(ctx.iter_events_by_kind_for_run_id('foreign_deal_on_epic')) == []
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.exchange_order_id == 'deal-W' and row.filled_qty == 0.0
    store.close()


def __test_a_held_market_take_over_is_not_rejected_when_its_trace_leaves_the_read__(tmp_path):
    # The inherited MARKET entry was traced to deal-L and held for its
    # reduced size. Later reads no longer show the trace (the deal claimed by
    # another row, a read cut short): the landing stays proven, so three such
    # reads never reject the row and release the entry for a second send.
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_crashed_entry(tmp_path, responses={
        ('history/activity', 'get'): {'activities': [
            __test_helper_activity(created_s + 0.4)]},
    })
    position = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-L', 'direction': 'BUY',
                             'size': 0.5, 'level': 1.1}}
    now = __test_helper_clocked(broker)
    __test_helper_pass(broker, {'deal-L': position})
    held = ctx.get_order(__test_helper_COID)
    assert held is not None and (held.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN)
    broker._responses[('history/activity', 'get')] = {'activities': []}

    for _ in range(4):
        now[0] += 15.0
        assert [ev for ev in __test_helper_pass(broker, {'deal-L': position})
                if ev.event_type == 'filled'] == []

    assert len(__test_helper_activity_reads(broker)) == 5
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'submitted' and row.closed_ts_ms is None
    assert ctx.replay()[1][__test_helper_COID].resolution is None
    assert inherited_dispatch_in_flight(ctx, 'L') == __test_helper_COID
    store.close()


def __test_helper_same_id_take_over(tmp_path):
    """An inherited working entry taken over as order deal-W, still unbooked."""
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, kind=ENTRY_KIND_WORKING, responses={
            ('history/activity', 'get'): {'activities': [
                __test_helper_activity(created_s + 0.4, deal_id='deal-W',
                                       activity_type='WORKING_ORDER'),
            ]},
        })
    assert __test_helper_pass(broker) == []
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.exchange_order_id == 'deal-W'
    assert (row.extras or {}).get(EXTRAS_KEY_TAKEN_OVER_AT) is not None
    return broker, store, ctx


def __test_a_take_over_executed_under_its_own_deal_id_is_held_when_reduced__(
        tmp_path, caplog):
    # The venue lists the executed order's position under the order's own
    # deal id, at half the submitted size: the snapshot's working-to-position
    # step must not book that unproven 0.5. The row is held and the operator
    # told once, however many polls show it.
    broker, store, ctx = __test_helper_same_id_take_over(tmp_path)
    position = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-W', 'direction': 'BUY',
                             'size': 0.5, 'level': 1.2}}

    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            assert [ev for ev in __test_helper_pass(broker, {'deal-W': position})
                    if ev.event_type == 'filled'] == []

    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed' and row.closed_ts_ms is None
    assert row.filled_qty == 0.0 and (row.extras or {}).get('kind') == ENTRY_KIND_WORKING
    assert (row.extras or {}).get('entry_filled_at') is None
    assert (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None
    assert len(__test_helper_unproven_warnings(caplog)) == 1
    store.close()


def __test_a_take_over_executed_under_its_own_deal_id_at_full_size_is_booked_once__(tmp_path):
    broker, store, ctx = __test_helper_same_id_take_over(tmp_path)
    position = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-W', 'direction': 'BUY',
                             'size': 1.0, 'level': 1.2}}

    fills = [ev for ev in __test_helper_pass(broker, {'deal-W': position})
             if ev.event_type == 'filled']

    assert [(ev.order.client_order_id, ev.fill_qty) for ev in fills] \
        == [(__test_helper_COID, 1.0)]
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.filled_qty == 1.0
    assert (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is None
    assert [ev for ev in __test_helper_pass(broker, {'deal-W': position})
            if ev.event_type == 'filled'] == []
    store.close()


def __test_helper_two_crashed_entries(tmp_path, *, responses: dict):
    """A process POSTed entries ``A`` and ``B`` alike at the same moment and ended.

    :return: ``(broker, store, ctx, created_s)`` after the restart.
    """
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx = base._make_broker(tmp_path, responses=responses)
    for coid, intent in (('coid-a', 'A'), ('coid-b', 'B')):
        create_entry_order_row(
            ctx, coid=coid, symbol='EURUSD', side='buy', qty=1.0,
            intent_key=intent, pine_entry_id=intent, kind=ENTRY_KIND_POSITION,
            order_type='market',
        )
        ctx._store._conn.execute(
            "UPDATE orders SET created_ts_ms = ? WHERE client_order_id = ?",
            (int(created_s * 1000), coid),
        )
    ctx = __test_helper_restart(store, ctx, broker)
    return broker, store, ctx, created_s


def __test_a_trace_two_inherited_submissions_match_lands_neither__(tmp_path, caplog):
    # Entries A and B of the same side and size were sent within the match
    # window and one deal exists: either may have opened it. A was cancelled
    # by Pine meanwhile. Handing the deal to A would book B's position to a
    # cancelled entry and then release B to be sent again, so neither is
    # booked or released, and the operator is told once per row.
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_two_crashed_entries(tmp_path, responses={
        ('history/activity', 'get'): {'activities': [__test_helper_activity(created_s + 0.4)]},
    })
    cancelled = ctx.get_order('coid-a')
    assert cancelled is not None
    ctx.upsert_order('coid-a', extras={
        **(cancelled.extras or {}),
        EXTRAS_KEY_CANCEL_REQUESTED: {'cancel_coid': 'cancel-a', 'intent_key': 'A'},
    })
    position = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-L', 'direction': 'BUY',
                             'size': 1.0, 'level': 1.1}}
    now = __test_helper_clocked(broker)

    with caplog.at_level(logging.WARNING):
        for _ in range(5):
            assert [ev for ev in __test_helper_pass(broker, {'deal-L': position})
                    if ev.event_type == 'filled'] == []
            now[0] += 15.0

    assert len(__test_helper_activity_reads(broker)) == 10
    assert ctx.find_by_ref('deal_id', 'deal-L') is None
    for coid in ('coid-a', 'coid-b'):
        row = ctx.get_order(coid)
        assert row is not None and row.state == 'submitted' and row.closed_ts_ms is None
        assert row.exchange_order_id is None
        assert ctx.replay()[1][coid].resolution is None
    warnings = [r.getMessage() for r in caplog.records
                if 'cannot be told apart' in r.getMessage()]
    assert len(warnings) == 2
    assert any('coid-a' in w for w in warnings) and any('coid-b' in w for w in warnings)
    store.close()


def __test_the_startup_pass_lands_no_submission_on_a_contested_trace__(tmp_path):
    # The same two submissions at startup: one matching activity is no proof
    # for either, so both stay pending instead of the first taking the deal.
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_two_crashed_entries(tmp_path, responses={
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': [__test_helper_activity(created_s + 0.4)]},
    })

    asyncio.run(broker._recover_in_flight_submissions())

    assert ctx.find_by_ref('deal_id', 'deal-L') is None
    for coid in ('coid-a', 'coid-b'):
        row = ctx.get_order(coid)
        assert row is not None and row.state == 'submitted' and row.closed_ts_ms is None
        assert row.exchange_order_id is None
    store.close()


def __test_an_inherited_submission_stays_held_on_a_failed_activity_read__(tmp_path):
    broker, store, ctx, _created = __test_helper_crashed_entry(tmp_path, responses={
        ('error', 'history/activity', 'get'): ExchangeConnectionError("venue down"),
    })
    now = __test_helper_clocked(broker)

    for _ in range(4):
        assert __test_helper_pass(broker) == []
        now[0] += 15.0

    assert len(__test_helper_activity_reads(broker)) == 4
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'submitted' and row.closed_ts_ms is None
    assert ctx.replay()[1][__test_helper_COID].resolution is None
    store.close()


def __test_an_inherited_submission_with_a_trace_outside_the_window_stays_held__(tmp_path):
    # Same epic, direction and size, 20 s off the row's creation, on no
    # other row: it may be this POST's trace under a skewed venue clock, so
    # neither a landing nor an absence is proven.
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_crashed_entry(tmp_path, responses={
        ('history/activity', 'get'): {'activities': [
            __test_helper_activity(created_s + 20.0, deal_id='deal-X'),
        ]},
    })
    now = __test_helper_clocked(broker)

    for _ in range(4):
        assert __test_helper_pass(broker) == []
        now[0] += 15.0

    assert len(__test_helper_activity_reads(broker)) == 4
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'submitted' and row.exchange_order_id is None
    assert ctx.replay()[1][__test_helper_COID].resolution is None
    store.close()


def __test_an_inherited_submission_older_than_the_activity_history_stays_held__(
        tmp_path, caplog):
    # The venue's activity history reaches back one day: past that, whether
    # the POST opened a deal cannot be read, so the row is held and the
    # operator is told once.
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, age_s=86400.0, responses={('history/activity', 'get'): {'activities': []}},
    )
    now = __test_helper_clocked(broker)

    with caplog.at_level(logging.WARNING):
        for _ in range(4):
            assert __test_helper_pass(broker) == []
            now[0] += 15.0

    assert __test_helper_activity_reads(broker) == []
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'submitted' and row.closed_ts_ms is None
    warnings = [r.getMessage() for r in caplog.records
                if 'older than the venue' in r.getMessage()]
    assert len(warnings) == 1 and __test_helper_COID in warnings[0]
    store.close()


def __test_an_inherited_submission_with_a_reference_is_settled_by_its_confirm__(tmp_path):
    # A row an earlier process left in server_ref_seen has its dealReference:
    # the confirms retry answers it like a parked one of this process.
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, state=STATE_SERVER_REF_SEEN, deal_reference='o-ref-L', responses={
            ('confirms/o-ref-L', 'get'): {'dealStatus': 'REJECTED',
                                          'rejectReason': 'RISK_CHECK'},
            ('history/activity', 'get'): {'activities': []},
        })

    assert __test_helper_pass(broker) == []

    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'rejected'
    assert ctx.replay()[1][__test_helper_COID].resolution == 'rejected'
    store.close()


def __test_a_parked_submission_of_this_process_is_not_looked_up_by_activity__(tmp_path):
    # Only inherited rows are the activity lookup's; a disposition_unknown
    # row of this process without a reference keeps its existing handling.
    broker, store, ctx = base._make_broker(tmp_path, responses={
        ('history/activity', 'get'): {'activities': []},
    })
    create_entry_order_row(
        ctx, coid=__test_helper_COID, symbol='EURUSD', side='buy', qty=1.0,
        intent_key='L', pine_entry_id='L', kind=ENTRY_KIND_POSITION, order_type='market',
    )
    ctx.set_order_state(__test_helper_COID, STATE_DISPOSITION_UNKNOWN)
    ctx.record_park(__test_helper_COID, 'L')

    assert __test_helper_pass(broker) == []

    assert __test_helper_activity_reads(broker) == []
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == STATE_DISPOSITION_UNKNOWN
    store.close()
