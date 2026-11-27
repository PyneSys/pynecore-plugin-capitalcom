"""In-run resolution of entry submissions an earlier process left pending.

Capital.com carries no client order id, so the engine holds an intent's entry
while an inherited dispatch of it is unresolved. The plugin feeds the core
resolver with the activity history as evidence; these tests pin each verdict
and what it does to the row, the park and the events.
"""
import asyncio
import json
import logging
from types import SimpleNamespace
from datetime import UTC, datetime
from time import time as epoch_time

import pytest

from pynecore import lib
from pynecore.core.broker.disappearance import MissingResolution
from pynecore.core.broker.exceptions import ExchangeConnectionError
from pynecore.core.broker.journal import DispatchJournal
from pynecore.core.broker.models import (
    CancelDispositionOutcome,
    QuarantineKind,
    CancelIntent,
    DispatchEnvelope,
    LegType,
    OrderStatus,
    OrderType,
)
from pynecore.core.broker.parked_dispatch import (
    inherited_dispatch_in_flight,
    park_inherited_dispatches,
)
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.position import BrokerPosition
from pynecore.core.broker.storage import BrokerStore, RunContext
from pynecore.core.broker.sync_engine import OrderSyncEngine
from pynecore.lib.strategy import Order, _order_type_close, _order_type_entry
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    STATE_DISPOSITION_UNKNOWN,
    STATE_SERVER_REF_SEEN,
    create_entry_order_row,
)

from pynecore_capitalcom import order_history
from pynecore_capitalcom.models import _activity_fingerprint, _close_identity
from pynecore_capitalcom.exceptions import OrderNotFoundError
from pynecore_capitalcom.helpers import (
    EXTRAS_KEY_BOOKED_CLOSES,
    EXTRAS_KEY_CANCEL_REQUESTED,
    EXTRAS_KEY_DEFERRED_CLOSES,
    EXTRAS_KEY_ENTRY_PRICE_PENDING,
    EXTRAS_KEY_EXECUTION_HELD,
    EXTRAS_KEY_ORDER_LEVEL,
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


def __test_helper_book_reads(broker) -> list[str]:
    """The ``/positions`` reads made for an open-book verdict."""
    return [endpoint for endpoint, method, _data in broker._calls
            if endpoint == 'positions' and method == 'get']


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


def __test_a_held_take_over_deal_is_own_exposure_that_no_netting_write_reaches__(tmp_path):
    # The held working order executed into deal-P, which back-links it. The
    # deal came from the bot's own order, so it is the bot's own exposure:
    # get_position counts it and fetch_raw_positions lists it (unattributed),
    # while the startup adoption stays clamped to the journal's booked
    # exposure (the row's filled_qty is 0). It is never reported as foreign,
    # and until its fill is proven and booked every netting write it could
    # reach is refused like one against an unconfirmed own entry's deal.
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

    legs = asyncio.run(broker.fetch_raw_positions('EURUSD'))
    assert [(leg.leg_id, leg.entry_id, leg.qty) for leg in legs] == [('deal-P', None, 0.5)]
    exposure = asyncio.run(broker.get_position('EURUSD'))
    assert exposure is not None and exposure.side == 'long' and exposure.size == 0.5
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


def __test_a_trace_two_inherited_submissions_match_goes_to_the_earlier_one__(tmp_path):
    # Entries A and B of the same side and size were sent within the match
    # window and one deal exists. The entry POST carries nothing else the
    # venue shows (no client order id), so the two are the same order for
    # the account: the deal goes to the earlier submission (creation, then
    # client order id), and B, left without a deal, opened nothing and is
    # rejected after three reads. A was cancelled by Pine meanwhile: its
    # market entry had executed, so the cancel is a no-op and the fill is
    # booked.
    created_s = epoch_time() - __test_helper_AGE_S
    position = {'market': {'epic': 'EURUSD'},
                'position': {'dealId': 'deal-L', 'direction': 'BUY',
                             'size': 1.0, 'level': 1.1}}
    broker, store, ctx, _created = __test_helper_two_crashed_entries(tmp_path, responses={
        ('history/activity', 'get'): {'activities': [__test_helper_activity(created_s + 0.4)]},
        ('positions', 'get'): {'positions': [position]},
    })
    cancelled = ctx.get_order('coid-a')
    assert cancelled is not None
    ctx.upsert_order('coid-a', extras={
        **(cancelled.extras or {}),
        EXTRAS_KEY_CANCEL_REQUESTED: {'cancel_coid': 'cancel-a', 'intent_key': 'A'},
    })
    now = __test_helper_clocked(broker)

    fills = []
    for _ in range(4):
        fills += [ev for ev in __test_helper_pass(broker, {'deal-L': position})
                  if ev.event_type == 'filled']
        now[0] += 15.0

    assert [(ev.order.client_order_id, ev.order.id) for ev in fills] == [('coid-a', 'deal-L')]
    first = ctx.get_order('coid-a')
    assert first is not None and first.state == 'confirmed'
    assert first.exchange_order_id == 'deal-L'
    assert 'cancel_noop' in base._event_kinds(ctx, 'coid-a')
    second = ctx.get_order('coid-b')
    assert second is not None and second.state == 'rejected' and second.closed_ts_ms is not None
    assert ctx.replay()[1]['coid-b'].resolution == 'rejected'
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


def __test_helper_own_confirmed_entry(ctx: RunContext, *, sent_s: float, deal_id: str) -> None:
    """A market entry of this run whose deal id its confirm gave: a clock sample."""
    create_entry_order_row(
        ctx, coid='coid-own', symbol='EURUSD', side='buy', qty=2.0,
        intent_key='O', pine_entry_id='O', kind=ENTRY_KIND_POSITION, order_type='market',
    )
    ctx.upsert_order('coid-own', state='confirmed', exchange_order_id=deal_id)
    ctx._store._conn.execute(
        "UPDATE orders SET created_ts_ms = ? WHERE client_order_id = ?",
        (int(sent_s * 1000), 'coid-own'),
    )


def __test_a_trace_outside_the_window_lands_without_a_clock_sample__(tmp_path):
    # Same epic, direction and size, 20 s off the row's creation, on no
    # other row, and no own deal to measure the venue clock on: the deal may
    # be this POST's under a skewed venue clock, and nothing else explains
    # it, so the row lands on it and its fill is booked.
    created_s = epoch_time() - __test_helper_AGE_S
    position = __test_helper_position('deal-X')
    broker, store, ctx, _created = __test_helper_crashed_entry(tmp_path, responses={
        ('history/activity', 'get'): {'activities': [
            __test_helper_activity(created_s + 20.0, deal_id='deal-X'),
        ]},
    })

    events = __test_helper_pass(broker, {'deal-X': position})

    assert [(ev.order.client_order_id, ev.order.id) for ev in events
            if ev.event_type == 'filled'] == [(__test_helper_COID, 'deal-X')]
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed' and row.exchange_order_id == 'deal-X'
    store.close()


def __test_a_trace_outside_the_window_is_refuted_by_a_measured_clock__(tmp_path):
    # The bot's own confirmed entry opened its deal 0.4 s after its send:
    # the venue clock agrees with the local one, so a deal 20 s off the
    # row's send is not its POST's. The row opened nothing and is rejected
    # after three reads; the deal is someone else's.
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_crashed_entry(tmp_path, responses={
        ('history/activity', 'get'): {'activities': [
            __test_helper_activity(created_s + 20.0, deal_id='deal-X'),
            __test_helper_activity(created_s - 99.6, deal_id='deal-own', size=2.0),
        ]},
    })
    __test_helper_own_confirmed_entry(ctx, sent_s=created_s - 100.0, deal_id='deal-own')
    now = __test_helper_clocked(broker)

    for _ in range(3):
        assert __test_helper_pass(broker) == []
        now[0] += 15.0

    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'rejected' and row.exchange_order_id is None
    assert ctx.find_by_ref('deal_id', 'deal-X') is None
    store.close()


def __test_a_measured_clock_offset_pins_a_shifted_trace__(tmp_path):
    # The venue clock runs 20 s ahead: the bot's own entry's deal is dated
    # 20.3 s after its send. The deal dated 20.4 s after the row's send is
    # then pinned to it, and one dated 0.5 s after the send is not: the row
    # lands on the shifted one.
    created_s = epoch_time() - __test_helper_AGE_S
    position = __test_helper_position('deal-X')
    broker, store, ctx, _created = __test_helper_crashed_entry(tmp_path, responses={
        ('history/activity', 'get'): {'activities': [
            __test_helper_activity(created_s + 0.5, deal_id='deal-Y'),
            __test_helper_activity(created_s + 20.4, deal_id='deal-X'),
            __test_helper_activity(created_s - 79.7, deal_id='deal-own', size=2.0),
        ]},
    })
    __test_helper_own_confirmed_entry(ctx, sent_s=created_s - 100.0, deal_id='deal-own')

    events = __test_helper_pass(broker, {'deal-X': position})

    assert [(ev.order.client_order_id, ev.order.id) for ev in events
            if ev.event_type == 'filled'] == [(__test_helper_COID, 'deal-X')]
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.exchange_order_id == 'deal-X'
    store.close()


def __test_an_inherited_submission_older_than_the_activity_history_with_nothing_open_is_rejected__(
        tmp_path):
    # The venue's activity history reaches back one day: past that, the
    # POST's trace cannot be read, but whatever it opened would still be
    # listed. Nothing of the row's epic and direction is open, so nothing
    # of the POST is exposure: after three such reads the row is rejected
    # and the engine may send the entry again, no activity read is made and
    # nobody is asked to look.
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, age_s=86400.0, responses={('history/activity', 'get'): {'activities': []}},
    )
    now = __test_helper_clocked(broker)

    for _ in range(3):
        assert __test_helper_pass(broker) == []
        now[0] += 15.0

    assert __test_helper_activity_reads(broker) == []
    assert len(__test_helper_book_reads(broker)) == 3
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'rejected' and row.closed_ts_ms is not None
    assert ctx.replay()[1][__test_helper_COID].resolution == 'rejected'
    # The verdict states that no history source was read, once.
    absent = base._event_payloads(ctx, 'inherited_submission_absent_without_history')
    assert [payload['evidence'] for payload in absent] == ['open_book_only']
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


@pytest.mark.parametrize('state', ['submitted', STATE_SERVER_REF_SEEN, STATE_DISPOSITION_UNKNOWN])
def __test_a_cancelled_inherited_working_submission_is_deleted_once_it_lands__(tmp_path, state):
    # Pine cancels an entry whose POST an earlier process never saw answered.
    # The row has no deal id to DELETE, so the cancel is stamped on it and the
    # row stays pending: the hold on the intent and the resolver's lookups
    # stay with it, and the order is DELETEd the moment the venue shows it
    # landed. Measured live (Capital.com lane, 2026-08-17): the cancel moved
    # such a row to ``cancel_pending``, a state nothing resolves, and the row
    # was adopted on every restart for three months, counted as an own
    # opposite-side netting candidate.
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, kind=ENTRY_KIND_WORKING, state=state, responses={
            ('history/activity', 'get'): {'activities': [
                __test_helper_activity(created_s + 0.4, deal_id='deal-W',
                                       activity_type='WORKING_ORDER'),
            ]},
            ('workingorders/deal-W', 'delete'): {},
        })
    env = DispatchEnvelope(
        intent=CancelIntent(pine_id='L', symbol='EURUSD'),
        run_tag='test', bar_ts_ms=1700000000000,
    )

    assert asyncio.run(broker.execute_cancel(env)) is True

    assert not any(method == 'delete' for _endpoint, method, _data in broker._calls)
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == state and row.closed_ts_ms is None
    assert (row.extras or {})[EXTRAS_KEY_CANCEL_REQUESTED]['intent_key'] == 'L'
    assert inherited_dispatch_in_flight(ctx, 'L') == __test_helper_COID

    assert __test_helper_pass(broker) == []

    assert ('workingorders/deal-W', 'delete') in [
        (endpoint, method) for endpoint, method, _data in broker._calls]
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.closed_ts_ms is not None
    assert not any(r.client_order_id == __test_helper_COID for r in ctx.iter_live_orders())
    assert 'cancelled' in base._event_kinds(ctx, __test_helper_COID)
    store.close()


def __test_the_startup_pass_leaves_a_cancelled_submission_to_the_resolver__(tmp_path):
    # The startup pass would confirm the landed working order and make it a
    # live order the cancel never reaches. A submission Pine already
    # cancelled stays pending instead; the in-run resolver's take-over
    # DELETEs it.
    created_s = epoch_time() - __test_helper_AGE_S
    landed = __test_helper_activity(created_s + 0.4, deal_id='deal-W',
                                    activity_type='WORKING_ORDER')
    broker, store, ctx, _created = __test_helper_crashed_entry(
        tmp_path, kind=ENTRY_KIND_WORKING, responses={
            ('positions', 'get'): {'positions': []},
            ('workingorders', 'get'): {'workingOrders': [{
                'marketData': {'epic': 'EURUSD'},
                'workingOrderData': {'dealId': 'deal-W', 'epic': 'EURUSD',
                                     'direction': 'BUY', 'orderSize': 1.0,
                                     'orderType': 'LIMIT', 'orderLevel': 1.1},
            }]},
            ('history/activity', 'get'): {'activities': [landed]},
            ('workingorders/deal-W', 'delete'): {},
        })
    row = ctx.get_order(__test_helper_COID)
    assert row is not None
    ctx.upsert_order(__test_helper_COID, extras={
        **(row.extras or {}),
        EXTRAS_KEY_CANCEL_REQUESTED: {'cancel_coid': 'cancel-l', 'intent_key': 'L'},
    })

    asyncio.run(broker._recover_in_flight_submissions())

    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'submitted' and row.closed_ts_ms is None
    assert row.exchange_order_id is None

    assert __test_helper_pass(broker) == []

    assert ('workingorders/deal-W', 'delete') in [
        (endpoint, method) for endpoint, method, _data in broker._calls]
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.closed_ts_ms is not None
    assert 'cancelled' in base._event_kinds(ctx, __test_helper_COID)
    store.close()


def __test_helper_stamp(when_s: float) -> str:
    """A venue ``createdDateUTC`` / ``dateUTC`` stamp of an epoch time."""
    stamp = datetime.fromtimestamp(when_s, tz=UTC)
    return stamp.strftime('%Y-%m-%dT%H:%M:%S.') + f"{stamp.microsecond // 1000:03d}"


def __test_helper_crashed_entries(tmp_path, entries: list[tuple[str, str, str]], *,
                                  responses: dict, kind: str = ENTRY_KIND_POSITION,
                                  age_s: float = __test_helper_AGE_S,
                                  order_level: float | None = None):
    """A process POSTed one buy of 1.0 per ``(coid, intent, pine id)`` at one moment and ended.

    :return: ``(broker, store, ctx, created_s)`` after the restart.
    """
    created_s = epoch_time() - age_s
    broker, store, ctx = base._make_broker(tmp_path, responses=responses)
    for coid, intent, pine_id in entries:
        create_entry_order_row(
            ctx, coid=coid, symbol='EURUSD', side='buy', qty=1.0,
            intent_key=intent, pine_entry_id=pine_id, kind=kind,
            order_type='market' if kind == ENTRY_KIND_POSITION else 'limit',
        )
        ctx._store._conn.execute(
            "UPDATE orders SET created_ts_ms = ? WHERE client_order_id = ?",
            (int(created_s * 1000), coid),
        )
        if order_level is not None:
            row = ctx.get_order(coid)
            assert row is not None
            ctx.upsert_order(coid, extras={**(row.extras or {}),
                                           EXTRAS_KEY_ORDER_LEVEL: order_level})
    ctx = __test_helper_restart(store, ctx, broker)
    return broker, store, ctx, created_s


def __test_helper_position(deal_id: str, *, created_s: float | None = None,
                           working_order_id: str | None = None, size: float = 1.0) -> dict:
    position: dict = {'dealId': deal_id, 'direction': 'BUY', 'size': size, 'level': 1.1}
    if created_s is not None:
        position['createdDateUTC'] = __test_helper_stamp(created_s)
    if working_order_id is not None:
        position['workingOrderId'] = working_order_id
    return {'market': {'epic': 'EURUSD'}, 'position': position}


def __test_two_interchangeable_inherited_submissions_are_dealt_one_deal_each__(tmp_path):
    # Two entries of the same Pine entry, side, size, kind and protection were
    # sent at the same moment and two deals opened. Which POST opened which
    # deal is unknowable and changes nothing the account or the strategy can
    # observe, so the deals are dealt out in a fixed order: the earlier
    # submission (creation, then client order id) takes the earlier deal.
    created_s = epoch_time() - __test_helper_AGE_S
    first = __test_helper_position('deal-1')
    second = __test_helper_position('deal-2')
    broker, store, ctx, _created = __test_helper_crashed_entries(
        tmp_path, [('coid-1', 'L#1', 'L'), ('coid-2', 'L#2', 'L')], responses={
            ('history/activity', 'get'): {'activities': [
                __test_helper_activity(created_s + 0.6, deal_id='deal-2'),
                __test_helper_activity(created_s + 0.3, deal_id='deal-1'),
            ]},
        })

    events = __test_helper_pass(broker, {'deal-1': first, 'deal-2': second})

    fills = sorted((ev.order.client_order_id, ev.order.id) for ev in events
                   if ev.event_type == 'filled')
    assert fills == [('coid-1', 'deal-1'), ('coid-2', 'deal-2')]
    for coid, deal_id in (('coid-1', 'deal-1'), ('coid-2', 'deal-2')):
        row = ctx.get_order(coid)
        assert row is not None and row.state == 'confirmed'
        assert row.exchange_order_id == deal_id
    store.close()


def __test_an_interchangeable_submission_left_without_a_deal_is_rejected__(tmp_path):
    # Two interchangeable submissions, one deal: the earlier takes it; the
    # other has no deal left once the first one tracks it, so it reads absent
    # and is rejected after three reads, and the engine may send it again.
    created_s = epoch_time() - __test_helper_AGE_S
    position = __test_helper_position('deal-1')
    broker, store, ctx, _created = __test_helper_crashed_entries(
        tmp_path, [('coid-1', 'L#1', 'L'), ('coid-2', 'L#2', 'L')], responses={
            ('history/activity', 'get'): {'activities': [
                __test_helper_activity(created_s + 0.3, deal_id='deal-1'),
            ]},
        })
    now = __test_helper_clocked(broker)

    fills = []
    for _ in range(4):
        fills += [ev for ev in __test_helper_pass(broker, {'deal-1': position})
                  if ev.event_type == 'filled']
        now[0] += 15.0

    assert [(ev.order.client_order_id, ev.order.id) for ev in fills] == [('coid-1', 'deal-1')]
    first = ctx.get_order('coid-1')
    assert first is not None and first.state == 'confirmed'
    assert first.exchange_order_id == 'deal-1'
    second = ctx.get_order('coid-2')
    assert second is not None and second.state == 'rejected' and second.closed_ts_ms is not None
    assert ctx.replay()[1]['coid-2'].resolution == 'rejected'
    store.close()


def __test_a_deal_another_pyne_run_owns_is_no_trace_of_an_inherited_submission__(tmp_path):
    # The only matching activity is of a deal another Pyne run on the same
    # account and epic tracks: it is that run's deal, never this POST's
    # trace, so the row reads absent and is rejected.
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_crashed_entry(tmp_path, responses={
        ('history/activity', 'get'): {'activities': [
            __test_helper_activity(created_s + 0.4, deal_id='deal-X'),
        ]},
    })
    other = store.open_run(
        RunIdentity(strategy_id="other", symbol="EURUSD", timeframe="60",
                    account_id="test-account", label='other'),
        script_source="// other",
    )
    other.upsert_order('other-coid', symbol='EURUSD', side='buy', qty=1.0,
                       state='confirmed', exchange_order_id='deal-X',
                       intent_key='O', pine_entry_id='O')
    assert ctx.foreign_live_exchange_order_ids() == {'deal-X'}
    now = __test_helper_clocked(broker)

    for _ in range(3):
        assert __test_helper_pass(broker) == []
        now[0] += 15.0

    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'rejected' and row.exchange_order_id is None
    assert ctx.find_by_ref('deal_id', 'deal-X') is None
    store.close()


def __test_an_old_inherited_market_submission_lands_on_the_one_open_deal__(tmp_path):
    # Past the activity history, the one open, untracked deal of the row's
    # epic, direction and size, opened when the row was sent, is the POST's:
    # it lands there and its fill is booked once. A deal opened long after
    # the row is no candidate of a market POST.
    broker, store, ctx, created_s = __test_helper_crashed_entries(
        tmp_path, [(__test_helper_COID, 'L', 'L')], age_s=86400.0, responses={})
    landed = __test_helper_position('deal-L', created_s=created_s + 1.0)
    later = __test_helper_position('deal-M', created_s=created_s + 3600.0)
    broker._responses[('positions', 'get')] = {'positions': [landed, later]}

    events = __test_helper_pass(broker, {'deal-L': landed, 'deal-M': later})

    fills = [ev for ev in events if ev.event_type == 'filled']
    assert [(ev.order.client_order_id, ev.order.id) for ev in fills] \
        == [(__test_helper_COID, 'deal-L')]
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed' and row.exchange_order_id == 'deal-L'
    assert __test_helper_activity_reads(broker) == []
    store.close()


def __test_an_old_inherited_working_submission_lands_on_its_resting_order__(tmp_path):
    # Past the activity history, the resting order of the row's epic,
    # direction, size and placed level, created when the row was sent, is
    # the POST's; one at another level is not.
    broker, store, ctx, created_s = __test_helper_crashed_entries(
        tmp_path, [(__test_helper_COID, 'L', 'L')], kind=ENTRY_KIND_WORKING,
        age_s=86400.0, order_level=1.1, responses={})

    def resting(deal_id: str, level: float) -> dict:
        return {'marketData': {'epic': 'EURUSD'},
                'workingOrderData': {'dealId': deal_id, 'epic': 'EURUSD', 'direction': 'BUY',
                                     'orderSize': 1.0, 'orderType': 'LIMIT',
                                     'orderLevel': level,
                                     'createdDateUTC': __test_helper_stamp(created_s + 1.0)}}

    broker._responses[('workingorders', 'get')] = {
        'workingOrders': [resting('deal-W', 1.1), resting('deal-V', 1.2)]}

    assert __test_helper_pass(broker) == []

    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed' and row.exchange_order_id == 'deal-W'
    store.close()


def __test_an_old_executed_working_submission_lands_on_the_order_its_position_names__(
        tmp_path):
    # Past the activity history, one position created after the row fits
    # the working POST's execution (its creation is the execution's time,
    # never the send's). Nothing else explains it: the row lands on the
    # order the position back-links, and the take-over books the fill once.
    broker, store, ctx, created_s = __test_helper_crashed_entries(
        tmp_path, [(__test_helper_COID, 'L', 'L')], kind=ENTRY_KIND_WORKING,
        age_s=86400.0, responses={})
    position = __test_helper_position('deal-P', created_s=created_s + 3600.0,
                                      working_order_id='deal-W')
    broker._responses[('positions', 'get')] = {'positions': [position]}

    events = __test_helper_pass(broker, {'deal-P': position})

    assert [(ev.order.client_order_id, ev.order.id) for ev in events
            if ev.event_type == 'filled'] == [(__test_helper_COID, 'deal-P')]
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed' and row.exchange_order_id == 'deal-P'
    store.close()


def __test_helper_quarantine(broker) -> list:
    """Wire the engine's quarantine: the latches, and the write block they impose."""
    latched: list = []
    broker.quarantine_sink = lambda reason, context, *, kind, cause=None: latched.append(
        (reason, context, kind))
    broker.venue_writes_blocked = lambda: bool(latched)
    return latched


def __test_two_open_deals_for_one_old_submission_are_an_external_intervention__(tmp_path):
    # Past the activity history, two open positions fit one working POST:
    # one POST opens one deal, so one of them is someone else's, and which
    # is unprovable. That is reported once as an external intervention; the
    # quarantine forbids venue writes, and the row is left as it is without
    # another venue read. Once the operator resumed, the earliest open deal
    # goes to the submission and the other stays someone else's.
    broker, store, ctx, created_s = __test_helper_crashed_entries(
        tmp_path, [(__test_helper_COID, 'L', 'L')], kind=ENTRY_KIND_WORKING,
        age_s=86400.0, responses={})
    first = __test_helper_position('deal-P', created_s=created_s + 3600.0,
                                   working_order_id='deal-W')
    second = __test_helper_position('deal-Q', created_s=created_s + 7200.0,
                                    working_order_id='deal-V')
    broker._responses[('positions', 'get')] = {'positions': [first, second]}
    latched = __test_helper_quarantine(broker)
    now = __test_helper_clocked(broker)
    snapshot = {'deal-P': first, 'deal-Q': second}

    for _ in range(4):
        assert __test_helper_pass(broker, snapshot) == []
        now[0] += 15.0

    assert len(latched) == 1
    reason, context, kind = latched[0]
    assert kind == QuarantineKind.EXTERNAL_INTERVENTION
    assert context['deals'] == ['deal-W', 'deal-V'] and context['submissions'] == [
        __test_helper_COID]
    assert len(__test_helper_book_reads(broker)) == 1
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'submitted' and row.exchange_order_id is None

    latched.clear()
    events = __test_helper_pass(broker, snapshot)

    assert [(ev.order.client_order_id, ev.order.id) for ev in events
            if ev.event_type == 'filled'] == [(__test_helper_COID, 'deal-P')]
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.exchange_order_id == 'deal-P'
    assert latched == []
    assert ctx.find_by_ref('deal_id', 'deal-Q') is None
    store.close()


def __test_two_pinned_deals_for_one_submission_under_the_ignore_policy_land_the_earlier__(
        tmp_path):
    # Two deals opened within the match window of one market POST: someone
    # else trades the epic. The 'ignore' policy only audits that; the
    # earliest open deal goes to the submission, the other is someone
    # else's, and nothing latches.
    created_s = epoch_time() - __test_helper_AGE_S
    early = __test_helper_position('deal-1')
    late = __test_helper_position('deal-2')
    broker, store, ctx, _created = __test_helper_crashed_entry(tmp_path, responses={
        ('history/activity', 'get'): {'activities': [
            __test_helper_activity(created_s + 0.6, deal_id='deal-2'),
            __test_helper_activity(created_s + 0.3, deal_id='deal-1'),
        ]},
        ('positions', 'get'): {'positions': [early, late]},
    })
    latched = __test_helper_quarantine(broker)
    broker.on_unexpected_cancel = 'ignore'

    events = __test_helper_pass(broker, {'deal-1': early, 'deal-2': late})

    assert [(ev.order.client_order_id, ev.order.id) for ev in events
            if ev.event_type == 'filled'] == [(__test_helper_COID, 'deal-1')]
    assert latched == []
    assert len(list(ctx.iter_events_by_kind_for_run_id('external_intervention_ignored'))) == 1
    assert ctx.find_by_ref('deal_id', 'deal-2') is None
    store.close()


def __test_two_pinned_deals_for_one_submission_latch_and_hold_the_row__(tmp_path):
    # The same surplus under the default 'stop' policy: the external
    # intervention latches once, the row stays pending and is not read again
    # while the quarantine forbids venue writes.
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_crashed_entry(tmp_path, responses={
        ('history/activity', 'get'): {'activities': [
            __test_helper_activity(created_s + 0.6, deal_id='deal-2'),
            __test_helper_activity(created_s + 0.3, deal_id='deal-1'),
        ]},
    })
    latched = __test_helper_quarantine(broker)
    now = __test_helper_clocked(broker)

    for _ in range(4):
        assert __test_helper_pass(broker) == []
        now[0] += 15.0

    assert [kind for _reason, _context, kind in latched] == [
        QuarantineKind.EXTERNAL_INTERVENTION]
    assert len(__test_helper_activity_reads(broker)) == 1
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'submitted' and row.exchange_order_id is None
    assert ctx.replay()[1][__test_helper_COID].resolution is None
    store.close()


def __test_resting_orders_listed_at_their_levels_go_to_the_submission_of_that_level__(
        tmp_path):
    # Two resting entries of the same side and size at 1.1 and 1.2, sent
    # together; their two orders are listed at those levels. The level the
    # venue shows tells them apart, so each lands on the order of its level,
    # although the fixed order alone would have crossed them.
    created_s = epoch_time() - __test_helper_AGE_S

    def resting(deal_id: str, level: float) -> dict:
        return {'marketData': {'epic': 'EURUSD'},
                'workingOrderData': {'dealId': deal_id, 'epic': 'EURUSD', 'direction': 'BUY',
                                     'orderSize': 1.0, 'orderType': 'LIMIT',
                                     'orderLevel': level}}

    broker, store, ctx = base._make_broker(tmp_path, responses={
        ('history/activity', 'get'): {'activities': [
            __test_helper_activity(created_s + 0.3, deal_id='deal-high',
                                   activity_type='WORKING_ORDER'),
            __test_helper_activity(created_s + 0.6, deal_id='deal-low',
                                   activity_type='WORKING_ORDER'),
        ]},
        ('workingorders', 'get'): {'workingOrders': [resting('deal-high', 1.2),
                                                     resting('deal-low', 1.1)]},
    })
    for coid, intent, level in (('coid-1', 'L#1', 1.1), ('coid-2', 'L#2', 1.2)):
        create_entry_order_row(
            ctx, coid=coid, symbol='EURUSD', side='buy', qty=1.0, intent_key=intent,
            pine_entry_id=intent, kind=ENTRY_KIND_WORKING, order_type='limit',
        )
        row = ctx.get_order(coid)
        assert row is not None
        ctx.upsert_order(coid, extras={**(row.extras or {}), EXTRAS_KEY_ORDER_LEVEL: level})
        ctx._store._conn.execute(
            "UPDATE orders SET created_ts_ms = ? WHERE client_order_id = ?",
            (int(created_s * 1000), coid),
        )
    ctx = __test_helper_restart(store, ctx, broker)

    assert __test_helper_pass(broker) == []

    for coid, deal_id in (('coid-1', 'deal-low'), ('coid-2', 'deal-high')):
        row = ctx.get_order(coid)
        assert row is not None and row.state == 'confirmed' and row.exchange_order_id == deal_id
    store.close()


def __test_helper_closed_deal(created_s: float) -> list[dict]:
    """The landed market deal's opening, priced, and its full close a minute later."""
    opening = {**__test_helper_activity(created_s + 0.4), 'level': 1.1, 'status': 'EXECUTED'}
    return [opening, __test_helper_close_activity(created_s + 60.0, 'deal-L', 1.0)]


def __test_helper_engine(broker, ctx: RunContext) -> OrderSyncEngine:
    """The sync engine of the run, wired to the plugin like the runner does."""
    engine = OrderSyncEngine(broker=broker, position=BrokerPosition(), symbol='EURUSD',
                             run_tag=ctx.run_tag, mintick=0.00001, store_ctx=ctx)
    broker.quarantine_sink = engine.record_quarantine
    return engine


def __test_helper_deliver(engine: OrderSyncEngine, events: list) -> None:
    """The event stream hands the events over and the engine drains them."""
    for event in events:
        engine.on_order_event(event)
    engine._drain_events()


def __test_a_landed_submission_whose_deal_closed_since_is_settled_from_the_history__(
        tmp_path, monkeypatch):
    # The inherited MARKET entry landed on deal-L, which a minute later was
    # closed in full; the venue no longer lists it. The stream will never
    # deliver that execution, so the plugin hands the row to the running
    # engine's held-execution recovery: the deal is mapped to the row, its
    # opening queued ahead of every later fill, its close queued behind it,
    # and the engine asked. The engine takes the row over (confirmed on the
    # deal, the entry no longer held as in flight), the deal's absence is no
    # disappearance while the engine holds it, and fetch_order_execution
    # answers by the deal id with the opening's fill under the stream's fill
    # id. Once the engine booked it, the next activity read delivers the
    # close, the book matches the flat venue, and the row is torn down.
    created_s = epoch_time() - __test_helper_AGE_S
    activities = __test_helper_closed_deal(created_s)
    broker, store, ctx, _created = __test_helper_crashed_entry(tmp_path, responses={
        ('history/activity', 'get'): {'activities': activities},
    })
    monkeypatch.setattr(lib, '_script', SimpleNamespace(initial_capital=1_000_000.0))
    engine = __test_helper_engine(broker, ctx)
    engine.reconcile()
    engine.sync(1700000000000)

    assert __test_helper_pass(broker) == []

    assert ctx.execution_hold_requests()[__test_helper_COID]['order_id'] == 'deal-L'
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'submitted' and row.exchange_order_id is None
    assert (row.extras or {})[EXTRAS_KEY_EXECUTION_HELD]['deal_id'] == 'deal-L'
    assert (row.extras or {})[EXTRAS_KEY_ENTRY_PRICE_PENDING] == activities[0]['dateUTC']
    assert _close_identity(activities[1]) in (row.extras or {})[EXTRAS_KEY_DEFERRED_CLOSES]
    mapped = ctx.find_by_ref('deal_id', 'deal-L')
    assert mapped is not None and mapped.client_order_id == __test_helper_COID
    assert 'execution_handed_to_engine' in base._event_kinds(ctx, __test_helper_COID)

    engine._consume_execution_hold_requests()

    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed' and row.exchange_order_id == 'deal-L'
    assert inherited_dispatch_in_flight(ctx, 'L') is None
    assert 'deal-L' in ctx.held_execution_order_ids()
    assert broker._is_disappearance_exempt(row)
    assert not ctx.awaits_venue_answer(row)

    execution = asyncio.run(broker.fetch_order_execution('deal-L', None))
    assert execution is not None and execution.order is not None
    assert execution.order.status == OrderStatus.FILLED
    assert execution.order.filled_qty == 1.0 and execution.order.average_fill_price == 1.1
    assert execution.fills is not None and len(execution.fills) == 1
    assert execution.fills[0].fill_id == _activity_fingerprint(activities[0])

    asyncio.run(broker._settle_own_troubles({}))
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.closed_ts_ms is None, "torn down while the engine holds it"

    engine.reconcile()
    engine.apply_async_events()
    engine.reconcile()
    receipt = ctx.fill_receipt(execution.fills[0].fill_id)
    assert receipt is not None and receipt.booked and receipt.qty == pytest.approx(1.0)
    assert engine._position.size == 1.0

    asyncio.run(broker._settle_own_troubles({}))
    delivered = asyncio.run(base._drain_agen(broker._process_activity(activities, {})))
    assert [(ev.event_type, ev.leg_type, ev.fill_qty) for ev in delivered] \
        == [('filled', LegType.CLOSE, 1.0)]
    __test_helper_deliver(engine, delivered)
    engine.reconcile()
    engine._resolve_entry_block_causes(now_ms=int(epoch_time() * 1000) + 3_600_000)
    assert engine._position.size == 0.0
    assert ctx.fill_evidence_holds() == {}
    assert 'fill_evidence:L' not in engine._entry_block_causes
    asyncio.run(broker._settle_own_troubles({}))

    row = ctx.get_order(__test_helper_COID)
    assert row is not None
    assert row.closed_ts_ms is not None or (row.extras or {}).get('natural_close_at') is not None
    assert (row.extras or {}).get('entry_filled_at') is not None
    assert EXTRAS_KEY_EXECUTION_HELD not in (row.extras or {})
    # A copy the stream reads later is a replay, never a second close.
    replayed = asyncio.run(base._drain_agen(broker._process_activity(activities, {})))
    assert [ev for ev in replayed if ev.event_type == 'filled'] == []
    store.close()


def __test_a_landed_deal_missing_from_one_snapshot_is_not_handed_over__(tmp_path):
    # The snapshot does not list the deal, but the history shows no close of
    # it: the read may lag the deal's opening. Nothing is handed over and the
    # row stays pending for the next snapshot.
    created_s = epoch_time() - __test_helper_AGE_S
    opening = {**__test_helper_activity(created_s + 0.4), 'level': 1.1}
    broker, store, ctx, _created = __test_helper_crashed_entry(tmp_path, responses={
        ('history/activity', 'get'): {'activities': [opening]},
    })

    assert __test_helper_pass(broker) == []

    assert ctx.execution_hold_requests() == {}
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'submitted'
    assert EXTRAS_KEY_EXECUTION_HELD not in (row.extras or {})
    store.close()


def __test_a_handed_over_execution_the_history_no_longer_proves_blocks_entries__(tmp_path):
    # The hand-over happened, then the history rolled past the deal: the
    # plugin cannot tell the execution, so the engine's no-evidence rule
    # applies: new entries wait under fill_evidence:L and the engine keeps
    # asking. The row is no pending dispatch any more, so it shadows no
    # foreign deal opened when it was sent, its deal's absence is no
    # disappearance, and the bot does not tear it down while the engine
    # holds the order.
    created_s = epoch_time() - __test_helper_AGE_S
    broker, store, ctx, _created = __test_helper_crashed_entry(tmp_path, responses={
        ('history/activity', 'get'): {'activities': __test_helper_closed_deal(created_s)},
    })
    assert __test_helper_pass(broker) == []
    engine = __test_helper_engine(broker, ctx)
    engine._consume_execution_hold_requests()
    broker._responses[('history/activity', 'get')] = {'activities': []}

    assert asyncio.run(broker.fetch_order_execution('deal-L', None)) is None
    engine._expire_fill_evidence_holds(now_ms=int(epoch_time() * 1000))
    assert 'fill_evidence:L' in engine._entry_block_causes

    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed' and row.exchange_order_id == 'deal-L'
    assert not ctx.awaits_venue_answer(row)
    assert broker._unconfirmed_own_entries() == []
    foreign = __test_helper_position('deal-F', created_s=created_s + 1.0)
    assert [(deal['deal_id'], deal.get('ownership_pending'))
            for deal in broker._foreign_deals([foreign], 'EURUSD', direction='BUY')] \
        == [('deal-F', None)]
    assert broker._is_disappearance_exempt(row)
    asyncio.run(broker._settle_own_troubles({}))
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.closed_ts_ms is None
    assert (row.extras or {}).get('natural_close_at') is None
    store.close()


def __test_a_held_market_take_over_whose_deal_vanished_beyond_the_history_is_handed_over__(
        tmp_path, monkeypatch):
    # An old inherited MARKET entry was taken over on a reduced deal, so its
    # fill is held for proof; past the history only a stable /positions size
    # could prove it. The deal is gone and stays gone: nothing the plugin can
    # read will ever prove the fill, so after the stable window the row goes
    # to the engine's held-execution recovery instead of staying held for
    # good; its held take-over stamp is replaced.
    broker, store, ctx, created_s = __test_helper_crashed_entries(
        tmp_path, [(__test_helper_COID, 'L', 'L')], age_s=86400.0, responses={})
    reduced = __test_helper_position('deal-L', created_s=created_s + 1.0, size=0.5)
    broker._responses[('positions', 'get')] = {'positions': [reduced]}
    assert __test_helper_pass(broker, {'deal-L': reduced}) == []
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN)

    clock = [epoch_time()]
    monkeypatch.setattr(order_history, 'epoch_time', lambda: clock[0])
    broker._responses[('history/activity', 'get')] = {'activities': []}
    for _ in range(2):
        broker._take_over_proof_read_at = 0.0
        assert asyncio.run(base._drain_agen(broker._resolve_unproven_take_overs({}))) == []
        assert ctx.execution_hold_requests() == {}
        clock[0] += 60.0
    clock[0] += 60.0
    broker._take_over_proof_read_at = 0.0
    assert asyncio.run(base._drain_agen(broker._resolve_unproven_take_overs({}))) == []

    assert ctx.execution_hold_requests()[__test_helper_COID]['order_id'] == 'deal-L'
    row = ctx.get_order(__test_helper_COID)
    assert row is not None
    assert EXTRAS_KEY_TAKE_OVER_UNPROVEN not in (row.extras or {})
    assert (row.extras or {})[EXTRAS_KEY_EXECUTION_HELD]['deal_id'] == 'deal-L'
    store.close()


def __test_helper_offline_execution(tmp_path):
    """A taken-over resting entry executed while no process ran, its position closed since.

    :return: ``(broker, store, ctx, activities)`` after the restart, before the
        startup pass.
    """
    broker, store, ctx = base._make_broker(tmp_path, responses={})
    create_entry_order_row(
        ctx, coid=__test_helper_COID, symbol='EURUSD', side='buy', qty=1.0,
        intent_key='L', pine_entry_id='L', kind=ENTRY_KIND_WORKING, order_type='limit',
    )
    row = ctx.get_order(__test_helper_COID)
    assert row is not None
    ctx.upsert_order(__test_helper_COID, state='confirmed', exchange_order_id='deal-W',
                     extras={**(row.extras or {}), EXTRAS_KEY_TAKEN_OVER_AT: epoch_time() - 3600})
    ctx.add_ref(__test_helper_COID, 'deal_id', 'deal-W')
    ctx = __test_helper_restart(store, ctx, broker)
    executed_s = epoch_time() - 600.0
    executed = {**__test_helper_activity(executed_s, deal_id='deal-W',
                                         activity_type='WORKING_ORDER'), 'status': 'EXECUTED'}
    opening = {**__test_helper_activity(executed_s, deal_id='deal-P'), 'status': 'EXECUTED',
               'level': 1.1}
    opening['details'] = {**opening['details'], 'workingOrderId': 'deal-W', 'level': 1.1}
    activities = [executed, opening,
                  __test_helper_close_activity(executed_s + 120.0, 'deal-P', 1.0)]
    broker._responses[('history/activity', 'get')] = {'activities': activities}
    return broker, store, ctx, activities


def __test_helper_startup_pass(broker, activities: list[dict]) -> None:
    broker._retire_startup_orphans({}, {}, closed_deals=set(), activities=activities)


def __test_an_offline_executed_working_order_whose_deal_closed_is_settled_from_the_history__(
        tmp_path, monkeypatch):
    # The taken-over resting entry executed into deal-P while no process ran,
    # and deal-P was closed since. The startup pass no longer retires the row
    # as an order that "executed into no deal": before the engine's first
    # sync it hands the row to the engine over deal-P, the deal the execution
    # opened. The engine leaves the confirmed row as it is and holds the entry
    # over deal-P; fetch_order_execution answers by deal-P with the opening's
    # fill under the stream's fill id; the engine books it and takes the
    # venue's flat book as the close; the row is then torn down, and a copy
    # of the deal's activities the stream reads later books nothing.
    broker, store, ctx, activities = __test_helper_offline_execution(tmp_path)

    __test_helper_startup_pass(broker, activities)

    assert ctx.execution_hold_requests()[__test_helper_COID]['order_id'] == 'deal-P'
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.closed_ts_ms is None
    assert row.state == 'confirmed' and row.exchange_order_id == 'deal-W'
    assert (row.extras or {})[EXTRAS_KEY_EXECUTION_HELD] == {
        'deal_id': 'deal-P', 'at': pytest.approx(epoch_time(), abs=60.0), 'requested': True}
    assert _close_identity(activities[2]) in (row.extras or {})[EXTRAS_KEY_BOOKED_CLOSES]
    mapped = ctx.find_by_ref('deal_id', 'deal-P')
    assert mapped is not None and mapped.client_order_id == __test_helper_COID
    assert 'working_order_vanished_unattributed' not in base._event_kinds(
        ctx, __test_helper_COID)

    monkeypatch.setattr(lib, '_script', SimpleNamespace(initial_capital=1_000_000.0))
    engine = __test_helper_engine(broker, ctx)
    engine._consume_execution_hold_requests()
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed' and row.exchange_order_id == 'deal-W'
    assert ctx.fill_evidence_holds() == {'L': ('deal-P',)}
    assert broker._is_disappearance_exempt(row)

    execution = asyncio.run(broker.fetch_order_execution('deal-P', None))
    assert execution is not None and execution.order is not None
    assert execution.order.status == OrderStatus.FILLED
    assert execution.order.filled_qty == 1.0 and execution.order.average_fill_price == 1.1
    assert execution.fills is not None and len(execution.fills) == 1
    assert execution.fills[0].fill_id == _activity_fingerprint(activities[1])

    engine.reconcile()
    engine.apply_async_events()
    engine.reconcile()
    receipt = ctx.fill_receipt(execution.fills[0].fill_id)
    assert receipt is not None and receipt.booked and receipt.qty == pytest.approx(1.0)
    assert ctx.fill_evidence_holds() == {}
    assert engine._position.size == 0.0

    asyncio.run(broker._settle_own_troubles({}))
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and (row.extras or {}).get('natural_close_at') is not None
    assert EXTRAS_KEY_EXECUTION_HELD not in (row.extras or {})
    replayed = asyncio.run(base._drain_agen(broker._process_activity(activities, {})))
    assert [ev for ev in replayed if ev.event_type == 'filled'] == []
    store.close()


def __test_an_offline_executed_working_order_the_history_no_longer_proves_blocks_entries__(
        tmp_path):
    # The hand-over happened at startup, then the history rolled past the
    # deal: the engine's no-evidence rule holds new entries under
    # fill_evidence:L while it keeps asking; the row stays live, its deal's
    # absence is no disappearance, and the bot does not tear it down.
    broker, store, ctx, activities = __test_helper_offline_execution(tmp_path)
    __test_helper_startup_pass(broker, activities)
    engine = __test_helper_engine(broker, ctx)
    engine._consume_execution_hold_requests()
    broker._responses[('history/activity', 'get')] = {'activities': []}

    assert asyncio.run(broker.fetch_order_execution('deal-P', None)) is None
    engine._expire_fill_evidence_holds(now_ms=int(epoch_time() * 1000))
    assert 'fill_evidence:L' in engine._entry_block_causes

    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.closed_ts_ms is None and row.state == 'confirmed'
    assert broker._is_disappearance_exempt(row)
    assert __test_helper_pass(broker) == []
    asyncio.run(broker._settle_own_troubles({}))
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.closed_ts_ms is None
    assert (row.extras or {}).get('natural_close_at') is None
    store.close()


def __test_an_offline_executed_working_order_handed_over_survives_a_restart__(tmp_path):
    # The process stops after the startup hand-over and before the engine
    # took the request. The next start keeps the row (it is never retired as
    # vanished), the stored request stands, and that start's engine holds
    # the entry over deal-P.
    broker, store, ctx, activities = __test_helper_offline_execution(tmp_path)
    __test_helper_startup_pass(broker, activities)
    assert __test_helper_COID in ctx.execution_hold_requests()

    ctx = __test_helper_restart(store, ctx, broker)
    __test_helper_startup_pass(broker, activities)

    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.closed_ts_ms is None and row.state == 'confirmed'
    assert ctx.execution_hold_requests()[__test_helper_COID]['order_id'] == 'deal-P'
    engine = __test_helper_engine(broker, ctx)
    engine._consume_execution_hold_requests()
    assert ctx.fill_evidence_holds() == {'L': ('deal-P',)}
    assert 'deal-P' in ctx.held_execution_order_ids()
    store.close()


def __test_a_working_take_over_whose_execution_vanished_beyond_the_history_is_handed_over__(
        tmp_path, monkeypatch):
    # A taken-over resting entry whose execution's position was reduced, so
    # its fill is held for proof; past the history only a stable /positions
    # size could prove it. The position is gone and stays gone: after the
    # stable window the confirmed row goes to the engine over that position.
    broker, store, ctx, created_s = __test_helper_crashed_entries(
        tmp_path, [(__test_helper_COID, 'L', 'L')], kind=ENTRY_KIND_WORKING,
        age_s=600.0, responses={})
    broker._responses[('history/activity', 'get')] = {'activities': [
        __test_helper_activity(created_s + 0.4, deal_id='deal-W',
                               activity_type='WORKING_ORDER'),
    ]}
    reduced = __test_helper_position('deal-P', working_order_id='deal-W', size=0.5)
    reduced['position']['createdDateUTC'] = ''
    assert __test_helper_pass(broker, {'deal-P': reduced}) == []
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed'
    assert (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN)

    clock = [epoch_time()]
    monkeypatch.setattr(order_history, 'epoch_time', lambda: clock[0])
    broker._responses[('history/activity', 'get')] = {'activities': []}
    for _ in range(3):
        broker._take_over_proof_read_at = 0.0
        assert asyncio.run(base._drain_agen(broker._resolve_unproven_take_overs({}))) == []
        clock[0] += 61.0

    assert ctx.execution_hold_requests()[__test_helper_COID]['order_id'] == 'deal-P'
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'confirmed'
    assert EXTRAS_KEY_TAKE_OVER_UNPROVEN not in (row.extras or {})
    store.close()


def __test_helper_execution_activities(executed_s: float, *, shape: str = 'backlinked',
                                       closed: bool = True) -> list[dict]:
    """The history of order deal-W's execution, ``executed_s`` ago, and of its deal's close.

    ``backlinked``: the venue's working-order machinery executed deal-W into
    the fresh deal deal-P, whose opening back-links it (a resting order, or a
    market entry run that way, measured cycle 84). ``own_deal``: a market
    entry opened deal-W itself.
    """
    if shape == 'own_deal':
        opening = {**__test_helper_activity(executed_s, deal_id='deal-W'), 'status': 'EXECUTED',
                   'level': 1.1}
        opening['details'] = {**opening['details'], 'level': 1.1}
        activities = [opening]
        deal_id = 'deal-W'
    else:
        executed = {**__test_helper_activity(executed_s, deal_id='deal-W',
                                             activity_type='WORKING_ORDER'),
                    'status': 'EXECUTED'}
        opening = {**__test_helper_activity(executed_s, deal_id='deal-P'), 'status': 'EXECUTED',
                   'level': 1.1}
        opening['details'] = {**opening['details'], 'workingOrderId': 'deal-W', 'level': 1.1}
        activities = [executed, opening]
        deal_id = 'deal-P'
    if closed:
        activities.append(__test_helper_close_activity(executed_s + 20.0, deal_id, 1.0))
    return activities


__test_helper_NOT_FOUND = {
    ('error', f'positions/{deal_id}', 'get'): OrderNotFoundError(
        "API error occured: error.not-found.dealId", ref_type='deal_id')
    for deal_id in ('deal-W', 'deal-P')
}


def __test_helper_in_run_execution(tmp_path, *, taken_over: bool = True, kind: str = 'working',
                                   shape: str = 'backlinked', closed: bool = True,
                                   missing_s: float = 3600.0, responses=None):
    """An own entry of the running process the venue no longer lists, past every grace.

    Its order deal-W executed a few minutes ago (:func:`__test_helper_execution_activities`),
    and the deal it opened is gone before any ``/positions`` read listed it.
    A taken-over order executed before its id was known, so the poll read
    those activities as someone else's; the stream booked none of them.
    ``kind`` is ``working`` (a resting order) or ``market`` (an entry the
    venue accepted without opening it at once).

    :return: ``(broker, store, ctx, row, activities)``.
    """
    activities = __test_helper_execution_activities(epoch_time() - 300.0, shape=shape,
                                                    closed=closed)
    broker, store, ctx = base._make_broker(tmp_path, responses={
        ('history/activity', 'get'): {'activities': activities},
        **__test_helper_NOT_FOUND,
        **(responses or {}),
    })
    create_entry_order_row(
        ctx, coid=__test_helper_COID, symbol='EURUSD', side='buy', qty=1.0,
        intent_key='L', pine_entry_id='L',
        kind=ENTRY_KIND_WORKING if kind == 'working' else ENTRY_KIND_POSITION,
        order_type='limit' if kind == 'working' else 'market',
    )
    row = ctx.get_order(__test_helper_COID)
    assert row is not None
    extras = {**(row.extras or {}), 'missing_pending_since': epoch_time() - missing_s}
    if taken_over:
        extras[EXTRAS_KEY_TAKEN_OVER_AT] = epoch_time() - 360.0
    ctx.upsert_order(__test_helper_COID, state='confirmed', exchange_order_id='deal-W',
                     extras=extras)
    ctx.add_ref(__test_helper_COID, 'deal_id', 'deal-W')
    row = ctx.get_order(__test_helper_COID)
    assert row is not None
    return broker, store, ctx, row, activities


@pytest.mark.parametrize('taken_over, kind, shape', [
    (True, 'working', 'backlinked'),
    (False, 'working', 'backlinked'),
    (False, 'market', 'backlinked'),
    (False, 'market', 'own_deal'),
], ids=['taken_over_working', 'own_working', 'market_via_working_order', 'market_own_deal'])
def __test_an_entry_executed_and_closed_between_two_polls_is_settled_from_the_history__(
        tmp_path, monkeypatch, taken_over, kind, shape):
    # The order vanished and no poll saw its position: the deal it opened was
    # closed in between, and the stream booked nothing of it. The
    # disappearance verdict no longer calls it an external cancel: the
    # history shows the order's execution and its deal's close, the probe
    # confirms the deal is gone, and the row goes to the running engine's
    # held-execution recovery. The engine books the opening; the close joins
    # the deal's delivery queue behind it, and the next activity read
    # delivers it, so the book matches the venue's flat position, the block
    # ends and the row is torn down. A later copy books nothing.
    broker, store, ctx, row, activities = __test_helper_in_run_execution(
        tmp_path, taken_over=taken_over, kind=kind, shape=shape)
    deal = 'deal-W' if shape == 'own_deal' else 'deal-P'
    opening, close = activities[-2], activities[-1]
    monkeypatch.setattr(lib, '_script', SimpleNamespace(initial_capital=1_000_000.0))
    engine = __test_helper_engine(broker, ctx)
    engine.reconcile()
    engine.sync(1700000000000)

    verdict = asyncio.run(broker._confirm_missing_cancelled(row))

    assert verdict.resolution is MissingResolution.INCONCLUSIVE
    assert ctx.execution_hold_requests()[__test_helper_COID]['order_id'] == deal
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.closed_ts_ms is None
    assert row.state == 'confirmed' and row.exchange_order_id == 'deal-W'
    assert (row.extras or {})[EXTRAS_KEY_EXECUTION_HELD]['deal_id'] == deal
    assert _close_identity(close) in (row.extras or {})[EXTRAS_KEY_DEFERRED_CLOSES]
    mapped = ctx.find_by_ref('deal_id', deal)
    assert mapped is not None and mapped.client_order_id == __test_helper_COID
    handed = [json.loads(r['payload']) for r in ctx._store._conn.execute(
        "SELECT payload FROM events WHERE client_order_id = ? AND kind = ? ORDER BY id",
        (__test_helper_COID, 'execution_handed_to_engine'))]
    assert [payload['proof'] for payload in handed] == ['executed_and_closed_between_polls']
    assert broker._is_disappearance_exempt(row)

    engine._consume_execution_hold_requests()
    assert ctx.fill_evidence_holds() == {'L': (deal,)}

    execution = asyncio.run(broker.fetch_order_execution(deal, None))
    assert execution is not None and execution.order is not None
    assert execution.order.status == OrderStatus.FILLED
    assert execution.order.filled_qty == 1.0 and execution.order.average_fill_price == 1.1
    assert execution.fills is not None and len(execution.fills) == 1
    assert execution.fills[0].fill_id == _activity_fingerprint(opening)

    engine.reconcile()
    engine.apply_async_events()
    engine.reconcile()
    receipt = ctx.fill_receipt(execution.fills[0].fill_id)
    assert receipt is not None and receipt.booked and receipt.qty == pytest.approx(1.0)
    assert engine._position.size == 1.0

    asyncio.run(broker._settle_own_troubles({}))
    delivered = asyncio.run(base._drain_agen(broker._process_activity(activities, {})))
    assert [(ev.event_type, ev.leg_type, ev.fill_qty) for ev in delivered] \
        == [('filled', LegType.CLOSE, 1.0)]
    __test_helper_deliver(engine, delivered)
    engine.reconcile()
    assert engine._position.size == 0.0
    # The venue's next read, past the re-read backoff, matches the book.
    engine._resolve_entry_block_causes(now_ms=int(epoch_time() * 1000) + 3_600_000)
    assert 'fill_evidence:L' not in engine._entry_block_causes
    assert ctx.fill_evidence_holds() == {}
    assert [row.kind for row in ctx.iter_open_quarantines()
            if row.kind == QuarantineKind.EXTERNAL_INTERVENTION.value] == []

    asyncio.run(broker._settle_own_troubles({}))
    row = ctx.get_order(__test_helper_COID)
    assert row is not None
    assert row.closed_ts_ms is not None or (row.extras or {}).get('natural_close_at') is not None
    assert EXTRAS_KEY_EXECUTION_HELD not in (row.extras or {})
    replayed = asyncio.run(base._drain_agen(broker._process_activity(activities, {})))
    assert [ev for ev in replayed if ev.event_type == 'filled'] == []
    assert 'cancelled' not in base._event_kinds(ctx, __test_helper_COID)
    store.close()


def __test_a_failed_history_read_keeps_a_vanished_working_order_for_a_later_verdict__(
        tmp_path, monkeypatch):
    # The history read that would show the execution fails: the verdict stays
    # inconclusive, nothing is handed over, cancelled or latched, and the row
    # stays live. Within the read interval the history is not read again; past
    # it the read succeeds and the row is handed over.
    broker, store, ctx, row, _activities = __test_helper_in_run_execution(
        tmp_path, responses={
            ('error', 'history/activity', 'get'): ExchangeConnectionError("venue down"),
        })
    latched = __test_helper_quarantine(broker)
    clock = [epoch_time()]
    monkeypatch.setattr(order_history, 'epoch_time', lambda: clock[0])

    for _ in range(2):
        verdict = asyncio.run(broker._confirm_missing_cancelled(row))
        assert verdict.resolution is MissingResolution.INCONCLUSIVE
        clock[0] += 10.0
    assert len(__test_helper_activity_reads(broker)) == 1
    assert ctx.execution_hold_requests() == {}
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.closed_ts_ms is None and row.state == 'confirmed'
    assert EXTRAS_KEY_EXECUTION_HELD not in (row.extras or {})
    assert latched == []

    del broker._responses[('error', 'history/activity', 'get')]
    clock[0] += 30.0
    verdict = asyncio.run(broker._confirm_missing_cancelled(row))
    assert verdict.resolution is MissingResolution.INCONCLUSIVE
    assert len(__test_helper_activity_reads(broker)) == 2
    assert ctx.execution_hold_requests()[__test_helper_COID]['order_id'] == 'deal-P'
    assert latched == []
    store.close()


def __test_a_vanished_working_order_the_history_shows_no_execution_of_is_cancelled__(tmp_path):
    # The genuine external cancel is unchanged: the history holds nothing of
    # the order's execution, so the verdict is the cancel it always was and
    # nothing is handed to the engine.
    broker, store, ctx, row, _activities = __test_helper_in_run_execution(
        tmp_path, responses={('history/activity', 'get'): {'activities': []}})

    verdict = asyncio.run(broker._confirm_missing_cancelled(row))

    assert verdict.resolution is MissingResolution.CANCELLED
    assert ctx.execution_hold_requests() == {}
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and EXTRAS_KEY_EXECUTION_HELD not in (row.extras or {})
    store.close()


def __test_a_working_order_whose_execution_the_stream_reads_is_booked_by_the_stream__(tmp_path):
    # The normal in-run path is unchanged: the poll reads the order's
    # execution and the close of the deal it opened, the activity bridge moves
    # the row onto deal-P and the stream books the entry and the close. The
    # row is a closed position, so no verdict reads the history for it and
    # nothing is handed to the engine.
    broker, store, ctx, row, activities = __test_helper_in_run_execution(
        tmp_path, taken_over=False)

    events = asyncio.run(base._drain_agen(broker._process_activity(activities, {})))

    assert [(ev.event_type, ev.fill_qty) for ev in events] == [('filled', 1.0), ('filled', 1.0)]
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.exchange_order_id == 'deal-P'
    assert (row.extras or {}).get('kind') == ENTRY_KIND_POSITION
    assert (row.extras or {}).get('natural_close_at') is not None
    reads = len(__test_helper_activity_reads(broker))
    verdict = asyncio.run(broker._confirm_missing_cancelled(row))
    assert verdict.resolution is MissingResolution.CLOSED
    assert len(__test_helper_activity_reads(broker)) == reads
    assert ctx.execution_hold_requests() == {}
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and EXTRAS_KEY_EXECUTION_HELD not in (row.extras or {})
    store.close()


def __test_an_execution_whose_deal_is_gone_without_a_close_is_handed_over_once_absent__(tmp_path):
    # The history shows the order's execution into deal-P but no close of it
    # yet, and the probe says deal-P is gone. A close activity may still be
    # unpublished, so the row stays (never cancelled) until it has been
    # missing for the stable window; then it goes to the engine, which adopts
    # the venue's flat book as the close.
    broker, store, ctx, row, _activities = __test_helper_in_run_execution(
        tmp_path, taken_over=False, closed=False, missing_s=90.0)

    verdict = asyncio.run(broker._confirm_missing_cancelled(row))
    assert verdict.resolution is MissingResolution.INCONCLUSIVE
    assert ctx.execution_hold_requests() == {}

    ctx.upsert_order(__test_helper_COID, extras={**(row.extras or {}),
                                                 'missing_pending_since': epoch_time() - 3600.0})
    broker._closed_deal_read_at.clear()
    row = ctx.get_order(__test_helper_COID)
    assert row is not None
    verdict = asyncio.run(broker._confirm_missing_cancelled(row))

    assert verdict.resolution is MissingResolution.INCONCLUSIVE
    assert ctx.execution_hold_requests()[__test_helper_COID]['order_id'] == 'deal-P'
    handed = base._event_payloads(ctx, 'execution_handed_to_engine')
    assert [payload['proof'] for payload in handed] == ['executed_and_absent_from_positions']
    assert 'cancelled' not in base._event_kinds(ctx, __test_helper_COID)
    store.close()


def __test_an_execution_into_a_live_deal_is_never_cancelled__(tmp_path):
    # The history shows the order executed into deal-P and the probe finds
    # deal-P live: the order is not cancelled and nothing is handed over;
    # the stream or the snapshot books the live deal.
    broker, store, ctx, row, _activities = __test_helper_in_run_execution(
        tmp_path, taken_over=False, closed=False)
    del broker._responses[('error', 'positions/deal-P', 'get')]

    verdict = asyncio.run(broker._confirm_missing_cancelled(row))

    assert verdict.resolution is MissingResolution.INCONCLUSIVE
    assert ctx.execution_hold_requests() == {}
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.closed_ts_ms is None
    assert EXTRAS_KEY_EXECUTION_HELD not in (row.extras or {})
    store.close()


@pytest.mark.parametrize('shape', ['backlinked', 'own_deal'])
def __test_an_offline_executed_market_entry_whose_deal_closed_is_handed_over_at_startup__(
        tmp_path, shape):
    # A market entry the venue accepted without opening it at once (its row
    # holds no fill) executed while no process ran, into a deal closed since.
    # The startup pass no longer retires the row as residue: it hands it to
    # the engine over the deal the execution opened.
    broker, store, ctx = base._make_broker(tmp_path, responses={})
    create_entry_order_row(
        ctx, coid=__test_helper_COID, symbol='EURUSD', side='buy', qty=1.0,
        intent_key='L', pine_entry_id='L', kind=ENTRY_KIND_POSITION, order_type='market',
    )
    ctx.upsert_order(__test_helper_COID, state='confirmed', exchange_order_id='deal-W')
    ctx.add_ref(__test_helper_COID, 'deal_id', 'deal-W')
    ctx = __test_helper_restart(store, ctx, broker)
    activities = __test_helper_execution_activities(epoch_time() - 600.0, shape=shape)
    broker._responses[('history/activity', 'get')] = {'activities': activities}

    __test_helper_startup_pass(broker, activities)

    deal = 'deal-W' if shape == 'own_deal' else 'deal-P'
    assert ctx.execution_hold_requests()[__test_helper_COID]['order_id'] == deal
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.closed_ts_ms is None
    assert (row.extras or {})[EXTRAS_KEY_EXECUTION_HELD]['deal_id'] == deal
    assert 'startup_orphan_retired' not in base._event_kinds(ctx, __test_helper_COID)
    store.close()


def __test_helper_landed_submission(tmp_path, *, kind: str, age_s: float,
                                    activities: list[dict]):
    """A submission an earlier process POSTed, its confirm ACCEPTED on deal-W, no snapshot listing it.

    :return: ``(broker, store, ctx)`` after the restart, before the recovery.
    """
    broker, store, ctx, _created = __test_helper_crashed_entries(
        tmp_path, [(__test_helper_COID, 'L', 'L')], kind=kind, age_s=age_s, responses={
            ('positions', 'get'): {'positions': []},
            ('workingorders', 'get'): {'workingOrders': []},
            ('history/activity', 'get'): {'activities': activities},
            ('confirms/REF-L', 'get'): {'dealStatus': 'ACCEPTED', 'status': 'OPEN',
                                        'dealId': 'deal-W', 'level': 1.1, 'size': 1.0},
        })
    ctx.set_order_state(__test_helper_COID, 'server_ref_seen')
    row = ctx.get_order(__test_helper_COID)
    assert row is not None
    ctx.upsert_order(__test_helper_COID,
                     extras={**(row.extras or {}), 'deal_reference': 'REF-L'})
    ctx.add_ref(__test_helper_COID, 'deal_reference', 'REF-L')
    return broker, store, ctx


@pytest.mark.parametrize('kind, shape', [
    (ENTRY_KIND_POSITION, 'own_deal'),
    (ENTRY_KIND_POSITION, 'backlinked'),
    (ENTRY_KIND_WORKING, 'backlinked'),
], ids=['market_own_deal', 'market_via_working_order', 'working'])
def __test_a_landed_submission_whose_deal_closed_is_confirmed_and_handed_over__(
        tmp_path, kind, shape):
    # The confirm says the POST landed on deal-W, no snapshot lists it, and
    # the history shows the order's execution and the close of its deal.
    # Recovery no longer rejects it (which would send the entry again next to
    # that execution): it is confirmed on deal-W and handed to the engine
    # over the deal its order executed into.
    activities = __test_helper_execution_activities(epoch_time() - 150.0, shape=shape)
    broker, store, ctx = __test_helper_landed_submission(
        tmp_path, kind=kind, age_s=200.0, activities=activities)

    asyncio.run(broker._recover_in_flight_submissions())

    deal = 'deal-W' if shape == 'own_deal' else 'deal-P'
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.closed_ts_ms is None
    assert row.state == 'confirmed' and row.exchange_order_id == 'deal-W'
    assert ctx.execution_hold_requests()[__test_helper_COID]['order_id'] == deal
    assert [payload.get('recovery_path')
            for payload in base._event_payloads(ctx, 'recovered_confirmed')] \
        == ['confirm_get_executed_deal']
    assert base._event_payloads(ctx, 'recovered_rejected') == []
    store.close()


def __test_a_landed_submission_older_than_the_history_is_handed_over_without_an_opening__(
        tmp_path, monkeypatch):
    # The submission is older than the 24-hour history: nothing can tell what
    # it executed. It is confirmed and handed to the engine without an
    # opening; the engine's no-evidence rule keeps new entries waiting
    # instead of the entry going out a second time.
    broker, store, ctx = __test_helper_landed_submission(
        tmp_path, kind=ENTRY_KIND_POSITION, age_s=90000.0, activities=[])

    asyncio.run(broker._recover_in_flight_submissions())

    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.closed_ts_ms is None and row.state == 'confirmed'
    assert ctx.execution_hold_requests()[__test_helper_COID]['order_id'] == 'deal-W'
    assert [payload.get('recovery_path')
            for payload in base._event_payloads(ctx, 'recovered_confirmed')] \
        == ['confirm_get_beyond_history']

    monkeypatch.setattr(lib, '_script', SimpleNamespace(initial_capital=1_000_000.0))
    engine = __test_helper_engine(broker, ctx)
    engine._consume_execution_hold_requests()
    assert asyncio.run(broker.fetch_order_execution('deal-W', None)) is None
    engine._expire_fill_evidence_holds(now_ms=int(epoch_time() * 1000))
    assert 'fill_evidence:L' in engine._entry_block_causes
    store.close()


def __test_a_parked_market_entry_run_through_a_working_order_and_closed_is_handed_over__(
        tmp_path):
    # A market entry of this process parked on a confirms glitch: the confirm
    # comes back accepted on deal-W, which the snapshot does not list,
    # because the venue ran it through its working-order machinery into
    # deal-P (cycle 84), closed since. The history shows deal-P's opening
    # back-linking deal-W and its close: the row goes to the engine over
    # deal-P instead of staying pending for good.
    activities = __test_helper_execution_activities(epoch_time() - 150.0)
    broker, store, ctx = base._make_broker(tmp_path, responses={
        ('history/activity', 'get'): {'activities': activities},
        ('confirms/REF-L', 'get'): {'dealStatus': 'ACCEPTED', 'dealId': 'deal-W'},
    })
    create_entry_order_row(
        ctx, coid=__test_helper_COID, symbol='EURUSD', side='buy', qty=1.0,
        intent_key='L', pine_entry_id='L', kind=ENTRY_KIND_POSITION, order_type='market',
    )
    ctx.set_order_state(__test_helper_COID, STATE_DISPOSITION_UNKNOWN)
    row = ctx.get_order(__test_helper_COID)
    assert row is not None
    ctx.upsert_order(__test_helper_COID,
                     extras={**(row.extras or {}), 'deal_reference': 'REF-L'})
    ctx.add_ref(__test_helper_COID, 'deal_reference', 'REF-L')

    events = asyncio.run(base._drain_agen(broker._resolve_parked_submission_dispositions({})))

    assert events == []
    assert ctx.execution_hold_requests()[__test_helper_COID]['order_id'] == 'deal-P'
    row = ctx.get_order(__test_helper_COID)
    assert row is not None and row.closed_ts_ms is None
    assert (row.extras or {})[EXTRAS_KEY_EXECUTION_HELD]['deal_id'] == 'deal-P'
    store.close()


@pytest.mark.parametrize('activity_status, closed', [
    (None, False), ('EXECUTED', False), ('DELETED', True),
], ids=['no_activity', 'executed', 'deleted'])
def __test_a_cancel_of_an_own_resting_order_gone_on_its_delete_closes_it_only_on_the_venues_word__(
        tmp_path, activity_status, closed):
    # Pine cancels an own resting entry; the DELETE answers 404, which an
    # executed order answers too. The row is closed only when the venue's
    # activity shows the order ended unfilled; otherwise it stays live (the
    # cancel reports unsettled), so its execution can still reach the
    # engine and the entry is never sent again next to it.
    activities = ([] if activity_status is None else [
        {**__test_helper_activity(epoch_time() - 5.0, deal_id='deal-W',
                                  activity_type='WORKING_ORDER'), 'status': activity_status}])
    broker, store, ctx = base._make_broker(tmp_path, responses={
        ('history/activity', 'get'): {'activities': activities},
        ('error', 'workingorders/deal-W', 'delete'): OrderNotFoundError(
            "API error occured: error.not-found.dealId", ref_type='deal_id'),
    })
    create_entry_order_row(
        ctx, coid=__test_helper_COID, symbol='EURUSD', side='buy', qty=1.0,
        intent_key='L', pine_entry_id='L', kind=ENTRY_KIND_WORKING, order_type='limit',
    )
    ctx.upsert_order(__test_helper_COID, state='confirmed', exchange_order_id='deal-W')
    ctx.add_ref(__test_helper_COID, 'deal_id', 'deal-W')
    cancel = DispatchEnvelope(intent=CancelIntent(pine_id='L', symbol='EURUSD'),
                              run_tag='test', bar_ts_ms=1700000000000)

    settled = asyncio.run(broker.execute_cancel(cancel))

    row = ctx.get_order(__test_helper_COID)
    assert row is not None
    assert settled is closed
    assert (row.closed_ts_ms is not None) is closed
    assert ('cancel_unsettled_execution' in base._event_kinds(ctx, __test_helper_COID)) \
        is not closed
    store.close()


__test_helper_RESTING = {'workingOrderData': {
    'dealId': 'deal-W', 'epic': 'EURUSD', 'direction': 'BUY', 'orderSize': 1.0,
    'orderLevel': 1.1, 'orderType': 'LIMIT'}, 'marketData': {'epic': 'EURUSD'}}


def __test_helper_dispatched_entry(tmp_path, monkeypatch, *, with_exit: bool = False):
    """A running engine that dispatched the limit entry L in this process: resting as deal-W.

    :return: ``(broker, store, ctx, engine, position)``.
    """
    broker, store, ctx = base._make_broker(tmp_path, responses={
        ('workingorders', 'post'): {'dealReference': 'ref-W'},
        ('confirms/ref-W', 'get'): {'dealStatus': 'ACCEPTED', 'status': 'OPEN',
                                    'dealId': 'deal-W', 'level': 1.1, 'size': 1.0},
        ('workingorders', 'get'): {'workingOrders': [__test_helper_RESTING]},
        ('positions', 'get'): {'positions': []},
        ('history/activity', 'get'): {'activities': []},
        ('positions/deal-P', 'put'): {'dealReference': 'ref-B'},
        ('confirms/ref-B', 'get'): {'dealStatus': 'ACCEPTED', 'status': 'AMENDED',
                                    'dealId': 'deal-P'},
    })
    monkeypatch.setattr(lib, '_script', SimpleNamespace(initial_capital=1_000_000.0,
                                                        pyramiding=1))
    position = BrokerPosition()
    engine = OrderSyncEngine(broker=broker, position=position, symbol='EURUSD',
                             run_tag=ctx.run_tag, mintick=0.00001, store_ctx=ctx)
    broker.quarantine_sink = engine.record_quarantine
    engine.reconcile()
    position.entry_orders['L'] = Order('L', 1.0, order_type=_order_type_entry, limit=1.1)
    if with_exit:
        position.exit_orders[('X', 'L')] = Order(
            'L', -1.0, order_type=_order_type_close, exit_id='X', limit=1.2, stop=1.0)
    engine.sync(1700000000000)
    assert 'L' in engine._active_intents and engine._order_mapping['L'] == ['deal-W']
    broker._responses[('workingorders', 'get')] = {'workingOrders': []}
    return broker, store, ctx, engine, position


def __test_helper_entry_posts(broker) -> int:
    return [call[:2] for call in broker._calls].count(('workingorders', 'post'))


def __test_an_entry_the_engine_dispatched_that_executed_and_closed_unseen_is_booked_once__(
        tmp_path, monkeypatch):
    # The engine dispatched L in this process and maps its order deal-W. The
    # order executes into deal-P and deal-P closes before any poll sees it.
    # The disappearance verdict hands the row over: the engine takes the hold
    # over deal-P although its Pine intent is active, reads and books the
    # opening once, the stream delivers the close behind it, the book matches
    # the flat venue and the block ends. The entry is never sent again.
    broker, store, ctx, engine, position = __test_helper_dispatched_entry(tmp_path, monkeypatch)
    activities = __test_helper_execution_activities(epoch_time() - 300.0)
    broker._responses[('history/activity', 'get')] = {'activities': activities}
    broker._responses.update(__test_helper_NOT_FOUND)
    row = next(row for row in ctx.iter_live_orders() if row.intent_key == 'L')
    ctx.upsert_order(row.client_order_id, extras={**(row.extras or {}),
                                                  'missing_pending_since': epoch_time() - 3600.0})
    row = ctx.get_order(row.client_order_id)
    assert row is not None

    verdict = asyncio.run(broker._confirm_missing_cancelled(row))
    assert verdict.resolution is MissingResolution.INCONCLUSIVE
    assert ctx.execution_hold_requests()[row.client_order_id]['order_id'] == 'deal-P'

    engine.sync(1700000060000)
    engine.reconcile()
    engine.apply_async_events()
    engine.reconcile()
    assert 'fill_evidence:L' in engine._entry_block_causes
    assert position.size == 1.0
    receipt = ctx.fill_receipt(_activity_fingerprint(activities[1]))
    assert receipt is not None and receipt.booked and receipt.qty == pytest.approx(1.0)

    asyncio.run(broker._settle_own_troubles({}))
    delivered = asyncio.run(base._drain_agen(broker._process_activity(activities, {})))
    assert [(ev.event_type, ev.leg_type, ev.fill_qty) for ev in delivered] \
        == [('filled', LegType.CLOSE, 1.0)]
    __test_helper_deliver(engine, delivered)
    engine.reconcile()
    engine._resolve_entry_block_causes(now_ms=int(epoch_time() * 1000) + 3_600_000)
    assert position.size == 0.0
    assert 'fill_evidence:L' not in engine._entry_block_causes

    for bar in range(2, 5):
        engine.sync(1700000000000 + bar * 60_000)
    asyncio.run(broker._settle_own_troubles({}))
    assert __test_helper_entry_posts(broker) == 1
    assert [r.client_order_id for r in ctx.iter_live_orders()
            if r.intent_key == 'L' and (r.extras or {}).get('natural_close_at') is None] == []
    assert [r.client_order_id for r in ctx.iter_live_orders()
            if EXTRAS_KEY_EXECUTION_HELD in (r.extras or {})] == []
    replayed = asyncio.run(base._drain_agen(broker._process_activity(activities, {})))
    assert [ev for ev in replayed if ev.event_type == 'filled'] == []
    store.close()


def __test_helper_live_deal(size: float = 1.0) -> dict:
    """deal-P, opened seconds ago by deal-W's execution, live at ``size``."""
    return __test_helper_position('deal-P', created_s=epoch_time() - 5.0,
                                  working_order_id='deal-W', size=size)


def __test_a_missed_execution_into_a_live_deal_is_booked_at_once_and_protected__(
        tmp_path, monkeypatch):
    # The engine's order deal-W executed into deal-P, which is live, and the
    # poll has not read the execution's activity. The snapshot that misses
    # the order and lists the one deal back-linking it books the opening at
    # once from its size and level: the engine holds the position, its
    # bracket goes onto deal-P, and the execution's activity read later is a
    # replay.
    broker, store, ctx, engine, position = __test_helper_dispatched_entry(
        tmp_path, monkeypatch, with_exit=True)
    live = __test_helper_live_deal()
    broker._responses[('positions', 'get')] = {'positions': [live]}

    events = __test_helper_pass(broker, {'deal-P': live})

    assert [(ev.event_type, ev.leg_type, ev.fill_qty, ev.fill_price) for ev in events] \
        == [('filled', LegType.ENTRY, 1.0, 1.1)]
    row = next(row for row in ctx.iter_live_orders() if row.intent_key == 'L')
    assert row.exchange_order_id == 'deal-P' and (row.extras or {}).get('kind') == 'position'
    __test_helper_deliver(engine, events)
    calls = len(broker._calls)
    engine.sync(1700000060000)
    assert position.size == 1.0
    assert ('positions/deal-P', 'put') in [call[:2] for call in broker._calls[calls:]]
    assert {(r.extras or {}).get('leg_kind') for r in ctx.iter_live_orders()} >= {'tp', 'sl'}

    late = __test_helper_execution_activities(epoch_time() - 20.0, closed=False)
    replayed = asyncio.run(base._drain_agen(broker._process_activity(late, {'deal-P': live})))
    assert [ev for ev in replayed if ev.event_type == 'filled'] == []
    assert __test_helper_entry_posts(broker) == 1
    store.close()


def __test_a_missed_execution_into_a_reduced_live_deal_is_booked_from_its_opening__(
        tmp_path, monkeypatch):
    # The live deal back-linking the gone order is smaller than the order:
    # a reduction already happened, so the snapshot cannot tell what was
    # filled. Nothing is booked from it; the entry is held, and the deal's
    # opening in the history books the order's full size, with the
    # reduction queued behind it.
    broker, store, ctx, engine, position = __test_helper_dispatched_entry(tmp_path, monkeypatch)
    live = __test_helper_live_deal(size=0.5)
    broker._responses[('positions', 'get')] = {'positions': [live]}

    assert __test_helper_pass(broker, {'deal-P': live}) == []
    row = next(row for row in ctx.iter_live_orders() if row.intent_key == 'L')
    assert (row.extras or {}).get(EXTRAS_KEY_TAKE_OVER_UNPROVEN) is not None

    executed_s = epoch_time() - 30.0
    activities = __test_helper_execution_activities(executed_s, closed=False)
    activities.append(__test_helper_close_activity(executed_s + 10.0, 'deal-P', 0.5))
    broker._responses[('history/activity', 'get')] = {'activities': activities}
    broker._take_over_proof_read_at = 0.0
    events = asyncio.run(base._drain_agen(broker._resolve_unproven_take_overs({'deal-P': live})))

    assert [(ev.event_type, ev.leg_type, ev.fill_qty) for ev in events] \
        == [('filled', LegType.ENTRY, 1.0)]
    __test_helper_deliver(engine, events)
    closes = asyncio.run(base._drain_agen(broker._process_activity(activities, {'deal-P': live})))
    assert [(ev.event_type, ev.leg_type, ev.fill_qty) for ev in closes] \
        == [('filled', LegType.CLOSE, 0.5)]
    __test_helper_deliver(engine, closes)
    assert position.size == 0.5
    assert __test_helper_entry_posts(broker) == 1
    store.close()
