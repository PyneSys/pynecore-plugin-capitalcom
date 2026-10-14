"""
@pyne

External-intervention detection contract tests.

Someone else moving or removing the bot's SL/TP, or closing the bot's
position by hand, must latch the external-intervention quarantine. The
bot's own writes and the venue's own mechanics must never do so:

* Protection levels: every own ``PUT /positions/{dealId}`` is recorded on
  the core protection guard before it leaves; the ``/positions`` poll
  judges the venue's ``stopLevel`` / ``profitLevel`` / ``trailingStop`` /
  ``stopDistance`` against them, and a mismatch is reported only once two
  consecutive polls show it.
* Closes: the bot's own DELETE / reduce POST carries ``source=USER`` like
  an operator's close; the own-close record written before the request
  tells them apart. Someone else's close is booked, then reported. TP /
  SL / stop-out / system / dealing-desk closes are never reported.
"""
import asyncio
import logging
import time
from datetime import UTC, datetime

import pytest

from pynecore.core.broker.exceptions import (
    ExchangeConnectionError,
    ExchangeOrderRejectedError,
    OrderDispositionUnknownError,
    UnexpectedCancelError,
)
from pynecore.core.broker.idempotency import KIND_CLOSE
from pynecore.core.broker.models import (
    CancelIntent,
    CloseIntent,
    DispatchEnvelope,
    ExitIntent,
    LegType,
    QuarantineKind,
)
from pynecore.core.broker.native_failsafe_manager import NativeBracketSnapshot
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore

from pynecore_capitalcom import CapitalCom, CapitalComConfig
from pynecore_capitalcom.dispatch_hooks import _CapitalComCancelHooks


def main():
    pass


# ``minStepDistance`` is the price tick: 0.01, so levels match within 0.005.
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
    """Capital.com plugin with canned per-endpoint responses."""

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


def _config() -> CapitalComConfig:
    return CapitalComConfig(
        demo=True, user_email='test@example.com', api_key='k', api_password='p',
    )


def _new_broker(ctx, *, responses=None) -> tuple[_FakeBroker, list]:
    """A plugin instance on ``ctx`` with a recording quarantine sink."""
    resp: dict = {('markets/EURUSD', 'get'): _RULES_RESP}
    if responses:
        resp.update(responses)
    broker = _FakeBroker(config=_config(), responses=resp)
    broker.symbol = 'EURUSD'
    broker._account_id = 'capitalcom-demo-test-account'
    broker.store_ctx = ctx
    latched: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: latched.append(
        (reason, context, kind))
    return broker, latched


def _make_broker(tmp_path, *, responses=None):
    store = BrokerStore(tmp_path / 'broker.sqlite', plugin_name=CapitalCom.plugin_name)
    identity = RunIdentity(
        strategy_id='test', symbol='EURUSD', timeframe='60',
        account_id='acc', label=None,
    )
    ctx = store.open_run(identity, script_source='// test')
    broker, latched = _new_broker(ctx, responses=responses)
    return broker, store, ctx, latched


def _seed_position(ctx, *, coid='coid-entry', deal_id='deal-L', qty=2.0, side='buy'):
    """A filled bot position row, as the entry path leaves it."""
    ctx.upsert_order(coid, symbol='EURUSD', side=side, qty=qty, filled_qty=qty,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id=deal_id,
                     extras={'kind': 'position', 'entry_filled_at': time.time()})
    ctx.add_ref(coid, 'deal_id', deal_id)


def _attach(broker, *, tp=None, sl=None, trail_offset=None):
    """Run the bot's own bracket attach (PUT accepted)."""
    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'attach'}
    broker._responses[('confirms/attach', 'get')] = {'dealStatus': 'ACCEPTED'}
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='X', from_entry='Long', symbol='EURUSD', side='sell',
            qty=2.0, tp_price=tp, sl_price=sl, trail_offset=trail_offset,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.execute_exit(env))


def _position(*, size=2.0, stop=None, profit=None, trailing=False, distance=None):
    pos: dict = {'dealId': 'deal-L', 'direction': 'BUY', 'size': size}
    if stop is not None:
        pos['stopLevel'] = stop
    if profit is not None:
        pos['profitLevel'] = profit
    if trailing:
        pos['trailingStop'] = True
        pos['stopDistance'] = distance
    return {'deal-L': {'market': {'epic': 'EURUSD'}, 'position': pos}}


async def _drain(agen):
    return [ev async for ev in agen]


def _poll(broker, positions, *, read_started_at=None):
    """One snapshot reconcile, read after every own write so far."""
    started = time.time() if read_started_at is None else read_started_at
    return asyncio.run(_drain(broker._reconcile_snapshot(
        positions, {}, read_started_at=started,
    )))


def _now_iso(offset_s: float = 0.0) -> str:
    """The venue's ``dateUTC`` format for now plus ``offset_s``."""
    stamp = datetime.fromtimestamp(time.time() + offset_s, UTC)
    return stamp.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]


def _close_activity(*, source='USER', size=2.0, level=1.12, date_utc=None):
    return {
        'dateUTC': date_utc or _now_iso(), 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': source,
        'epic': 'EURUSD',
        'details': {'direction': 'SELL', 'size': size, 'level': level},
    }


def _event_kinds(ctx) -> list[str]:
    return [r['kind'] for r in ctx._store._conn.execute(
        "SELECT kind FROM events WHERE run_instance_id = ?",
        (ctx.run_instance_id,),
    )]


# === Moved / removed protection =============================================

def __test_removed_stop_is_reported_on_the_second_poll__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)

    _poll(broker, _position(stop=1.05, profit=1.20))
    assert latched == []
    _poll(broker, _position(profit=1.20))
    assert latched == [], "one poll alone never proves an edit"
    _poll(broker, _position(profit=1.20))
    assert len(latched) == 1
    reason, context, kind = latched[0]
    assert kind is QuarantineKind.EXTERNAL_INTERVENTION
    assert context['protection_key'] == 'position:deal-L'
    assert context['observed'] == [None, 1.20, False, None]
    assert 'deal-L' in reason
    store.close()


def __test_removed_take_profit_is_reported__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    _poll(broker, _position(stop=1.05))
    _poll(broker, _position(stop=1.05))
    assert len(latched) == 1
    store.close()


def __test_moved_stop_beyond_half_a_tick_is_reported__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    _poll(broker, _position(stop=1.04, profit=1.20))
    _poll(broker, _position(stop=1.04, profit=1.20))
    assert len(latched) == 1
    store.close()


def __test_moved_take_profit_is_reported__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    _poll(broker, _position(stop=1.05, profit=1.25))
    _poll(broker, _position(stop=1.05, profit=1.25))
    assert len(latched) == 1
    store.close()


def __test_sub_tick_difference_is_not_reported__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    for _ in range(3):
        _poll(broker, _position(stop=1.0520, profit=1.1980))
    assert latched == []
    store.close()


def __test_trailing_turned_off_is_reported__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, trail_offset=0.5)
    put = [c for c in broker._calls if c[1] == 'put'][-1]
    assert put[2] == {'profitLevel': 1.20, 'trailingStop': True, 'stopDistance': 0.5}
    _poll(broker, _position(stop=1.07, profit=1.20, trailing=True, distance=0.5))
    _poll(broker, _position(stop=1.07, profit=1.20))
    assert latched == []
    _poll(broker, _position(stop=1.07, profit=1.20))
    assert len(latched) == 1
    store.close()


def __test_changed_trail_distance_is_reported_after_the_second_poll__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, trail_offset=0.5)
    _poll(broker, _position(stop=1.07, trailing=True, distance=0.8))
    assert latched == []
    _poll(broker, _position(stop=1.08, trailing=True, distance=0.8))
    assert len(latched) == 1
    store.close()


def __test_venue_moving_a_trailing_stop_is_not_reported__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, trail_offset=0.5)
    for stop in (1.07, 1.09, 1.12, 1.15):
        _poll(broker, _position(stop=stop, profit=1.20, trailing=True, distance=0.5))
    assert latched == []
    store.close()


def __test_live_modify_exit_command_suspends_judgement__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    # An amend dispatched for this position whose outcome is still open.
    ctx.upsert_order('cmd-modify', symbol='EURUSD', side='sell', qty=2.0,
                     state='disposition_unknown',
                     extras={'kind': 'modify_exit', 'target_coid': 'coid-entry'})
    for _ in range(3):
        _poll(broker, _position(stop=1.00, profit=1.30))
    assert latched == []
    store.close()


def __test_own_amend_is_never_reported__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('confirms/amend', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'amend'}
    old = DispatchEnvelope(
        intent=ExitIntent(pine_id='X', from_entry='Long', symbol='EURUSD',
                          side='sell', qty=2.0, tp_price=1.20, sl_price=1.05),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    new = DispatchEnvelope(
        intent=ExitIntent(pine_id='X', from_entry='Long', symbol='EURUSD',
                          side='sell', qty=2.0, tp_price=1.30, sl_price=1.10),
        run_tag='test', bar_ts_ms=1700000060000,
    )
    asyncio.run(broker.modify_exit(old, new))
    # A lagging snapshot still carries the old levels, then the new ones.
    for positions in (_position(stop=1.05, profit=1.20),
                      _position(stop=1.10, profit=1.30),
                      _position(stop=1.10, profit=1.30)):
        _poll(broker, positions)
    assert latched == []
    store.close()


def __test_snapshot_read_before_the_last_own_write_is_skipped__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    started_before_write = time.time() - 30.0
    _attach(broker, tp=1.20, sl=1.05)
    for _ in range(3):
        _poll(broker, _position(), read_started_at=started_before_write)
    assert latched == []
    store.close()


def __test_pending_trailing_activation_suspends_judgement__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    sl_row = next(r for r in ctx.iter_live_orders()
                  if (r.extras or {}).get('leg_kind') == 'sl')
    extras = dict(sl_row.extras or {})
    extras['trail_state'] = 'activating'
    ctx.upsert_order(sl_row.client_order_id, extras=extras)
    for _ in range(3):
        _poll(broker, _position(stop=1.07, profit=1.20, trailing=True, distance=0.4))
    assert latched == []
    store.close()


def __test_failsafe_registered_parent_is_left_to_the_manager__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    # A software partial-bracket leg: the engine's native fail-safe manager
    # owns this parent's stop and judges it itself.
    ctx.upsert_order('leg-partial', symbol='EURUSD', side='sell', qty=1.0,
                     state='partial_bracket_leg',
                     extras={'leg_kind': 'sl_partial',
                             'parent_entry_dispatch_ref': 'coid-entry'})
    for _ in range(3):
        _poll(broker, _position(stop=1.00))
    assert latched == []
    store.close()


def __test_failsafe_put_hands_the_position_to_the_manager__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('confirms/fs', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'fs'}
    asyncio.run(broker.publish_native_failsafe_sl(NativeBracketSnapshot(
        parent_entry_dispatch_ref='coid-entry', symbol='EURUSD',
        parent_side='long', stop_level=1.02, profit_level=None,
        trailing_stop=None, generation=1,
    )))
    for _ in range(3):
        _poll(broker, _position(stop=1.02))
    assert latched == []
    row = ctx.get_order('coid-entry')
    assert row is not None
    assert (row.extras or {}).get('native_failsafe_owned_at') is not None
    assert 'protection_levels' not in (row.extras or {})
    store.close()


def __test_persisted_levels_survive_a_restart__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    row = ctx.get_order('coid-entry')
    assert row is not None
    assert (row.extras or {}).get('protection_levels', {}).get('key') == 'position:deal-L'

    restarted, restarted_latched = _new_broker(ctx)
    for _ in range(3):
        _poll(restarted, _position(stop=1.05, profit=1.20))
    assert restarted_latched == [], "the restored own levels match the venue"
    _poll(restarted, _position(profit=1.20))
    _poll(restarted, _position(profit=1.20))
    assert len(restarted_latched) == 1, "the restored levels are still judged"
    assert latched == []
    store.close()


def __test_own_leg_cancel_keeps_the_sister_and_is_not_reported__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('confirms/cxl', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'cxl'}
    tp_row = next(r for r in ctx.iter_live_orders()
                  if (r.extras or {}).get('leg_kind') == 'tp')
    # Cancel only the TP leg: the PUT replaces every bracket field, so the
    # live SL must ride along.
    asyncio.run(_CapitalComCancelHooks(plugin=broker).submit_cancel(
        coid='cmd-cancel',
        intent=CancelIntent(pine_id='X', symbol='EURUSD', from_entry='Long'),
        targets=[tp_row],
    ))
    put = [c for c in broker._calls if c[1] == 'put'][-1]
    assert put[2] == {'profitLevel': None, 'stopLevel': 1.05}
    for _ in range(3):
        _poll(broker, _position(stop=1.05))
    assert latched == []
    store.close()


def __test_own_exit_cancel_clears_both_legs_without_a_report__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('confirms/cxl', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'cxl'}
    env = DispatchEnvelope(
        intent=CancelIntent(pine_id='X', symbol='EURUSD', from_entry='Long'),
        run_tag='test', bar_ts_ms=1700000060000,
    )
    asyncio.run(broker.execute_cancel(env))
    puts = [c[2] for c in broker._calls if c[1] == 'put'][1:]
    assert len(puts) == 2
    assert puts[-1] in ({'profitLevel': None}, {'stopLevel': None})
    for _ in range(3):
        _poll(broker, _position())
    assert latched == []
    store.close()


def __test_size_change_rebaselines_instead_of_reporting__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    _poll(broker, _position(stop=1.05, profit=1.20))
    # A reduction (own or the venue's) whose effect on the bracket the venue
    # does not document: the residual's levels become the new baseline.
    for _ in range(3):
        _poll(broker, _position(size=1.0, profit=1.20))
    assert latched == []
    assert 'protection_adopted_after_size_change' in _event_kinds(ctx)
    # ...and are judged from then on.
    _poll(broker, _position(size=1.0))
    _poll(broker, _position(size=1.0))
    assert len(latched) == 1
    store.close()


# === Manual closes ===========================================================

def __test_unexplained_user_full_close_is_booked_then_reported__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    events = asyncio.run(_drain(broker._process_activity([_close_activity()])))
    assert len(events) == 1
    assert events[0].leg_type == LegType.CLOSE
    assert events[0].fill_qty == 2.0
    assert len(latched) == 1
    reason, context, kind = latched[0]
    assert kind is QuarantineKind.EXTERNAL_INTERVENTION
    assert context['deal_id'] == 'deal-L'
    assert 'deal-L' in reason
    store.close()


def __test_unexplained_user_partial_close_is_booked_then_reported__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    events = asyncio.run(_drain(broker._process_activity(
        [_close_activity(size=0.5)], _position(size=1.5),
    )))
    assert len(events) == 1 and events[0].fill_qty == 0.5
    assert len(latched) == 1
    store.close()


def __test_own_full_close_activity_is_not_reported__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions/deal-L', 'delete'): {'dealReference': 'del'},
    })
    _seed_position(ctx)
    env = DispatchEnvelope(
        intent=CloseIntent(pine_id='Long', symbol='EURUSD', side='sell', qty=2.0),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.execute_close(env))
    events = asyncio.run(_drain(broker._process_activity([_close_activity()])))
    assert len(events) == 1 and events[0].leg_type == LegType.CLOSE
    assert latched == []
    assert 'own_close_activity_attributed' in _event_kinds(ctx)
    store.close()


def __test_late_own_close_activity_after_the_command_closed_is_not_reported__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions/deal-L', 'delete'): {'dealReference': 'del'},
    })
    _seed_position(ctx)
    deal_time = _now_iso()
    env = DispatchEnvelope(
        intent=CloseIntent(pine_id='Long', symbol='EURUSD', side='sell', qty=2.0),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.execute_close(env))
    ctx.close_order(env.client_order_id(KIND_CLOSE))
    # The venue publishes the deal's activity a minute later; a fresh
    # plugin instance (restart) reads it.
    restarted, restarted_latched = _new_broker(ctx)
    events = asyncio.run(_drain(restarted._process_activity(
        [_close_activity(date_utc=deal_time)],
    )))
    assert len(events) == 1
    assert restarted_latched == [] and latched == []
    store.close()


def __test_own_partial_close_activity_is_not_reported__(tmp_path):
    snapshot = {'positions': [{'market': {'epic': 'EURUSD'},
                               'position': {'dealId': 'deal-L', 'direction': 'BUY',
                                            'size': 2.0}}]}
    residual = {'positions': [{'market': {'epic': 'EURUSD'},
                               'position': {'dealId': 'deal-L', 'direction': 'BUY',
                                            'size': 1.0}}]}
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): [snapshot, snapshot, residual],
        ('positions', 'post'): {'dealReference': 'dr-1'},
        ('confirms/dr-1', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    _seed_position(ctx)
    env = DispatchEnvelope(
        intent=CloseIntent(pine_id='Long', symbol='EURUSD', side='sell', qty=1.0),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.execute_close(env))
    activity = _close_activity(size=1.0)
    events = asyncio.run(_drain(broker._process_activity([activity], _position(size=1.0))))
    assert len(events) == 1 and events[0].fill_qty == 1.0
    assert latched == []
    # A second, unrelated reduction of the same size is not explained by
    # the consumed record.
    other = _close_activity(size=1.0, level=1.13, date_utc=_now_iso(5.0))
    asyncio.run(_drain(broker._process_activity([other], _position(size=0.5))))
    assert len(latched) == 1
    store.close()


def __test_venue_and_desk_closes_are_never_reported__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    for index, source in enumerate(('TP', 'SL', 'CLOSE_OUT', 'SYSTEM', 'DEALER')):
        coid = f'coid-{index}'
        deal_id = f'deal-{index}'
        _seed_position(ctx, coid=coid, deal_id=deal_id)
        activity = _close_activity(source=source)
        activity['dealId'] = deal_id
        asyncio.run(_drain(broker._process_activity([activity])))
    assert latched == []
    assert 'dealer_close_booked' in _event_kinds(ctx)
    store.close()


def __test_system_full_close_is_booked_as_a_venue_close__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    events = asyncio.run(_drain(broker._process_activity(
        [_close_activity(source='SYSTEM')],
    )))
    assert len(events) == 1
    event = events[0]
    assert event.event_type == 'filled'
    assert event.leg_type == LegType.CLOSE
    assert event.order.side == 'sell'
    assert event.fill_qty == 2.0 and event.fill_price == 1.12
    row = ctx.get_order('coid-entry')
    assert row is not None
    assert (row.extras or {}).get('natural_close_at') is not None
    assert latched == []
    assert 'entry_fill_replay_suppressed' not in _event_kinds(ctx)
    store.close()


def __test_system_partial_close_is_booked_as_a_venue_close__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    events = asyncio.run(_drain(broker._process_activity(
        [_close_activity(source='SYSTEM', size=1.0)], _position(size=1.0),
    )))
    assert len(events) == 1
    event = events[0]
    assert event.leg_type == LegType.CLOSE
    assert event.order.side == 'sell'
    assert event.fill_qty == 1.0
    row = ctx.get_order('coid-entry')
    assert row is not None
    assert (row.extras or {}).get('natural_close_at') is None
    assert (row.extras or {}).get('close_event_yielded_at') is not None
    assert latched == []
    store.close()


def __test_system_same_direction_activity_on_an_unfilled_entry_is_not_a_close__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=2.0,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id='deal-L', extras={'kind': 'position'})
    ctx.add_ref('coid-entry', 'deal_id', 'deal-L')
    activity = _close_activity(source='SYSTEM')
    activity['details']['direction'] = 'BUY'
    events = asyncio.run(_drain(broker._process_activity([activity])))
    assert len(events) == 1
    assert events[0].leg_type == LegType.ENTRY
    assert events[0].order.side == 'buy'
    row = ctx.get_order('coid-entry')
    assert row is not None
    assert (row.extras or {}).get('natural_close_at') is None
    assert latched == []
    store.close()


def __test_user_close_explained_by_an_own_opposite_fill_is_not_reported__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    # An own opposite market entry dispatched before the reduction nets the
    # long down under the long's dealId on a netting account.
    ctx.upsert_order('coid-short', symbol='EURUSD', side='sell', qty=1.0,
                     state='confirmed', pine_entry_id='Short',
                     exchange_order_id='deal-S', extras={'kind': 'position'})
    asyncio.run(_drain(broker._process_activity(
        [_close_activity(size=1.0, date_utc=_now_iso(5.0))], _position(size=1.0),
    )))
    assert latched == []
    kinds = _event_kinds(ctx)
    assert 'user_close_attributed_to_own_netting' in kinds
    assert 'user_close_ambiguous_netting' not in kinds
    store.close()


def __test_user_close_explained_only_by_a_resting_order_is_ambiguous__(tmp_path, caplog):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    # A resting opposite working entry may have executed and netted the long
    # down — or someone else closed it while the order still rests.
    ctx.upsert_order('coid-short-wo', symbol='EURUSD', side='sell', qty=1.0,
                     state='confirmed', pine_entry_id='Short',
                     exchange_order_id='wo-1', extras={'kind': 'working'})
    with caplog.at_level(logging.ERROR):
        asyncio.run(_drain(broker._process_activity(
            [_close_activity(size=1.0)], _position(size=1.0),
        )))
    assert latched == []
    kinds = _event_kinds(ctx)
    assert 'user_close_ambiguous_netting' in kinds
    assert 'user_close_attributed_to_own_netting' not in kinds
    assert 'manual_close_report_pending' not in kinds
    assert any(r.levelno == logging.ERROR and 'coid-short-wo' in r.getMessage()
               for r in caplog.records)
    store.close()


# === Refused vs unknown own writes ==========================================

def _amend(broker):
    """The bot's own amend from TP 1.20 / SL 1.05 to TP 1.30 / SL 1.10."""
    old = DispatchEnvelope(
        intent=ExitIntent(pine_id='X', from_entry='Long', symbol='EURUSD',
                          side='sell', qty=2.0, tp_price=1.20, sl_price=1.05),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    new = DispatchEnvelope(
        intent=ExitIntent(pine_id='X', from_entry='Long', symbol='EURUSD',
                          side='sell', qty=2.0, tp_price=1.30, sl_price=1.10),
        run_tag='test', bar_ts_ms=1700000060000,
    )
    asyncio.run(broker.modify_exit(old, new))


def __test_amend_confirmed_rejected_stops_its_levels_being_acceptable__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('confirms/amend', 'get'): {'dealStatus': 'REJECTED', 'reason': 'x'},
    })
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'amend'}
    with pytest.raises(ExchangeOrderRejectedError):
        _amend(broker)
    _poll(broker, _position(stop=1.05, profit=1.20))
    assert latched == []
    # Someone else puts exactly the refused levels on the position.
    for _ in range(2):
        _poll(broker, _position(stop=1.10, profit=1.30))
    assert len(latched) == 1
    store.close()


def __test_amend_put_refused_by_the_venue_stops_its_levels_being_acceptable__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    broker._responses[('error', 'positions/deal-L', 'put')] = (
        ExchangeOrderRejectedError('error.invalid.stoploss'))
    with pytest.raises(ExchangeOrderRejectedError):
        _amend(broker)
    for _ in range(2):
        _poll(broker, _position(stop=1.10, profit=1.30))
    assert len(latched) == 1
    store.close()


def __test_amend_with_unknown_outcome_keeps_its_levels_acceptable__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    broker._responses[('error', 'positions/deal-L', 'put')] = (
        ExchangeConnectionError('timeout'))
    with pytest.raises(OrderDispositionUnknownError):
        _amend(broker)
    # The PUT landed after all: the venue carries the bot's own new levels.
    for _ in range(3):
        _poll(broker, _position(stop=1.10, profit=1.30))
    assert latched == []
    store.close()


# === Bracket-leg cancel ======================================================

def __test_tp_cancel_keeps_a_fixed_stop_awaiting_trailing_activation__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('confirms/attach', 'get'): {'dealStatus': 'ACCEPTED'},
        ('confirms/cxl', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    _seed_position(ctx)
    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'attach'}
    asyncio.run(broker.execute_exit(DispatchEnvelope(
        intent=ExitIntent(
            pine_id='X', from_entry='Long', symbol='EURUSD', side='sell',
            qty=2.0, tp_price=1.20, sl_price=1.05,
            trail_price=1.15, trail_offset=0.002,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )))
    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'cxl'}
    tp_row = next(r for r in ctx.iter_live_orders()
                  if (r.extras or {}).get('leg_kind') == 'tp')
    asyncio.run(_CapitalComCancelHooks(plugin=broker).submit_cancel(
        coid='cmd-cancel',
        intent=CancelIntent(pine_id='X', symbol='EURUSD', from_entry='Long'),
        targets=[tp_row],
    ))
    put = [c for c in broker._calls if c[1] == 'put'][-1]
    assert put[2] == {'profitLevel': None, 'stopLevel': 1.05}
    store.close()


def __test_rejected_leg_cancel_keeps_the_leg_live__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('confirms/cxl', 'get'): {'dealStatus': 'REJECTED', 'reason': 'x'},
    })
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'cxl'}
    tp_row = next(r for r in ctx.iter_live_orders()
                  if (r.extras or {}).get('leg_kind') == 'tp')
    with pytest.raises(ExchangeOrderRejectedError):
        asyncio.run(_CapitalComCancelHooks(plugin=broker).submit_cancel(
            coid='cmd-cancel',
            intent=CancelIntent(pine_id='X', symbol='EURUSD', from_entry='Long'),
            targets=[tp_row],
        ))
    still = ctx.get_order(tp_row.client_order_id)
    assert still is not None and still.closed_ts_ms is None
    assert 'cancel_rejected' in _event_kinds(ctx)
    # The venue still carries the original bracket: nothing to report.
    for _ in range(3):
        _poll(broker, _position(stop=1.05, profit=1.20))
    assert latched == []
    store.close()


def __test_unreadable_leg_cancel_confirm_is_ambiguous__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'cxl'}
    broker._responses[('error', 'confirms/cxl', 'get')] = (
        ExchangeOrderRejectedError('error.not-found.dealReference'))
    tp_row = next(r for r in ctx.iter_live_orders()
                  if (r.extras or {}).get('leg_kind') == 'tp')
    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(_CapitalComCancelHooks(plugin=broker).submit_cancel(
            coid='cmd-cancel',
            intent=CancelIntent(pine_id='X', symbol='EURUSD', from_entry='Long'),
            targets=[tp_row],
        ))
    still = ctx.get_order(tp_row.client_order_id)
    assert still is not None and still.closed_ts_ms is None
    store.close()


# === Size-change adoption vs own writes ======================================

def __test_adoption_skips_a_snapshot_read_before_an_own_amend__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('confirms/amend', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    _poll(broker, _position(stop=1.05, profit=1.20))
    # A partial close: the adoption waits for a snapshot read after it.
    _poll(broker, _position(size=1.0, stop=1.05, profit=1.20),
          read_started_at=time.time() - 10.0)
    read_started = time.time() - 5.0
    # The bot amends while that snapshot read is still in flight.
    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'amend'}
    _amend(broker)
    _poll(broker, _position(size=1.0, stop=1.05, profit=1.20),
          read_started_at=read_started)
    for _ in range(3):
        _poll(broker, _position(size=1.0, stop=1.10, profit=1.30))
    assert latched == []
    store.close()


# === Manual closes: attribution limits and delivery ==========================

def __test_opposite_order_smaller_than_the_close_does_not_explain_it__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    ctx.upsert_order('coid-short-wo', symbol='EURUSD', side='sell', qty=0.1,
                     state='confirmed', pine_entry_id='Short',
                     exchange_order_id='wo-1', extras={'kind': 'working'})
    asyncio.run(_drain(broker._process_activity(
        [_close_activity(size=0.5)], _position(size=1.5),
    )))
    assert len(latched) == 1
    store.close()


def __test_manual_close_is_reported_when_the_consumer_stops_at_the_event__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)

    async def consume_one_and_stop():
        gen = broker._process_activity([_close_activity()])
        event = await gen.__anext__()
        assert event.leg_type == LegType.CLOSE
        await gen.aclose()

    asyncio.run(consume_one_and_stop())
    assert len(latched) == 1
    store.close()


def _restart(store, ctx):
    """A new process instance of the same logical run, as after a crash."""
    ctx.close()
    identity = RunIdentity(
        strategy_id='test', symbol='EURUSD', timeframe='60',
        account_id='acc', label=None,
    )
    return _new_broker(store.open_run(identity, script_source='// test'))


def __test_manual_close_report_lost_to_a_process_kill_is_sent_on_restart__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    broker._send_manual_close_report = lambda report: None
    events = asyncio.run(_drain(broker._process_activity([_close_activity()])))
    assert len(events) == 1 and latched == []

    restarted, restarted_latched = _restart(store, ctx)
    restarted._send_pending_manual_close_reports()
    assert len(restarted_latched) == 1
    reason, context, kind = restarted_latched[0]
    assert kind is QuarantineKind.EXTERNAL_INTERVENTION
    assert context['deal_id'] == 'deal-L'
    assert 'deal-L' in reason

    again, again_latched = _restart(store, restarted.store_ctx)
    again._send_pending_manual_close_reports()
    assert again_latched == []
    store.close()


def __test_manual_close_report_killed_before_its_sent_mark_is_sent_again__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    broker._mark_manual_close_report_sent = lambda report: None
    asyncio.run(_drain(broker._process_activity([_close_activity()])))
    assert len(latched) == 1

    restarted, restarted_latched = _restart(store, ctx)
    restarted._send_pending_manual_close_reports()
    assert len(restarted_latched) == 1
    store.close()


def __test_sent_manual_close_report_is_not_repeated_on_restart__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    asyncio.run(_drain(broker._process_activity([_close_activity()])))
    assert len(latched) == 1

    restarted, restarted_latched = _restart(store, ctx)
    restarted._send_pending_manual_close_reports()
    assert restarted_latched == []
    store.close()


def __test_halting_manual_close_report_halts_only_once__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    broker.on_unexpected_cancel = 'halt'
    with pytest.raises(UnexpectedCancelError):
        asyncio.run(_drain(broker._process_activity([_close_activity()])))

    restarted, restarted_latched = _restart(store, ctx)
    restarted.on_unexpected_cancel = 'halt'
    restarted._send_pending_manual_close_reports()
    assert restarted_latched == []
    store.close()


class _Killed(BaseException):
    """A process kill at a chosen write."""


def __test_manual_close_killed_before_its_report_is_persisted_replays_on_restart__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    log_event = ctx.log_event

    def killed_at_pending(kind, **kwargs):
        if kind == 'manual_close_report_pending':
            raise _Killed
        log_event(kind, **kwargs)

    ctx.log_event = killed_at_pending
    activity = _close_activity()
    with pytest.raises(_Killed):
        asyncio.run(_drain(broker._process_activity([activity])))
    assert latched == []
    assert 'activity_processed' not in _event_kinds(ctx)
    del ctx.log_event

    restarted, restarted_latched = _restart(store, ctx)
    asyncio.run(restarted._load_activity_cursor_from_events())
    restarted._send_pending_manual_close_reports()
    events = asyncio.run(_drain(restarted._process_activity([activity])))
    assert len(events) == 1
    assert len(restarted_latched) == 1
    assert restarted_latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    store.close()


def __test_overlapping_writes_settle_their_own_generation__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    body_a = {'stopLevel': 1.04, 'profitLevel': 1.20}
    body_b = {'stopLevel': 1.03, 'profitLevel': 1.20}
    first_a = broker._protection_write('deal-L', body_a)
    b = broker._protection_write('deal-L', body_b)
    last_a = broker._protection_write('deal-L', body_a)
    write_first_a = first_a.__enter__()
    write_b = b.__enter__()
    write_last_a = last_a.__enter__()

    write_first_a.refused()
    first_a.__exit__(None, None, None)
    write_last_a.accepted()
    last_a.__exit__(None, None, None)
    write_b.accepted()
    b.__exit__(None, None, None)

    _poll(broker, _position(stop=1.03, profit=1.20))
    _poll(broker, _position(stop=1.04, profit=1.20))
    _poll(broker, _position(stop=1.04, profit=1.20))
    assert latched == []
    store.close()
