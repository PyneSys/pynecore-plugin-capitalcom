"""
@pyne

Foreign exposure and external-change contract tests.

A deal on the bot's epic that this run's journal did not produce is never the
bot's: it is not counted, closed, amended or netted, and its own changes are
never reported. Any change to the bot's OWN venue state that neither the bot
nor the venue's mechanics caused is an external intervention:

* Reads count own deals only; each foreign deal is logged once.
* A netting ``POST /positions`` (reduce or opposite entry) cannot name a
  ``dealId``; the venue nets it FIFO. An entry is refused while a foreign deal
  sits on the side it nets, a partial close while one as old as its target or
  older does; a blocked exit latches only when the block is continuous.
* A deal the venue re-keyed an own entry into while the bot was down stays
  the run's own after the restart.
* A bot position the bot never protected is judged against "nothing set".
* A resting bot entry order's level is judged like a stop.
* Close attribution: a close is judged once per close, reported at once even
  without a price, dated against the measured venue clock, and only rows that
  could still have netted the position explain it.
"""
import asyncio
import json
import logging
import time
from datetime import UTC, datetime

import httpx
import pytest

from pynecore.core.broker.disappearance import MissingResolution
from pynecore.core.broker.exceptions import (
    ExchangeOrderRejectedError,
    OrderDispositionUnknownError,
    OrderSkippedByPlugin,
)
from pynecore.core.broker.idempotency import KIND_CLOSE, KIND_ENTRY, KIND_MODIFY_ENTRY
from pynecore.core.broker.models import (
    VENUE_DRIVEN_CANCEL_REASONS,
    CancelIntent,
    CloseIntent,
    DispatchEnvelope,
    EntryIntent,
    ExitIntent,
    LegType,
    OrderType,
    QuarantineKind,
)
from pynecore.core.broker.native_failsafe_manager import NativeBracketSnapshot
from pynecore.core.broker.protection_guard import rebaseline_after_resume
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore, RunContext
from pynecore.core.broker.store_helpers import KIND_PARTIAL_CLOSE

from pynecore_capitalcom import CapitalCom, CapitalComConfig, foreign, intervention
from pynecore_capitalcom.exceptions import OrderNotFoundError
from pynecore_capitalcom.helpers import EXTRAS_KEY_OWN_LEVEL_WRITES
from pynecore_capitalcom.models import _activity_fingerprint


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


def _make_broker(
        tmp_path, *, responses=None,
) -> tuple[_FakeBroker, BrokerStore, RunContext, list]:
    store = BrokerStore(tmp_path / 'broker.sqlite', plugin_name=CapitalCom.plugin_name)
    identity = RunIdentity(
        strategy_id='test', symbol='EURUSD', timeframe='60',
        account_id='acc', label=None,
    )
    ctx = store.open_run(identity, script_source='// test')
    broker, latched = _make_plugin(ctx, responses=responses)
    return broker, store, ctx, latched


def _make_plugin(ctx, *, responses=None) -> tuple[_FakeBroker, list]:
    """A plugin process on the journal ``ctx`` (a second call is a restart)."""
    resp: dict = {('markets/EURUSD', 'get'): _RULES_RESP}
    if responses:
        resp.update(responses)
    broker: _FakeBroker = _FakeBroker(
        config=CapitalComConfig(demo=True, user_email='test@example.com',
                                api_key='k', api_password='p'),
        responses=resp,
    )
    broker.symbol = 'EURUSD'
    broker._account_id = 'capitalcom-demo-test-account'
    broker.store_ctx = ctx
    # The venue publishes every activity at once here (the tests that model
    # the real publication lag restore it); see ``_poll``.
    broker._level_edit_settle_s = 0.0
    latched: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: latched.append(
        (reason, context, kind))
    return broker, latched


def _seed_position(ctx, *, coid='coid-entry', deal_id='deal-L', qty=2.0, side='buy'):
    """A filled bot position row, as the entry path leaves it."""
    ctx.upsert_order(coid, symbol='EURUSD', side=side, qty=qty, filled_qty=qty,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id=deal_id,
                     extras={'kind': 'position', 'entry_filled_at': time.time()})
    ctx.add_ref(coid, 'deal_id', deal_id)


def _seed_working(ctx, *, coid='coid-wo', deal_id='wo-1', side='buy', qty=1.0,
                  level: float | None = 1.20, order_type='limit', extras=None):
    """A resting bot entry order, as the entry path leaves it.

    ``level=None`` leaves out the placed level, like a row journaled before
    the entry path recorded it.
    """
    placed = {'order_level': level} if level is not None else {}
    ctx.upsert_order(coid, symbol='EURUSD', side=side, qty=qty,
                     state='confirmed', pine_entry_id='Long',
                     exchange_order_id=deal_id,
                     extras={'kind': 'working', 'order_type': order_type,
                             **placed, **(extras or {})})
    ctx.add_ref(coid, 'deal_id', deal_id)


def _raw(deal_id, direction, size, *, created='2026-10-15T10:00:00.000', **extra):
    """One ``GET /positions`` row on EURUSD."""
    return {'market': {'epic': 'EURUSD', 'bid': 1.111, 'offer': 1.113},
            'position': {'dealId': deal_id, 'direction': direction, 'size': size,
                         'createdDateUTC': created, **extra}}


def _by_deal(*rows):
    return {row['position']['dealId']: row for row in rows}


def _attach(broker, *, tp=None, sl=None):
    """Run the bot's own bracket attach on ``deal-L`` (PUT accepted)."""
    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'attach'}
    broker._responses[('confirms/attach', 'get')] = {'dealStatus': 'ACCEPTED'}
    env = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='X', from_entry='Long', symbol='EURUSD', side='sell',
            qty=2.0, tp_price=tp, sl_price=sl,
        ),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    asyncio.run(broker.execute_exit(env))


async def _drain(agen):
    return [ev async for ev in agen]


def _poll(broker, positions, working=None, *, activity_read_at=None):
    """One snapshot reconcile, read after every own write so far.

    The poll's activity read follows its snapshot reads, as in ``_poll_once``;
    ``activity_read_at`` dates it otherwise.
    """
    read_started_at = time.time()
    broker._activity_polled_at = (time.time() if activity_read_at is None
                                  else activity_read_at)
    return asyncio.run(_drain(broker._reconcile_snapshot(
        positions, working or {}, read_started_at=read_started_at,
    )))


def _now_iso(offset_s: float = 0.0) -> str:
    """The venue's ``dateUTC`` format for now plus ``offset_s``."""
    stamp = datetime.fromtimestamp(time.time() + offset_s, UTC)
    return stamp.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]


def _iso(epoch_s: float) -> str:
    """The venue's ``dateUTC`` format for ``epoch_s``."""
    return datetime.fromtimestamp(epoch_s, UTC).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]


def _edit_shape(working: bool) -> dict:
    return ({'type': 'WORKING_ORDER', 'status': 'MODIFIED'} if working
            else {'type': 'EDIT_STOP_AND_LIMIT', 'status': 'ACCEPTED'})


def _own_write_echo_rows(ctx, deal_id='deal-L', *, working=False) -> list[dict]:
    """The ``USER`` edit activity the venue publishes for each own write the
    bot recorded on the key, dated when the write was sent."""
    key = ('working:' if working else 'position:') + deal_id
    row = ctx.find_by_ref('deal_id', deal_id)
    saved = (row.extras or {}).get(EXTRAS_KEY_OWN_LEVEL_WRITES) if row is not None else None
    return [{'dateUTC': _iso(sent_at), 'dealId': deal_id, 'source': 'USER',
             'epic': 'EURUSD', **_edit_shape(working)}
            for sent_at, _done_at in (saved or {}).get(key) or []]


def _own_write_echoes(broker, deal_id='deal-L', *, working=False) -> None:
    """Feed the echo of every own write on the key (:func:`_own_write_echo_rows`)."""
    for echo in _own_write_echo_rows(broker.store_ctx, deal_id, working=working):
        broker._note_level_edit(echo)


def _someone_else_edits(broker, deal_id='deal-L', *, working=False) -> None:
    """Feed the edit activities the venue publishes up to someone else's change.

    One ``USER`` edit per accepted write: the echo of every own write, and one
    for the change someone else made just now, which no own write explains.
    """
    _own_write_echoes(broker, deal_id, working=working)
    broker._note_level_edit({'dateUTC': _now_iso(0.01), 'dealId': deal_id, 'source': 'USER',
                             'epic': 'EURUSD', **_edit_shape(working)})


def _close_activity(*, source='USER', size=2.0, level=1.12, date_utc=None):
    details: dict = {'direction': 'SELL', 'size': size}
    if level is not None:
        details['level'] = level
    return {
        'dateUTC': date_utc or _now_iso(), 'dealId': 'deal-L',
        'type': 'POSITION', 'status': 'EXECUTED', 'source': source,
        'epic': 'EURUSD', 'details': details,
    }


def _events(ctx, kind: str) -> list[dict]:
    return [
        {'exchange_order_id': row['exchange_order_id'],
         **json.loads(row['payload'] or '{}')}
        for row in ctx._store._conn.execute(
            "SELECT exchange_order_id, payload FROM events "
            "WHERE run_instance_id = ? AND kind = ? ORDER BY id",
            (ctx.run_instance_id, kind),
        )
    ]


def _event_kinds(ctx) -> list[str]:
    return [r['kind'] for r in ctx._store._conn.execute(
        "SELECT kind FROM events WHERE run_instance_id = ?",
        (ctx.run_instance_id,),
    )]


def _posts(broker) -> list:
    return [c for c in broker._calls if c[:2] == ('positions', 'post')]


def _position_gets(broker) -> int:
    return sum(1 for c in broker._calls if c[:2] == ('positions', 'get'))


# === Reads count own deals only ==============================================

def __test_same_direction_foreign_deal_is_not_own_exposure__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            _raw('deal-F', 'BUY', 1.0, created='2026-10-15T09:00:00.000'),
            _raw('deal-L', 'BUY', 2.0),
        ]},
    })
    _seed_position(ctx)

    first = asyncio.run(broker.get_position('EURUSD'))
    second = asyncio.run(broker.get_position('EURUSD'))

    assert first is not None and first.side == 'long' and first.size == 2.0
    assert second is not None and second.size == 2.0
    assert [e['exchange_order_id'] for e in _events(ctx, 'foreign_deal_on_epic')] == [
        'deal-F'], "a foreign deal is logged once, not on every read"
    assert latched == []
    store.close()


def __test_hedging_raw_legs_are_own_only__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            _raw('deal-F', 'BUY', 1.0, created='2026-10-15T09:00:00.000'),
            _raw('deal-L', 'BUY', 2.0),
        ]},
    })
    broker._hedging_enabled = True
    broker.position_port = broker
    _seed_position(ctx)

    legs = asyncio.run(broker.fetch_raw_positions('EURUSD'))

    assert [leg.leg_id for leg in legs] == ['deal-L']
    store.close()


def __test_market_entry_executed_under_a_fresh_deal_id_is_owned__(tmp_path):
    """The venue may run a market entry through its working-order machinery:
    the position's ``workingOrderId`` back-links the confirm's ``dealId``."""
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            _raw('deal-X', 'BUY', 2.0, workingOrderId='deal-confirm'),
        ]},
    })
    _seed_position(ctx, deal_id='deal-confirm')

    position = asyncio.run(broker.get_position('EURUSD'))

    assert position is not None and position.size == 2.0
    assert _events(ctx, 'foreign_deal_on_epic') == []
    store.close()


def _seed_unconfirmed_entry(ctx, *, coid='coid-new', side='buy', state='server_ref_seen'):
    """An own market entry whose POST left but whose confirm has not named its deal."""
    ctx.upsert_order(coid, symbol='EURUSD', side=side, qty=1.0, state=state,
                     pine_entry_id='Long',
                     extras={'kind': 'position', 'order_type': 'market',
                             'deal_reference': 'o_0e5466d9'})
    ctx.add_ref(coid, 'deal_reference', 'o_0e5466d9')


def __test_own_deal_before_its_confirm_is_not_logged_as_foreign__(tmp_path):
    """A ``/positions`` row carries ``p_<dealId>``, never the POST's ``o_`` reference:
    until the confirm names the deal, a deal the entry may have opened is not
    provably foreign."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_unconfirmed_entry(ctx)
    fresh = _raw('D1', 'BUY', 1.0, created=_now_iso(), dealReference='p_D1',
                 workingOrderId='WO1')
    older = _raw('deal-F', 'BUY', 1.0, created=_now_iso(-3600.0))
    later = _raw('deal-G', 'BUY', 1.0, created=_now_iso(3600.0))
    opposite = _raw('deal-S', 'SELL', 1.0, created=_now_iso())

    broker._note_foreign_deals([fresh, older, later, opposite], 'EURUSD')

    assert sorted(e['exchange_order_id'] for e in _events(ctx, 'foreign_deal_on_epic')) == [
        'deal-F', 'deal-G', 'deal-S'], "only deals the pending entry cannot have opened"
    assert 'D1' not in broker._foreign_deals_logged

    # The confirm names the deal: it is the run's own and never logged.
    ctx.upsert_order('coid-new', state='confirmed', exchange_order_id='D1')
    broker._note_foreign_deals([fresh, older, later, opposite], 'EURUSD')
    assert len(_events(ctx, 'foreign_deal_on_epic')) == 3
    assert latched == []
    store.close()


def __test_partial_close_blocked_only_by_an_unconfirmed_deal_waits_without_a_latch__(
        tmp_path):
    pending = _raw('deal-P', 'BUY', 1.0, created=_now_iso(-10.0))
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            _raw('deal-A', 'BUY', 2.0, created=_now_iso()), pending]},
    })
    _seed_position(ctx, deal_id='deal-A')
    _seed_unconfirmed_entry(ctx, state='disposition_unknown')

    with pytest.raises(OrderSkippedByPlugin) as refused:
        asyncio.run(broker.execute_close(DispatchEnvelope(
            intent=CloseIntent(pine_id='Long', symbol='EURUSD', side='sell', qty=1.0),
            run_tag='test', bar_ts_ms=1700000000000,
        )))

    assert refused.value.reason == 'unconfirmed_deal_blocks_partial_close'
    assert _posts(broker) == []
    assert latched == [], "the blocking deal may be the run's own"
    store.close()


# === Netting POSTs never net a foreign deal ==================================

def __test_partial_close_is_refused_while_a_foreign_deal_shares_the_side__(tmp_path):
    foreign = _raw('deal-F', 'BUY', 1.0, created='2026-10-15T09:00:00.000')
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [foreign, _raw('deal-A', 'BUY', 2.0)]},
    })
    _seed_position(ctx, deal_id='deal-A')

    def close(bar_ts_ms):
        return DispatchEnvelope(
            intent=CloseIntent(pine_id='Long', symbol='EURUSD', side='sell', qty=1.0),
            run_tag='test', bar_ts_ms=bar_ts_ms,
        )

    with pytest.raises(OrderSkippedByPlugin) as refused:
        asyncio.run(broker.execute_close(close(1700000000000)))
    assert refused.value.reason == 'foreign_deal_blocks_partial_close'
    assert _posts(broker) == []
    assert latched == [], "a foreign deal that may leave in seconds is no quarantine"
    assert len(_events(ctx, 'foreign_deal_blocks_own_write')) == 1
    gets = _position_gets(broker)

    # The next bar re-evaluates the close: the poll's snapshot alone refuses
    # it, without another read, another ERROR or a quarantine.
    broker._remember_positions_snapshot(
        _by_deal(foreign, _raw('deal-A', 'BUY', 2.0)), time.time())
    with pytest.raises(OrderSkippedByPlugin):
        asyncio.run(broker.execute_close(close(1700000060000)))
    assert _position_gets(broker) == gets
    assert _posts(broker) == []
    assert latched == []
    assert len(_events(ctx, 'foreign_deal_blocks_own_write')) == 1
    entry = ctx.get_order('coid-entry')
    assert entry is not None and entry.state == 'confirmed'
    assert entry.filled_qty == 2.0

    # Once the foreign deal is gone the close goes through.
    own = _raw('deal-A', 'BUY', 2.0)
    broker._remember_positions_snapshot(_by_deal(own), time.time())
    broker._responses[('positions', 'get')] = [
        {'positions': [own]}, {'positions': [own]},
        {'positions': [_raw('deal-A', 'BUY', 1.0)]},
    ]
    broker._responses[('positions', 'post')] = {'dealReference': 'dr-1'}
    broker._responses[('confirms/dr-1', 'get')] = {'dealStatus': 'ACCEPTED'}
    result = asyncio.run(broker.execute_close(close(1700000120000)))
    assert result.id == 'dr-1'
    assert len(_posts(broker)) == 1
    store.close()


def __test_partial_close_goes_out_next_to_a_younger_foreign_deal__(tmp_path):
    """The venue nets the reduce POST FIFO: a same-direction deal someone else
    opened after the bot's own (an add, another run) is never reached by a
    partial close, so it neither blocks the exit nor latches anything."""
    own = _raw('deal-A', 'BUY', 2.0, created='2026-10-15T10:00:00.000')
    younger = _raw('deal-F', 'BUY', 1.0, created='2026-10-15T10:05:00.000')
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): [
            {'positions': [own, younger]}, {'positions': [own, younger]},
            {'positions': [_raw('deal-A', 'BUY', 1.0, created='2026-10-15T10:00:00.000'),
                           younger]},
        ],
        ('positions', 'post'): {'dealReference': 'dr-1'},
        ('confirms/dr-1', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    _seed_position(ctx, deal_id='deal-A')
    broker._remember_positions_snapshot(_by_deal(own, younger), time.time())

    result = asyncio.run(broker.execute_close(_partial_close(1700000000000)))

    assert result.id == 'dr-1'
    assert len(_posts(broker)) == 1
    assert latched == []
    assert _events(ctx, 'foreign_deal_blocks_own_write') == []
    entry = ctx.get_order('coid-entry')
    assert entry is not None and entry.filled_qty == pytest.approx(1.0)
    store.close()


def __test_partial_close_is_refused_under_the_lock_when_a_foreign_deal_is_older__(tmp_path):
    """The locked pre-POST read decides: a foreign deal older than the target
    is the one the venue would net first."""
    own = _raw('deal-A', 'BUY', 2.0, created='2026-10-15T10:00:00.000')
    older = _raw('deal-F', 'BUY', 1.0, created='2026-10-15T09:55:00.000')
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): [{'positions': [own]}, {'positions': [older, own]}],
    })
    _seed_position(ctx, deal_id='deal-A')

    with pytest.raises(OrderSkippedByPlugin) as refused:
        asyncio.run(broker.execute_close(_partial_close(1700000000000)))

    assert refused.value.reason == 'foreign_deal_blocks_partial_close'
    assert _posts(broker) == []
    assert latched == []
    store.close()


def __test_foreign_over_close_during_an_own_partial_close_is_reported_not_halted__(tmp_path):
    """Someone else's opposite POST consumed the rest of the bot's deal while
    its own reduce POST was in flight: the opposite residual the confirm does
    not name is never deleted, the process is not halted, and the activity of
    the target reports the intervention."""
    own = _raw('deal-L', 'BUY', 2.0)
    residual = _raw('deal-F', 'SELL', 1.0, created=_now_iso())
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): [{'positions': [own]}, {'positions': [own]},
                               {'positions': [residual]}],
        ('positions', 'post'): {'dealReference': 'dr-1'},
        ('confirms/dr-1', 'get'): {
            'dealStatus': 'ACCEPTED',
            'affectedDeals': [{'dealId': 'deal-L', 'status': 'PARTIALLY_CLOSED'}],
        },
    })
    _seed_position(ctx)

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_close(_partial_close(1700000000000)))

    assert not any(call[1] == 'delete' for call in broker._calls)
    assert latched == [], "the confirm shows the opposite deal is not the bot's"
    reported = _events(ctx, 'partial_close_unattributed_opposite')
    assert len(reported) == 1
    assert reported[0]['opposite_deal_ids'] == ['deal-F'] and reported[0]['attributable']

    # Both closes of the vanished deal arrive in one batch: the first does not
    # tear the row down, so the second is booked and judged, not a replay.
    own_part = dict(_close_activity(size=1.0, date_utc=_now_iso()))
    foreign_rest = dict(_close_activity(size=1.0, date_utc=_now_iso(0.2)))
    events = asyncio.run(_drain(broker._process_activity(
        [own_part, foreign_rest], _by_deal(residual))))

    assert [(e.leg_type, e.fill_qty) for e in events] == [
        (LegType.CLOSE, 1.0), (LegType.CLOSE, 1.0)]
    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    entry = ctx.get_order('coid-entry')
    assert entry is not None and entry.filled_qty == 0.0
    assert (entry.extras or {}).get('natural_close_at') is not None
    store.close()


@pytest.mark.parametrize('closer, reported', [('SL', False), ('USER', True)])
def __test_unattributable_opposite_after_an_own_partial_close_latches_nothing__(
        tmp_path, caplog, closer, reported):
    """The confirm of the bot's reduce POST is gone, and an opposite deal shows
    up: someone else's, or the POST's own reverse leg. Neither proves that
    someone else changed the bot's state: ERROR plus an audit event, the deal is
    never deleted, nothing is latched and the process is not halted. The
    target's own activities then book the venue truth: a stop-loss taking the
    rest is the venue's, someone else's close of it is an intervention."""
    own = _raw('deal-L', 'BUY', 2.0)
    residual = _raw('deal-F', 'SELL', 1.0, created=_now_iso())
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): [{'positions': [own]}, {'positions': [own]},
                               {'positions': [residual]}],
        ('positions', 'post'): {'dealReference': 'dr-1'},
        ('error', 'confirms/dr-1', 'get'): OrderNotFoundError(
            'confirm TTL expired', ref_type='deal_reference'),
    })
    _seed_position(ctx)

    with caplog.at_level(logging.ERROR, logger='pynecore_capitalcom.foreign'):
        with pytest.raises(OrderDispositionUnknownError):
            asyncio.run(broker.execute_close(_partial_close(1700000000000)))

    assert not any(call[1] == 'delete' for call in broker._calls)
    assert latched == []
    reported_opposite = _events(ctx, 'partial_close_unattributed_opposite')
    assert len(reported_opposite) == 1
    assert reported_opposite[0]['opposite_deal_ids'] == ['deal-F']
    assert reported_opposite[0]['attributable'] is False
    assert any('deal-F' in r.getMessage() for r in caplog.records)

    own_part = dict(_close_activity(size=1.0, date_utc=_now_iso()))
    rest = dict(_close_activity(source=closer, size=1.0, date_utc=_now_iso(0.2)))
    events = asyncio.run(_drain(broker._process_activity(
        [own_part, rest], _by_deal(residual))))

    rest_leg = LegType.CLOSE if reported else LegType.STOP_LOSS
    assert [(e.leg_type, e.fill_qty) for e in events] == [
        (LegType.CLOSE, 1.0), (rest_leg, 1.0)]
    if reported:
        assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    else:
        assert latched == []
    _poll(broker, _by_deal(residual))
    command = ctx.get_order(_partial_close(1700000000000).client_order_id(KIND_CLOSE))
    assert command is not None and command.closed_ts_ms is not None
    assert len(_events(ctx, 'partial_close_own_activity_matched')) == 1
    store.close()


@pytest.mark.parametrize('size, confirmed, own', [
    (1.0, False, True), (0.5, False, False), (1.0, True, False),
])
def __test_unconfirmed_reverse_leg_beside_a_stopped_out_target_is_closed_again__(
        tmp_path, size, confirmed, own):
    """The bot's reduce POST has no confirm, and its target was stopped out in
    full before the POST executed: with nothing left to net, the POST opened a
    fresh opposite deal of its whole size. Once the fill-report grace passes
    without an activity of the POST's own, that deal is the bot's own reverse
    leg: closed again, never left unprotected on the venue. An opposite deal of
    another size, or one a confirm naming the POST's deals leaves out, is not
    what the POST opened and stays untouched. Nothing is latched either way."""
    own_deal = _raw('deal-L', 'BUY', 2.0)
    opposite = _raw('deal-R', 'SELL', size, created=_now_iso())
    confirm: dict = {
        ('confirms/dr-1', 'get'): {
            'dealStatus': 'ACCEPTED',
            'affectedDeals': [{'dealId': 'deal-L', 'status': 'PARTIALLY_CLOSED'}],
        },
    } if confirmed else {
        ('error', 'confirms/dr-1', 'get'): OrderNotFoundError(
            'confirm TTL expired', ref_type='deal_reference'),
    }
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): [{'positions': [own_deal]}, {'positions': [own_deal]},
                               {'positions': [opposite]}],
        ('positions', 'post'): {'dealReference': 'dr-1'},
        **confirm,
    })
    _seed_position(ctx)

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_close(_partial_close(1700000000000)))
    assert not any(call[1] == 'delete' for call in broker._calls)
    stop_out = dict(_close_activity(source='SL', size=2.0, date_utc=_now_iso()))
    events = asyncio.run(_drain(broker._process_activity([stop_out], _by_deal(opposite))))
    assert [(e.leg_type, e.fill_qty) for e in events] == [(LegType.STOP_LOSS, 2.0)]
    _poll(broker, _by_deal(opposite))
    assert not any(call[1] == 'delete' for call in broker._calls), "decided after the grace"

    broker.fill_report_grace_s = 0.0
    _poll(broker, _by_deal(opposite))

    deletes = [call[0] for call in broker._calls if call[1] == 'delete']
    command = ctx.get_order(_partial_close(1700000000000).client_order_id(KIND_CLOSE))
    assert command is not None
    assert latched == []
    if not own:
        assert deletes == []
        assert command.state == 'rejected'
        assert len(_events(ctx, 'partial_close_settled_beside_unattributed_opposite')) == 1
        store.close()
        return
    assert deletes == ['positions/deal-R']
    attributed = _events(ctx, 'partial_close_reverse_leg_attributed')
    assert len(attributed) == 1 and attributed[0]['exchange_order_id'] == 'deal-R'
    assert command.state == 'disposition_unknown', "parked until the reverse leg is gone"
    _poll(broker, {})
    command = ctx.get_order(_partial_close(1700000000000).client_order_id(KIND_CLOSE))
    assert command is not None and command.state == 'rejected'
    assert _events(ctx, 'partial_close_settled_beside_unattributed_opposite') == []
    assert latched == []
    store.close()


def __test_stale_cached_foreign_deal_never_latches_a_partial_close__(tmp_path):
    """The poll's snapshot saw a foreign deal that is gone by the time the
    strategy's partial exit fires: a fresh read decides, nothing is latched."""
    own = _raw('deal-L', 'BUY', 2.0)
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): [{'positions': [own]}, {'positions': [own]},
                               {'positions': [_raw('deal-L', 'BUY', 1.0)]}],
        ('positions', 'post'): {'dealReference': 'dr-1'},
        ('confirms/dr-1', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    _seed_position(ctx)
    foreign = _raw('deal-F', 'BUY', 1.0, created='2026-10-15T09:00:00.000')
    broker._remember_positions_snapshot(_by_deal(foreign, own), time.time() - 5.0)

    result = asyncio.run(broker.execute_close(DispatchEnvelope(
        intent=CloseIntent(pine_id='Long', symbol='EURUSD', side='sell', qty=1.0),
        run_tag='test', bar_ts_ms=1700000000000,
    )))

    assert result.id == 'dr-1'
    assert len(_posts(broker)) == 1
    assert latched == []
    assert _events(ctx, 'foreign_deal_blocks_own_write') == []
    store.close()


def __test_opposite_market_entry_is_refused_while_a_foreign_deal_would_net__(tmp_path):
    foreign = _raw('deal-F', 'SELL', 1.0, created='2026-10-15T09:00:00.000')
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [foreign]},
        ('positions', 'post'): {'dealReference': 'ref-s'},
        ('confirms/ref-s', 'get'): {
            'dealStatus': 'ACCEPTED', 'status': 'OPEN', 'dealId': 'deal-S',
            'level': 1.11, 'size': 1.0, 'affectedDeals': [{'dealId': 'deal-S'}],
        },
    })

    def entry(side, bar_ts_ms, pine_id):
        return DispatchEnvelope(
            intent=EntryIntent(pine_id=pine_id, symbol='EURUSD', side=side, qty=1.0,
                               order_type=OrderType.MARKET),
            run_tag=ctx.run_tag, bar_ts_ms=bar_ts_ms,
        )

    # Refused under the account write lock: nothing sent, the row is rejected.
    buy = entry('buy', 1700000000000, 'Long')
    with pytest.raises(OrderSkippedByPlugin) as refused:
        asyncio.run(broker.execute_entry(buy))
    assert refused.value.reason == 'foreign_deal_blocks_opposite_entry'
    assert _posts(broker) == []
    assert all(row.state == 'rejected' for row in ctx.iter_live_orders())
    assert latched == [], "a separate deal is no reason to stop the bot"
    assert len(_events(ctx, 'foreign_deal_blocks_own_write')) == 1

    # Refused from the poll's snapshot before anything is journaled.
    broker._remember_positions_snapshot(_by_deal(foreign), time.time())
    gets = _position_gets(broker)
    rows_before = len(list(ctx.iter_live_orders()))
    with pytest.raises(OrderSkippedByPlugin):
        asyncio.run(broker.execute_entry(entry('buy', 1700000060000, 'Long')))
    assert _position_gets(broker) == gets
    assert len(list(ctx.iter_live_orders())) == rows_before
    assert _posts(broker) == []

    # The same direction as the foreign deal nets nothing: it goes through.
    asyncio.run(broker.execute_entry(entry('sell', 1700000120000, 'Short')))
    assert len(_posts(broker)) == 1
    assert latched == []
    store.close()


def _working_posts(broker) -> list:
    return [c for c in broker._calls if c[:2] == ('workingorders', 'post')]


def __test_resting_entry_is_not_placed_while_a_foreign_deal_would_net_it__(tmp_path):
    """A resting entry executes later, FIFO into the oldest opposite deal: the
    poll's snapshot refuses it at placement while a foreign deal sits there."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    foreign_sell = _raw('deal-F', 'SELL', 1.0, created='2026-10-15T09:00:00.000')
    broker._remember_positions_snapshot(_by_deal(foreign_sell), time.time())

    with pytest.raises(OrderSkippedByPlugin) as refused:
        asyncio.run(broker.execute_entry(_limit_envelope(1.10)))

    assert refused.value.reason == 'foreign_deal_blocks_resting_entry'
    assert _working_posts(broker) == []
    assert list(ctx.iter_live_orders()) == [], "nothing is journaled for a refused signal"
    assert latched == [], "a separate deal is no reason to stop the bot"
    assert len(_events(ctx, 'foreign_deal_blocks_own_write')) == 1

    # A resting entry in the foreign deal's own direction nets nothing.
    broker._responses[('workingorders', 'post')] = {'dealReference': 'o_wo'}
    broker._responses[('confirms/o_wo', 'get')] = {
        'dealStatus': 'ACCEPTED', 'status': 'ACCEPTED', 'dealId': 'wo-S',
        'level': 1.30, 'size': 1.0, 'affectedDeals': [{'dealId': 'wo-S'}],
    }
    asyncio.run(broker.execute_entry(DispatchEnvelope(
        intent=EntryIntent(pine_id='Short', symbol='EURUSD', side='sell', qty=1.0,
                           order_type=OrderType.LIMIT, limit=1.30, stop=None),
        run_tag='test', bar_ts_ms=1700000060000,
    )))
    assert len(_working_posts(broker)) == 1
    assert latched == []
    store.close()


def _failing_locked_read(broker) -> None:
    """The ``GET /positions`` under the account write lock times out."""
    plain = broker._call_serialized_write

    def call(endpoint, *, data=None, method='post'):
        if (endpoint, method) == ('positions', 'get'):
            broker._calls.append((endpoint, method, data))
            raise httpx.ReadTimeout('read timed out')
        return plain(endpoint, data=data, method=method)

    broker._call_serialized_write = call


def __test_market_entry_whose_preflight_read_fails_is_skipped_not_rejected__(tmp_path):
    """Nothing left the client: the signal is retried next sync, and the skip
    never counts toward the engine's bounded entry-reject budget."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _failing_locked_read(broker)

    with pytest.raises(OrderSkippedByPlugin) as skipped:
        asyncio.run(broker.execute_entry(DispatchEnvelope(
            intent=EntryIntent(pine_id='Long', symbol='EURUSD', side='buy', qty=1.0,
                               order_type=OrderType.MARKET),
            run_tag=ctx.run_tag, bar_ts_ms=1700000000000,
        )))

    assert skipped.value.reason == 'entry_preflight_unavailable'
    assert skipped.value.reason != 'broker_rejected_entry'
    assert _posts(broker) == []
    # The persist-first row is terminal and closed, so the retry next sync
    # starts from a run that owns no live order.
    assert list(ctx.iter_live_orders()) == []
    rows = ctx._store._conn.execute(
        "SELECT state, closed_ts_ms FROM orders",
    ).fetchall()
    assert rows and all(row['state'] == 'rejected' and row['closed_ts_ms'] is not None
                        for row in rows)
    assert latched == []
    store.close()


def __test_partial_close_whose_preflight_read_fails_is_skipped_not_parked__(tmp_path):
    """The read under the account write lock failed before the reduce POST: a
    refusal re-evaluated next bar, never an ambiguous dispatch."""
    own = _raw('deal-L', 'BUY', 2.0)
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [own]},
    })
    _seed_position(ctx)
    _failing_locked_read(broker)

    with pytest.raises(OrderSkippedByPlugin) as skipped:
        asyncio.run(broker.execute_close(DispatchEnvelope(
            intent=CloseIntent(pine_id='Long', symbol='EURUSD', side='sell', qty=1.0),
            run_tag='test', bar_ts_ms=1700000000000,
        )))

    assert skipped.value.reason == 'close_preflight_unavailable'
    assert _posts(broker) == []
    entry = ctx.get_order('coid-entry')
    assert entry is not None and entry.state == 'confirmed' and entry.filled_qty == 2.0
    assert not (entry.extras or {}).get('own_close_expected')
    assert all(row.state != 'disposition_unknown' for row in ctx.iter_live_orders())
    assert latched == []
    store.close()


def _partial_close(bar_ts_ms, *, qty=1.0, trigger=False):
    if trigger:
        intent = CloseIntent(
            pine_id='__pyne_partial_trigger__TP\0Long\0tp', symbol='EURUSD', side='sell',
            qty=qty, synthetic_kind='partial_trigger', target_entry_id='Long',
        )
    else:
        intent = CloseIntent(pine_id='Long', symbol='EURUSD', side='sell', qty=qty)
    return DispatchEnvelope(intent=intent, run_tag='test', bar_ts_ms=bar_ts_ms)


def _clock_at(monkeypatch, offset_s: float) -> None:
    """The foreign-block clock reads now plus ``offset_s``."""
    at = time.time() + offset_s
    monkeypatch.setattr(foreign, 'epoch_time', lambda: at)


def __test_partial_trigger_blocked_past_the_bound_latches_once__(tmp_path, monkeypatch):
    """A software partial-take-profit trigger re-arms after every refusal and
    fires again under the same close identity: its block clock accumulates."""
    foreign_buy = _raw('deal-F', 'BUY', 1.0, created='2026-10-15T09:00:00.000')
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [foreign_buy, _raw('deal-L', 'BUY', 2.0)]},
    })
    _seed_position(ctx)

    _clock_at(monkeypatch, 0.0)
    with pytest.raises(OrderSkippedByPlugin) as refused:
        asyncio.run(broker.execute_close(_partial_close(1700000000000, trigger=True)))
    assert refused.value.reason == 'foreign_deal_blocks_partial_close'
    _clock_at(monkeypatch, 60.0)
    with pytest.raises(OrderSkippedByPlugin):
        asyncio.run(broker.execute_close(_partial_close(1700000060000, trigger=True)))
    assert latched == [], "blocked for less than the bound"

    for offset_s, bar_ts_ms in ((121.0, 1700000120000), (181.0, 1700000180000)):
        _clock_at(monkeypatch, offset_s)
        with pytest.raises(OrderSkippedByPlugin):
            asyncio.run(broker.execute_close(_partial_close(bar_ts_ms, trigger=True)))
    assert len(latched) == 1, "the block latches once"
    assert latched[0][2] is QuarantineKind.ENTRY_BLOCK
    assert 'blocked for' in latched[0][0]
    assert _posts(broker) == []
    assert len(_events(ctx, 'foreign_deal_blocks_own_write')) == 1
    assert len(_events(ctx, 'foreign_deal_blocks_exit_latched')) == 1
    store.close()


def __test_exit_unblocked_before_the_bound_restarts_its_block_clock__(tmp_path, monkeypatch):
    """A foreign deal that leaves only delays the exit; a later, separate block
    is timed from its own first refusal."""
    own = _raw('deal-L', 'BUY', 2.0)
    first_foreign = _raw('deal-F', 'BUY', 1.0, created='2026-10-15T09:00:00.000')
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [first_foreign, own]},
    })
    _seed_position(ctx)

    _clock_at(monkeypatch, 0.0)
    with pytest.raises(OrderSkippedByPlugin):
        asyncio.run(broker.execute_close(_partial_close(1700000000000)))

    # The foreign deal left: the partial close goes through.
    _clock_at(monkeypatch, 60.0)
    broker._responses[('positions', 'get')] = [
        {'positions': [own]}, {'positions': [own]},
        {'positions': [_raw('deal-L', 'BUY', 1.0)]},
    ]
    broker._responses[('positions', 'post')] = {'dealReference': 'dr-1'}
    broker._responses[('confirms/dr-1', 'get')] = {'dealStatus': 'ACCEPTED'}
    assert asyncio.run(broker.execute_close(_partial_close(1700000060000))).id == 'dr-1'
    assert len(_posts(broker)) == 1

    # Another foreign deal blocks the next partial close 70 s later: 130 s
    # after the first refusal, but its own block has only just started.
    later_foreign = _raw('deal-G', 'BUY', 1.0, created='2026-10-15T09:30:00.000')
    broker._responses[('positions', 'get')] = {
        'positions': [later_foreign, _raw('deal-L', 'BUY', 1.0)]}
    _clock_at(monkeypatch, 130.0)
    with pytest.raises(OrderSkippedByPlugin) as refused:
        asyncio.run(broker.execute_close(_partial_close(1700000120000, qty=0.5)))
    assert refused.value.reason == 'foreign_deal_blocks_partial_close'
    assert latched == []
    assert len(_posts(broker)) == 1
    store.close()


def __test_block_the_poll_saw_end_never_times_a_later_block__(tmp_path, monkeypatch):
    """One refusal, then the foreign deal leaves while the exit is not
    re-evaluated: the poll ends the block, so a different deal blocking the
    same exit two hours later starts a clock of its own."""
    own = _raw('deal-L', 'BUY', 2.0)
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            _raw('deal-F', 'BUY', 1.0, created='2026-10-15T09:00:00.000'), own]},
    })
    _seed_position(ctx)

    _clock_at(monkeypatch, 0.0)
    with pytest.raises(OrderSkippedByPlugin):
        asyncio.run(broker.execute_close(_partial_close(1700000000000, trigger=True)))
    broker._remember_positions_snapshot(_by_deal(own), time.time() + 30.0)
    assert broker._foreign_exit_blocks == {}

    broker._responses[('positions', 'get')] = {'positions': [
        _raw('deal-G', 'BUY', 1.0, created='2026-10-15T09:30:00.000'), own]}
    _clock_at(monkeypatch, 7200.0)
    with pytest.raises(OrderSkippedByPlugin) as refused:
        asyncio.run(broker.execute_close(_partial_close(1700007200000, trigger=True)))
    assert refused.value.reason == 'foreign_deal_blocks_partial_close'
    assert latched == [], "a new block is timed from its own first refusal"
    store.close()


def __test_same_deal_blocking_the_exit_again_after_the_poll_ended_its_block_latches__(
        tmp_path, monkeypatch):
    """The poll ended the first block (the own deal's stop closed it); the
    strategy re-enters under the same entry id and the same foreign deal blocks
    the new exit under the same close identity. A cached snapshot never repeats
    a refusal whose block clock is not running: a fresh read starts the new
    block, which latches once past the bound."""
    foreign_buy = _raw('deal-F', 'BUY', 1.0, created='2026-10-15T09:00:00.000')
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [foreign_buy, _raw('deal-L', 'BUY', 2.0)]},
    })
    _seed_position(ctx)
    base = time.time()

    _clock_at(monkeypatch, 0.0)
    with pytest.raises(OrderSkippedByPlugin):
        asyncio.run(broker.execute_close(_partial_close(1700000000000)))
    broker._remember_positions_snapshot(_by_deal(foreign_buy), base + 30.0)
    assert broker._foreign_exit_blocks == {}
    ctx.close_order('coid-entry')

    own = _raw('deal-L2', 'BUY', 2.0, created='2026-10-15T10:30:00.000')
    _seed_position(ctx, coid='coid-entry2', deal_id='deal-L2')
    broker._responses[('positions', 'get')] = {'positions': [foreign_buy, own]}
    for offset_s in (60.0, 120.0, 181.0):
        _clock_at(monkeypatch, offset_s)
        broker._remember_positions_snapshot(_by_deal(foreign_buy, own), base + offset_s - 1.0)
        reads = _position_gets(broker)
        with pytest.raises(OrderSkippedByPlugin) as refused:
            asyncio.run(broker.execute_close(
                _partial_close(1700000000000 + int(offset_s) * 1000)))
        assert refused.value.reason == 'foreign_deal_blocks_partial_close'
        if offset_s == 60.0:
            assert _position_gets(broker) > reads, "a fresh read starts the new block"
            assert latched == []
    assert len(latched) == 1 and latched[0][2] is QuarantineKind.ENTRY_BLOCK
    assert "['deal-F']" in latched[0][0]
    assert _posts(broker) == []
    store.close()


def __test_refusals_no_read_connects_never_latch__(tmp_path, monkeypatch):
    """Two refusals two hours apart with no read in between prove no
    continuous block."""
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            _raw('deal-F', 'BUY', 1.0, created='2026-10-15T09:00:00.000'),
            _raw('deal-L', 'BUY', 2.0)]},
    })
    _seed_position(ctx)

    for offset_s, bar_ts_ms in ((0.0, 1700000000000), (7200.0, 1700007200000)):
        _clock_at(monkeypatch, offset_s)
        with pytest.raises(OrderSkippedByPlugin):
            asyncio.run(broker.execute_close(_partial_close(bar_ts_ms, trigger=True)))
    assert latched == []

    # From here the block is continuous: it latches once past the bound.
    for offset_s, bar_ts_ms in ((7260.0, 1700007260000), (7321.0, 1700007320000)):
        _clock_at(monkeypatch, offset_s)
        with pytest.raises(OrderSkippedByPlugin):
            asyncio.run(broker.execute_close(_partial_close(bar_ts_ms, trigger=True)))
    assert len(latched) == 1 and latched[0][2] is QuarantineKind.ENTRY_BLOCK
    store.close()


def __test_latched_exit_names_the_other_pyne_run_that_owns_the_blocking_deal__(
        tmp_path, monkeypatch):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            _raw('deal-B', 'BUY', 1.0, created='2026-10-15T09:00:00.000'),
            _raw('deal-L', 'BUY', 2.0),
        ]},
    })
    _seed_position(ctx)
    sibling = store.open_run(RunIdentity(
        strategy_id='sibling', symbol='EURUSD', timeframe='60',
        account_id='acc', label=None,
    ), script_source='// sibling')
    sibling.upsert_order('coid-b', symbol='EURUSD', side='buy', qty=1.0, filled_qty=1.0,
                         state='confirmed', pine_entry_id='B', exchange_order_id='deal-B',
                         extras={'kind': 'position'})

    _clock_at(monkeypatch, 0.0)
    with pytest.raises(OrderSkippedByPlugin) as refused:
        asyncio.run(broker.execute_close(_partial_close(1700000000000)))
    assert 'another Pyne run' in str(refused.value)
    blocks = _events(ctx, 'foreign_deal_blocks_own_write')
    assert len(blocks) == 1 and blocks[0]['other_pyne_run'] == ['deal-B']
    assert latched == []

    # The poll in between still shows the block.
    broker._remember_positions_snapshot(
        _by_deal(_raw('deal-B', 'BUY', 1.0, created='2026-10-15T09:00:00.000'),
                 _raw('deal-L', 'BUY', 2.0)), time.time() + 60.0)
    _clock_at(monkeypatch, 121.0)
    with pytest.raises(OrderSkippedByPlugin):
        asyncio.run(broker.execute_close(_partial_close(1700000060000)))
    assert len(latched) == 1
    reason, context, kind = latched[0]
    assert kind is QuarantineKind.ENTRY_BLOCK
    assert 'owned by another Pyne run' in reason
    assert context['other_pyne_run'] == ['deal-B']
    store.close()


# === Own position judged against what the bot set ============================

def __test_unprotected_position_reports_a_stop_someone_else_adds__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)

    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0)))
    assert 'protection_baseline_adopted' in _event_kinds(ctx)
    _someone_else_edits(broker)
    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.05)))
    assert latched == []
    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.05)))
    assert len(latched) == 1
    assert latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    store.close()


@pytest.mark.parametrize('added', [
    {'stopLevel': 1.05}, {'profitLevel': 1.20}, {'trailingStop': True, 'stopDistance': 0.02},
])
def __test_level_added_with_a_reduction_of_a_bare_position_is_reported__(tmp_path, added):
    """A reduction of the position never protects it: a stop, take-profit or
    trailing stop that shows up with it on a position the bot left bare is
    judged, not taken as the new baseline."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0)))

    reduced = _by_deal(_raw('deal-L', 'BUY', 1.0, **added))
    _someone_else_edits(broker)
    for _ in range(3):
        _poll(broker, reduced)

    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    assert _events(ctx, 'protection_adopted_after_size_change') == []
    store.close()


def __test_reduction_of_a_bare_position_keeps_it_bare__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0)))
    for _ in range(3):
        _poll(broker, _by_deal(_raw('deal-L', 'BUY', 1.0)))
    assert latched == []
    assert len(_events(ctx, 'protection_adopted_after_size_change')) == 1

    stopped = _by_deal(_raw('deal-L', 'BUY', 1.0, stopLevel=1.05))
    _someone_else_edits(broker)
    _poll(broker, stopped)
    _poll(broker, stopped)
    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    store.close()


def _event_payloads(ctx, kind: str) -> list[dict]:
    return [{k: v for k, v in e.items() if k != 'exchange_order_id'} for e in _events(ctx, kind)]


def __test_released_failsafe_parent_keeps_the_bots_own_protection__(tmp_path):
    """A TP-only software partial leg makes the parent fail-safe owned without
    the manager ever writing its stop; the guard forgets the bot's levels. Once
    the leg retires unfilled, the venue's (own) protection is the baseline."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    ctx.upsert_order('leg-partial', symbol='EURUSD', side='sell', qty=1.0,
                     state='partial_bracket_leg',
                     extras={'leg_kind': 'tp_partial',
                             'parent_entry_dispatch_ref': 'coid-entry'})
    own = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.05, profitLevel=1.20))
    _poll(broker, own)
    ctx.set_order_state('leg-partial', 'cancelled')
    ctx.close_order('leg-partial')

    for _ in range(3):
        _poll(broker, own)
    assert latched == []
    adopted = _event_payloads(ctx, 'protection_baseline_adopted')
    assert [a['source'] for a in adopted] == ['native_failsafe_handover']

    _someone_else_edits(broker)
    moved = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))
    _poll(broker, moved)
    _poll(broker, moved)
    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    store.close()


def __test_own_bracket_without_recorded_levels_is_adopted_after_a_restart__(tmp_path):
    """A position whose bracket an earlier plugin version wrote has no persisted
    levels: its own leg rows explain the venue, nothing is reported."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    row = ctx.get_order('coid-entry')
    assert row is not None
    extras = dict(row.extras or {})
    extras.pop('protection_levels')
    ctx.upsert_order('coid-entry', extras=extras)

    restarted, restarted_latched = _make_plugin(ctx)
    own = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.05, profitLevel=1.20))
    for _ in range(3):
        _poll(restarted, own)
    assert restarted_latched == []
    assert [a['source'] for a in _event_payloads(ctx, 'protection_baseline_adopted')] == [
        'own_bracket_legs']

    removed = _by_deal(_raw('deal-L', 'BUY', 2.0, profitLevel=1.20))
    _someone_else_edits(restarted)
    _poll(restarted, removed)
    _poll(restarted, removed)
    assert len(restarted_latched) == 1
    store.close()


def __test_unexplained_protection_without_a_record_is_logged_not_reported__(
        tmp_path, caplog):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    stopped = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.05))

    with caplog.at_level('ERROR', logger='pynecore_capitalcom.intervention'):
        for _ in range(3):
            _poll(broker, stopped)

    assert latched == [], "nothing shows who set the stop"
    unverified = _event_payloads(ctx, 'protection_baseline_unverified')
    assert len(unverified) == 1 and unverified[0]['levels'][0] == pytest.approx(1.05)
    assert any('no own record explains' in r.getMessage() for r in caplog.records)
    store.close()


def __test_foreign_deal_level_changes_are_never_judged__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    for stop in (None, 1.05, 1.06, 1.07):
        extra = {} if stop is None else {'stopLevel': stop}
        _poll(broker, _by_deal(
            _raw('deal-L', 'BUY', 2.0),
            _raw('deal-F', 'BUY', 1.0, created='2026-10-15T09:00:00.000', **extra),
        ))
    assert latched == []
    store.close()


def _set_pending_trail(ctx, *, activation=1.10, distance=0.04):
    sl_row = next(r for r in ctx.iter_live_orders()
                  if (r.extras or {}).get('leg_kind') == 'sl')
    extras = dict(sl_row.extras or {})
    extras['trail_state'] = 'pending'
    extras['trail_activation_price'] = activation
    ctx.upsert_order(sl_row.client_order_id, trailing_distance=distance, extras=extras)
    return sl_row.client_order_id


def __test_pending_trailing_stop_is_judged__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    _set_pending_trail(ctx, activation=1.50)

    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.05, profitLevel=1.20)))
    _someone_else_edits(broker)
    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20)))
    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20)))
    assert len(latched) == 1
    store.close()


def __test_trailing_activation_keeps_own_take_profit_and_never_adopts_a_foreign_edit__(
        tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    sl_coid = _set_pending_trail(ctx, activation=1.10, distance=0.04)
    broker._calls.clear()

    edited = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.30))
    asyncio.run(broker._trailing_activation_monitor(edited))
    assert [c for c in broker._calls if c[1] == 'put'] == []
    sl_row = ctx.get_order(sl_coid)
    assert sl_row is not None and (sl_row.extras or {}).get('trail_state') == 'pending'

    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'act'}
    broker._responses[('confirms/act', 'get')] = {'dealStatus': 'ACCEPTED'}
    own = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.05, profitLevel=1.20))
    asyncio.run(broker._trailing_activation_monitor(own))
    puts = [c for c in broker._calls if c[1] == 'put']
    assert len(puts) == 1
    assert puts[0][2] == {'trailingStop': True, 'stopDistance': pytest.approx(0.04),
                          'profitLevel': pytest.approx(1.20)}
    store.close()


def _level_edit(source: str) -> dict:
    return {
        'dateUTC': _now_iso(), 'dealId': 'deal-L', 'type': 'EDIT_STOP_AND_LIMIT',
        'status': 'ACCEPTED', 'source': source, 'epic': 'EURUSD',
        'details': {'stopLevel': 1.07, 'limitLevel': 1.20},
    }


@pytest.mark.parametrize('source, reported', [
    ('SYSTEM', False), ('DEALER', False), ('USER', True),
])
def __test_level_edit_is_judged_by_its_activity_source__(tmp_path, caplog, source, reported):
    """The venue (``SYSTEM``) or its dealing desk (``DEALER``) moving a bot
    stop is the venue's own mechanics: its levels become the baseline. The
    same edit by a ``USER`` is someone else's intervention."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.05, profitLevel=1.20)))
    _own_write_echoes(broker)

    assert asyncio.run(_drain(broker._process_activity([_level_edit(source)]))) == []
    edited = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))
    with caplog.at_level(logging.INFO, logger='pynecore_capitalcom.intervention'):
        for _ in range(3):
            _poll(broker, edited)

    if reported:
        assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
        assert _events(ctx, 'protection_venue_edit_adopted') == []
        store.close()
        return
    assert latched == []
    adopted = _events(ctx, 'protection_venue_edit_adopted')
    assert len(adopted) == 1 and adopted[0]['source'] == source
    expected_level = logging.WARNING if source == 'DEALER' else logging.INFO
    assert any(r.levelno == expected_level and 'edited its stop/limit levels' in r.getMessage()
               for r in caplog.records)
    # The adopted levels are the baseline the guard judges from now on.
    moved = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.09, profitLevel=1.20))
    assert asyncio.run(_drain(broker._process_activity([dict(
        _level_edit('USER'), dateUTC=_now_iso(1.0))]))) == []
    _poll(broker, moved)
    _poll(broker, moved)
    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    store.close()


def _start_with_edit(ctx, edit: dict, venue: dict):
    """A restarted process whose startup read holds ``edit``; its first polls
    look back 60 s only and never see it again."""
    restarted, restarted_latched = _make_plugin(ctx, responses={
        ('positions', 'get'): {'positions': [venue]},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': [*_own_write_echo_rows(ctx), edit, {
            'dateUTC': _now_iso(-1.0), 'dealId': 'deal-Z', 'type': 'POSITION',
            'status': 'EXECUTED', 'source': 'USER', 'epic': 'GBPUSD',
            'details': {'direction': 'BUY', 'size': 1.0, 'level': 1.3}}]},
    })
    asyncio.run(restarted._load_activity_cursor_from_events())
    asyncio.run(restarted._recover_in_flight_submissions())
    for _ in range(3):
        _poll(restarted, _by_deal(venue))
    return restarted_latched


@pytest.mark.parametrize('source, reported', [
    ('SYSTEM', False), ('DEALER', False), ('USER', True),
])
def __test_level_edit_made_while_the_bot_was_down_is_judged_by_its_source__(
        tmp_path, source, reported):
    """The guard restores the bot's own levels from the journal after a
    restart; who changed them while no process ran comes from the startup
    activity read."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.05, profitLevel=1.20)))
    edit = dict(_level_edit(source), dateUTC=_now_iso(1.0))
    edited = _raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20)

    restarted_latched = _start_with_edit(ctx, edit, edited)

    if reported:
        assert len(restarted_latched) == 1
        assert restarted_latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    else:
        assert restarted_latched == []
        assert len(_events(ctx, 'protection_venue_edit_adopted')) == 1
    store.close()


def __test_venue_edit_older_than_the_bots_own_write_never_explains_a_change_after_a_restart__(
        tmp_path):
    """The bot wrote its levels after the venue's edit: that edit explains
    nothing the venue shows after a restart, even without the bot's own echo
    in the startup read."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.05, profitLevel=1.20)))
    older = dict(_level_edit('SYSTEM'), dateUTC=_now_iso(-600.0))
    edited = _raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20)

    restarted_latched = _start_with_edit(ctx, older, edited)

    assert len(restarted_latched) == 1
    assert restarted_latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    assert _events(ctx, 'protection_venue_edit_adopted') == []
    store.close()


def _published_lag_broker(tmp_path):
    """A plugin that waits for the venue's real activity publication lag."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    broker._level_edit_settle_s = intervention._LEVEL_EDIT_SETTLE_S
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.05, profitLevel=1.20)))
    return broker, store, ctx, latched


@pytest.mark.parametrize('source, reported', [
    ('SYSTEM', False), ('DEALER', False), ('USER', True),
])
def __test_level_edit_whose_activity_is_published_late_is_judged_by_its_source__(
        tmp_path, source, reported):
    """The venue publishes an edit's activity up to a minute after the
    position shows it: the verdict on a confirmed mismatch waits for an
    activity read that started that long after the mismatch. Someone else's
    edit, once read, decides it at once."""
    broker, store, ctx, latched = _published_lag_broker(tmp_path)
    edited = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))
    for _ in range(3):
        _poll(broker, edited)
    assert latched == [], "held until the activity feed covers the change"
    assert _events(ctx, 'protection_venue_edit_adopted') == []

    _own_write_echoes(broker)
    assert asyncio.run(_drain(broker._process_activity([_level_edit(source)]))) == []
    _poll(broker, edited)
    if reported:
        assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
        assert _events(ctx, 'protection_venue_edit_adopted') == []
        store.close()
        return
    assert latched == [], "an activity read right after the mismatch proves nothing"
    _poll(broker, edited, activity_read_at=time.time() + intervention._LEVEL_EDIT_SETTLE_S)

    assert latched == []
    adopted = _events(ctx, 'protection_venue_edit_adopted')
    assert len(adopted) == 1 and adopted[0]['source'] == source
    store.close()


def __test_venue_edit_never_covers_a_later_edit_whose_activity_is_published_late__(tmp_path):
    """A venue edit already read must not explain a mismatch that someone
    else's later edit, its activity not yet published, made."""
    broker, store, ctx, latched = _published_lag_broker(tmp_path)
    assert asyncio.run(_drain(broker._process_activity([dict(
        _level_edit('SYSTEM'), dateUTC=_now_iso(-1.0))]))) == []
    edited = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))
    for _ in range(3):
        _poll(broker, edited)
    assert latched == []

    _own_write_echoes(broker)
    assert asyncio.run(_drain(broker._process_activity([_level_edit('USER')]))) == []
    _poll(broker, edited, activity_read_at=time.time() + intervention._LEVEL_EDIT_SETTLE_S)

    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    assert _events(ctx, 'protection_venue_edit_adopted') == []
    store.close()


def __test_venue_edit_held_through_an_activity_read_outage_is_still_the_venues__(
        tmp_path, monkeypatch):
    """The venue moves the bot's stop and the verdict waits for the feed to
    settle; then the activity reads fail for longer than an edit is
    remembered. The edits of a deal a verdict waits on are its evidence: the
    first read after the outage still finds the venue's edit."""
    broker, store, ctx, latched = _published_lag_broker(tmp_path)
    _feed(broker, _level_edit('SYSTEM'))
    edited = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))
    for _ in range(3):
        _poll(broker, edited)
    assert latched == [] and _events(ctx, 'level_edit_verdict_held')

    later = time.time() + intervention._LEVEL_EDIT_RETAIN_S + 100.0
    monkeypatch.setattr(intervention, 'epoch_time', lambda: later)
    # The read after the outage serves some other deal's edit, not this one.
    _feed(broker, {'dateUTC': _now_iso(1.0), 'dealId': 'deal-F', 'type': 'EDIT_STOP_AND_LIMIT',
                   'status': 'ACCEPTED', 'source': 'USER', 'epic': 'EURUSD'})
    _poll(broker, edited, activity_read_at=later)

    assert latched == []
    adopted = _events(ctx, 'protection_venue_edit_adopted')
    assert len(adopted) == 1 and adopted[0]['source'] == 'SYSTEM'
    assert _events(ctx, 'protection_edit_unattributed') == []
    store.close()


def __test_venue_edit_read_before_an_outage_explains_the_change_confirmed_after_it__(
        tmp_path, monkeypatch):
    """A venue edit read before the snapshot shows its change, then an
    activity-read outage longer than an edit is remembered: the read after the
    outage serves the edit again, below the cursor's watermark by then, and
    that keeps it as the evidence of the change confirmed afterwards."""
    broker, store, ctx, latched = _published_lag_broker(tmp_path)
    system_edit = _level_edit('SYSTEM')
    # A later activity on the bot's deal moves the cursor's watermark more
    # than the publication lag past the edit.
    swap = {'dateUTC': _now_iso(120.0), 'dealId': 'deal-L', 'type': 'SWAP',
            'status': 'ACCEPTED', 'source': 'SYSTEM', 'epic': 'EURUSD'}
    _feed(broker, system_edit, swap)

    later = time.time() + intervention._LEVEL_EDIT_RETAIN_S + 100.0
    monkeypatch.setattr(intervention, 'epoch_time', lambda: later)
    other_edit = {'dateUTC': _now_iso(130.0), 'dealId': 'deal-F',
                  'type': 'EDIT_STOP_AND_LIMIT', 'status': 'ACCEPTED', 'source': 'USER',
                  'epic': 'EURUSD'}
    _feed(broker, system_edit, swap, other_edit)
    edited = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))
    for _ in range(3):
        _poll(broker, edited)
    _poll(broker, edited, activity_read_at=time.time() + intervention._LEVEL_EDIT_SETTLE_S)

    assert latched == []
    adopted = _events(ctx, 'protection_venue_edit_adopted')
    assert len(adopted) == 1 and adopted[0]['source'] == 'SYSTEM'
    assert _events(ctx, 'protection_edit_unattributed') == []
    store.close()


@pytest.mark.parametrize('source', ['SL', 'TP', 'CLOSE_OUT', '', 'SYSTEM', 'DEALER'])
def __test_venue_edit_dated_with_the_own_echo_is_the_newest__(tmp_path, source):
    """The venue dates its edit in the same millisecond as the echo of the
    bot's own write: the venue's edit made the change, whatever its source
    is called."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.05, profitLevel=1.20)))
    echoes = _own_write_echo_rows(ctx)
    for echo in echoes:
        broker._note_level_edit(echo)

    _feed(broker, dict(_level_edit(source), dateUTC=echoes[-1]['dateUTC']))
    edited = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))
    for _ in range(3):
        _poll(broker, edited)

    assert latched == []
    assert len(_events(ctx, 'protection_venue_edit_adopted')) == 1
    store.close()

def __test_change_no_edit_activity_explains_is_reported__(tmp_path):
    """The feed covered the change and holds no edit that made it: neither the
    bot nor the venue moved the bot's levels, so the change is someone
    else's."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.05, profitLevel=1.20)))
    edited = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))

    for _ in range(4):
        _poll(broker, edited)

    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    assert _events(ctx, 'protection_venue_edit_adopted') == []
    store.close()


@pytest.mark.parametrize('source', ['SL', 'TP', 'CLOSE_OUT', ''])
def __test_level_edit_from_any_venue_source_is_the_venues__(tmp_path, source):
    """Only ``USER`` is a client's request: an edit from any other source is
    the venue's own (``DEALER`` its dealing desk's), the same reading the
    close and cancel attribution use."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.05, profitLevel=1.20)))
    _own_write_echoes(broker)

    _feed(broker, _level_edit(source))
    edited = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))
    for _ in range(3):
        _poll(broker, edited)

    assert latched == []
    adopted = _events(ctx, 'protection_venue_edit_adopted')
    assert len(adopted) == 1 and adopted[0]['source'] == source
    store.close()


def _feed(broker, *activities, read_at=None):
    """Hand ``activities`` to the poll's activity processing as one read.

    ``read_at`` is when the read returned (it samples the venue clock).
    """
    assert asyncio.run(_drain(broker._process_activity(
        list(activities), read_at=read_at))) == []


def _edit_at(source: str, offset_s: float) -> dict:
    return dict(_level_edit(source), dateUTC=_now_iso(offset_s))


def _protection_puts(broker) -> list:
    return [c for c in broker._calls if c[:2] == ('positions/deal-L', 'put')]


def _modify_stop(broker, old_sl: float, new_sl: float):
    """The strategy moves its exit's stop (``modify_exit``, PUT accepted)."""
    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'amend'}
    broker._responses[('confirms/amend', 'get')] = {'dealStatus': 'ACCEPTED'}

    def exit_envelope(sl_price):
        return DispatchEnvelope(
            intent=ExitIntent(pine_id='X', from_entry='Long', symbol='EURUSD', side='sell',
                              qty=2.0, tp_price=1.20, sl_price=sl_price),
            run_tag='test', bar_ts_ms=1700000000000,
        )

    return asyncio.run(broker.modify_exit(exit_envelope(old_sl), exit_envelope(new_sl)))


@pytest.mark.parametrize('source, reported', [
    ('SYSTEM', False), ('DEALER', False), ('USER', True),
])
def __test_own_exit_write_waits_while_the_verdict_on_a_change_is_held__(
        tmp_path, source, reported):
    """The bot does not write a position's protection while who changed it is
    still being read: an amend or a fresh attach would replace levels someone
    else may have set before the quarantine latches. Nothing is sent; the exit
    goes out once the change is taken as the venue's, never after someone
    else's."""
    broker, store, ctx, latched = _published_lag_broker(tmp_path)
    edited = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))
    for _ in range(3):
        _poll(broker, edited)
    sent = len(_protection_puts(broker))

    with pytest.raises(OrderSkippedByPlugin) as amend:
        _modify_stop(broker, 1.05, 1.04)
    assert amend.value.reason == 'protection_verdict_pending'
    with pytest.raises(OrderSkippedByPlugin) as attach:
        _attach(broker, tp=1.20, sl=1.04)
    assert attach.value.reason == 'protection_verdict_pending'
    assert len(_protection_puts(broker)) == sent, "nothing reaches the venue"
    assert latched == []

    # The attach's own edit, then the edit that moved the stop.
    _feed(broker, _edit_at('USER', 0.01), _edit_at(source, 0.5), read_at=time.time() + 0.6)
    _poll(broker, edited, activity_read_at=time.time() + intervention._LEVEL_EDIT_SETTLE_S)

    decided = _events(ctx, 'level_edit_verdict_decided')
    assert [event['verdict'] for event in decided] == ['foreign' if reported else 'venue'], \
        "the edit read decided the verdict"
    if reported:
        assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
        assert len(_protection_puts(broker)) == sent
        store.close()
        return
    assert latched == []
    adopted = _events(ctx, 'protection_venue_edit_adopted')
    assert len(adopted) == 1 and adopted[0]['source'] == source
    _attach(broker, tp=1.20, sl=1.04)
    assert len(_protection_puts(broker)) == sent + 1
    store.close()


def __test_someone_elses_edit_read_while_the_verdict_waits_decides_it_at_once__(tmp_path):
    """A ``USER`` edit after the bot's last write that no own write explains is
    someone else's whatever the venue publishes later: it latches at once,
    without waiting out the publication window."""
    broker, store, ctx, latched = _published_lag_broker(tmp_path)
    _feed(broker, _edit_at('USER', 0.01), read_at=time.time() + 0.1)
    edited = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))
    for _ in range(3):
        _poll(broker, edited)
    assert latched == []

    _feed(broker, _edit_at('USER', 0.5), read_at=time.time() + 0.6)
    _poll(broker, edited)

    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    decided = _events(ctx, 'level_edit_verdict_decided')
    assert len(decided) == 1 and decided[0]['verdict'] == 'foreign'
    assert decided[0]['feed_settled'] is False
    store.close()


def __test_venue_edit_after_someone_elses_edit_never_absorbs_it__(tmp_path):
    """Someone else moves the bot's stop, then the venue edits the position
    while the verdict waits: the venue's later edit does not explain the
    earlier change, which is someone else's."""
    broker, store, ctx, latched = _published_lag_broker(tmp_path)
    # The venue's edit activity of the bot's own attach.
    _feed(broker, _edit_at('USER', 0.01), read_at=time.time() + 0.1)
    edited = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))
    for _ in range(3):
        _poll(broker, edited)
    assert latched == []

    _feed(broker, _edit_at('USER', 0.5), _edit_at('SYSTEM', 1.0), read_at=time.time() + 2.0)
    _poll(broker, edited, activity_read_at=time.time() + intervention._LEVEL_EDIT_SETTLE_S)

    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    assert _events(ctx, 'protection_venue_edit_adopted') == []
    store.close()


def __test_venue_edits_again_while_the_verdict_waits_never_end_in_a_quarantine__(tmp_path):
    """The venue moves the bot's stop, and its dealing desk moves it again
    before the verdict on the first change is decided: only the venue touched
    the position, so the levels it shows last become the baseline and nothing
    is reported, then or on the polls after."""
    broker, store, ctx, latched = _published_lag_broker(tmp_path)
    first = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))
    for _ in range(3):
        _poll(broker, first)
    assert latched == []

    second = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.08, profitLevel=1.20))
    _feed(broker, _edit_at('SYSTEM', -0.5), _edit_at('DEALER', 0.5), read_at=time.time() + 0.6)
    for _ in range(2):
        _poll(broker, second)
    renewed = _events(ctx, 'level_edit_verdict_levels_changed')
    assert len(renewed) == 1 and renewed[0]['observed'][0] == pytest.approx(1.08)
    settle = intervention._LEVEL_EDIT_SETTLE_S
    for _ in range(4):
        _poll(broker, second, activity_read_at=time.time() + settle)

    assert latched == []
    decided = _events(ctx, 'level_edit_verdict_decided')
    assert [event['verdict'] for event in decided] == ['venue']
    adopted = _events(ctx, 'protection_venue_edit_adopted')
    assert len(adopted) == 1 and adopted[0]['source'] == 'DEALER'
    assert adopted[0]['observed'][0] == pytest.approx(1.08)
    store.close()


def __test_venue_edit_read_before_any_snapshot_shows_it_waits_for_one__(tmp_path):
    """The dealing desk moves the bot's stop again after the first change was
    confirmed, and the activity read that settles the verdict is the first to
    hold its edit: the snapshot read before it still shows the first levels.
    The verdict waits for a snapshot read after the edit, so the dealing
    desk's levels, not the first ones, become the baseline."""
    broker, store, ctx, latched = _published_lag_broker(tmp_path)
    first = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))
    for _ in range(3):
        _poll(broker, first)
    assert latched == []

    settle = intervention._LEVEL_EDIT_SETTLE_S
    snapshot_started_at = time.time()
    _feed(broker, _edit_at('SYSTEM', -0.5), _edit_at('DEALER', 0.0), read_at=time.time() + 0.1)
    broker._activity_polled_at = time.time() + settle
    assert asyncio.run(_drain(broker._reconcile_snapshot(
        first, {}, read_started_at=snapshot_started_at))) == []
    assert _events(ctx, 'level_edit_verdict_decided') == [], \
        "no snapshot read after the dealing desk's edit yet"

    second = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.08, profitLevel=1.20))
    for _ in range(2):
        _poll(broker, second, activity_read_at=time.time() + settle)
    for _ in range(3):
        _poll(broker, second, activity_read_at=time.time() + 2 * settle)

    assert latched == []
    adopted = _events(ctx, 'protection_venue_edit_adopted')
    assert len(adopted) == 1 and adopted[0]['observed'][0] == pytest.approx(1.08)
    store.close()


def __test_user_edit_an_operator_resume_accepted_never_decides_a_later_venue_edit__(tmp_path):
    """Someone moved the bot's stop and the operator resumed the run: the
    restarted process reads that edit again from its startup activity read,
    but the resume accepted it. A later edit by the venue is the venue's own,
    never reported because of the edit the resume accepted."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.05, profitLevel=1.20)))
    # The venue's edit activity of the bot's own attach, then someone else's edit.
    own_echo = _edit_at('USER', 0.01)
    accepted_edit = _edit_at('USER', 0.5)
    rebaseline_after_resume(ctx)
    operator_left = _raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20)

    restarted, restarted_latched = _make_plugin(ctx, responses={
        ('positions', 'get'): {'positions': [operator_left]},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': [accepted_edit, own_echo]},
    })
    restarted.operator_resume_ts_ms = int((time.time() + 1.0) * 1000)
    asyncio.run(restarted._load_activity_cursor_from_events())
    asyncio.run(restarted._recover_in_flight_submissions())
    for _ in range(3):
        _poll(restarted, _by_deal(operator_left))
    assert restarted_latched == []

    _feed(restarted, _edit_at('SYSTEM', 2.0), read_at=time.time() + 2.1)
    moved = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.08, profitLevel=1.20))
    for _ in range(3):
        # Snapshots read after the resume, as the venue edit was made.
        read_started_at = time.time() + 2.5
        restarted._activity_polled_at = read_started_at
        assert asyncio.run(_drain(restarted._reconcile_snapshot(
            moved, {}, read_started_at=read_started_at))) == []

    assert restarted_latched == []
    decided = _events(ctx, 'level_edit_verdict_decided')
    assert [event['verdict'] for event in decided] == ['venue']
    adopted = _events(ctx, 'protection_venue_edit_adopted')
    assert len(adopted) == 1 and adopted[0]['source'] == 'SYSTEM'
    store.close()


def __test_venue_edit_a_verdict_accepted_never_explains_a_later_change__(tmp_path):
    """A venue edit already taken as the venue's explains nothing after it: a
    later change no edit activity explains is reported."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _attach(broker, tp=1.20, sl=1.05)
    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.05, profitLevel=1.20)))
    _feed(broker, _level_edit('SYSTEM'))
    edited = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))
    for _ in range(3):
        _poll(broker, edited)
    assert latched == []
    assert len(_events(ctx, 'protection_venue_edit_adopted')) == 1

    moved = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.09, profitLevel=1.20))
    _poll(broker, moved)
    _poll(broker, moved)

    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    assert len(_events(ctx, 'protection_venue_edit_adopted')) == 1
    store.close()


def __test_verdict_held_when_the_process_restarts_is_still_decided__(tmp_path):
    """A change of the bot's stop confirmed before a restart is decided by the
    restarted process, against the time it was confirmed; the restarted bot
    does not write the protection before that either."""
    broker, store, ctx, latched = _published_lag_broker(tmp_path)
    edited = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))
    for _ in range(3):
        _poll(broker, edited)
    with pytest.raises(OrderSkippedByPlugin):
        _modify_stop(broker, 1.05, 1.04)

    restarted, restarted_latched = _make_plugin(ctx)
    restarted._level_edit_settle_s = intervention._LEVEL_EDIT_SETTLE_S
    restarted._reload_held_edit_verdicts()
    assert len(_events(ctx, 'level_edit_verdict_reloaded')) == 1
    with pytest.raises(OrderSkippedByPlugin):
        _attach(restarted, tp=1.20, sl=1.04)
    _feed(restarted, _edit_at('USER', 0.01), _edit_at('USER', 0.5), read_at=time.time() + 0.6)
    _poll(restarted, edited, activity_read_at=time.time() + intervention._LEVEL_EDIT_SETTLE_S)

    decided = _events(ctx, 'level_edit_verdict_decided')
    assert [event['verdict'] for event in decided] == ['foreign']
    assert len(restarted_latched) == 1
    assert restarted_latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    store.close()


@pytest.mark.parametrize('confirmed_after_resume', [False, True])
def __test_verdict_held_over_an_operator_resume_is_decided_against_the_resume__(
        tmp_path, confirmed_after_resume):
    """The operator stops the bot while a verdict waits and resumes it: the
    resume accepted the venue as it stood, so a change confirmed before the
    resume is never reported by the restarted process. A change confirmed
    after the resume (a process applying the same resume again) still is."""
    resumed_at_ms = int(time.time() * 1000) - 1
    broker, store, ctx, latched = _published_lag_broker(tmp_path)
    edited = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))
    if confirmed_after_resume:
        broker.operator_resume_ts_ms = resumed_at_ms
    for _ in range(3):
        _poll(broker, edited)
    assert latched == []
    if not confirmed_after_resume:
        resumed_at_ms = int(time.time() * 1000) + 1
        rebaseline_after_resume(ctx)

    restarted, restarted_latched = _make_plugin(ctx)
    restarted._level_edit_settle_s = intervention._LEVEL_EDIT_SETTLE_S
    restarted.operator_resume_ts_ms = resumed_at_ms
    restarted._reload_held_edit_verdicts()
    assert len(_events(ctx, 'level_edit_verdict_reloaded')) == 1
    _feed(restarted, _edit_at('USER', 0.01), _edit_at('USER', 0.5), read_at=time.time() + 0.6)
    _poll(restarted, edited, activity_read_at=time.time() + intervention._LEVEL_EDIT_SETTLE_S)

    decided = _events(ctx, 'level_edit_verdict_decided')
    assert [event['verdict'] for event in decided] == ['foreign']
    accepted = _events(ctx, 'external_intervention_accepted_by_resume')
    if confirmed_after_resume:
        assert len(restarted_latched) == 1
        assert restarted_latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
        assert accepted == []
    else:
        assert restarted_latched == []
        assert len(accepted) == 1 and accepted[0]['resume_ts_ms'] == resumed_at_ms
    store.close()


@pytest.mark.parametrize('read_in_flight', [False, True])
@pytest.mark.parametrize('source', ['SYSTEM', 'DEALER'])
def __test_failsafe_stop_written_while_a_verdict_waits_never_reports_a_venue_edit__(
        tmp_path, source, read_in_flight):
    """The native fail-safe manager's stop write is the bot's own: its ``USER``
    edit activity never decides a verdict held on the position, also when a
    poll reads it while the write is still in flight, and the venue edit
    published after it is still taken as the venue's."""
    broker, store, ctx, latched = _published_lag_broker(tmp_path)
    _feed(broker, _edit_at('USER', 0.01), read_at=time.time() + 0.1)
    edited = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))
    for _ in range(3):
        _poll(broker, edited)

    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'failsafe'}
    broker._responses[('confirms/failsafe', 'get')] = {'dealStatus': 'ACCEPTED'}

    def late_feed():
        # The venue's edit that made the change, published late, then the
        # manager's own write.
        return [_edit_at(source, 0.2), _edit_at('USER', 0.5)]

    ran = (_read_while_in_flight(broker, 'positions/deal-L', late_feed)
           if read_in_flight else None)
    asyncio.run(broker.publish_native_failsafe_sl(NativeBracketSnapshot(
        parent_entry_dispatch_ref='coid-entry', symbol='EURUSD', parent_side='long',
        stop_level=1.04, profit_level=1.20, trailing_stop=None, generation=1,
    )))
    if ran is not None:
        assert ran, "the poll read the feed while the write was in flight"
        assert latched == [], "the write's own echo decided nothing while it was in flight"
    else:
        _feed(broker, *late_feed(), read_at=time.time() + 0.6)
    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.04, profitLevel=1.20)),
          activity_read_at=time.time() + intervention._LEVEL_EDIT_SETTLE_S)

    assert latched == []
    adopted = _events(ctx, 'protection_venue_edit_adopted')
    assert len(adopted) == 1 and adopted[0]['source'] == source
    store.close()


@pytest.mark.parametrize('source', ['SYSTEM', 'DEALER'])
def __test_unguarded_own_write_echo_read_in_flight_never_decides_a_held_verdict__(
        tmp_path, source):
    """An own protection write of a position the guard does not judge (the
    native fail-safe manager owns its stop) is the bot's own as well: a poll
    reading its edit activity while the write waits for its confirm decides
    nothing, and the venue's edit read with it is still the venue's."""
    broker, store, ctx, latched = _published_lag_broker(tmp_path)
    _feed(broker, _edit_at('USER', 0.01), read_at=time.time() + 0.1)
    edited = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))
    for _ in range(3):
        _poll(broker, edited)
    # A live software partial leg: the manager owns the parent's stop.
    ctx.upsert_order('leg-partial', symbol='EURUSD', side='sell', qty=1.0,
                     state='partial_bracket_leg',
                     extras={'leg_kind': 'tp_partial',
                             'parent_entry_dispatch_ref': 'coid-entry'})
    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'leg'}
    broker._responses[('confirms/leg', 'get')] = {'dealStatus': 'ACCEPTED'}
    ran = _read_while_in_flight(broker, 'confirms/leg', lambda: [
        _edit_at(source, 0.2), _edit_at('USER', 0.5)])

    asyncio.run(broker.amend_bracket(
        'EURUSD', 'deal-L', side='sell', tp_price=1.20, sl_price=1.04,
        trail_offset=None, coid='coid-leg-amend',
    ))
    assert ran, "the poll read the feed while the write was in flight"
    assert latched == []
    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.04, profitLevel=1.20)),
          activity_read_at=time.time() + intervention._LEVEL_EDIT_SETTLE_S)

    assert latched == []
    adopted = _events(ctx, 'protection_venue_edit_adopted')
    assert len(adopted) == 1 and adopted[0]['source'] == source
    store.close()


@pytest.mark.parametrize('read_in_flight', [False, True])
@pytest.mark.parametrize('source, reported', [
    ('SYSTEM', False), ('DEALER', False), ('USER', True),
])
def __test_own_write_during_a_held_verdict_waits_for_an_edit_published_late__(
        tmp_path, source, reported, read_in_flight):
    """A position protection write the bot does not defer (the one-way
    emulator's leg amend) goes out while the verdict on a change of the
    position waits. The change stays judged by the edit that made it, read
    late: a venue or dealing-desk edit is the venue's and the bot's own
    levels stand, someone else's edit is reported. The write's own edit
    activity never decides the verdict, also when a poll reads it while the
    write still waits for its confirm."""
    broker, store, ctx, latched = _published_lag_broker(tmp_path)
    _feed(broker, _edit_at('USER', 0.01), read_at=time.time() + 0.1)
    edited = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20))
    for _ in range(3):
        _poll(broker, edited)
    assert latched == []

    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'leg'}
    broker._responses[('confirms/leg', 'get')] = {'dealStatus': 'ACCEPTED'}

    def late_feed():
        # The edit that made the change, published late, then the write's own.
        return [_edit_at(source, 0.2), _edit_at('USER', 0.5)]

    ran = _read_while_in_flight(broker, 'confirms/leg', late_feed) if read_in_flight else None
    asyncio.run(broker.amend_bracket(
        'EURUSD', 'deal-L', side='sell', tp_price=1.20, sl_price=1.04,
        trail_offset=None, coid='coid-leg-amend',
    ))
    if ran is not None:
        assert ran, "the poll read the feed while the write was in flight"
        assert latched == [], "the write's own echo decided nothing while it was in flight"
    else:
        _feed(broker, *late_feed(), read_at=time.time() + 0.6)
    assert len(_events(ctx, 'level_edit_verdict_kept_past_own_write')) == 1
    own = _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.04, profitLevel=1.20))
    _poll(broker, own, activity_read_at=time.time() + intervention._LEVEL_EDIT_SETTLE_S)

    decided = _events(ctx, 'level_edit_verdict_decided')
    assert [event['verdict'] for event in decided] == ['foreign' if reported else 'venue']
    assert decided[0]['provisional'] is True
    if reported:
        assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
        assert _events(ctx, 'protection_venue_edit_adopted') == []
        store.close()
        return
    assert latched == []
    adopted = _events(ctx, 'protection_venue_edit_adopted')
    assert len(adopted) == 1 and adopted[0]['source'] == source
    assert adopted[0]['own_write_while_held'] is True
    # The bot's own levels stand: the venue carrying them is no change.
    _poll(broker, own)
    _poll(broker, own)
    assert latched == []
    store.close()


def __test_foreign_edit_around_a_reduction_decided_after_an_own_write_is_reported_once__(
        tmp_path):
    """Someone else moves the stop of the bot's position around its own partial
    close, and the edit's activity is published late. The bot writes its
    protection while the verdict waits (the one-way emulator's leg amend). The
    edit is reported once; the bot's own levels stay the guard's baseline, so
    the venue carrying them is never reported as a second change."""
    broker, store, ctx, latched = _published_lag_broker(tmp_path)
    _feed(broker, _edit_at('USER', 0.01), read_at=time.time() + 0.1)
    with broker._own_close(['deal-L'], full=False, size=1.0):
        pass
    reduced = _by_deal(_raw('deal-L', 'BUY', 1.0, stopLevel=1.07, profitLevel=1.20))
    for _ in range(3):
        _poll(broker, reduced)
    assert latched == [], "held until the activity feed covers the change"
    assert _events(ctx, 'protection_adopted_after_size_change') == []

    broker._responses[('positions/deal-L', 'put')] = {'dealReference': 'leg'}
    broker._responses[('confirms/leg', 'get')] = {'dealStatus': 'ACCEPTED'}
    asyncio.run(broker.amend_bracket(
        'EURUSD', 'deal-L', side='sell', tp_price=1.20, sl_price=1.04,
        trail_offset=None, coid='coid-leg-amend',
    ))
    assert len(_events(ctx, 'level_edit_verdict_kept_past_own_write')) == 1
    _feed(broker, _edit_at('USER', 0.2), _edit_at('USER', 0.5), read_at=time.time() + 0.6)
    own = _by_deal(_raw('deal-L', 'BUY', 1.0, stopLevel=1.04, profitLevel=1.20))
    _poll(broker, own, activity_read_at=time.time() + intervention._LEVEL_EDIT_SETTLE_S)

    decided = _events(ctx, 'level_edit_verdict_decided')
    assert [(event['verdict'], event['provisional']) for event in decided] == [
        ('foreign', True)]
    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    assert 'reduction' in latched[0][0]
    for _ in range(4):
        _poll(broker, own, activity_read_at=time.time() + intervention._LEVEL_EDIT_SETTLE_S)
    assert len(latched) == 1, "the bot's own levels on the venue are no second change"
    store.close()


def _partial_close_of_a_protected_position(
        broker, *, edit: dict | None, moved_before: bool):
    """The bot's position carries its own SL 1.05 / TP 1.20, ``edit`` (if any)
    is read and the stop shows at 1.07 — on one poll before the bot's own
    partial close goes out when ``moved_before``, else only with the reduced
    size."""
    _attach(broker, tp=1.20, sl=1.05)
    _feed(broker, _edit_at('USER', 0.01), read_at=time.time() + 0.1)
    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.05, profitLevel=1.20)))
    if edit is not None:
        _feed(broker, edit, read_at=time.time() + 0.6)
    if moved_before:
        _poll(broker, _by_deal(_raw('deal-L', 'BUY', 2.0, stopLevel=1.07, profitLevel=1.20)))
    with broker._own_close(['deal-L'], full=False, size=1.0):
        pass
    reduced = _by_deal(_raw('deal-L', 'BUY', 1.0, stopLevel=1.07, profitLevel=1.20))
    for _ in range(3):
        _poll(broker, reduced)


def __test_someone_elses_edit_around_an_own_partial_close_is_reported__(tmp_path):
    """Levels a protected position shows after the bot's own partial close are
    taken as the venue's only when no one else's edit explains them: someone
    else's stop move is reported, never adopted as the bot's protection."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)

    _partial_close_of_a_protected_position(
        broker, edit=_edit_at('USER', 0.5), moved_before=True)

    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    assert 'reduction' in latched[0][0]
    assert _events(ctx, 'protection_adopted_after_size_change') == []
    store.close()


@pytest.mark.parametrize('source', [None, 'SYSTEM', 'DEALER'])
def __test_levels_the_venue_leaves_after_an_own_partial_close_are_adopted__(
        tmp_path, caplog, source):
    """No edit, or a venue or dealing-desk one: the reduction's levels are the
    venue's and become the accepted protection (the dealing desk's loudly)."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)

    with caplog.at_level(logging.INFO, logger='pynecore_capitalcom.intervention'):
        _partial_close_of_a_protected_position(
            broker, edit=_edit_at(source, 0.5) if source is not None else None,
            moved_before=False)

    assert latched == []
    adopted = _events(ctx, 'protection_adopted_after_size_change')
    assert len(adopted) == 1 and adopted[0]['source'] == source
    assert adopted[0]['levels'][0] == pytest.approx(1.07)
    assert any(r.levelno == logging.WARNING and 'dealing desk' in r.getMessage()
               for r in caplog.records) is (source == 'DEALER')
    store.close()


# === Resting bot entry orders ===============================================

def _working(level, *, order_type='LIMIT'):
    return {'wo-1': {'marketData': {'epic': 'EURUSD'},
                     'workingOrderData': {'dealId': 'wo-1', 'epic': 'EURUSD',
                                          'direction': 'BUY', 'orderSize': 1.0,
                                          'orderLevel': level, 'orderType': order_type}}}


def _limit_envelope(limit, bar_ts_ms=1700000000000):
    return DispatchEnvelope(
        intent=EntryIntent(pine_id='Long', symbol='EURUSD', side='buy', qty=1.0,
                           order_type=OrderType.LIMIT, limit=limit, stop=None),
        run_tag='test', bar_ts_ms=bar_ts_ms,
    )


def _place_working(broker, level) -> DispatchEnvelope:
    """Place the bot's resting LIMIT entry ``wo-1`` through the real entry path."""
    broker._responses[('workingorders', 'post')] = {'dealReference': 'o_wo'}
    broker._responses[('confirms/o_wo', 'get')] = {
        'dealStatus': 'ACCEPTED', 'status': 'ACCEPTED', 'dealId': 'wo-1',
        'level': level, 'size': 1.0, 'affectedDeals': [{'dealId': 'wo-1'}],
    }
    envelope = _limit_envelope(level)
    asyncio.run(broker.execute_entry(envelope))
    return envelope


def _amend_with_lost_confirm(broker, ctx, old, new) -> None:
    """An own amend whose PUT got a reference but whose confirm read timed out,
    later resolved as refused (as recovery's confirm read decides it)."""
    broker._responses[('workingorders/wo-1', 'put')] = {'dealReference': 'amend'}
    broker._responses[('error', 'confirms/amend', 'get')] = httpx.ReadTimeout('timed out')
    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.modify_entry(old, new))
    command = new.client_order_id(KIND_MODIFY_ENTRY)
    row = ctx.get_order(command)
    assert row is not None and row.state == 'disposition_unknown'
    ctx.set_order_state(command, 'rejected')


def __test_working_order_level_moved_by_someone_else_is_reported__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _place_working(broker, 1.20)
    row = ctx.get_order(_limit_envelope(1.20).client_order_id(KIND_ENTRY))
    assert row is not None and row.state == 'confirmed'
    assert (row.extras or {}).get('order_level') == pytest.approx(1.20)

    _poll(broker, {}, _working(1.20))
    _someone_else_edits(broker, 'wo-1', working=True)
    _poll(broker, {}, _working(1.25))
    assert latched == []
    _poll(broker, {}, _working(1.25))
    assert len(latched) == 1
    reason, context, kind = latched[0]
    assert kind is QuarantineKind.EXTERNAL_INTERVENTION
    assert 'wo-1' in reason and 'working order' in reason
    store.close()


def _working_with(level=1.20, **attributes):
    orders = _working(level)
    orders['wo-1']['workingOrderData'].update(attributes)
    return orders


@pytest.mark.parametrize('attributes', [
    {'stopDistance': 25.0},
    {'profitDistance': 40.0},
    {'goodTillDate': '2026-10-20T12:00:00', 'timeInForce': 'GOOD_TILL_DATE'},
    {'guaranteedStop': True, 'stopDistance': 30.0},
])
def __test_working_order_attribute_someone_else_adds_is_reported__(tmp_path, attributes):
    """The bot never puts a stop, take-profit, expiry or stop flag on its
    resting entry: one appearing there is an edit of the bot's own order."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _place_working(broker, 1.20)
    plain = _working_with(timeInForce='GOOD_TILL_CANCELLED', stopDistance=None,
                          profitDistance=None, goodTillDate=None, guaranteedStop=False)
    _poll(broker, {}, plain)
    _poll(broker, {}, plain)
    assert latched == []

    _someone_else_edits(broker, 'wo-1', working=True)
    _poll(broker, {}, _working_with(**attributes))
    assert latched == [], "a second read decides"
    _poll(broker, {}, _working_with(**attributes))
    _poll(broker, {}, _working_with(**attributes))
    assert len(latched) == 1
    reason, context, kind = latched[0]
    assert kind is QuarantineKind.EXTERNAL_INTERVENTION
    assert 'wo-1' in reason and context['observed_attributes']
    store.close()


def __test_working_order_attributes_an_operator_resume_accepted_are_the_baseline__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _place_working(broker, 1.20)
    broker.operator_resume_ts_ms = int(time.time() * 1000)
    stopped = _working_with(stopDistance=25.0)

    for _ in range(3):
        _poll(broker, {}, stopped)
    assert latched == [], "the resume accepted the order as the venue showed it"
    assert len(_events(ctx, 'working_order_attributes_accepted_by_resume')) == 1

    # A process restarting under the same resume judges against what was accepted.
    restarted, restarted_latched = _make_plugin(ctx)
    restarted.operator_resume_ts_ms = broker.operator_resume_ts_ms
    _poll(restarted, {}, stopped)
    _someone_else_edits(restarted, 'wo-1', working=True)
    widened = _working_with(stopDistance=50.0)
    _poll(restarted, {}, widened)
    _poll(restarted, {}, widened)
    assert len(restarted_latched) == 1
    assert restarted_latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    store.close()


def __test_working_order_attributes_no_edit_explains_are_reported__(tmp_path):
    """An attribute on the bot's resting order that the covered feed
    attributes to no edit is someone else's: reported once."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _place_working(broker, 1.20)
    _poll(broker, {}, _working(1.20))
    stopped = _working_with(stopDistance=25.0)

    for _ in range(4):
        _poll(broker, {}, stopped)

    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    store.close()


def _working_order_edit(source: str, level: float = 1.25) -> dict:
    return {
        'dateUTC': _now_iso(), 'dealId': 'wo-1', 'type': 'WORKING_ORDER',
        'status': 'MODIFIED', 'source': source, 'epic': 'EURUSD',
        'details': {'direction': 'BUY', 'size': 1.0, 'level': level},
    }


@pytest.mark.parametrize('source, reported', [
    ('SYSTEM', False), ('DEALER', False), ('USER', True),
])
def __test_working_order_level_edit_is_judged_by_its_activity_source__(
        tmp_path, caplog, source, reported):
    """The venue or its dealing desk moving the bot's resting order is not an
    intervention, exactly as on a position: the venue's level is accepted."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _place_working(broker, 1.20)
    _poll(broker, {}, _working(1.20))

    assert asyncio.run(_drain(broker._process_activity([_working_order_edit(source)]))) == []
    with caplog.at_level(logging.INFO, logger='pynecore_capitalcom.intervention'):
        for _ in range(3):
            _poll(broker, {}, _working(1.25))

    if reported:
        assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
        assert _events(ctx, 'protection_venue_edit_adopted') == []
        store.close()
        return
    assert latched == []
    adopted = _events(ctx, 'protection_venue_edit_adopted')
    assert len(adopted) == 1 and adopted[0]['source'] == source
    assert adopted[0]['protection_key'] == 'working:wo-1'
    expected_level = logging.WARNING if source == 'DEALER' else logging.INFO
    assert any(r.levelno == expected_level and 'edited its order level' in r.getMessage()
               for r in caplog.records)
    # The accepted level is the baseline from now on.
    assert asyncio.run(_drain(broker._process_activity([dict(
        _working_order_edit('USER', 1.30), dateUTC=_now_iso(1.0))]))) == []
    _poll(broker, {}, _working(1.30))
    _poll(broker, {}, _working(1.30))
    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    store.close()


@pytest.mark.parametrize('source, reported', [('DEALER', False), ('USER', True)])
def __test_working_order_attribute_edit_is_judged_by_its_activity_source__(
        tmp_path, source, reported):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _place_working(broker, 1.20)
    _poll(broker, {}, _working(1.20))

    assert asyncio.run(_drain(broker._process_activity([_working_order_edit(source, 1.20)]))) == []
    for _ in range(3):
        _poll(broker, {}, _working_with(stopDistance=25.0))

    if reported:
        assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
        store.close()
        return
    assert latched == []
    adopted = _events(ctx, 'working_order_venue_edit_adopted')
    assert len(adopted) == 1 and adopted[0]['attributes'] == [['stopDistance', '25.0']]
    store.close()


@pytest.mark.parametrize('source, reported', [('DEALER', False), ('USER', True)])
def __test_working_order_attribute_edit_published_late_is_judged_by_its_source__(
        tmp_path, source, reported):
    broker, store, ctx, latched = _make_broker(tmp_path)
    broker._level_edit_settle_s = intervention._LEVEL_EDIT_SETTLE_S
    _place_working(broker, 1.20)
    _poll(broker, {}, _working(1.20))
    for _ in range(3):
        _poll(broker, {}, _working_with(stopDistance=25.0))
    assert latched == [], "held until the activity feed covers the change"

    assert asyncio.run(_drain(broker._process_activity([_working_order_edit(source, 1.20)]))) == []
    _poll(broker, {}, _working_with(stopDistance=25.0),
          activity_read_at=time.time() + intervention._LEVEL_EDIT_SETTLE_S)

    if reported:
        assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
        assert _events(ctx, 'working_order_venue_edit_adopted') == []
    else:
        assert latched == []
        assert len(_events(ctx, 'working_order_venue_edit_adopted')) == 1
    store.close()


@pytest.mark.parametrize('source, reported', [('DEALER', False), ('USER', True)])
def __test_own_amend_during_a_held_attribute_verdict_waits_for_an_edit_published_late__(
        tmp_path, source, reported):
    """A resting order's amend cannot wait and goes out while the verdict on
    an attribute waits; it does not decide the verdict early. The edit that
    set the attribute decides once the feed covers the change, judged against
    the measured venue clock; the amend's own edit activity never does."""
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('workingorders/wo-1', 'put'): {'dealReference': 'amend'},
        ('confirms/amend', 'get'): {'dealStatus': 'ACCEPTED', 'status': 'OPEN',
                                    'dealId': 'wo-1', 'level': 1.22},
    })
    broker._level_edit_settle_s = intervention._LEVEL_EDIT_SETTLE_S
    old = _place_working(broker, 1.20)
    _poll(broker, {}, _working(1.20))
    for _ in range(3):
        _poll(broker, {}, _working_with(stopDistance=25.0))

    asyncio.run(broker.modify_entry(old, _limit_envelope(1.22, 1700000060000)))
    assert latched == [], "nothing read so far shows who set the stop"
    _feed(broker,
          dict(_working_order_edit(source, 1.20), dateUTC=_now_iso(-3.0)),
          dict(_working_order_edit('USER', 1.22), dateUTC=_now_iso(0.5)),
          read_at=time.time() + 0.6)
    assert broker._venue_clock_samples, "the read sampled the venue clock"
    _poll(broker, {}, _working_with(1.22, stopDistance=25.0),
          activity_read_at=time.time() + intervention._LEVEL_EDIT_SETTLE_S)

    decided = _events(ctx, 'level_edit_verdict_decided')
    assert [event['verdict'] for event in decided] == ['foreign' if reported else 'venue'], \
        "the edit that set the attribute decided the verdict, not the amend's own edit"
    if reported:
        assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
        assert _events(ctx, 'working_order_venue_edit_adopted') == []
    else:
        assert latched == []
        assert len(_events(ctx, 'working_order_venue_edit_adopted')) == 1
    store.close()


def _read_while_in_flight(broker, endpoint: str, activities):
    """Run one poll's activity read and verdict pass while the own write's
    request to ``endpoint`` is still awaited.

    :return: The endpoints the read ran during (one at most).
    """
    call = broker._call
    ran: list = []

    async def read_during_a_poll(endpoint_called, *, data=None, method='post'):
        if endpoint_called == endpoint and not ran:
            ran.append(endpoint)
            assert await _drain(broker._process_activity(
                activities(), read_at=time.time() + 0.6)) == []
            broker._activity_polled_at = time.time()
            broker._resolve_held_edit_verdicts()
        return await call(endpoint_called, data=data, method=method)

    broker._call = read_during_a_poll
    return ran


@pytest.mark.parametrize('source, reported', [
    ('SYSTEM', False), ('DEALER', False), ('USER', True),
])
def __test_own_amend_echo_read_while_the_amend_is_in_flight_never_decides_a_held_verdict__(
        tmp_path, source, reported):
    """A poll can read the amend's own edit activity while the amend still
    waits for its confirm: that echo is the bot's own write, so it never
    decides the verdict held on the order as someone else's. The edit that
    set the attribute decides it once the write settled."""
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('workingorders/wo-1', 'put'): {'dealReference': 'amend'},
        ('confirms/amend', 'get'): {'dealStatus': 'ACCEPTED', 'status': 'OPEN',
                                    'dealId': 'wo-1', 'level': 1.22},
    })
    broker._level_edit_settle_s = intervention._LEVEL_EDIT_SETTLE_S
    old = _place_working(broker, 1.20)
    _poll(broker, {}, _working(1.20))
    for _ in range(3):
        _poll(broker, {}, _working_with(stopDistance=25.0))
    ran = _read_while_in_flight(broker, 'confirms/amend', lambda: [
        dict(_working_order_edit(source, 1.20), dateUTC=_now_iso(-3.0)),
        dict(_working_order_edit('USER', 1.22), dateUTC=_now_iso(0.0)),
    ])

    asyncio.run(broker.modify_entry(old, _limit_envelope(1.22, 1700000060000)))
    assert ran, "the poll read the feed while the amend was in flight"
    assert latched == [], "the amend's own echo decided nothing while it was in flight"
    _poll(broker, {}, _working_with(1.22, stopDistance=25.0),
          activity_read_at=time.time() + intervention._LEVEL_EDIT_SETTLE_S)

    decided = _events(ctx, 'level_edit_verdict_decided')
    assert [event['verdict'] for event in decided] == ['foreign' if reported else 'venue']
    if reported:
        assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
        assert _events(ctx, 'working_order_venue_edit_adopted') == []
    else:
        assert latched == []
        assert len(_events(ctx, 'working_order_venue_edit_adopted')) == 1
    store.close()


def __test_someone_elses_edit_before_an_own_amend_outranks_an_older_venue_edit__(tmp_path):
    """Someone else sets a stop on the bot's resting order after an older
    venue edit, and the bot amends the order while the verdict waits: the
    ``USER`` edit made just before the amend is someone else's, hidden
    neither by the older venue edit nor by the amend's own edit activity."""
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('workingorders/wo-1', 'put'): {'dealReference': 'amend'},
        ('confirms/amend', 'get'): {'dealStatus': 'ACCEPTED', 'status': 'OPEN',
                                    'dealId': 'wo-1', 'level': 1.22},
    })
    broker._level_edit_settle_s = intervention._LEVEL_EDIT_SETTLE_S
    old = _place_working(broker, 1.20)
    _poll(broker, {}, _working(1.20))
    for _ in range(3):
        _poll(broker, {}, _working_with(stopDistance=25.0))

    asyncio.run(broker.modify_entry(old, _limit_envelope(1.22, 1700000060000)))
    _feed(broker,
          dict(_working_order_edit('SYSTEM', 1.20), dateUTC=_now_iso(-10.0)),
          dict(_working_order_edit('USER', 1.20), dateUTC=_now_iso(-3.0)),
          dict(_working_order_edit('USER', 1.22), dateUTC=_now_iso(0.5)),
          read_at=time.time() + 0.6)
    _poll(broker, {}, _working_with(1.22, stopDistance=25.0),
          activity_read_at=time.time() + intervention._LEVEL_EDIT_SETTLE_S)

    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    assert _events(ctx, 'working_order_venue_edit_adopted') == []
    store.close()


def __test_own_amend_while_a_resting_order_verdict_waits_survives_a_restart__(tmp_path):
    """Someone else moves the bot's resting order, and the bot's own amend of
    it goes out while the verdict waits: a restart before the verdict still
    reports the move, although the venue then shows the bot's own level."""
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('workingorders/wo-1', 'put'): {'dealReference': 'amend'},
        ('confirms/amend', 'get'): {'dealStatus': 'ACCEPTED', 'status': 'OPEN',
                                    'dealId': 'wo-1', 'level': 1.22},
    })
    broker._level_edit_settle_s = intervention._LEVEL_EDIT_SETTLE_S
    old = _place_working(broker, 1.20)
    _poll(broker, {}, _working(1.20))
    for _ in range(3):
        _poll(broker, {}, _working(1.25))
    asyncio.run(broker.modify_entry(old, _limit_envelope(1.22, 1700000060000)))
    assert latched == []

    restarted, restarted_latched = _make_plugin(ctx)
    restarted._level_edit_settle_s = intervention._LEVEL_EDIT_SETTLE_S
    restarted._reload_held_edit_verdicts()
    _feed(restarted,
          dict(_working_order_edit('USER', 1.25), dateUTC=_now_iso(-3.0)),
          dict(_working_order_edit('USER', 1.22), dateUTC=_now_iso(0.5)),
          read_at=time.time() + 0.6)
    for _ in range(2):
        _poll(restarted, {}, _working(1.22),
              activity_read_at=time.time() + intervention._LEVEL_EDIT_SETTLE_S)

    assert len(restarted_latched) == 1
    assert restarted_latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    assert 'wo-1' in restarted_latched[0][0]
    store.close()


def __test_own_working_order_amend_is_not_reported__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('workingorders/wo-1', 'put'): {'dealReference': 'amend'},
        ('confirms/amend', 'get'): {'dealStatus': 'ACCEPTED', 'status': 'OPEN',
                                    'dealId': 'wo-1', 'level': 1.25},
    })
    old = _place_working(broker, 1.20)
    new = _limit_envelope(1.25, 1700000060000)
    _poll(broker, {}, _working(1.20))

    asyncio.run(broker.modify_entry(old, new))
    for _ in range(3):
        _poll(broker, {}, _working(1.25))
    assert latched == []
    store.close()


def __test_refused_ambiguous_own_amend_is_not_reported_after_a_restart__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    old = _place_working(broker, 1.20)
    _amend_with_lost_confirm(broker, ctx, old, _limit_envelope(1.25, 1700000060000))

    restarted, latched = _make_plugin(ctx)
    for _ in range(3):
        _poll(restarted, {}, _working(1.20))
    assert latched == [], "the venue still carries the bot's own placed level"
    _someone_else_edits(restarted, 'wo-1', working=True)
    _poll(restarted, {}, _working(1.30))
    _poll(restarted, {}, _working(1.30))
    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    store.close()


def __test_ambiguous_amend_of_an_order_without_a_recorded_level_keeps_the_old_level__(
        tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    old, new = _limit_envelope(1.20), _limit_envelope(1.25, 1700000060000)
    _seed_working(ctx, coid=old.client_order_id(KIND_ENTRY), level=None)
    _amend_with_lost_confirm(broker, ctx, old, new)

    for _ in range(3):
        _poll(broker, {}, _working(1.20))
    assert latched == [], "the amend's previous level is the bot's own"
    store.close()


def __test_off_grid_placed_level_accepts_either_grid_neighbour__(tmp_path):
    """The bot places a limit between two ticks and the confirm does not echo
    the level: how the venue keeps it on its 0.01 grid is undocumented, so
    either neighbour is the bot's own level; a move of more is not."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    broker._responses[('workingorders', 'post')] = {'dealReference': 'o_wo'}
    broker._responses[('confirms/o_wo', 'get')] = {
        'dealStatus': 'ACCEPTED', 'status': 'ACCEPTED', 'dealId': 'wo-1',
        'size': 1.0, 'affectedDeals': [{'dealId': 'wo-1'}],
    }
    asyncio.run(broker.execute_entry(_limit_envelope(1.234567)))

    for level in (1.23, 1.23, 1.24, 1.24, 1.24):
        _poll(broker, {}, _working(level))
    assert latched == []

    _someone_else_edits(broker, 'wo-1', working=True)
    _poll(broker, {}, _working(1.26))
    _poll(broker, {}, _working(1.26))
    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    store.close()


def __test_echoed_placed_level_is_the_bots_own__(tmp_path):
    """The confirm echoes the grid level the venue kept the order at: that
    level is the bot's own, judged at half a tick."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    broker._responses[('workingorders', 'post')] = {'dealReference': 'o_wo'}
    broker._responses[('confirms/o_wo', 'get')] = {
        'dealStatus': 'ACCEPTED', 'status': 'ACCEPTED', 'dealId': 'wo-1',
        'level': 1.26, 'size': 1.0, 'affectedDeals': [{'dealId': 'wo-1'}],
    }
    asyncio.run(broker.execute_entry(_limit_envelope(1.254)))
    row = ctx.get_order(_limit_envelope(1.254).client_order_id(KIND_ENTRY))
    assert row is not None and (row.extras or {}).get('order_level') == pytest.approx(1.26)

    for _ in range(3):
        _poll(broker, {}, _working(1.26))
    assert latched == []
    _someone_else_edits(broker, 'wo-1', working=True)
    _poll(broker, {}, _working(1.27))
    _poll(broker, {}, _working(1.27))
    assert len(latched) == 1, "one tick off an echoed grid level is someone else's"
    store.close()


def __test_own_amend_to_an_off_grid_level_accepts_the_venue_echo__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('workingorders/wo-1', 'put'): {'dealReference': 'amend'},
        ('confirms/amend', 'get'): {'dealStatus': 'ACCEPTED', 'status': 'OPEN',
                                    'dealId': 'wo-1', 'level': 1.26},
    })
    old = _place_working(broker, 1.20)
    _poll(broker, {}, _working(1.20))

    asyncio.run(broker.modify_entry(old, _limit_envelope(1.254, 1700000060000)))
    for _ in range(3):
        _poll(broker, {}, _working(1.26))
    assert latched == [], "the venue kept the bot's own amend at 1.26"

    _someone_else_edits(broker, 'wo-1', working=True)
    _poll(broker, {}, _working(1.29))
    _poll(broker, {}, _working(1.29))
    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    store.close()


def __test_own_working_order_netting_a_foreign_deal_is_never_promoted__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_working(ctx, side='buy')
    foreign = _raw('deal-F', 'SELL', 1.0, created='2026-10-15T09:00:00.000')
    activity = {
        'dateUTC': _now_iso(), 'dealId': 'deal-F', 'type': 'POSITION',
        'status': 'EXECUTED', 'source': 'USER', 'epic': 'EURUSD',
        'details': {'workingOrderId': 'wo-1', 'direction': 'BUY', 'size': 1.0,
                    'level': 1.11},
    }

    for _ in range(2):
        events = asyncio.run(_drain(broker._process_activity([activity], _by_deal(foreign))))
        assert events == []
    row = ctx.get_order('coid-wo')
    assert row is not None
    assert (row.extras or {}).get('kind') == 'working'
    assert row.exchange_order_id == 'wo-1'
    assert ctx.find_by_ref('deal_id', 'deal-F') is None
    assert len(latched) == 1 and latched[0][2] is QuarantineKind.ENTRY_BLOCK
    blocks = _events(ctx, 'foreign_deal_blocks_own_write')
    assert len(blocks) == 1
    assert blocks[0]['reason'] == 'own_working_order_netted_foreign_deal'
    store.close()


def _execute_into_foreign_deal(tmp_path, *, netted_first: bool):
    """Own BUY working order ``wo-1`` executes on a netting account and only
    nets the foreign SELL ``deal-F``; both activities in one batch."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_working(ctx, side='buy')
    foreign = _raw('deal-F', 'SELL', 1.0, created='2026-10-15T09:00:00.000')
    now = _now_iso()
    netted = {
        'dateUTC': now, 'dealId': 'deal-F', 'type': 'POSITION',
        'status': 'EXECUTED', 'source': 'USER', 'epic': 'EURUSD',
        'details': {'workingOrderId': 'wo-1', 'direction': 'BUY', 'size': 1.0,
                    'level': 1.11},
    }
    executed = {
        'dateUTC': now, 'dealId': 'wo-1', 'type': 'WORKING_ORDER',
        'status': 'EXECUTED', 'source': 'USER', 'epic': 'EURUSD',
        'details': {'direction': 'BUY', 'size': 1.0, 'level': 1.11},
    }
    batch = [netted, executed] if netted_first else [executed, netted]
    events = asyncio.run(_drain(broker._process_activity(batch, _by_deal(foreign))))
    return store, ctx, latched, events


@pytest.mark.parametrize('netted_first', [True, False])
def __test_own_working_order_that_only_netted_a_foreign_deal_books_no_entry__(
        tmp_path, netted_first):
    """The run holds no exposure from the execution: no ENTRY fill is booked,
    the row retires and the engine gets the entry back as rejected."""
    store, ctx, latched, events = _execute_into_foreign_deal(
        tmp_path, netted_first=netted_first)

    assert [(e.event_type, e.fill_qty) for e in events] == [('rejected', None)]
    row = ctx.get_order('coid-wo')
    assert row is not None and row.closed_ts_ms is not None
    assert row.filled_qty == 0.0
    assert len(latched) == 1 and latched[0][2] is QuarantineKind.ENTRY_BLOCK
    assert len(_events(ctx, 'own_working_order_retired_netted_foreign_deal')) == 1
    store.close()


@pytest.mark.parametrize('execution_first', [True, False])
def __test_own_working_order_that_netted_a_foreign_deal_books_only_the_deal_it_opened__(
        tmp_path, execution_first):
    """A BUY 2 working order executes into a foreign SELL 1 on a netting
    account: it consumes that deal and opens one of 1 with the rest. Only the
    opened deal is the run's exposure."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_working(ctx, side='buy', qty=2.0)
    # The last poll saw the foreign deal; the execution consumed it.
    broker._remember_positions_snapshot(
        _by_deal(_raw('deal-F', 'SELL', 1.0, created='2026-10-15T09:00:00.000')),
        time.time() - 5.0)
    now = _now_iso()
    netted = {
        'dateUTC': now, 'dealId': 'deal-F', 'type': 'POSITION',
        'status': 'EXECUTED', 'source': 'USER', 'epic': 'EURUSD',
        'details': {'workingOrderId': 'wo-1', 'direction': 'BUY', 'size': 1.0,
                    'level': 1.11},
    }
    opened = {
        'dateUTC': now, 'dealId': 'deal-N', 'type': 'POSITION',
        'status': 'EXECUTED', 'source': 'USER', 'epic': 'EURUSD',
        'details': {'workingOrderId': 'wo-1', 'direction': 'BUY', 'size': 1.0,
                    'level': 1.11},
    }
    executed = {
        'dateUTC': now, 'dealId': 'wo-1', 'type': 'WORKING_ORDER',
        'status': 'EXECUTED', 'source': 'USER', 'epic': 'EURUSD',
        'details': {'direction': 'BUY', 'size': 2.0, 'level': 1.11},
    }
    batch = [executed, netted, opened] if execution_first else [netted, opened, executed]
    positions = _by_deal(_raw('deal-N', 'BUY', 1.0, created=now, workingOrderId='wo-1'))

    events = asyncio.run(_drain(broker._process_activity(batch, positions)))

    fills = [(e.leg_type, e.fill_qty) for e in events if e.event_type == 'filled']
    assert fills == [(LegType.ENTRY, 1.0)], "only the opened deal is booked"
    row = ctx.find_by_ref('deal_id', 'deal-N')
    assert row is not None and row.client_order_id == 'coid-wo'
    assert row.filled_qty == pytest.approx(1.0)
    assert ctx.find_by_ref('deal_id', 'deal-F') is None
    assert len(latched) == 1 and latched[0][2] is QuarantineKind.ENTRY_BLOCK
    blocks = _events(ctx, 'foreign_deal_blocks_own_write')
    assert [b['reason'] for b in blocks] == ['own_working_order_netted_foreign_deal']

    # A replay of the same batch books nothing again.
    assert asyncio.run(_drain(broker._process_activity(batch, positions))) == []
    assert len(latched) == 1
    store.close()


def _retire_into_foreign_deal_before_its_residual_shows(broker, ctx):
    """Own BUY 2 working order ``wo-1`` nets the foreign SELL 1 ``deal-F`` and
    opens ``deal-N`` with the rest, but the first poll shows no trace of
    ``deal-N`` yet: the execution is retired as one that only netted."""
    _seed_working(ctx, side='buy', qty=2.0)
    broker._remember_positions_snapshot(
        _by_deal(_raw('deal-F', 'SELL', 1.0, created='2026-10-15T09:00:00.000')),
        time.time() - 5.0)
    now = _now_iso()
    netted = {
        'dateUTC': now, 'dealId': 'deal-F', 'type': 'POSITION',
        'status': 'EXECUTED', 'source': 'USER', 'epic': 'EURUSD',
        'details': {'workingOrderId': 'wo-1', 'direction': 'BUY', 'size': 1.0,
                    'level': 1.11},
    }
    executed = {
        'dateUTC': now, 'dealId': 'wo-1', 'type': 'WORKING_ORDER',
        'status': 'EXECUTED', 'source': 'USER', 'epic': 'EURUSD',
        'details': {'direction': 'BUY', 'size': 2.0, 'level': 1.11},
    }
    opened = {
        'dateUTC': now, 'dealId': 'deal-N', 'type': 'POSITION',
        'status': 'EXECUTED', 'source': 'USER', 'epic': 'EURUSD',
        'details': {'workingOrderId': 'wo-1', 'direction': 'BUY', 'size': 1.0,
                    'level': 1.11},
    }
    events = asyncio.run(_drain(broker._process_activity([executed, netted], {})))
    assert [(e.event_type, e.fill_qty) for e in events] == [('rejected', None)]
    row = ctx.get_order('coid-wo')
    assert row is not None and row.closed_ts_ms is not None
    deal_n = _raw('deal-N', 'BUY', 1.0, created=now, workingOrderId='wo-1')
    return [executed, netted, opened], deal_n


def __test_deal_a_retired_own_working_order_opened_later_is_reported__(tmp_path):
    """The deal the order opened surfaces a poll after the retirement: it is
    the bot's own unbooked exposure, named as such (ERROR, audit event,
    ``ENTRY_BLOCK``), never filed as someone else's deal."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    batch, deal_n = _retire_into_foreign_deal_before_its_residual_shows(broker, ctx)
    assert [k for _, _, k in latched] == [QuarantineKind.ENTRY_BLOCK]

    for _ in range(2):
        assert asyncio.run(_drain(broker._process_activity(batch, _by_deal(deal_n)))) == []

    residual = _events(ctx, 'own_working_order_residual_unbooked')
    assert [e['exchange_order_id'] for e in residual] == ['deal-N'], "reported once"
    assert residual[0]['netted_deal_id'] == 'deal-F'
    assert len(latched) == 2 and latched[1][2] is QuarantineKind.ENTRY_BLOCK
    assert 'deal-N' in latched[1][0] and 'close it on the venue' in latched[1][0]
    broker._responses[('positions', 'get')] = {'positions': [deal_n]}
    assert asyncio.run(broker.get_position('EURUSD')) is None, "the engine booked no entry"
    assert _events(ctx, 'foreign_deal_on_epic') == []

    # A restarted process remembers the report.
    restarted, restarted_latched = _start_process(ctx, positions=[deal_n], activities=batch)
    asyncio.run(_drain(restarted._process_activity(batch, _by_deal(deal_n))))
    assert restarted_latched == []
    assert len(_events(ctx, 'own_working_order_residual_unbooked')) == 1
    store.close()


def __test_deal_a_retired_own_working_order_opened_is_reported_after_a_restart__(tmp_path):
    """The process ended before the deal surfaced: the startup pass names it
    as the retired order's residual instead of an untracked deal."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _, deal_n = _retire_into_foreign_deal_before_its_residual_shows(broker, ctx)

    restarted, restarted_latched = _start_process(ctx, positions=[deal_n], activities=[])

    assert [e['exchange_order_id'] for e in _events(ctx, 'own_working_order_residual_unbooked')] \
        == ['deal-N']
    assert len(restarted_latched) == 1 and restarted_latched[0][2] is QuarantineKind.ENTRY_BLOCK
    assert _events(ctx, 'foreign_deal_on_epic_at_startup') == []
    store.close()


# === Close attribution =======================================================

def __test_foreign_partial_close_lowers_the_run_exposure__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)

    asyncio.run(_drain(broker._process_activity(
        [_close_activity(size=0.5)], _by_deal(_raw('deal-L', 'BUY', 1.5)),
    )))

    row = ctx.get_order('coid-entry')
    assert row is not None and row.filled_qty == pytest.approx(1.5)
    retired = _events(ctx, 'partial_close_journal_retired')
    assert len(retired) == 1 and retired[0]['verdict'] == 'foreign'
    assert len(latched) == 1
    store.close()


def __test_dealer_partial_close_lowers_the_run_exposure_without_a_report__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)

    asyncio.run(_drain(broker._process_activity(
        [_close_activity(source='DEALER', size=0.5)],
        _by_deal(_raw('deal-L', 'BUY', 1.5)),
    )))

    row = ctx.get_order('coid-entry')
    assert row is not None and row.filled_qty == pytest.approx(1.5)
    assert [e['verdict'] for e in _events(ctx, 'partial_close_journal_retired')] == ['dealer']
    assert latched == []
    store.close()


def __test_close_on_a_foreign_deal_is_ignored__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    activity = dict(_close_activity(size=0.5), dealId='deal-F')

    events = asyncio.run(_drain(broker._process_activity(
        [activity], _by_deal(_raw('deal-L', 'BUY', 2.0), _raw('deal-F', 'BUY', 0.5)),
    )))

    assert events == []
    assert latched == []
    row = ctx.get_order('coid-entry')
    assert row is not None and row.filled_qty == 2.0
    store.close()


@pytest.mark.parametrize('source', ['', 'LIQUIDATION'])
def __test_close_from_any_venue_source_is_booked_as_the_venues_close__(tmp_path, source):
    """A close of a filled bot position whose source is neither a client's
    nor the dealing desk's is the venue's own close: booked as a close,
    never as an entry fill, and never reported."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)

    events = asyncio.run(_drain(broker._process_activity([_close_activity(source=source)])))

    assert [(e.leg_type, e.order.side, e.fill_qty) for e in events] == [
        (LegType.CLOSE, 'sell', 2.0)]
    assert latched == []
    store.close()


def __test_own_opposite_row_whose_deal_closed_does_not_explain_a_close__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    ctx.upsert_order('coid-short', symbol='EURUSD', side='sell', qty=1.0, filled_qty=1.0,
                     state='confirmed', pine_entry_id='Short',
                     exchange_order_id='deal-S',
                     extras={'kind': 'position', 'entry_filled_at': time.time(),
                             'natural_close_at': time.time()})

    asyncio.run(_drain(broker._process_activity(
        [_close_activity(size=1.0)], _by_deal(_raw('deal-L', 'BUY', 1.0)),
    )))

    assert len(latched) == 1
    assert 'user_close_attributed_to_own_netting' not in _event_kinds(ctx)
    store.close()


def __test_filled_own_working_order_explains_a_netting_close__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _seed_working(ctx, coid='coid-short-wo', deal_id='wo-S', side='sell',
                  extras={'entry_filled_at': time.time()})

    asyncio.run(_drain(broker._process_activity(
        [_close_activity(size=1.0)], _by_deal(_raw('deal-L', 'BUY', 1.0)),
    )))

    assert latched == []
    kinds = _event_kinds(ctx)
    assert 'user_close_attributed_to_own_netting' in kinds
    assert 'user_close_ambiguous_netting' not in kinds
    store.close()


def __test_close_dated_before_the_own_request_is_reported__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions/deal-L', 'delete'): {'dealReference': 'del'},
    })
    _seed_position(ctx)
    asyncio.run(broker.execute_close(DispatchEnvelope(
        intent=CloseIntent(pine_id='Long', symbol='EURUSD', side='sell', qty=2.0),
        run_tag='test', bar_ts_ms=1700000000000,
    )))
    # A measured venue clock in step with the local one.
    broker._note_venue_clock([{'dateUTC': _now_iso()}], time.time())

    asyncio.run(_drain(broker._process_activity(
        [_close_activity(date_utc=_now_iso(-60.0))],
    )))

    assert len(latched) == 1, "a close a minute before the DELETE left is someone else's"
    store.close()


def __test_own_close_after_a_forward_local_clock_step_is_own__(tmp_path, monkeypatch):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions/deal-L', 'delete'): {'dealReference': 'del'},
    })
    _seed_position(ctx)
    # A measured venue clock in step with the local one, then the local wall
    # clock steps 20 s ahead (an NTP step, a wake from sleep).
    broker._note_venue_clock([{'dateUTC': _now_iso()}], time.time())
    monkeypatch.setattr(intervention, 'epoch_time', lambda: time.time() + 20.0)
    asyncio.run(broker.execute_close(DispatchEnvelope(
        intent=CloseIntent(pine_id='Long', symbol='EURUSD', side='sell', qty=2.0),
        run_tag='test', bar_ts_ms=1700000000000,
    )))

    asyncio.run(_drain(broker._process_activity(
        [_close_activity(date_utc=_now_iso(0.1))],
    )))

    assert latched == [], "the venue executed the bot's own DELETE"
    assert len(_events(ctx, 'own_close_activity_attributed')) == 1
    store.close()


def __test_own_close_before_a_backward_local_clock_step_is_own__(tmp_path, monkeypatch):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions/deal-L', 'delete'): {'dealReference': 'del'},
    })
    _seed_position(ctx)
    asyncio.run(broker.execute_close(DispatchEnvelope(
        intent=CloseIntent(pine_id='Long', symbol='EURUSD', side='sell', qty=2.0),
        run_tag='test', bar_ts_ms=1700000000000,
    )))
    # The local wall clock steps 20 s back, then the venue clock is measured.
    monkeypatch.setattr(intervention, 'epoch_time', lambda: time.time() - 20.0)
    broker._note_venue_clock([{'dateUTC': _now_iso()}], time.time() - 20.0)

    asyncio.run(_drain(broker._process_activity(
        [_close_activity(date_utc=_now_iso(0.1))],
    )))

    assert latched == [], "the venue executed the bot's own DELETE"
    store.close()


def __test_priceless_foreign_close_is_reported_at_once_and_booked_once__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    date_utc = _now_iso()
    priceless = _close_activity(level=None, date_utc=date_utc)

    for _ in range(2):
        events = asyncio.run(_drain(broker._process_activity([priceless])))
        assert events == []
    assert len(latched) == 1, "who closed it does not wait for the price"

    priced = _close_activity(level=1.12, date_utc=date_utc)
    events = asyncio.run(_drain(broker._process_activity([priced])))
    assert len(events) == 1 and events[0].fill_qty == 2.0
    # A copy the venue re-issues with a top-level level is a new fingerprint:
    # only the close's level-independent identity tells it is the same close.
    republished = {**_close_activity(level=1.121, date_utc=date_utc), 'level': 1.121}
    assert _activity_fingerprint(republished) != _activity_fingerprint(priced)
    events = asyncio.run(_drain(broker._process_activity([republished])))
    assert events == [], "a re-issued copy of a booked close is not booked again"
    assert any(e['reason'] == 'close_already_accounted'
               for e in _events(ctx, 'close_fill_replay_suppressed'))
    assert len(latched) == 1
    store.close()


# === Disappearance and parked partial closes =================================

def __test_vanished_position_waits_for_its_close_activity__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('error', 'positions/deal-L', 'get'): OrderNotFoundError(
            "API error occured: error.not-found.dealId", ref_type='deal_id'),
        ('positions', 'get'): {'positions': []},
    })
    _seed_position(ctx)
    row = ctx.get_order('coid-entry')
    assert row is not None
    ctx.upsert_order('coid-entry', extras={**(row.extras or {}),
                                           'missing_pending_since': time.time()})

    verdict = asyncio.run(broker._confirm_missing_cancelled(ctx.get_order('coid-entry')))
    assert verdict.resolution is MissingResolution.INCONCLUSIVE
    assert broker._calls == []

    row = ctx.get_order('coid-entry')
    assert row is not None
    ctx.upsert_order('coid-entry', extras={**(row.extras or {}),
                                           'missing_pending_since': time.time() - 200.0})
    verdict = asyncio.run(broker._confirm_missing_cancelled(ctx.get_order('coid-entry')))
    assert verdict.resolution is MissingResolution.CLOSED
    assert latched == []
    assert len(_events(ctx, 'position_vanished_unattributed')) == 1
    store.close()


def _consuming_order(*, source='USER', direction='SELL', size=3.0, offset_s=-140.0):
    """Someone's opposite market order on the epic, as the venue logs it.

    A netting account nets it FIFO against the oldest deals of the other
    side; a deal it consumes whole is reported on this consuming deal only.
    """
    return {'dateUTC': _now_iso(offset_s), 'dealId': 'deal-X', 'type': 'POSITION',
            'status': 'ACCEPTED', 'source': source, 'epic': 'EURUSD',
            'details': {'direction': direction, 'size': size, 'level': 1.11}}


def _vanish_after_the_grace(broker, ctx):
    """The disappearance verdict on ``deal-L``, missed past the fill-report grace."""
    row = ctx.get_order('coid-entry')
    assert row is not None
    ctx.upsert_order('coid-entry', extras={**(row.extras or {}),
                                           'missing_pending_since': time.time() - 130.0})
    return asyncio.run(broker._confirm_missing_cancelled(ctx.get_order('coid-entry')))


def _vanishing_broker(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('error', 'positions/deal-L', 'get'): OrderNotFoundError(
            "API error occured: error.not-found.dealId", ref_type='deal_id'),
        ('positions', 'get'): {'positions': []},
    })
    _seed_position(ctx)
    broker._remember_positions_snapshot(_by_deal(_raw('deal-L', 'BUY', 2.0)), time.time() - 150.0)
    return broker, store, ctx, latched


def __test_foreign_order_that_netted_the_bot_deal_away_is_reported__(tmp_path):
    """Someone else adds to the bot's side and then sells more than that add:
    the venue nets the order FIFO, the bot's older deal goes first and is
    logged on the consuming deal only. The bot's deal just vanishes, and the
    consuming order proves whose close it was."""
    broker, store, ctx, latched = _vanishing_broker(tmp_path)
    assert asyncio.run(_drain(broker._process_activity([_consuming_order()]))) == []
    assert _events(ctx, 'external_activity_ignored')

    verdict = _vanish_after_the_grace(broker, ctx)

    assert verdict.resolution is MissingResolution.CLOSED
    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    assert 'closed on the venue by someone else' in latched[0][0]
    assert latched[0][1]['consumer']['dealId'] == 'deal-X'
    assert len(_events(ctx, 'position_netted_away_by_foreign_order')) == 1
    assert _events(ctx, 'position_vanished_unattributed') == []
    store.close()


@pytest.mark.parametrize('consumer', [
    {'source': 'DEALER'}, {'direction': 'BUY'}, {'size': 1.0}, {'offset_s': -600.0},
])
def __test_vanished_bot_deal_without_a_consuming_order_is_logged_not_reported__(
        tmp_path, consumer):
    """A dealing-desk order, a same-direction add, an order too small to take
    the bot's whole deal, or one long before the deal was last seen: none of
    them consumed it. Nothing shows who closed it: ERROR and audit only."""
    broker, store, ctx, latched = _vanishing_broker(tmp_path)
    assert asyncio.run(_drain(broker._process_activity([_consuming_order(**consumer)]))) == []

    verdict = _vanish_after_the_grace(broker, ctx)

    assert verdict.resolution is MissingResolution.CLOSED
    assert latched == []
    assert len(_events(ctx, 'position_vanished_unattributed')) == 1
    assert _events(ctx, 'position_netted_away_by_foreign_order') == []
    store.close()


def __test_consuming_order_an_own_entry_may_have_sent_is_logged_not_reported__(tmp_path):
    """An own opposite entry still waiting for its ``dealId`` may be the
    consuming order: the evidence is ambiguous, so ERROR and audit only."""
    broker, store, ctx, latched = _vanishing_broker(tmp_path)
    _seed_unconfirmed_entry(ctx, coid='coid-short', side='sell')
    assert asyncio.run(_drain(broker._process_activity([_consuming_order()]))) == []

    verdict = _vanish_after_the_grace(broker, ctx)

    assert verdict.resolution is MissingResolution.CLOSED
    assert latched == []
    ambiguous = _events(ctx, 'position_netted_away_ambiguous')
    assert len(ambiguous) == 1 and 'dealId' in ambiguous[0]['own_explanation']
    assert len(_events(ctx, 'position_vanished_unattributed')) == 1
    store.close()


def _park_partial_close(ctx, *, sent_at, matched):
    ctx.upsert_order('coid-entry', symbol='EURUSD', side='buy', qty=2.0, filled_qty=2.0,
                     state='confirmed', pine_entry_id='Long', exchange_order_id='orig',
                     extras={'kind': 'position', 'entry_filled_at': sent_at - 10.0,
                             'own_close_expected': [{
                                 'full': False, 'size': 1.0, 'sent_at': sent_at,
                                 'done_at': sent_at, 'matched': matched,
                             }]})
    ctx.upsert_order('cmd-partial', symbol='EURUSD', side='sell', qty=1.0,
                     state='disposition_unknown', pine_entry_id='Long',
                     extras={'kind': KIND_PARTIAL_CLOSE, 'target_deal_id': 'orig',
                             'target_direction': 'BUY', 'pre_target_units': 200,
                             'intent_units': 100, 'own_close_sent_at': sent_at,
                             'target_coid': 'coid-entry'})


def __test_parked_partial_close_settles_from_its_matched_own_activity__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _park_partial_close(ctx, sent_at=time.time() - 5.0, matched=[_now_iso(-4.0)])

    # The target diverged from the expected residual (someone else took more).
    asyncio.run(broker._resolve_partial_close_dispositions(
        {'orig': _raw('orig', 'BUY', 0.5)}))

    command = ctx.get_order('cmd-partial')
    assert command is not None and command.closed_ts_ms is not None
    assert len(_events(ctx, 'partial_close_own_activity_matched')) == 1
    # The own reduction leaves the run's exposure cursor; the extra 0.5 is
    # someone else's and leaves it through its own activity.
    target = ctx.get_order('coid-entry')
    assert target is not None and target.filled_qty == pytest.approx(1.0)
    retired = _events(ctx, 'partial_close_journal_retired')
    assert len(retired) == 1 and retired[0]['command'] == 'cmd-partial'
    store.close()


def __test_parked_partial_close_at_the_expected_residual_lowers_the_cursor_once__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _park_partial_close(ctx, sent_at=time.time() - 5.0, matched=[])

    for _ in range(2):
        asyncio.run(broker._resolve_partial_close_dispositions(
            {'orig': _raw('orig', 'BUY', 1.0)}))

    command = ctx.get_order('cmd-partial')
    assert command is not None and command.closed_ts_ms is not None
    target = ctx.get_order('coid-entry')
    assert target is not None and target.filled_qty == pytest.approx(1.0)
    assert len(_events(ctx, 'partial_close_journal_retired')) == 1
    store.close()


def __test_parked_partial_close_without_own_activity_is_rejected_after_grace__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _park_partial_close(ctx, sent_at=time.time() - 200.0, matched=[])

    asyncio.run(broker._resolve_partial_close_dispositions(
        {'orig': _raw('orig', 'BUY', 2.0)}))

    command = ctx.get_order('cmd-partial')
    assert command is not None and command.state == 'rejected'
    assert len(_events(ctx, 'partial_close_no_own_reduction')) == 1
    assert latched == []
    store.close()


def __test_parked_partial_close_with_a_target_reduced_by_someone_else_settles__(
        tmp_path, caplog):
    """No own activity within the grace, while the target shrank by an amount
    the POST does not explain: the POST reduced nothing. The command settles,
    so the target is no longer busy and its protection is judged again."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _park_partial_close(ctx, sent_at=time.time() - 200.0, matched=[])
    unprotected = _by_deal(_raw('orig', 'BUY', 1.5))

    with caplog.at_level(logging.ERROR, logger='pynecore_capitalcom.reconcile'):
        _poll(broker, unprotected)

    command = ctx.get_order('cmd-partial')
    assert command is not None and command.state == 'rejected'
    settled = _events(ctx, 'partial_close_no_own_reduction')
    assert len(settled) == 1
    assert settled[0]['target_reduced'] is True and settled[0]['target_units'] == 150
    assert any('no activity of its own' in r.getMessage() for r in caplog.records)
    target = ctx.get_order('coid-entry')
    assert target is not None and target.filled_qty == pytest.approx(2.0), (
        "someone else's reduction leaves the cursor through its own activity")
    assert latched == []

    _poll(broker, unprotected)
    _someone_else_edits(broker, 'orig')
    stopped = _by_deal(_raw('orig', 'BUY', 1.5, stopLevel=1.05))
    _poll(broker, stopped)
    _poll(broker, stopped)
    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    store.close()


def __test_close_only_a_resting_order_could_explain_is_reported_once_the_order_still_rests__(
        tmp_path):
    """A working-order read well after the close shows the own opposite order
    still resting at its full size: it netted nothing, the close is someone
    else's — the run's exposure cursor drops by it and it is reported."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _seed_working(ctx, coid='coid-short-wo', deal_id='wo-S', side='sell', qty=1.0,
                  level=1.30)
    broker._note_venue_clock([{'dateUTC': _now_iso()}], time.time())
    events = asyncio.run(_drain(broker._process_activity(
        [_close_activity(size=1.0, date_utc=_now_iso(-60.0))],
        _by_deal(_raw('deal-L', 'BUY', 1.0)),
    )))
    assert [(e.leg_type, e.fill_qty) for e in events] == [(LegType.CLOSE, 1.0)]
    assert latched == []
    assert 'user_close_netting_pending' in _event_kinds(ctx)

    resting = {'wo-S': {'marketData': {'epic': 'EURUSD'},
                        'workingOrderData': {'dealId': 'wo-S', 'epic': 'EURUSD',
                                             'direction': 'SELL', 'orderSize': 1.0,
                                             'orderLevel': 1.30, 'orderType': 'LIMIT'}}}
    for _ in range(2):
        asyncio.run(_drain(broker._reconcile_snapshot(
            _by_deal(_raw('deal-L', 'BUY', 1.0)), resting, read_started_at=time.time(),
        )))

    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    entry = ctx.get_order('coid-entry')
    assert entry is not None and entry.filled_qty == pytest.approx(1.0)
    resolved = _events(ctx, 'user_close_netting_resolved')
    assert [r['verdict'] for r in resolved] == ['foreign']
    assert 'user_close_ambiguous_netting' not in _event_kinds(ctx)
    store.close()


def _cancel_resting_short(broker) -> None:
    asyncio.run(broker.execute_cancel(DispatchEnvelope(
        intent=CancelIntent(pine_id='Long', symbol='EURUSD'),
        run_tag='test', bar_ts_ms=1700000060000,
    )))


def _close_judged_against_a_resting_short(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    _seed_working(ctx, coid='coid-short-wo', deal_id='wo-S', side='sell', qty=1.0,
                  level=1.30)
    broker._note_venue_clock([{'dateUTC': _now_iso()}], time.time())
    asyncio.run(_drain(broker._process_activity(
        [_close_activity(size=1.0, date_utc=_now_iso())],
        _by_deal(_raw('deal-L', 'BUY', 1.0)),
    )))
    assert 'user_close_netting_pending' in _event_kinds(ctx)
    assert latched == []
    return broker, store, ctx, latched


def __test_close_a_resting_order_could_explain_is_reported_once_its_own_cancel_lands__(
        tmp_path):
    """The bot cancels the candidate order after the close: the venue taking
    the DELETE proves the order still rested, so it netted nothing and the
    close is someone else's — without waiting for a later order read."""
    broker, store, ctx, latched = _close_judged_against_a_resting_short(tmp_path)
    broker._responses[('workingorders/wo-S', 'delete')] = {'dealReference': 'del-S'}
    _cancel_resting_short(broker)
    short = ctx.get_order('coid-short-wo')
    assert short is not None and short.closed_ts_ms is not None

    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 1.0)))

    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    assert [r['verdict'] for r in _events(ctx, 'user_close_netting_resolved')] == ['foreign']
    entry = ctx.get_order('coid-entry')
    assert entry is not None and entry.filled_qty == pytest.approx(1.0)
    store.close()


def __test_close_a_resting_order_could_explain_stays_ambiguous_when_the_order_was_gone__(
        tmp_path):
    """The DELETE found nothing to cancel: the order may have executed at the
    close, so the close stays ambiguous (ERROR, never reported)."""
    broker, store, ctx, latched = _close_judged_against_a_resting_short(tmp_path)
    broker._responses[('error', 'workingorders/wo-S', 'delete')] = OrderNotFoundError(
        "API error occured: error.not-found.dealId", ref_type='deal_id')
    _cancel_resting_short(broker)

    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 1.0)))
    assert latched == []
    assert _events(ctx, 'user_close_netting_resolved') == []
    broker._netting_pending = {
        key: (since - 400.0, pending) for key, (since, pending) in broker._netting_pending.items()}
    _poll(broker, _by_deal(_raw('deal-L', 'BUY', 1.0)))

    assert latched == []
    assert [r['verdict'] for r in _events(ctx, 'user_close_netting_resolved')] == [
        'ambiguous_netting']
    store.close()


# === Closes while the bot was down ===========================================

def _restart_with_history(ctx, *, positions, activities):
    broker, latched = _make_plugin(ctx, responses={
        ('positions', 'get'): {'positions': positions},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': activities},
    })
    asyncio.run(broker._recover_in_flight_submissions())
    return broker, latched


def __test_foreign_order_that_netted_the_bot_deal_away_while_down_is_reported_once__(tmp_path):
    """The netting consumption happened while the bot was down: the startup
    reconciliation finds the bot's deal gone and the consuming order in the
    history, and reports the same foreign close once."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    consumer = _consuming_order(offset_s=-10.0)

    restarted, restarted_latched = _restart_with_history(ctx, positions=[], activities=[consumer])

    assert len(restarted_latched) == 1
    assert restarted_latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    assert 'closed on the venue by someone else' in restarted_latched[0][0]
    row = ctx.get_order('coid-entry')
    assert row is not None and row.closed_ts_ms is not None
    netted = _events(ctx, 'position_netted_away_by_foreign_order')
    assert len(netted) == 1 and netted[0]['at_startup'] is True
    assert _events(ctx, 'position_vanished_unattributed') == []

    again, again_latched = _restart_with_history(ctx, positions=[], activities=[consumer])
    assert again_latched == [], "reported once, not on every restart"
    store.close()


def __test_manual_close_while_the_bot_was_down_is_reported_once_and_not_booked__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    close = _close_activity(date_utc=_now_iso(-30.0))

    restarted, restarted_latched = _restart_with_history(ctx, positions=[], activities=[close])

    assert len(restarted_latched) == 1
    assert restarted_latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    row = ctx.get_order('coid-entry')
    assert row is not None and row.closed_ts_ms is not None
    accounted = _events(ctx, 'startup_close_accounted')
    assert len(accounted) == 1 and accounted[0]['verdict'] == 'foreign'
    assert _events(ctx, 'position_vanished_unattributed') == []
    # The engine adopts the venue's flat book: the close is never a fill.
    assert asyncio.run(_drain(restarted._process_activity([close]))) == []

    again, again_latched = _restart_with_history(ctx, positions=[], activities=[close])
    assert again_latched == [], "reported once, not on every restart"
    store.close()


@pytest.mark.parametrize('venue_size', [None, 1.0])
def __test_legacy_adopted_deal_closed_while_the_bot_was_down_is_not_reported__(
        tmp_path, venue_size):
    """A row an earlier version seeded for an untracked deal is not the run's
    exposure: its owner closing or reducing that deal while the bot was down
    is never judged or reported; startup only retires the row."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    coid = '__pyne_adopted__EURUSD__deal-L'
    ctx.upsert_order(coid, symbol='EURUSD', side='buy', qty=2.0, filled_qty=2.0,
                     state='confirmed', exchange_order_id='deal-L',
                     extras={'kind': 'position', 'entry_filled_at': time.time(),
                             'adopted_startup': True})
    ctx.add_ref(coid, 'deal_id', 'deal-L')
    size = 2.0 if venue_size is None else 2.0 - venue_size
    close = _close_activity(size=size, date_utc=_now_iso(-30.0))
    positions = [] if venue_size is None else [_raw('deal-L', 'BUY', venue_size)]

    restarted, restarted_latched = _start_process(ctx, positions=positions, activities=[close])

    assert restarted_latched == []
    assert _events(ctx, 'manual_close_report_pending') == []
    assert _events(ctx, 'startup_close_accounted') == []
    assert len(_events(ctx, 'startup_adopted_row_retired')) == 1
    assert list(ctx.iter_live_orders()) == []
    assert not any(call[1] in {'delete', 'post', 'put'} for call in restarted._calls)
    store.close()


def __test_take_profit_while_the_bot_was_down_is_not_reported__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    take_profit = _close_activity(source='TP', date_utc=_now_iso(-30.0))

    restarted, restarted_latched = _restart_with_history(
        ctx, positions=[], activities=[take_profit])

    assert restarted_latched == []
    row = ctx.get_order('coid-entry')
    assert row is not None and row.closed_ts_ms is not None
    accounted = _events(ctx, 'startup_close_accounted')
    assert len(accounted) == 1 and accounted[0]['verdict'] == 'venue'
    assert _events(ctx, 'position_vanished_unattributed') == []
    assert asyncio.run(_drain(restarted._process_activity([take_profit]))) == []
    store.close()


def __test_partial_manual_close_while_the_bot_was_down_lowers_the_cursor__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    close = _close_activity(size=0.5, date_utc=_now_iso(-30.0))

    restarted, restarted_latched = _restart_with_history(
        ctx, positions=[_raw('deal-L', 'BUY', 1.5)], activities=[close])

    assert len(restarted_latched) == 1
    row = ctx.get_order('coid-entry')
    assert row is not None and row.closed_ts_ms is None
    assert row.filled_qty == pytest.approx(1.5)
    assert asyncio.run(_drain(restarted._process_activity(
        [close], _by_deal(_raw('deal-L', 'BUY', 1.5))))) == []
    store.close()


def __test_close_the_startup_snapshot_does_not_hold_is_left_to_the_poll__(tmp_path):
    """The snapshot still shows the full own size: the close came after the
    read and is booked by the poll like any other."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    close = _close_activity(size=0.5, date_utc=_now_iso(-1.0))

    restarted, restarted_latched = _restart_with_history(
        ctx, positions=[_raw('deal-L', 'BUY', 2.0)], activities=[close])

    assert restarted_latched == []
    assert _events(ctx, 'startup_close_accounted') == []
    events = asyncio.run(_drain(restarted._process_activity(
        [close], _by_deal(_raw('deal-L', 'BUY', 1.5)))))
    assert [(e.leg_type, e.fill_qty) for e in events] == [(LegType.CLOSE, 0.5)]
    assert len(restarted_latched) == 1
    store.close()


@pytest.mark.parametrize('resume_offset_s, reported', [(0.0, False), (-60.0, True)])
def __test_manual_close_while_down_is_judged_against_the_operator_resume__(
        tmp_path, resume_offset_s, reported):
    """The operator closed part of the bot position while the bot was down,
    then resumed it: the resume accepted the venue as it stood, so the close
    is booked but not reported. A close dated after the resume is judged."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    close = _close_activity(size=0.5, date_utc=_now_iso(-30.0))
    restarted, restarted_latched = _make_plugin(ctx, responses={
        ('positions', 'get'): {'positions': [_raw('deal-L', 'BUY', 1.5)]},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': [close]},
    })
    restarted.operator_resume_ts_ms = int((time.time() + resume_offset_s) * 1000)

    asyncio.run(restarted._recover_in_flight_submissions())

    row = ctx.get_order('coid-entry')
    assert row is not None and row.filled_qty == pytest.approx(1.5), "the venue truth is booked"
    accepted = _events(ctx, 'external_intervention_accepted_by_resume')
    if reported:
        assert len(restarted_latched) == 1
        assert restarted_latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
        assert accepted == []
    else:
        assert restarted_latched == []
        assert len(accepted) == 1
        assert accepted[0]['changed_at_ms'] <= restarted.operator_resume_ts_ms
    assert len(_events(ctx, 'manual_close_report_sent')) == 1
    store.close()


def _start_process(ctx, *, positions, activities):
    """A restarted plugin process through its startup passes, as ``connect`` runs them."""
    broker, latched = _make_plugin(ctx, responses={
        ('positions', 'get'): {'positions': positions},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': activities},
    })
    asyncio.run(broker._load_activity_cursor_from_events())
    broker._send_pending_manual_close_reports()
    broker._reload_netting_pending()
    asyncio.run(broker._recover_in_flight_submissions())
    return broker, latched


def __test_priceless_close_reported_before_a_restart_is_not_reported_again__(tmp_path):
    """A close without a price is reported at once but booked only once a
    priced copy comes; the startup pass of a process restarted in between
    judges it again and must not report it a second time."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    close = _close_activity(level=None, date_utc=_now_iso(-20.0))
    asyncio.run(_drain(broker._process_activity([close], {})))
    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION

    restarted, restarted_latched = _start_process(ctx, positions=[], activities=[close])

    assert restarted_latched == [], "the close was reported already"
    assert len(_events(ctx, 'manual_close_report_sent')) == 1
    store.close()


def __test_re_issued_copy_of_a_booked_close_is_not_booked_again_at_startup__(tmp_path):
    """The venue re-issues a booked partial close under a new fingerprint;
    the startup pass finds the copy before the snapshot read and must neither
    lower the exposure cursor again nor report the close again."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx)
    date_utc = _now_iso(-30.0)
    priced = _close_activity(size=0.5, level=1.12, date_utc=date_utc)
    asyncio.run(_drain(broker._process_activity(
        [priced], _by_deal(_raw('deal-L', 'BUY', 1.5)))))
    row = ctx.get_order('coid-entry')
    assert row is not None and row.filled_qty == pytest.approx(1.5)
    assert len(latched) == 1
    republished = {**_close_activity(size=0.5, level=1.121, date_utc=date_utc), 'level': 1.121}
    assert _activity_fingerprint(republished) != _activity_fingerprint(priced)
    # A fresh activity elsewhere on the account samples the venue clock, so
    # the copy is provably older than the startup snapshot.
    clock = {'dateUTC': _now_iso(-1.0), 'dealId': 'deal-Z', 'type': 'POSITION',
             'status': 'EXECUTED', 'source': 'USER', 'epic': 'GBPUSD',
             'details': {'direction': 'BUY', 'size': 1.0, 'level': 1.3}}

    restarted, restarted_latched = _start_process(
        ctx, positions=[_raw('deal-L', 'BUY', 1.5)],
        activities=[priced, republished, clock])

    row = ctx.get_order('coid-entry')
    assert row is not None and row.filled_qty == pytest.approx(1.5)
    assert restarted_latched == []
    assert _events(ctx, 'startup_close_accounted') == []
    assert len(_events(ctx, 'manual_close_report_sent')) == 1
    assert any(e.get('already_booked') for e in _events(ctx, 'activity_processed'))
    store.close()


def __test_own_partial_close_the_poll_never_saw_is_not_booked_again_after_a_restart__(
        tmp_path):
    """The own partial close lowered the run's exposure cursor when it
    returned; the process stopped before its poll read the close. The engine
    adopts the cursor-clamped venue size after the restart, so the close
    must not reach it again as a fill."""
    own = _raw('deal-L', 'BUY', 2.0)
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): [
            {'positions': [own]}, {'positions': [own]},
            {'positions': [_raw('deal-L', 'BUY', 1.0)]},
        ],
        ('positions', 'post'): {'dealReference': 'dr-1'},
        ('confirms/dr-1', 'get'): {'dealStatus': 'ACCEPTED'},
    })
    _seed_position(ctx)
    broker._remember_positions_snapshot(_by_deal(own), time.time())
    asyncio.run(broker.execute_close(_partial_close(1700000000000)))
    row = ctx.get_order('coid-entry')
    assert row is not None and row.filled_qty == pytest.approx(1.0)
    close = _close_activity(size=1.0, date_utc=_now_iso(-2.0))

    restarted, restarted_latched = _start_process(
        ctx, positions=[_raw('deal-L', 'BUY', 1.0)], activities=[close])

    assert restarted_latched == []
    accounted = _events(ctx, 'startup_close_accounted')
    assert len(accounted) == 1 and accounted[0]['verdict'] == 'own'
    row = ctx.get_order('coid-entry')
    assert row is not None and row.filled_qty == pytest.approx(1.0), "lowered once"
    events = asyncio.run(_drain(restarted._process_activity(
        [close], _by_deal(_raw('deal-L', 'BUY', 1.0)))))
    assert events == [], "the adopted size already holds the close"
    store.close()


def __test_parked_own_partial_close_settled_at_startup_is_not_booked_again__(tmp_path):
    """The reduce POST's answer never came and the process stopped; startup
    recovery proves the close from the target's size and lowers the exposure
    cursor. The close's activity must not reach the engine as a fill on top."""
    own = _raw('deal-L', 'BUY', 2.0)
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('positions', 'get'): [{'positions': [own]}, {'positions': [own]}],
        ('error', 'positions', 'post'): httpx.ReadTimeout('timed out'),
    })
    _seed_position(ctx)
    broker._remember_positions_snapshot(_by_deal(own), time.time())
    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_close(_partial_close(1700000000000)))
    row = ctx.get_order('coid-entry')
    assert row is not None and row.filled_qty == pytest.approx(2.0)
    close = _close_activity(size=1.0, date_utc=_now_iso(-2.0))

    restarted, restarted_latched = _start_process(
        ctx, positions=[_raw('deal-L', 'BUY', 1.0)], activities=[close])

    assert restarted_latched == []
    row = ctx.get_order('coid-entry')
    assert row is not None and row.filled_qty == pytest.approx(1.0), "lowered once"
    accounted = _events(ctx, 'startup_close_accounted')
    assert len(accounted) == 1 and accounted[0]['verdict'] == 'own'
    assert asyncio.run(_drain(restarted._process_activity(
        [close], _by_deal(_raw('deal-L', 'BUY', 1.0))))) == []
    store.close()


# === Resting orders cancelled while the bot was down =========================

def _working_order_cancel(*, source='USER', status='CANCELLED', date_utc=None):
    return {
        'dateUTC': date_utc or _now_iso(-30.0), 'dealId': 'wo-1',
        'type': 'WORKING_ORDER', 'status': status, 'source': source,
        'epic': 'EURUSD',
        'details': {'direction': 'BUY', 'size': 1.0, 'level': 1.20},
    }


@pytest.mark.parametrize('source, status, reported', [
    ('USER', 'CANCELLED', True),
    ('USER', 'DELETED', True),
    ('SYSTEM', 'CANCELLED', False),
    ('DEALER', 'CANCELLED', False),
    ('SYSTEM', 'EXPIRED', False),
])
def __test_resting_order_cancelled_while_the_bot_was_down_is_judged_by_its_source__(
        tmp_path, source, status, reported):
    """Someone else cancelling the bot's resting entry while no process ran
    is an intervention, as it is while the bot runs; the venue's own cancel
    (or expiry) and the dealing desk's are not."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_working(ctx)
    cancel = _working_order_cancel(source=source, status=status)

    restarted, restarted_latched = _start_process(ctx, positions=[], activities=[cancel])

    row = ctx.get_order('coid-wo')
    assert row is not None and row.closed_ts_ms is not None
    if reported:
        assert len(restarted_latched) == 1
        assert restarted_latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
        assert 'wo-1' in restarted_latched[0][0]
    else:
        assert restarted_latched == []
    assert asyncio.run(_drain(restarted._process_activity([cancel]))) == []
    assert len(restarted_latched) == (1 if reported else 0), "judged once"
    store.close()


def __test_resting_order_cancel_reported_at_startup_survives_a_crash__(tmp_path):
    """The report is persisted with the retirement: a process that dies in
    between sends it on the next start."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_working(ctx)
    cancel = _working_order_cancel()
    crashed, crashed_latched = _make_plugin(ctx, responses={
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': [cancel]},
    })

    def crash(*_args, **_kwargs):
        raise RuntimeError('process died')

    crashed.report_external_intervention = crash  # type: ignore[method-assign]
    with pytest.raises(RuntimeError):
        asyncio.run(crashed._recover_in_flight_submissions())

    restarted, restarted_latched = _start_process(ctx, positions=[], activities=[cancel])

    assert len(restarted_latched) == 1
    assert restarted_latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    store.close()


@pytest.mark.parametrize('path', ['journal', 'with_outcome'])
def __test_own_cancel_whose_answer_was_lost_is_not_reported_after_a_restart__(tmp_path, path):
    """The bot's own DELETE timed out and the process stopped: the venue's
    ``USER`` cancel is the bot's own, whichever cancel path sent it."""
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('error', 'workingorders/wo-1', 'delete'): httpx.ReadTimeout('timed out'),
    })
    _seed_working(ctx)
    envelope = DispatchEnvelope(
        intent=CancelIntent(pine_id='Long', symbol='EURUSD'),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    with pytest.raises(OrderDispositionUnknownError):
        if path == 'journal':
            asyncio.run(broker.execute_cancel(envelope))
        else:
            asyncio.run(broker.execute_cancel_with_outcome(envelope))
    cancel = _working_order_cancel(date_utc=_now_iso(-1.0))

    restarted, restarted_latched = _start_process(ctx, positions=[], activities=[cancel])

    assert restarted_latched == []
    row = ctx.get_order('coid-wo')
    assert row is not None and row.closed_ts_ms is not None
    store.close()


def __test_refused_own_cancel_does_not_explain_a_later_cancel__(tmp_path):
    """A DELETE the venue refused left the order resting: a cancel after it is
    someone else's."""
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('error', 'workingorders/wo-1', 'delete'): ExchangeOrderRejectedError('refused'),
    })
    _seed_working(ctx)
    envelope = DispatchEnvelope(
        intent=CancelIntent(pine_id='Long', symbol='EURUSD'),
        run_tag='test', bar_ts_ms=1700000000000,
    )
    with pytest.raises(ExchangeOrderRejectedError):
        asyncio.run(broker.execute_cancel_with_outcome(envelope))
    cancel = _working_order_cancel(date_utc=_now_iso(-1.0))

    restarted, restarted_latched = _start_process(ctx, positions=[], activities=[cancel])

    assert len(restarted_latched) == 1
    store.close()


def _missed_since(ctx, coid: str, since: float) -> None:
    """The disappearance tracker first missed the row's deal at ``since``."""
    row = ctx.get_order(coid)
    assert row is not None
    ctx.upsert_order(coid, extras={**(row.extras or {}), 'missing_pending_since': since})


def _tracker_pass(broker):
    return asyncio.run(_drain(broker._missing_pending_tracker({}, {})))


@pytest.mark.parametrize('source, status', [
    ('SYSTEM', 'CANCELLED'), ('DEALER', 'CANCELLED'), ('SYSTEM', 'EXPIRED'), ('USER', 'EXPIRED'),
])
def __test_venue_end_of_a_resting_order_while_the_bot_runs_is_no_intervention__(
        tmp_path, source, status):
    """The venue (or its dealing desk) ending the bot's resting entry is its
    own mechanics: the engine never gets it as someone else's cancel, and the
    disappearance verdict retires the order without the policy."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_working(ctx)
    cancel = _working_order_cancel(source=source, status=status, date_utc=_now_iso(-1.0))

    assert asyncio.run(_drain(broker._process_activity([cancel], {}))) == []
    assert len(_events(ctx, 'working_order_venue_cancel')) == 1
    _missed_since(ctx, 'coid-wo', time.time() - 30.0)
    events = _tracker_pass(broker)

    assert [e.event_type for e in events] == ['cancelled']
    assert events[0].cancel_reason in VENUE_DRIVEN_CANCEL_REASONS
    assert latched == []
    row = ctx.get_order('coid-wo')
    assert row is not None and row.closed_ts_ms is not None
    store.close()


def __test_venue_rejected_resting_order_is_retired_as_the_venues__(tmp_path):
    """A resting order the venue rejected (a triggered order its matching
    engine refused) reaches the engine as a rejection, and the vanished row
    is retired as the venue's, never as an unexpected cancel."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_working(ctx)
    rejected = _working_order_cancel(source='SYSTEM', status='REJECTED',
                                     date_utc=_now_iso(-1.0))

    events = asyncio.run(_drain(broker._process_activity([rejected], {})))
    assert [e.event_type for e in events] == ['rejected']
    _missed_since(ctx, 'coid-wo', time.time() - 30.0)
    tracked = _tracker_pass(broker)

    assert [e.cancel_reason in VENUE_DRIVEN_CANCEL_REASONS for e in tracked] == [True]
    assert latched == []
    store.close()


def __test_someone_elses_cancel_of_a_resting_order_while_the_bot_runs_reaches_the_policy__(
        tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_working(ctx)
    cancel = _working_order_cancel(date_utc=_now_iso(-1.0))

    events = asyncio.run(_drain(broker._process_activity([cancel], {})))
    assert [e.event_type for e in events] == ['cancelled']
    assert events[0].cancel_reason is None
    _missed_since(ctx, 'coid-wo', time.time() - 30.0)
    _tracker_pass(broker)
    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    store.close()


def __test_vanished_resting_order_waits_for_its_late_activity__(tmp_path):
    """The venue publishes an activity up to a minute late: the verdict on a
    vanished resting order waits for it before taking the order as someone
    else's cancel."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_working(ctx)
    _missed_since(ctx, 'coid-wo', time.time() - 30.0)

    assert _tracker_pass(broker) == []
    assert latched == []
    cancel = _working_order_cancel(source='SYSTEM', date_utc=_now_iso(-31.0))
    assert asyncio.run(_drain(broker._process_activity([cancel], {}))) == []
    events = _tracker_pass(broker)
    assert [e.cancel_reason in VENUE_DRIVEN_CANCEL_REASONS for e in events] == [True]
    assert latched == []
    store.close()


def __test_vanished_resting_order_without_any_activity_is_a_cancel_after_the_lag__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_working(ctx)
    _missed_since(ctx, 'coid-wo', time.time() - 61.0)

    _tracker_pass(broker)

    assert len(latched) == 1 and latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    store.close()


def __test_resting_order_gone_without_any_activity_is_logged_not_reported__(
        tmp_path, caplog):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_working(ctx)

    with caplog.at_level(logging.ERROR, logger='pynecore_capitalcom.recovery'):
        restarted, restarted_latched = _start_process(ctx, positions=[], activities=[])

    assert restarted_latched == [], "nothing shows who cancelled it"
    vanished = _events(ctx, 'working_order_vanished_unattributed')
    assert len(vanished) == 1 and vanished[0]['exchange_order_id'] == 'wo-1'
    assert any('wo-1' in r.getMessage() for r in caplog.records)
    row = ctx.get_order('coid-wo')
    assert row is not None and row.closed_ts_ms is not None
    store.close()


# === Deals re-keyed while the bot was down ===================================

def __test_resting_entry_filled_while_the_bot_was_down_stays_owned__(tmp_path):
    """The venue opened the order's position under a fresh ``dealId`` that
    back-links the order: the row moves onto it and the run owns its fill."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_working(ctx)
    deal = _raw('deal-P', 'BUY', 1.0, created=_now_iso(-120.0), workingOrderId='wo-1')

    restarted, restarted_latched = _restart_with_history(
        ctx, positions=[deal], activities=[])

    row = ctx.get_order('coid-wo')
    assert row is not None and row.closed_ts_ms is None
    assert row.exchange_order_id == 'deal-P'
    assert (row.extras or {}).get('kind') == 'position'
    assert row.filled_qty == 1.0, "the run owns the fill the engine adopts"
    assert len(_events(ctx, 'startup_rekeyed_row_migrated')) == 1
    assert _events(ctx, 'foreign_deal_on_epic_at_startup') == []
    assert _events(ctx, 'startup_orphan_retired') == []
    restarted._responses[('positions', 'get')] = {'positions': [deal]}
    position = asyncio.run(restarted.get_position('EURUSD'))
    assert position is not None and position.side == 'long' and position.size == 1.0
    # The execution the poll reads later is the fill the engine adopted.
    executed = {
        'dateUTC': _now_iso(-30.0), 'dealId': 'wo-1', 'type': 'WORKING_ORDER',
        'status': 'EXECUTED', 'source': 'USER', 'epic': 'EURUSD',
        'details': {'direction': 'BUY', 'size': 1.0, 'level': 1.20},
    }
    assert asyncio.run(_drain(restarted._process_activity([executed], _by_deal(deal)))) == []
    assert restarted_latched == []
    store.close()


def __test_market_entry_re_keyed_while_the_bot_was_down_stays_owned__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_position(ctx, deal_id='deal-confirm')
    deal = _raw('deal-X', 'BUY', 2.0, created=_now_iso(-120.0),
                workingOrderId='deal-confirm')

    restarted, restarted_latched = _restart_with_history(
        ctx, positions=[deal], activities=[])

    row = ctx.get_order('coid-entry')
    assert row is not None and row.closed_ts_ms is None
    assert row.exchange_order_id == 'deal-X' and row.filled_qty == 2.0
    assert _events(ctx, 'position_vanished_unattributed') == []
    assert _events(ctx, 'foreign_deal_on_epic_at_startup') == []
    restarted._responses[('positions', 'get')] = {'positions': [deal]}
    position = asyncio.run(restarted.get_position('EURUSD'))
    assert position is not None and position.size == 2.0
    assert restarted_latched == []
    store.close()


def __test_re_keyed_deal_closed_in_part_by_someone_else_while_down_is_reported__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_working(ctx, qty=2.0)
    deal = _raw('deal-P', 'BUY', 1.5, created=_now_iso(-120.0), workingOrderId='wo-1')
    close = dict(_close_activity(size=0.5, date_utc=_now_iso(-60.0)), dealId='deal-P')

    restarted, restarted_latched = _restart_with_history(
        ctx, positions=[deal], activities=[close])

    assert len(restarted_latched) == 1
    assert restarted_latched[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    row = ctx.get_order('coid-wo')
    assert row is not None and row.filled_qty == pytest.approx(1.5)
    assert asyncio.run(_drain(restarted._process_activity([close], _by_deal(deal)))) == []
    store.close()


def __test_opposite_deal_naming_the_own_order_is_never_taken_over__(tmp_path):
    """A netting execution names the order on the opposite deal it reduced:
    that deal is not the order's."""
    broker, store, ctx, latched = _make_broker(tmp_path)
    _seed_working(ctx)
    netted = _raw('deal-F', 'SELL', 1.0, created='2026-10-15T09:00:00.000',
                  workingOrderId='wo-1')

    _restart_with_history(ctx, positions=[netted], activities=[])

    row = ctx.get_order('coid-wo')
    assert row is not None and row.closed_ts_ms is not None
    assert _events(ctx, 'startup_rekeyed_row_migrated') == []
    assert [e['exchange_order_id'] for e in _events(ctx, 'foreign_deal_on_epic_at_startup')] == [
        'deal-F']
    store.close()


# === Account mode ============================================================

def __test_account_mode_is_logged_once_per_process__(tmp_path):
    broker, store, ctx, latched = _make_broker(tmp_path, responses={
        ('accounts/preferences', 'get'): {'hedgingMode': False},
    })

    asyncio.run(broker._detect_account_mode())
    broker._connection_generation = 1
    asyncio.run(broker._detect_account_mode())

    detected = _events(ctx, 'account_mode_detected')
    assert len(detected) == 1
    assert detected[0]['mode'] == 'netting' and detected[0]['hedging'] is False
    store.close()

