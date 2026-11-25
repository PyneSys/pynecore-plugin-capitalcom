"""
@pyne
"""
import asyncio
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone
from time import time as epoch_time

import httpx
import pytest

from pynecore.core.broker.exceptions import (
    ExchangeOrderRejectedError,
    InsufficientMarginError,
    OrderDispositionUnknownError,
)
from pynecore.core.broker.idempotency import KIND_ENTRY
from pynecore.core.broker.models import (
    DispatchEnvelope, EntryIntent, OrderStatus, OrderType,
)
from pynecore.core.broker.position import BrokerPosition
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore
from pynecore.core.broker.sync_engine import OrderSyncEngine

from pynecore_capitalcom import CapitalCom, CapitalComConfig
from pynecore_capitalcom.exceptions import OrderNotFoundError
from pynecore_capitalcom.models import _InstrumentRules


# === Test fixtures ========================================================

class _FakeBroker(CapitalCom):
    """Same skeleton as ``test_capitalcom_broker._FakeBroker``.

    Returns canned responses from a dict keyed by ``(endpoint, method)``;
    optionally raises a pre-seeded exception keyed by
    ``('error', endpoint, method)``.
    """

    def __init__(self, *, symbol=None, timeframe=None, ohlcv_dir=None,
                 config=None, responses=None):
        super().__init__(symbol=symbol, timeframe=timeframe,
                         ohlcv_dir=ohlcv_dir, config=config)
        self._responses: dict = responses or {}
        self._calls: list = []
        # A netting-account market entry is sent under the account write
        # lock, which needs the resolved account identity.
        self._account_id = 'capitalcom-demo-contract-account'

    def _call_serialized_write(self, endpoint, *, data=None, method='post'):
        self._calls.append((endpoint, method, data))
        err = self._responses.get(('error', endpoint, method))
        if err is not None:
            raise err
        return self._responses.get((endpoint, method), {})

    async def _call(self, endpoint, *, data=None, method='post'):
        return self._call_serialized_write(endpoint, data=data, method=method)


def _make_config(**overrides) -> CapitalComConfig:
    defaults = dict(
        demo=True,
        user_email="contract@example.com",
        api_key="contract-key",
        api_password="contract-password",
    )
    defaults.update(overrides)
    return CapitalComConfig(**defaults)


def _seed_rules(broker: CapitalCom, *, epic: str = "EURUSD") -> None:
    """Pre-populate the rules cache so _get_instrument_rules is a no-op."""
    broker._instrument_rules_cache[epic] = _InstrumentRules(  # type: ignore[attr-defined]
        epic=epic,
        lot_step=0.01,
        min_size=0.01,
        min_stop_or_limit_distance=0.0,
        fetched_at=epoch_time(),
    )


def _open_store_ctx(tmp_path, broker: CapitalCom):
    store = BrokerStore(
        tmp_path / "broker.sqlite",
        plugin_name=broker.plugin_name,
    )
    identity = RunIdentity(
        strategy_id="contract", symbol="EURUSD", timeframe="60",
        account_id="contract-account",
    )
    ctx = store.open_run(identity, script_source="// contract")
    broker.store_ctx = ctx
    return store, ctx


def _make_envelope(intent: EntryIntent, run_tag: str) -> DispatchEnvelope:
    return DispatchEnvelope(
        intent=intent, run_tag=run_tag,
        bar_ts_ms=1_700_000_000_000, retry_seq=0,
    )


def _make_intent(
        *,
        pine_id: str = "Long",
        side: str = "buy",
        qty: float = 1.0,
        order_type: OrderType = OrderType.MARKET,
        limit: float | None = None,
        stop: float | None = None,
) -> EntryIntent:
    return EntryIntent(
        pine_id=pine_id, symbol="EURUSD", side=side, qty=qty,
        order_type=order_type, limit=limit, stop=stop,
    )


def _normalise_row(row) -> dict:
    """OrderRow -> dict; strip per-run timestamps and identity fields.

    ``client_order_id`` is deterministic from the COID formula given
    the same run_tag, so we strip it from the comparison and assert it
    separately as a non-empty string.
    """
    d = asdict(row)
    for key in ('created_ts_ms', 'updated_ts_ms', 'plugin_name',
                'client_order_id'):
        d.pop(key, None)
    return d


def _live_rows(ctx) -> list[dict]:
    rows = sorted(
        ctx.iter_live_orders(),
        key=lambda r: r.client_order_id,
    )
    return [_normalise_row(r) for r in rows]


def _closed_rows(ctx) -> list[dict]:
    rows = ctx._store._conn.execute(  # type: ignore[attr-defined]
        "SELECT client_order_id FROM orders "
        "WHERE run_instance_id = ? AND closed_ts_ms IS NOT NULL "
        "ORDER BY client_order_id",
        (ctx.run_instance_id,),
    ).fetchall()
    closed = [ctx.get_order(r['client_order_id']) for r in rows]
    return [_normalise_row(r) for r in closed if r is not None]


def _refs(ctx) -> list[tuple[str, str]]:
    rows = ctx._store._conn.execute(  # type: ignore[attr-defined]
        "SELECT ref_type, ref_value FROM order_refs "
        "WHERE run_instance_id = ? "
        "ORDER BY ref_type, ref_value",
        (ctx.run_instance_id,),
    ).fetchall()
    return [(r['ref_type'], r['ref_value']) for r in rows]


def _event_kinds(ctx) -> list[str]:
    rows = ctx._store._conn.execute(  # type: ignore[attr-defined]
        "SELECT kind FROM events WHERE run_instance_id = ? "
        "ORDER BY ts_ms ASC, id ASC",
        (ctx.run_instance_id,),
    ).fetchall()
    return [r['kind'] for r in rows]


def _run_dispatch(tmp_path, *, responses, intent_factory,
                  expect_exception=None):
    """Drive one dispatch through the journal path and return a snapshot.

    Returns a dict with ``result``, ``error``, ``rows`` (live),
    ``closed_rows``, ``refs``, and ``events`` so each contract test can pin the byte-level shape it
    cares about.
    """
    broker = _FakeBroker(config=_make_config(), responses=responses)
    _seed_rules(broker)
    store, ctx = _open_store_ctx(tmp_path, broker)
    envelope = _make_envelope(intent_factory(), ctx.run_tag)
    result = None
    error: BaseException | None = None
    try:
        try:
            result = asyncio.run(broker.execute_entry(envelope))
        except BaseException as exc:
            if expect_exception is None or not isinstance(exc, expect_exception):
                raise
            error = exc
        rows = _live_rows(ctx)
        closed_rows = _closed_rows(ctx)
        refs = _refs(ctx)
        events = _event_kinds(ctx)
    finally:
        ctx.close()
        store.close()
    return {
        'result': result, 'error': error,
        'rows': rows, 'closed_rows': closed_rows, 'refs': refs,
        'events': events,
    }


# === Pinned-state contract tests =========================================

def __test_contract_market_happy__(tmp_path):
    """MARKET entry happy path — pin order row, refs, events, ExchangeOrder."""
    out = _run_dispatch(
        tmp_path,
        responses={
            ('positions', 'post'): {'dealReference': 'ref-mkt'},
            ('confirms/ref-mkt', 'get'): {
                'dealStatus': 'ACCEPTED', 'status': 'OPEN',
                'dealId': 'deal-mkt', 'level': 1.1234, 'size': 1.0,
                'affectedDeals': [{'dealId': 'deal-mkt'}],
            },
        },
        intent_factory=lambda: _make_intent(order_type=OrderType.MARKET),
    )
    eo = out['result'][0]
    assert eo.id == 'deal-mkt'
    assert eo.status == OrderStatus.FILLED
    assert eo.filled_qty == 1.0
    assert eo.remaining_qty == 0.0
    assert eo.average_fill_price == 1.1234

    assert len(out['rows']) == 1
    row = out['rows'][0]
    assert row['state'] == 'confirmed'
    assert row['exchange_order_id'] == 'deal-mkt'
    assert row['qty'] == 1.0
    assert row['filled_qty'] == 1.0
    assert row['extras'] == {
        'kind': 'position', 'order_type': 'market',
        'deal_reference': 'ref-mkt', 'confirm_level': 1.1234,
    }
    assert out['refs'] == [
        ('deal_id', 'deal-mkt'),
        ('deal_reference', 'ref-mkt'),
    ]
    assert out['events'] == [
        'dispatch_submitted', 'deal_reference_seen', 'confirmed',
    ]


def __test_contract_limit_happy__(tmp_path):
    """LIMIT working order accepted but not filled — pin journal byte-shape."""
    out = _run_dispatch(
        tmp_path,
        responses={
            ('workingorders', 'post'): {'dealReference': 'ref-lim'},
            ('confirms/ref-lim', 'get'): {
                'dealStatus': 'ACCEPTED', 'status': 'ACCEPTED',
                'dealId': 'deal-lim', 'level': 1.20, 'size': 1.0,
                'affectedDeals': [{'dealId': 'deal-lim'}],
            },
        },
        intent_factory=lambda: _make_intent(
            order_type=OrderType.LIMIT, limit=1.20,
        ),
    )
    eo = out['result'][0]
    assert eo.order_type == OrderType.LIMIT
    assert eo.status == OrderStatus.OPEN
    assert eo.filled_qty == 0.0
    assert eo.average_fill_price is None

    row = out['rows'][0]
    assert row['state'] == 'confirmed'
    assert row['exchange_order_id'] == 'deal-lim'
    assert row['filled_qty'] == 0.0
    # ``confirm_level`` MUST NOT be persisted for non-filled rows — the
    # activity-poll recovery fallback would otherwise treat the working
    # order's reservation price as the fill price. ``order_level`` is the
    # placed level the working-order guard judges the venue against.
    assert row['extras'] == {
        'kind': 'working', 'order_type': 'limit',
        'deal_reference': 'ref-lim', 'order_level': 1.20,
    }
    assert out['refs'] == [
        ('deal_id', 'deal-lim'),
        ('deal_reference', 'ref-lim'),
    ]
    assert out['events'] == [
        'dispatch_submitted', 'deal_reference_seen', 'confirmed',
    ]


def __test_contract_submit_timeout__(tmp_path):
    """Network timeout during POST → state=disposition_unknown, no refs."""
    out = _run_dispatch(
        tmp_path,
        responses={
            ('error', 'positions', 'post'): httpx.TimeoutException("synthetic"),
        },
        intent_factory=lambda: _make_intent(order_type=OrderType.MARKET),
        expect_exception=OrderDispositionUnknownError,
    )
    assert isinstance(out['error'], OrderDispositionUnknownError)

    row = out['rows'][0]
    assert row['state'] == 'disposition_unknown'
    assert row['exchange_order_id'] is None
    assert row['extras'] == {'kind': 'position', 'order_type': 'market'}
    # No POST round-trip succeeded, so no deal_reference exists yet.
    assert out['refs'] == []
    assert out['events'] == ['dispatch_submitted', 'disposition_unknown']


def __test_contract_confirm_rejected__(tmp_path):
    """confirm dealStatus=REJECTED → state=rejected, deal_reference persisted."""
    out = _run_dispatch(
        tmp_path,
        responses={
            ('positions', 'post'): {'dealReference': 'ref-rej'},
            ('confirms/ref-rej', 'get'): {
                'dealStatus': 'REJECTED', 'reason': 'TRADING_CLOSED',
            },
        },
        intent_factory=lambda: _make_intent(order_type=OrderType.MARKET),
        expect_exception=ExchangeOrderRejectedError,
    )
    assert isinstance(out['error'], ExchangeOrderRejectedError)
    assert not isinstance(out['error'], InsufficientMarginError)

    # A definitive reject closes the row: the run owns no live order.
    assert out['rows'] == []
    row = out['closed_rows'][0]
    assert row['state'] == 'rejected'
    assert row['exchange_order_id'] is None
    # deal_reference stays on the row for audit; the ``order_refs`` lookup
    # rows go with the close, as a closed row needs no recovery.
    assert row['extras'] == {
        'kind': 'position', 'order_type': 'market',
        'deal_reference': 'ref-rej',
    }
    assert out['refs'] == []
    assert out['events'] == [
        'dispatch_submitted', 'deal_reference_seen', 'order_closed', 'rejected',
    ]


def __test_contract_confirm_rejected_margin_subclass__(tmp_path):
    """Margin reject raises InsufficientMarginError; row shape matches generic reject."""
    out = _run_dispatch(
        tmp_path,
        responses={
            ('positions', 'post'): {'dealReference': 'ref-mg'},
            ('confirms/ref-mg', 'get'): {
                'dealStatus': 'REJECTED', 'reason': 'INSUFFICIENT_MARGIN',
            },
        },
        intent_factory=lambda: _make_intent(order_type=OrderType.MARKET),
        expect_exception=ExchangeOrderRejectedError,  # parent class match
    )
    assert isinstance(out['error'], InsufficientMarginError)

    assert out['rows'] == []
    row = out['closed_rows'][0]
    assert row['state'] == 'rejected'
    assert row['extras']['deal_reference'] == 'ref-mg'
    assert out['events'] == [
        'dispatch_submitted', 'deal_reference_seen', 'order_closed', 'rejected',
    ]


def __test_contract_submit_no_deal_reference__(tmp_path):
    """POST succeeds but dealReference missing → disposition_unknown, no refs."""
    out = _run_dispatch(
        tmp_path,
        responses={('positions', 'post'): {'oops': 'no_ref_here'}},
        intent_factory=lambda: _make_intent(order_type=OrderType.MARKET),
        expect_exception=OrderDispositionUnknownError,
    )
    assert isinstance(out['error'], OrderDispositionUnknownError)

    row = out['rows'][0]
    assert row['state'] == 'disposition_unknown'
    assert row['exchange_order_id'] is None
    assert row['extras'] == {'kind': 'position', 'order_type': 'market'}
    assert out['refs'] == []
    assert out['events'] == ['dispatch_submitted', 'disposition_unknown']


def __test_contract_market_confirm_accepted_status__(tmp_path):
    """MARKET entry + confirm ``status='ACCEPTED'`` (not ``'OPEN'``).

    Pins the heuristic boundary in
    :meth:`_CapitalComEntryHooks.confirm_submission`:
    ``is_filled = (intent.order_type == MARKET and confirm_status == 'OPEN')``.
    A MARKET POST whose confirm comes back with a non-``OPEN`` status
    (broker booked the order but has not yet matched it) must persist
    as ``is_filled=False, filled_qty=0.0`` — the activity stream fill
    then promotes it later.
    """
    out = _run_dispatch(
        tmp_path,
        responses={
            ('positions', 'post'): {'dealReference': 'ref-mkt-accepted'},
            ('confirms/ref-mkt-accepted', 'get'): {
                'dealStatus': 'ACCEPTED', 'status': 'ACCEPTED',
                'dealId': 'deal-mkt-accepted', 'level': 0.0, 'size': 1.0,
                'affectedDeals': [{'dealId': 'deal-mkt-accepted'}],
            },
        },
        intent_factory=lambda: _make_intent(order_type=OrderType.MARKET),
    )
    eo = out['result'][0]
    assert eo.id == 'deal-mkt-accepted'
    assert eo.status == OrderStatus.OPEN
    assert eo.filled_qty == 0.0

    row = out['rows'][0]
    assert row['state'] == 'confirmed'
    assert row['exchange_order_id'] == 'deal-mkt-accepted'
    assert row['filled_qty'] == 0.0
    # ``confirm_level`` is not persisted for the deferred-fill case —
    # only filled-MARKET rows record it.
    assert row['extras'] == {
        'kind': 'position', 'order_type': 'market',
        'deal_reference': 'ref-mkt-accepted',
    }
    assert out['refs'] == [
        ('deal_id', 'deal-mkt-accepted'),
        ('deal_reference', 'ref-mkt-accepted'),
    ]
    assert out['events'] == [
        'dispatch_submitted', 'deal_reference_seen', 'confirmed',
    ]


# === Storage failure after the accepted POST =============================

_ACCEPTED_MARKET = {
    ('positions', 'post'): {'dealReference': 'ref-mkt'},
    ('confirms/ref-mkt', 'get'): {
        'dealStatus': 'ACCEPTED', 'status': 'OPEN', 'dealId': 'deal-mkt',
        'level': 1.1234, 'size': 1.0, 'affectedDeals': [{'dealId': 'deal-mkt'}],
    },
}

_LISTED_MARKET_DEAL = {'deal-mkt': {
    'position': {'dealId': 'deal-mkt', 'direction': 'BUY', 'size': 1.0, 'level': 1.1234,
                 'createdDateUTC': '2026-11-25T10:00:00.000'},
    'market': {'epic': 'EURUSD'},
}}


def _fail_once(ctx, name, matches):
    """Make the store method ``name`` raise a storage error on its first matching call."""
    original = getattr(ctx, name)

    def failing(*args, **kwargs):
        if matches(*args, **kwargs):
            setattr(ctx, name, original)
            raise sqlite3.OperationalError("disk I/O error")
        return original(*args, **kwargs)

    setattr(ctx, name, failing)


async def _drain(agen):
    return [event async for event in agen]


_POST_WRITE_FAILURES = {
    'deal_reference_ref': ('add_ref', lambda coid, ref_type, *_a, **_k: ref_type == 'deal_reference'),
    'deal_reference_audit': ('log_event', lambda kind, *_a, **_k: kind == 'deal_reference_seen'),
    'confirmed_state': ('upsert_order', lambda coid, **fields: fields.get('state') == 'confirmed'),
    'confirmed_audit': ('log_event', lambda kind, *_a, **_k: kind == 'confirmed'),
}


@pytest.mark.parametrize('failure', sorted(_POST_WRITE_FAILURES))
def __test_entry_whose_journal_write_after_the_post_failed_is_booked_by_the_poll__(
        tmp_path, failure):
    """The POST landed and the confirm reported the fill, but a journal write
    after the POST failed. The entry parks ``disposition_unknown`` with its
    ``dealReference`` instead of staying pending until a restart, and the next
    poll books its fill exactly once from the confirm and the positions
    snapshot."""
    broker = _FakeBroker(config=_make_config(), responses=dict(_ACCEPTED_MARKET))
    _seed_rules(broker)
    store, ctx = _open_store_ctx(tmp_path, broker)
    envelope = _make_envelope(_make_intent(order_type=OrderType.MARKET), ctx.run_tag)
    name, matches = _POST_WRITE_FAILURES[failure]
    _fail_once(ctx, name, matches)
    try:
        with pytest.raises(OrderDispositionUnknownError) as parked:
            asyncio.run(broker.execute_entry(envelope))
        coid = parked.value.client_order_id
        row = ctx.get_order(coid)
        assert row is not None and row.state == 'disposition_unknown'
        assert (row.extras or {}).get('deal_reference') == 'ref-mkt'

        events = asyncio.run(_drain(broker._reconcile_snapshot(_LISTED_MARKET_DEAL, {})))
        fills = [e for e in events if e.event_type == 'filled']
        assert [(e.order.client_order_id, e.fill_qty) for e in fills] == [(coid, 1.0)]
        row = ctx.get_order(coid)
        assert row is not None and row.state == 'confirmed'
        assert row.exchange_order_id == 'deal-mkt' and row.filled_qty == 1.0
        again = asyncio.run(_drain(broker._reconcile_snapshot(_LISTED_MARKET_DEAL, {})))
        assert [e for e in again if e.event_type == 'filled'] == []
    finally:
        ctx.close()
        store.close()


def __test_entry_park_whose_write_fails_too_is_committed_by_the_next_poll__(tmp_path):
    """The journal write after the POST fails, and so does the park write:
    the dispatch still reports a typed unknown disposition, and the next poll
    commits the park first and books the fill in the same pass."""
    broker = _FakeBroker(config=_make_config(), responses=dict(_ACCEPTED_MARKET))
    _seed_rules(broker)
    store, ctx = _open_store_ctx(tmp_path, broker)
    envelope = _make_envelope(_make_intent(order_type=OrderType.MARKET), ctx.run_tag)
    _fail_once(ctx, 'upsert_order', lambda coid, **fields: fields.get('state') == 'confirmed')
    for _ in range(2):
        # The Core journal's mark of the row and the plugin's park both fail.
        _fail_once(ctx, 'set_order_state',
                   lambda coid, state, *_a, **_k: state == 'disposition_unknown')
    try:
        with pytest.raises(OrderDispositionUnknownError) as parked:
            asyncio.run(broker.execute_entry(envelope))
        coid = parked.value.client_order_id
        row = ctx.get_order(coid)
        assert row is not None and row.state == 'server_ref_seen'

        events = asyncio.run(_drain(broker._reconcile_snapshot(_LISTED_MARKET_DEAL, {})))
        fills = [e for e in events if e.event_type == 'filled']
        assert [(e.order.client_order_id, e.fill_qty) for e in fills] == [(coid, 1.0)]
        again = asyncio.run(_drain(broker._reconcile_snapshot(_LISTED_MARKET_DEAL, {})))
        assert [e for e in again if e.event_type == 'filled'] == []
        assert broker._deferred_parks == {}
    finally:
        ctx.close()
        store.close()


@pytest.mark.parametrize('failure', ['deal_reference_ref', 'confirmed_state'])
def __test_entry_park_never_committed_is_recovered_once_by_the_restart__(tmp_path, failure):
    """The journal write after the POST fails, the park write fails too, and
    the process dies with the park still deferred: the row stays
    ``submitted`` (no ``dealReference`` journaled) or ``server_ref_seen``. The
    restart re-derives the entry from the venue (the stored reference, or the
    deal's open activity) and takes the deal over once: the row is confirmed
    filled on its deal, no POST goes out again, and no later poll books the
    fill a second time (the engine adopts the venue position at startup)."""
    broker = _FakeBroker(config=_make_config(), responses=dict(_ACCEPTED_MARKET))
    _seed_rules(broker)
    store, ctx = _open_store_ctx(tmp_path, broker)
    envelope = _make_envelope(_make_intent(order_type=OrderType.MARKET), ctx.run_tag)
    name, matches = _POST_WRITE_FAILURES[failure]
    _fail_once(ctx, name, matches)
    for _ in range(2):
        # The journal's own park of the row and the plugin's park both fail.
        if failure == 'deal_reference_ref':
            _fail_once(ctx, 'add_ref',
                       lambda coid, ref_type, *_a, **_k: ref_type == 'deal_reference')
        _fail_once(ctx, 'set_order_state',
                   lambda coid, state, *_a, **_k: state == 'disposition_unknown')
    with pytest.raises(OrderDispositionUnknownError) as parked:
        asyncio.run(broker.execute_entry(envelope))
    coid = parked.value.client_order_id
    assert list(broker._deferred_parks) == [coid]
    row = ctx.get_order(coid)
    assert row is not None
    assert row.state == ('submitted' if failure == 'deal_reference_ref' else 'server_ref_seen')
    created = row.created_ts_ms / 1000.0
    ctx.close()

    position = {'position': {**_LISTED_MARKET_DEAL['deal-mkt']['position'],
                             'dealReference': 'ref-mkt'},
                'market': {'epic': 'EURUSD'}}
    opened = datetime.fromtimestamp(created, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
    restarted = _FakeBroker(config=_make_config(), responses={
        **_ACCEPTED_MARKET,
        ('positions', 'get'): {'positions': [position]},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': [{
            'epic': 'EURUSD', 'dealId': 'deal-mkt', 'type': 'POSITION', 'status': 'ACCEPTED',
            'source': 'USER', 'dateUTC': opened,
            'details': {'direction': 'BUY', 'size': 1.0, 'level': 1.1234},
        }]},
    })
    _seed_rules(restarted)
    restarted.store_ctx = ctx = store.open_run(
        RunIdentity(strategy_id="contract", symbol="EURUSD", timeframe="60",
                    account_id="contract-account"),
        script_source="// contract",
    )
    try:
        asyncio.run(restarted._recover_in_flight_submissions())
        row = ctx.get_order(coid)
        assert row is not None and row.state == 'confirmed'
        assert row.exchange_order_id == 'deal-mkt' and row.filled_qty == 1.0
        assert ctx.find_by_ref('deal_id', 'deal-mkt').client_order_id == coid
        listed = {'deal-mkt': position}
        for _ in range(2):
            events = asyncio.run(_drain(restarted._reconcile_snapshot(listed, {})))
            assert [e for e in events if e.event_type == 'filled'] == []
        assert [c[:2] for c in restarted._calls].count(('positions', 'post')) == 0
    finally:
        ctx.close()
        store.close()


def __test_working_entry_whose_journal_write_after_the_post_failed_gets_its_order_id__(
        tmp_path):
    """A LIMIT entry the venue placed, whose confirmed state could not be
    journaled, learns its order id on the next poll, so the open-order view
    maps the venue's working order back to it."""
    broker = _FakeBroker(config=_make_config(), responses={
        ('workingorders', 'post'): {'dealReference': 'ref-lim'},
        ('confirms/ref-lim', 'get'): {
            'dealStatus': 'ACCEPTED', 'status': 'ACCEPTED', 'dealId': 'deal-lim',
            'level': 1.20, 'size': 1.0, 'affectedDeals': [{'dealId': 'deal-lim'}],
        },
    })
    _seed_rules(broker)
    store, ctx = _open_store_ctx(tmp_path, broker)
    envelope = _make_envelope(
        _make_intent(order_type=OrderType.LIMIT, limit=1.20), ctx.run_tag)
    _fail_once(ctx, 'upsert_order', lambda coid, **fields: fields.get('state') == 'confirmed')
    try:
        with pytest.raises(OrderDispositionUnknownError) as parked:
            asyncio.run(broker.execute_entry(envelope))
        coid = parked.value.client_order_id
        asyncio.run(_drain(broker._reconcile_snapshot({}, {})))
        row = ctx.get_order(coid)
        assert row is not None and row.state == 'confirmed'
        assert row.exchange_order_id == 'deal-lim'
        assert ctx.find_by_ref('deal_id', 'deal-lim').client_order_id == coid
    finally:
        ctx.close()
        store.close()


_REJECTED_MARKET = {
    ('positions', 'post'): {'dealReference': 'ref-rej'},
    ('confirms/ref-rej', 'get'): {'dealStatus': 'REJECTED', 'reason': 'RISK_CHECK'},
}


@pytest.mark.parametrize('after', ['running', 'restart', 'restart_confirm_expired'])
def __test_rejected_entry_whose_reject_write_failed_is_never_sent_again__(tmp_path, after):
    """The venue refused the entry, and the journal's booking of the refusal
    fails, and so does its owed retry: the dispatch still surfaces the reject,
    and the row is left ``server_ref_seen`` with its ``dealReference``. With ``running`` the
    engine's next sync lands the owed reject; with ``restart`` the process
    dies first and the restart's confirm read rejects the row; with
    ``restart_confirm_expired`` the venue no longer serves the confirm and
    lists nothing for the reference, and the inherited-submission resolver
    rejects the row once the venue's answer held. Either way the row ends
    rejected and closed, no POST goes out again for it, and nothing is
    booked."""
    broker = _FakeBroker(config=_make_config(), responses=dict(_REJECTED_MARKET))
    _seed_rules(broker)
    store, ctx = _open_store_ctx(tmp_path, broker)
    engine = OrderSyncEngine(broker=broker, position=BrokerPosition(), symbol='EURUSD',
                             run_tag=ctx.run_tag, mintick=0.00001, store_ctx=ctx)
    envelope = _make_envelope(_make_intent(order_type=OrderType.MARKET), ctx.run_tag)
    coid = envelope.client_order_id(KIND_ENTRY)
    for _ in range(2):
        # The journal's booking of the refusal fails, and so does its owed retry.
        _fail_once(ctx, 'set_order_state', lambda c, state, *_a, **_k: state == 'rejected')
    with pytest.raises(ExchangeOrderRejectedError):
        asyncio.run(broker.execute_entry(envelope))
    row = ctx.get_order(coid)
    assert row is not None and row.closed_ts_ms is None and row.state == 'server_ref_seen'
    events = asyncio.run(_drain(broker._reconcile_snapshot({}, {})))
    assert [e for e in events if e.event_type == 'filled'] == []
    assert ctx.get_order(coid).exchange_order_id is None

    if after == 'running':
        engine._flush_owed_store_writes()
        row = ctx.get_order(coid)
        assert row is not None and row.state == 'rejected' and row.closed_ts_ms is not None
        assert [c[:2] for c in broker._calls].count(('positions', 'post')) == 1
        ctx.close()
        store.close()
        return

    ctx.close()
    responses = {
        **_REJECTED_MARKET,
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
    }
    if after == 'restart_confirm_expired':
        responses[('error', 'confirms/ref-rej', 'get')] = OrderNotFoundError(
            'gone', ref_type='deal_reference')
    restarted = _FakeBroker(config=_make_config(), responses=responses)
    _seed_rules(restarted)
    restarted.store_ctx = ctx = store.open_run(
        RunIdentity(strategy_id="contract", symbol="EURUSD", timeframe="60",
                    account_id="contract-account"),
        script_source="// contract",
    )
    # The inherited-submission resolver looks at the row once it is past the
    # venue's indexing grace, a lookup interval apart.
    passes = iter(range(1, 100))
    restarted._inherited_resolver._clock = lambda: epoch_time() + 3600.0 * next(passes)
    try:
        asyncio.run(restarted._recover_in_flight_submissions())
        for _ in range(4):
            events = asyncio.run(_drain(restarted._reconcile_snapshot({}, {})))
            assert [e for e in events if e.event_type == 'filled'] == []
        row = ctx.get_order_for_run_id(coid)
        assert row is not None and row.exchange_order_id is None
        assert row.state == 'rejected' and row.closed_ts_ms is not None
        assert [c[:2] for c in restarted._calls].count(('positions', 'post')) == 0
    finally:
        ctx.close()
        store.close()
