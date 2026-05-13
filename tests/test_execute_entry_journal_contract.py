"""
@pyne
"""
import asyncio
from dataclasses import asdict
from time import time as epoch_time

import httpx
import pytest

from pynecore.core.broker.exceptions import (
    ExchangeOrderRejectedError,
    InsufficientMarginError,
    OrderDispositionUnknownError,
)
from pynecore.core.broker.models import (
    DispatchEnvelope, EntryIntent, OrderStatus, OrderType,
)
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore

from pynecore_capitalcom import CapitalCom, CapitalComConfig
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

    async def _call(self, endpoint, *, data=None, method='post'):
        self._calls.append((endpoint, method, data))
        err = self._responses.get(('error', endpoint, method))
        if err is not None:
            raise err
        return self._responses.get((endpoint, method), {})


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

    Returns a dict with ``result``, ``error``, ``rows``, ``refs``, and
    ``events`` so each contract test can pin the byte-level shape it
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
        refs = _refs(ctx)
        events = _event_kinds(ctx)
    finally:
        ctx.close()
        store.close()
    return {
        'result': result, 'error': error,
        'rows': rows, 'refs': refs, 'events': events,
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
    # order's reservation price as the fill price.
    assert row['extras'] == {
        'kind': 'working', 'order_type': 'limit',
        'deal_reference': 'ref-lim',
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

    row = out['rows'][0]
    assert row['state'] == 'rejected'
    assert row['exchange_order_id'] is None
    assert row['extras'] == {
        'kind': 'position', 'order_type': 'market',
        'deal_reference': 'ref-rej',
    }
    # deal_reference WAS recorded before the REJECTED confirm so the
    # recovery contract can resolve the row from a confirms/{ref} GET.
    assert out['refs'] == [('deal_reference', 'ref-rej')]
    assert out['events'] == [
        'dispatch_submitted', 'deal_reference_seen', 'rejected',
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

    row = out['rows'][0]
    assert row['state'] == 'rejected'
    assert row['extras']['deal_reference'] == 'ref-mg'
    assert out['events'] == [
        'dispatch_submitted', 'deal_reference_seen', 'rejected',
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
