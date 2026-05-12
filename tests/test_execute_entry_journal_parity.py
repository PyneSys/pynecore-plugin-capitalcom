"""
@pyne
"""
import asyncio
import json
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
        user_email="parity@example.com",
        api_key="parity-key",
        api_password="parity-password",
    )
    defaults.update(overrides)
    return CapitalComConfig(**defaults)


def _seed_rules(broker: CapitalCom, *, epic: str = "EURUSD") -> None:
    """Pre-populate the rules cache so _get_instrument_rules is a no-op.

    TTL with positive `fetched_at` keeps the cached row fresh for the
    test's lifetime (the cache lookup returns immediately).
    """
    broker._instrument_rules_cache[epic] = _InstrumentRules(  # type: ignore[attr-defined]
        epic=epic,
        lot_step=0.01,
        min_size=0.01,
        min_stop_or_limit_distance=0.0,
        fetched_at=epoch_time(),
    )


def _open_store_ctx(tmp_path, broker: CapitalCom, *, label: str | None = None):
    """Open a fresh BrokerStore + RunContext on ``broker``.

    The two parity paths each get a distinct ``run_label`` so the runs
    live in the same SQLite file but cannot collide on ``run_id``.
    """
    store = BrokerStore(
        tmp_path / "broker.sqlite",
        plugin_name=broker.plugin_name,
    )
    identity = RunIdentity(
        strategy_id="parity", symbol="EURUSD", timeframe="60",
        account_id="parity-account", label=label,
    )
    ctx = store.open_run(identity, script_source="// parity")
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


def _dump_row(row) -> dict:
    """Convert OrderRow → comparable dict (ts fields stripped).

    ``created_ts_ms`` / ``updated_ts_ms`` differ between two runs in
    the same test by wall-clock; ``closed_ts_ms`` is None for both.
    Everything else must match exactly.
    """
    d = asdict(row)
    d.pop('created_ts_ms', None)
    d.pop('updated_ts_ms', None)
    return d


def _dump_orders(ctx) -> list[dict]:
    """Snapshot of all orders for one run, sorted by COID for stability."""
    rows = sorted(
        ctx.iter_live_orders(),
        key=lambda r: r.client_order_id,
    )
    return [_dump_row(r) for r in rows]


def _dump_refs(ctx) -> list[tuple[str, str, str]]:
    """All order_refs for one run as (ref_type, ref_value, COID)."""
    rows = ctx._store._conn.execute(  # type: ignore[attr-defined]
        "SELECT ref_type, ref_value, client_order_id FROM order_refs "
        "WHERE run_instance_id = ? "
        "ORDER BY ref_type, ref_value",
        (ctx.run_instance_id,),
    ).fetchall()
    return [(r['ref_type'], r['ref_value'], r['client_order_id']) for r in rows]


def _dump_event_kinds(ctx) -> list[str]:
    """Audit event kinds for one run, in ts order."""
    rows = ctx._store._conn.execute(  # type: ignore[attr-defined]
        "SELECT kind FROM events WHERE run_instance_id = ? "
        "ORDER BY ts_ms ASC, id ASC",
        (ctx.run_instance_id,),
    ).fetchall()
    return [r['kind'] for r in rows]


def _dump_orders_with_rejected(ctx) -> list[dict]:
    """Include closed/rejected rows — execute_entry leaves them live with
    state='rejected' but :func:`iter_live_orders` only matches
    ``closed_ts_ms IS NULL`` rows; both implementations follow that
    contract so the live view is the right comparison.
    """
    return _dump_orders(ctx)


# === Parity drivers =======================================================

def _run_dispatch(
        *,
        broker: _FakeBroker,
        intent: EntryIntent,
        run_tag: str,
        flag: bool,
        monkeypatch: pytest.MonkeyPatch,
):
    """Drive one dispatch on ``broker``, flipping the journal flag.

    ``monkeypatch.setenv`` / ``delenv`` keeps the flag scoped to the
    individual call so the two paths cannot bleed into each other when
    a single test compares both.
    """
    if flag:
        monkeypatch.setenv('PYNECORE_BROKER_JOURNAL_ENTRY', '1')
    else:
        monkeypatch.delenv('PYNECORE_BROKER_JOURNAL_ENTRY', raising=False)
    envelope = _make_envelope(intent, run_tag)
    return asyncio.run(broker.execute_entry(envelope))


def _run_parity(
        tmp_path,
        *,
        responses: dict,
        intent_factory,
        monkeypatch: pytest.MonkeyPatch,
        expect_exception: type[BaseException] | None = None,
) -> dict:
    """Run both paths against identical mocked responses.

    Each path gets its own broker instance + its own ``run_label`` so
    the SQLite ``runs`` rows do not collide. The two ``RunContext``
    snapshots are returned so the caller can compare orders / refs /
    event kinds.
    """
    legacy_broker = _FakeBroker(config=_make_config(), responses=responses)
    journal_broker = _FakeBroker(config=_make_config(), responses=responses)
    _seed_rules(legacy_broker)
    _seed_rules(journal_broker)

    legacy_store, legacy_ctx = _open_store_ctx(tmp_path, legacy_broker, label='legacy')
    journal_store, journal_ctx = _open_store_ctx(
        tmp_path, journal_broker, label='journal',
    )
    legacy_run_tag = legacy_ctx.run_tag
    journal_run_tag = journal_ctx.run_tag

    legacy_result: list | None = None
    journal_result: list | None = None
    legacy_error: BaseException | None = None
    journal_error: BaseException | None = None

    try:
        try:
            legacy_result = _run_dispatch(
                broker=legacy_broker,
                intent=intent_factory(),
                run_tag=legacy_run_tag,
                flag=False,
                monkeypatch=monkeypatch,
            )
        except BaseException as exc:
            if expect_exception is None or not isinstance(exc, expect_exception):
                raise
            legacy_error = exc
        try:
            journal_result = _run_dispatch(
                broker=journal_broker,
                intent=intent_factory(),
                run_tag=journal_run_tag,
                flag=True,
                monkeypatch=monkeypatch,
            )
        except BaseException as exc:
            if expect_exception is None or not isinstance(exc, expect_exception):
                raise
            journal_error = exc

        legacy_orders = _dump_orders(legacy_ctx)
        journal_orders = _dump_orders(journal_ctx)
        legacy_refs = _dump_refs(legacy_ctx)
        journal_refs = _dump_refs(journal_ctx)
        legacy_events = _dump_event_kinds(legacy_ctx)
        journal_events = _dump_event_kinds(journal_ctx)
    finally:
        legacy_ctx.close()
        journal_ctx.close()
        legacy_store.close()
        journal_store.close()

    return {
        'legacy_result': legacy_result,
        'journal_result': journal_result,
        'legacy_error': legacy_error,
        'journal_error': journal_error,
        'legacy_orders': legacy_orders,
        'journal_orders': journal_orders,
        'legacy_refs': legacy_refs,
        'journal_refs': journal_refs,
        'legacy_events': legacy_events,
        'journal_events': journal_events,
    }


# === Parity assertions ====================================================

def _assert_order_parity(legacy: list[dict], journal: list[dict]) -> None:
    """Compare order rows tolerantly.

    Both rows carry the same ``client_order_id`` (deterministic from
    the COID formula given the same run_tag … wait — run_tag DIFFERS
    between the two runs because run_label differs). So COIDs differ
    between paths. Compare AFTER normalising COIDs.
    """
    assert len(legacy) == len(journal) == 1, (
        f"each path must end with exactly one row, "
        f"got legacy={len(legacy)} journal={len(journal)}"
    )
    a = dict(legacy[0])
    b = dict(journal[0])
    a.pop('client_order_id', None)
    b.pop('client_order_id', None)
    a.pop('plugin_name', None)
    b.pop('plugin_name', None)
    assert a == b, f"order row diverged:\nlegacy={a}\njournal={b}"


def _assert_refs_parity(legacy, journal) -> None:
    """ref_type / ref_value must match in count and content (COID stripped)."""
    a = sorted([(rt, rv) for (rt, rv, _coid) in legacy])
    b = sorted([(rt, rv) for (rt, rv, _coid) in journal])
    assert a == b, f"order_refs diverged: legacy={a} journal={b}"


def _assert_event_kinds_subset(legacy: list[str], journal: list[str]) -> None:
    """The journal path must emit the same audit MILESTONES as legacy.

    Payloads diverge by design — journal records ``phase`` /
    ``reason`` while legacy records ``endpoint`` / ``response``. The
    parity guarantee is on the *sequence of kinds*, not their bodies.
    """
    assert legacy == journal, (
        f"event-kind sequence diverged:\nlegacy={legacy}\njournal={journal}"
    )


# === Test cases ===========================================================

def __test_parity_market_happy__(tmp_path, monkeypatch):
    """MARKET entry, both paths produce identical persisted state."""
    responses = {
        ('positions', 'post'): {
            'dealReference': 'ref-mkt',
        },
        ('confirms/ref-mkt', 'get'): {
            'dealStatus': 'ACCEPTED',
            'status': 'OPEN',
            'dealId': 'deal-mkt',
            'level': 1.1234,
            'size': 1.0,
            'affectedDeals': [{'dealId': 'deal-mkt'}],
        },
    }
    out = _run_parity(
        tmp_path, responses=responses,
        intent_factory=lambda: _make_intent(order_type=OrderType.MARKET),
        monkeypatch=monkeypatch,
    )
    assert out['legacy_result'] is not None
    assert out['journal_result'] is not None
    # Returned ExchangeOrder semantics match.
    legacy_eo = out['legacy_result'][0]
    journal_eo = out['journal_result'][0]
    assert legacy_eo.id == journal_eo.id == 'deal-mkt'
    assert legacy_eo.status == journal_eo.status == OrderStatus.FILLED
    assert legacy_eo.filled_qty == journal_eo.filled_qty == 1.0
    assert legacy_eo.average_fill_price == journal_eo.average_fill_price == 1.1234

    _assert_order_parity(out['legacy_orders'], out['journal_orders'])
    _assert_refs_parity(out['legacy_refs'], out['journal_refs'])
    _assert_event_kinds_subset(out['legacy_events'], out['journal_events'])


def __test_parity_limit_happy__(tmp_path, monkeypatch):
    """LIMIT order accepted (no immediate fill) — parity end-state."""
    responses = {
        ('workingorders', 'post'): {
            'dealReference': 'ref-lim',
        },
        ('confirms/ref-lim', 'get'): {
            'dealStatus': 'ACCEPTED',
            'status': 'ACCEPTED',  # not filled
            'dealId': 'deal-lim',
            'level': 1.2000,
            'size': 1.0,
            'affectedDeals': [{'dealId': 'deal-lim'}],
        },
    }
    out = _run_parity(
        tmp_path, responses=responses,
        intent_factory=lambda: _make_intent(
            order_type=OrderType.LIMIT, limit=1.20,
        ),
        monkeypatch=monkeypatch,
    )
    legacy_eo = out['legacy_result'][0]
    journal_eo = out['journal_result'][0]
    assert legacy_eo.status == journal_eo.status == OrderStatus.OPEN
    assert legacy_eo.filled_qty == journal_eo.filled_qty == 0.0

    _assert_order_parity(out['legacy_orders'], out['journal_orders'])
    _assert_refs_parity(out['legacy_refs'], out['journal_refs'])
    _assert_event_kinds_subset(out['legacy_events'], out['journal_events'])


def __test_parity_submit_timeout__(tmp_path, monkeypatch):
    """Network timeout during POST → both paths land disposition_unknown."""
    responses = {
        ('error', 'positions', 'post'): httpx.TimeoutException("synthetic"),
    }
    out = _run_parity(
        tmp_path, responses=responses,
        intent_factory=lambda: _make_intent(order_type=OrderType.MARKET),
        monkeypatch=monkeypatch,
        expect_exception=OrderDispositionUnknownError,
    )
    # Both raised the same exception class.
    assert isinstance(out['legacy_error'], OrderDispositionUnknownError)
    assert isinstance(out['journal_error'], OrderDispositionUnknownError)

    _assert_order_parity(out['legacy_orders'], out['journal_orders'])
    # No deal_reference recorded — submit never returned one.
    _assert_refs_parity(out['legacy_refs'], out['journal_refs'])
    assert out['legacy_refs'] == [] == out['journal_refs']
    _assert_event_kinds_subset(out['legacy_events'], out['journal_events'])


def __test_parity_confirm_rejected__(tmp_path, monkeypatch):
    """confirm dealStatus REJECTED → both paths persist state='rejected'."""
    responses = {
        ('positions', 'post'): {'dealReference': 'ref-rej'},
        ('confirms/ref-rej', 'get'): {
            'dealStatus': 'REJECTED',
            'reason': 'TRADING_CLOSED',
        },
    }
    out = _run_parity(
        tmp_path, responses=responses,
        intent_factory=lambda: _make_intent(order_type=OrderType.MARKET),
        monkeypatch=monkeypatch,
        expect_exception=ExchangeOrderRejectedError,
    )
    assert isinstance(out['legacy_error'], ExchangeOrderRejectedError)
    assert isinstance(out['journal_error'], ExchangeOrderRejectedError)

    _assert_order_parity(out['legacy_orders'], out['journal_orders'])
    _assert_refs_parity(out['legacy_refs'], out['journal_refs'])
    # deal_reference WAS recorded before the REJECTED confirm.
    assert ('deal_reference', 'ref-rej') in [
        (rt, rv) for (rt, rv, _) in out['legacy_refs']
    ]
    _assert_event_kinds_subset(out['legacy_events'], out['journal_events'])


def __test_parity_confirm_rejected_margin_subclass__(tmp_path, monkeypatch):
    """Margin reject raises InsufficientMarginError on both paths."""
    responses = {
        ('positions', 'post'): {'dealReference': 'ref-mg'},
        ('confirms/ref-mg', 'get'): {
            'dealStatus': 'REJECTED',
            'reason': 'INSUFFICIENT_MARGIN',
        },
    }
    out = _run_parity(
        tmp_path, responses=responses,
        intent_factory=lambda: _make_intent(order_type=OrderType.MARKET),
        monkeypatch=monkeypatch,
        expect_exception=ExchangeOrderRejectedError,  # parent class match
    )
    assert isinstance(out['legacy_error'], InsufficientMarginError)
    assert isinstance(out['journal_error'], InsufficientMarginError)

    _assert_order_parity(out['legacy_orders'], out['journal_orders'])


def __test_parity_submit_no_deal_reference__(tmp_path, monkeypatch):
    """POST succeeds but ``dealReference`` missing → both raise OrderDispositionUnknownError."""
    responses = {
        ('positions', 'post'): {'oops': 'no_ref_here'},
    }
    out = _run_parity(
        tmp_path, responses=responses,
        intent_factory=lambda: _make_intent(order_type=OrderType.MARKET),
        monkeypatch=monkeypatch,
        expect_exception=OrderDispositionUnknownError,
    )
    assert isinstance(out['legacy_error'], OrderDispositionUnknownError)
    assert isinstance(out['journal_error'], OrderDispositionUnknownError)
    _assert_order_parity(out['legacy_orders'], out['journal_orders'])
