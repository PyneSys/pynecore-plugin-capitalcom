"""Recovery confirm-path contract tests.

@pyne

These tests pin down the BrokerStore end-state that the four
recovery confirm sequences must leave behind, regardless of which
exchange-side data source resolved the row:

* stored ``deal_reference`` matched against a positions/working snapshot
  (``_recover_submitted_row`` first branch),
* activity-heuristic single-match against ``/history/activity``
  (``_recover_submitted_row`` second branch),
* TTL fallback for a ``server_ref_seen`` row: the direct
  ``GET /confirms/{ref}`` raises :class:`OrderNotFoundError`, then the
  snapshot resolves it (``_recover_server_ref_seen_row`` first branch),
* direct ``/confirms/{ref}`` match (``_recover_server_ref_seen_row``
  second branch).

The assertions are deliberately store-shaped — they do not assume which
private recovery helper produced them — so the same tests stay valid
when the journal's :meth:`recover_pending` takes over in M3.

The load-bearing invariant is the ``('deal_id', X)`` row in
``order_refs``: the runtime activity poll uses
``ctx.find_by_ref('deal_id', X)`` to map an exchange-side ``dealId``
back to the local CO-ID, so a confirmed row without that ref is
silently invisible to the rest of the plugin.
"""
import asyncio

from pynecore.core.broker.models import EntryIntent  # noqa: F401 — keep import path warm
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore

from pynecore_capitalcom import CapitalCom, CapitalComConfig
from pynecore_capitalcom.exceptions import OrderNotFoundError


def main():
    pass


# === Test fixtures ========================================================

class _FakeBroker(CapitalCom):
    """Same skeleton as ``test_capitalcom_broker._FakeBroker``."""

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


def _open_broker(tmp_path, *, responses=None):
    """Construct a _FakeBroker with an opened BrokerStore + RunContext."""
    broker = _FakeBroker(config=_make_config(), responses=responses or {})
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
    return broker, store, ctx


def _assert_confirmed_contract(ctx, *, coid: str, deal_id: str) -> None:
    """Assert the four invariants every recovery confirm must produce."""
    row = ctx.get_order(coid)
    assert row is not None, f"Order row {coid!r} missing after recovery"
    assert row.state == 'confirmed', (
        f"Expected state='confirmed' after recovery, got {row.state!r}"
    )
    assert row.exchange_order_id == deal_id, (
        f"Expected exchange_order_id={deal_id!r}, "
        f"got {row.exchange_order_id!r}"
    )
    # The load-bearing ref: activity.py:_process_activity matches by
    # ``find_by_ref('deal_id', dealId)`` and silently drops events for
    # CO-IDs without this alias.
    by_ref = ctx.find_by_ref('deal_id', deal_id)
    assert by_ref is not None, (
        f"order_refs missing ('deal_id', {deal_id!r}) after recovery"
    )
    assert by_ref.client_order_id == coid


# === Tests ================================================================

def __test_recovery_contract_stored_ref_match__(tmp_path):
    """Path 1: ``submitted`` row + stored ``deal_reference`` resolves
    against the positions snapshot.

    Pins ``recovery.py:212-227``.
    """
    broker, store, ctx = _open_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            {'position': {'dealReference': 'REF-1', 'dealId': 'DEAL-1'}},
        ]},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
    })
    ctx.upsert_order(
        'coid-stored-ref', symbol='EURUSD', side='buy', qty=1.0,
        state='submitted', pine_entry_id='Long', intent_key='Long',
        extras={'kind': 'position', 'order_type': 'market',
                'deal_reference': 'REF-1'},
    )
    # The stored ref must also live in order_refs for the load-bearing
    # alias index — execute_entry writes it there before mirroring into
    # extras, so the contract test replicates that ordering.
    ctx.add_ref('coid-stored-ref', 'deal_reference', 'REF-1')

    asyncio.run(broker._recover_in_flight_submissions())

    _assert_confirmed_contract(
        ctx, coid='coid-stored-ref', deal_id='DEAL-1',
    )
    store.close()


def __test_recovery_contract_activity_single_match__(tmp_path):
    """Path 2: ``submitted`` row with no stored ref, single activity
    candidate matches epic+direction+size+±3s.

    Pins ``recovery.py:252-268``.
    """
    import datetime as _dt
    ts_ms = int(_dt.datetime(2026, 4, 21, 10, 0, 0,
                             tzinfo=_dt.UTC).timestamp() * 1000)
    broker, store, ctx = _open_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': [
            {'epic': 'EURUSD', 'direction': 'BUY', 'size': 1.0,
             'dateUTC': '2026-04-21T10:00:00.000', 'dealId': 'DEAL-2',
             'type': 'POSITION', 'status': 'EXECUTED'},
        ]},
    })
    ctx.upsert_order(
        'coid-activity', symbol='EURUSD', side='buy', qty=1.0,
        state='submitted', pine_entry_id='Long', intent_key='Long',
        extras={'kind': 'position', 'order_type': 'market'},
    )
    ctx._store._conn.execute(
        "UPDATE orders SET created_ts_ms = ? WHERE client_order_id = ?",
        (ts_ms, 'coid-activity'),
    )

    asyncio.run(broker._recover_in_flight_submissions())

    _assert_confirmed_contract(
        ctx, coid='coid-activity', deal_id='DEAL-2',
    )
    store.close()


def __test_recovery_contract_ttl_fallback_snapshot__(tmp_path):
    """Path 3: ``server_ref_seen`` row, direct ``/confirms`` GET raises
    :class:`OrderNotFoundError` (TTL expired), snapshot resolves the ref.

    Pins ``recovery.py:296-318``.
    """
    broker, store, ctx = _open_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            {'position': {'dealReference': 'REF-3', 'dealId': 'DEAL-3'}},
        ]},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
        ('error', 'confirms/REF-3', 'get'): OrderNotFoundError(
            "Capital confirm REF-3 not found (TTL expired)",
            ref_type='deal_reference',
        ),
    })
    ctx.upsert_order(
        'coid-ttl', symbol='EURUSD', side='buy', qty=1.0,
        state='server_ref_seen', pine_entry_id='Long', intent_key='Long',
        extras={'kind': 'position', 'order_type': 'market',
                'deal_reference': 'REF-3'},
    )
    ctx.add_ref('coid-ttl', 'deal_reference', 'REF-3')

    asyncio.run(broker._recover_in_flight_submissions())

    _assert_confirmed_contract(
        ctx, coid='coid-ttl', deal_id='DEAL-3',
    )
    store.close()


def __test_recovery_contract_confirm_get_direct__(tmp_path):
    """Path 4: ``server_ref_seen`` row, direct ``/confirms`` GET resolves
    with a ``dealId`` (the happy TTL-fresh path).

    Pins ``recovery.py:320-339``.
    """
    broker, store, ctx = _open_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
        ('confirms/REF-4', 'get'): {
            'dealStatus': 'ACCEPTED', 'status': 'OPEN',
            'dealId': 'DEAL-4', 'level': 1.1, 'size': 1.0,
        },
    })
    ctx.upsert_order(
        'coid-confirm', symbol='EURUSD', side='buy', qty=1.0,
        state='server_ref_seen', pine_entry_id='Long', intent_key='Long',
        extras={'kind': 'position', 'order_type': 'market',
                'deal_reference': 'REF-4'},
    )
    ctx.add_ref('coid-confirm', 'deal_reference', 'REF-4')

    asyncio.run(broker._recover_in_flight_submissions())

    _assert_confirmed_contract(
        ctx, coid='coid-confirm', deal_id='DEAL-4',
    )
    store.close()


def __test_recovery_contract_idempotency__(tmp_path):
    """Running recovery twice must not duplicate ``order_refs`` or
    mutate an already-confirmed row.

    The second pass walks live orders again; rows in ``state='confirmed'``
    fall through both branches in ``_recover_in_flight_submissions``
    (neither ``submitted`` / ``disposition_unknown`` nor
    ``server_ref_seen``). The contract: same end-state, same ref count.
    """
    broker, store, ctx = _open_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            {'position': {'dealReference': 'REF-5', 'dealId': 'DEAL-5'}},
        ]},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
    })
    ctx.upsert_order(
        'coid-idem', symbol='EURUSD', side='buy', qty=1.0,
        state='submitted', pine_entry_id='Long', intent_key='Long',
        extras={'kind': 'position', 'order_type': 'market',
                'deal_reference': 'REF-5'},
    )
    ctx.add_ref('coid-idem', 'deal_reference', 'REF-5')

    asyncio.run(broker._recover_in_flight_submissions())
    refs_first = sorted(ctx.iter_refs_for_coid('coid-idem'))
    row_first = ctx.get_order('coid-idem')

    asyncio.run(broker._recover_in_flight_submissions())
    refs_second = sorted(ctx.iter_refs_for_coid('coid-idem'))
    row_second = ctx.get_order('coid-idem')

    _assert_confirmed_contract(
        ctx, coid='coid-idem', deal_id='DEAL-5',
    )
    assert refs_first == refs_second, (
        f"Recovery refs diverged across passes: "
        f"first={refs_first!r}, second={refs_second!r}"
    )
    assert row_first.state == row_second.state
    assert row_first.exchange_order_id == row_second.exchange_order_id
    store.close()
