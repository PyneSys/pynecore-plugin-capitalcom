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
            {'epic': 'EURUSD', 'details': {'direction': 'BUY', 'size': 1.0},
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
    # The deal is absent from the positions snapshot: nothing proves the
    # exposure is still open, so the row is not credited a fill.
    assert ctx.get_order('coid-activity').filled_qty == 0.0
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
        # The confirmed deal must be alive in a snapshot — a confirm
        # whose deal is absent from both snapshots is stale (the
        # confirms cache outlives the deal) and resolves to rejected.
        ('positions', 'get'): {'positions': [
            {'position': {'dealId': 'DEAL-4'}},
        ]},
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


def _assert_rejected_contract(ctx, *, coid: str, recovery_path: str) -> None:
    """Assert the row was terminally rejected with the given path."""
    import json as _json
    row = ctx.get_order(coid)
    assert row is not None, f"Order row {coid!r} missing after recovery"
    assert row.state == 'rejected', (
        f"Expected state='rejected' after recovery, got {row.state!r}"
    )
    cur = ctx._store._conn.execute(
        "SELECT payload FROM events "
        "WHERE kind = 'recovered_rejected' AND client_order_id = ?",
        (coid,),
    )
    rows = cur.fetchall()
    assert len(rows) == 1, (
        f"Expected exactly one recovered_rejected event for {coid!r}, "
        f"got {len(rows)}"
    )
    payload = _json.loads(rows[0][0]) if rows[0][0] else {}
    assert payload.get('recovery_path') == recovery_path, (
        f"Expected recovery_path={recovery_path!r}, "
        f"got {payload.get('recovery_path')!r}"
    )


def __test_recovery_contract_confirm_get_rejected__(tmp_path):
    """``server_ref_seen`` row, direct ``/confirms`` GET reports
    ``dealStatus=REJECTED`` — terminal rejection, no live order row.
    """
    broker, store, ctx = _open_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
        ('confirms/REF-RJ', 'get'): {
            'dealStatus': 'REJECTED', 'status': 'REJECTED',
            'reason': 'INSUFFICIENT_FUNDS',
        },
    })
    ctx.upsert_order(
        'coid-rej', symbol='EURUSD', side='buy', qty=1.0,
        state='server_ref_seen', pine_entry_id='Long', intent_key='Long',
        extras={'kind': 'position', 'order_type': 'market',
                'deal_reference': 'REF-RJ'},
    )
    ctx.add_ref('coid-rej', 'deal_reference', 'REF-RJ')

    asyncio.run(broker._recover_in_flight_submissions())

    _assert_rejected_contract(
        ctx, coid='coid-rej', recovery_path='confirm_get_rejected',
    )
    store.close()


def __test_recovery_contract_confirm_get_stale_deal_rejects__(tmp_path):
    """``server_ref_seen`` row whose accepted confirm points at a deal
    absent from both account snapshots — the confirms cache outlives
    the deal, so a stale confirm from a long-dead submission must not
    fabricate a live unfilled order (which would trip the
    unexpected-cancel quarantine seconds later). Terminal rejection.
    """
    broker, store, ctx = _open_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
        ('confirms/REF-ST', 'get'): {
            'dealStatus': 'ACCEPTED', 'status': 'OPEN',
            'dealId': 'DEAL-STALE', 'level': 1.1, 'size': 1.0,
        },
    })
    ctx.upsert_order(
        'coid-stale', symbol='EURUSD', side='buy', qty=1.0,
        state='server_ref_seen', pine_entry_id='Long', intent_key='Long',
        extras={'kind': 'position', 'order_type': 'market',
                'deal_reference': 'REF-ST'},
    )
    ctx.add_ref('coid-stale', 'deal_reference', 'REF-ST')

    asyncio.run(broker._recover_in_flight_submissions())

    _assert_rejected_contract(
        ctx, coid='coid-stale', recovery_path='confirm_get_stale_deal',
    )
    store.close()


def __test_recovery_contract_journal_resolutions__(tmp_path):
    """All four recovery paths route through the journal and tag the
    canonical ``recovered_confirmed`` event with the right ``recovery_path``.

    One pass over four pending rows — each row designed to hit exactly
    one of the resolution branches. Asserts:

    * the row's end-state contract (state, exchange_order_id, deal_id ref),
    * the ``recovered_confirmed`` audit event carries the matching
      ``recovery_path`` payload key,
    * the plugin-specific event kinds removed in M3
      (``recovery_promoted_stored_ref`` etc.) are *absent*.
    """
    import datetime as _dt
    ts_ms = int(_dt.datetime(2026, 4, 21, 10, 0, 0,
                             tzinfo=_dt.UTC).timestamp() * 1000)
    broker, store, ctx = _open_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            # row 1 stored-ref hit
            {'position': {'dealReference': 'REF-A', 'dealId': 'DEAL-A'}},
            # row 3 ttl-fallback hit
            {'position': {'dealReference': 'REF-C', 'dealId': 'DEAL-C'}},
            # row 4 confirm-get hit — the deal must be alive in a
            # snapshot for the direct confirm to count.
            {'position': {'dealId': 'DEAL-D'}},
        ]},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': [
            # row 2 activity-single-match candidate
            {'epic': 'EURUSD', 'details': {'direction': 'BUY', 'size': 1.0},
             'dateUTC': '2026-04-21T10:00:00.000', 'dealId': 'DEAL-B',
             'type': 'POSITION', 'status': 'EXECUTED'},
        ]},
        # row 3 — TTL expired on direct confirm
        ('error', 'confirms/REF-C', 'get'): OrderNotFoundError(
            "Capital confirm REF-C not found (TTL expired)",
            ref_type='deal_reference',
        ),
        # row 4 — confirm GET resolves directly
        ('confirms/REF-D', 'get'): {
            'dealStatus': 'ACCEPTED', 'status': 'OPEN',
            'dealId': 'DEAL-D', 'level': 1.1, 'size': 1.0,
        },
    })

    # row 1 — stored deal_reference matches positions snapshot.
    ctx.upsert_order(
        'coid-A', symbol='EURUSD', side='buy', qty=1.0,
        state='submitted', pine_entry_id='Long', intent_key='Long',
        extras={'kind': 'position', 'order_type': 'market',
                'deal_reference': 'REF-A'},
    )
    ctx.add_ref('coid-A', 'deal_reference', 'REF-A')

    # row 2 — no stored ref, activity heuristic single match.
    ctx.upsert_order(
        'coid-B', symbol='EURUSD', side='buy', qty=1.0,
        state='submitted', pine_entry_id='Long', intent_key='Long',
        extras={'kind': 'position', 'order_type': 'market'},
    )
    ctx._store._conn.execute(
        "UPDATE orders SET created_ts_ms = ? WHERE client_order_id = ?",
        (ts_ms, 'coid-B'),
    )

    # row 3 — server_ref_seen, /confirms TTL-expired, snapshot fallback.
    ctx.upsert_order(
        'coid-C', symbol='EURUSD', side='buy', qty=1.0,
        state='server_ref_seen', pine_entry_id='Long', intent_key='Long',
        extras={'kind': 'position', 'order_type': 'market',
                'deal_reference': 'REF-C'},
    )
    ctx.add_ref('coid-C', 'deal_reference', 'REF-C')

    # row 4 — server_ref_seen, /confirms GET resolves with dealId.
    ctx.upsert_order(
        'coid-D', symbol='EURUSD', side='buy', qty=1.0,
        state='server_ref_seen', pine_entry_id='Long', intent_key='Long',
        extras={'kind': 'position', 'order_type': 'market',
                'deal_reference': 'REF-D'},
    )
    ctx.add_ref('coid-D', 'deal_reference', 'REF-D')

    asyncio.run(broker._recover_in_flight_submissions())

    # Each row landed in the right end-state.
    _assert_confirmed_contract(ctx, coid='coid-A', deal_id='DEAL-A')
    _assert_confirmed_contract(ctx, coid='coid-B', deal_id='DEAL-B')
    _assert_confirmed_contract(ctx, coid='coid-C', deal_id='DEAL-C')
    _assert_confirmed_contract(ctx, coid='coid-D', deal_id='DEAL-D')

    # The canonical ``recovered_confirmed`` event carries the plugin's
    # ``recovery_path`` annotation — that is the load-bearing forensic
    # signal in M3 (the legacy plugin-specific event kinds are gone).
    expected_paths = {
        'coid-A': 'stored_ref',
        'coid-B': 'activity_single_match',
        'coid-C': 'ttl_fallback_snapshot',
        'coid-D': 'confirm_get_direct',
    }
    observed_paths: dict[str, str] = {}
    for payload in ctx.iter_events_by_kind_since('recovered_confirmed', 0):
        coid = payload.get('client_order_id')
        path = payload.get('recovery_path')
        if isinstance(coid, str) and isinstance(path, str):
            observed_paths[coid] = path
    # iter_events_by_kind_since may not include client_order_id in payload
    # for older event writers; fall back to SQL when needed.
    if not observed_paths:
        cur = ctx._store._conn.execute(
            "SELECT client_order_id, payload FROM events "
            "WHERE kind = 'recovered_confirmed'",
        )
        import json as _json
        for coid_val, payload_blob in cur.fetchall():
            data = _json.loads(payload_blob) if payload_blob else {}
            path = data.get('recovery_path')
            if isinstance(path, str):
                observed_paths[coid_val] = path
    assert observed_paths == expected_paths, (
        f"recovery_path mismatch:\nexpected={expected_paths}\n"
        f"observed={observed_paths}"
    )

    # Plugin-specific event kinds retired in M3 must not appear.
    cur = ctx._store._conn.execute(
        "SELECT DISTINCT kind FROM events WHERE kind LIKE 'recovery_%'"
    )
    retired_kinds = {row[0] for row in cur.fetchall()}
    assert retired_kinds == set(), (
        f"Legacy plugin recovery event kinds resurfaced: {retired_kinds}"
    )

    store.close()


def __test_recovery_contract_working_order_stored_ref__(tmp_path):
    """LIMIT entry (working order) recovery via stored ``deal_reference``.

    Pins that ``extras['kind'] = 'working'`` rows route through the
    journal just like ``'position'`` rows, and that ``recovery_path``
    is ``'stored_ref'`` with ``matched_snapshot='working'`` in the
    context payload.
    """
    broker, store, ctx = _open_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': [
            {'workingOrderData': {
                'dealReference': 'REF-WO',
                'dealId': 'DEAL-WO',
            }},
        ]},
        ('history/activity', 'get'): {'activities': []},
    })
    ctx.upsert_order(
        'coid-wo', symbol='EURUSD', side='buy', qty=1.0,
        state='submitted', pine_entry_id='Long', intent_key='Long',
        extras={'kind': 'working', 'order_type': 'limit',
                'deal_reference': 'REF-WO'},
    )
    ctx.add_ref('coid-wo', 'deal_reference', 'REF-WO')

    asyncio.run(broker._recover_in_flight_submissions())

    _assert_confirmed_contract(ctx, coid='coid-wo', deal_id='DEAL-WO')

    # Inspect the recovered_confirmed event payload directly.
    import json as _json
    cur = ctx._store._conn.execute(
        "SELECT payload FROM events "
        "WHERE kind = 'recovered_confirmed' AND client_order_id = ?",
        ('coid-wo',),
    )
    rows = cur.fetchall()
    assert len(rows) == 1, (
        f"Expected exactly one recovered_confirmed event for coid-wo, "
        f"got {len(rows)}"
    )
    payload = _json.loads(rows[0][0]) if rows[0][0] else {}
    assert payload.get('recovery_path') == 'stored_ref'
    ctx_payload = payload.get('recovery_context') or {}
    assert ctx_payload.get('matched_snapshot') == 'working', (
        f"Expected matched_snapshot='working' in recovery_context, "
        f"got {ctx_payload!r}"
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


def __test_startup_note_is_scoped_to_the_run_epic__(tmp_path):
    """The account-wide positions snapshot is narrowed to the run's epic.

    An untracked deal on the run's epic is logged with a
    ``foreign_deal_on_epic_at_startup`` audit event; a deal on any other
    instrument is not this run's business at all (observed live: a lane
    switched from EURUSD to BTCUSD once seeded the old instrument's orphaned
    short into the fresh run's journal). Neither seeds a row.
    """
    broker, store, ctx = _open_broker(tmp_path)
    broker.symbol = "BTCUSD"

    broker._note_untracked_positions_at_startup({
        "positions": [
            {
                "market": {"epic": "EURUSD"},
                "position": {"dealId": "deal-eur", "direction": "SELL",
                             "size": 200.0},
            },
            {
                "market": {"epic": "BTCUSD"},
                "position": {"dealId": "deal-btc", "direction": "BUY",
                             "size": 0.01},
            },
        ],
    })

    assert list(ctx.iter_live_orders()) == []
    noted = [
        row["exchange_order_id"] for row in ctx._store._conn.execute(
            "SELECT exchange_order_id FROM events "
            "WHERE run_instance_id = ? AND kind = 'foreign_deal_on_epic_at_startup'",
            (ctx.run_instance_id,),
        )
    ]
    assert noted == ["deal-btc"]
    assert broker._foreign_deals_logged == {"deal-btc"}
    store.close()


def _measured_entry_activities() -> list[dict]:
    """The ``history/activity`` rows one market POST leaves, as measured.

    Captured from the Capital.com demo (2026-10-11): the market entry's
    ``POSITION`` open, the ``WORKING_ORDER`` it executed through, and a
    resting limit entry dispatched in the same second — all BUY 0.01 within
    one second, with direction and size inside ``details`` only.
    """
    return [
        {'dateUTC': '2026-10-11T09:26:01.966', 'epic': 'BTCUSD',
         'dealId': 'DEAL-POS', 'source': 'USER', 'type': 'POSITION',
         'status': 'ACCEPTED',
         'details': {'dealReference': 'p_DEAL-POS', 'workingOrderId': 'DEAL-MKT-WO',
                     'size': 0.01, 'direction': 'BUY', 'level': 83011.0}},
        {'dateUTC': '2026-10-11T09:26:01.966', 'epic': 'BTCUSD',
         'dealId': 'DEAL-MKT-WO', 'source': 'USER', 'type': 'WORKING_ORDER',
         'status': 'EXECUTED',
         'details': {'dealReference': 'o_market', 'size': 0.01, 'direction': 'BUY',
                     'level': 0}},
        {'dateUTC': '2026-10-11T09:26:01.348', 'epic': 'BTCUSD',
         'dealId': 'DEAL-LIMIT', 'source': 'USER', 'type': 'WORKING_ORDER',
         'status': 'CREATED',
         'details': {'dealReference': 'o_limit', 'size': 0.01, 'direction': 'BUY',
                     'level': 82693.05}},
    ]


def _submitted_btc_row(ctx, coid: str, kind: str, order_type: str) -> None:
    import datetime as _dt
    ts_ms = int(_dt.datetime(2026, 10, 11, 9, 26, 1, 660000,
                             tzinfo=_dt.UTC).timestamp() * 1000)
    ctx.upsert_order(
        coid, symbol='BTCUSD', side='buy', qty=0.01,
        state='submitted', pine_entry_id='L', intent_key='L',
        extras={'kind': kind, 'order_type': order_type},
    )
    ctx._store._conn.execute(
        "UPDATE orders SET created_ts_ms = ? WHERE client_order_id = ?",
        (ts_ms, coid),
    )


def __test_recovery_resolves_interrupted_market_entry_from_measured_activity__(tmp_path):
    """A market POST cut off before its response is owned again after restart.

    The run stopped right after the POST (no ``dealReference`` recorded) while
    the venue opened the position. Recovery must pick the ``POSITION`` open
    among the three same-size activities, and credit the fill from the live
    snapshot: otherwise startup adoption files the run's own exposure as
    another run's (measured live: capitalcom cycle 269, 2026-10-11).
    """
    broker, store, ctx = _open_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            {'market': {'epic': 'BTCUSD'},
             'position': {'dealId': 'DEAL-POS', 'dealReference': 'p_DEAL-POS',
                          'direction': 'BUY', 'size': 0.01, 'level': 83011.0}},
        ]},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': _measured_entry_activities()},
    })
    broker.symbol = 'BTCUSD'
    _submitted_btc_row(ctx, 'coid-market', 'position', 'market')

    asyncio.run(broker._recover_in_flight_submissions())

    _assert_confirmed_contract(ctx, coid='coid-market', deal_id='DEAL-POS')
    row = ctx.get_order('coid-market')
    assert row.filled_qty == 0.01
    assert row.extras.get('confirm_level') == 83011.0
    assert row.extras.get('entry_filled_at')
    assert not any(
        live.extras.get('adopted_startup') for live in ctx.iter_live_orders()
    )
    store.close()


def __test_recovery_resolves_interrupted_resting_entry_by_created_working_order__(tmp_path):
    """A resting entry is traced by the working order it created, unfilled."""
    broker, store, ctx = _open_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': [
            {'marketData': {'epic': 'BTCUSD'},
             'workingOrderData': {'dealId': 'DEAL-LIMIT', 'direction': 'BUY',
                                  'orderSize': 0.01, 'orderLevel': 82693.05}},
        ]},
        ('history/activity', 'get'): {'activities': _measured_entry_activities()},
    })
    broker.symbol = 'BTCUSD'
    _submitted_btc_row(ctx, 'coid-limit', 'working', 'limit')

    asyncio.run(broker._recover_in_flight_submissions())

    _assert_confirmed_contract(ctx, coid='coid-limit', deal_id='DEAL-LIMIT')
    assert ctx.get_order('coid-limit').filled_qty == 0.0
    store.close()


def __test_recovery_skips_activity_of_a_deal_another_row_tracks__(tmp_path):
    """A deal already journaled under another row is no candidate."""
    broker, store, ctx = _open_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': _measured_entry_activities()},
    })
    broker.symbol = 'BTCUSD'
    ctx.upsert_order(
        'coid-other', symbol='BTCUSD', side='buy', qty=0.01,
        state='confirmed', pine_entry_id='L2', intent_key='L2',
        exchange_order_id='DEAL-POS', extras={'kind': 'position'},
    )
    ctx.add_ref('coid-other', 'deal_id', 'DEAL-POS')
    _submitted_btc_row(ctx, 'coid-market', 'position', 'market')

    asyncio.run(broker._recover_in_flight_submissions())

    assert ctx.get_order('coid-market').state == 'submitted'
    store.close()


def __test_recovery_confirm_get_direct_credits_live_market_fill__(tmp_path):
    """A recovered market entry whose deal is open carries its fill."""
    broker, store, ctx = _open_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': [
            {'market': {'epic': 'EURUSD'},
             'position': {'dealId': 'DEAL-4', 'direction': 'BUY', 'size': 1.0,
                          'level': 1.1}},
        ]},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': []},
        ('confirms/REF-4', 'get'): {
            'dealStatus': 'ACCEPTED', 'status': 'OPEN',
            'dealId': 'DEAL-4', 'level': 1.1, 'size': 1.0,
        },
    })
    broker.symbol = 'EURUSD'
    ctx.upsert_order(
        'coid-confirm', symbol='EURUSD', side='buy', qty=1.0,
        state='server_ref_seen', pine_entry_id='Long', intent_key='Long',
        extras={'kind': 'position', 'order_type': 'market',
                'deal_reference': 'REF-4'},
    )
    ctx.add_ref('coid-confirm', 'deal_reference', 'REF-4')

    asyncio.run(broker._recover_in_flight_submissions())

    _assert_confirmed_contract(ctx, coid='coid-confirm', deal_id='DEAL-4')
    row = ctx.get_order('coid-confirm')
    assert row.filled_qty == 1.0
    assert row.extras.get('confirm_level') == 1.1
    store.close()


def __test_recovery_counts_one_candidate_per_deal__(tmp_path):
    """Several activities of one deal are one candidate, not an ambiguity."""
    activities = _measured_entry_activities()
    activities.append({
        'dateUTC': '2026-10-11T09:26:01.500', 'epic': 'BTCUSD',
        'dealId': 'DEAL-LIMIT', 'source': 'USER', 'type': 'WORKING_ORDER',
        'status': 'ACCEPTED',
        'details': {'size': 0.01, 'direction': 'BUY', 'level': 82693.05},
    })
    broker, store, ctx = _open_broker(tmp_path, responses={
        ('positions', 'get'): {'positions': []},
        ('workingorders', 'get'): {'workingOrders': []},
        ('history/activity', 'get'): {'activities': activities},
    })
    broker.symbol = 'BTCUSD'
    _submitted_btc_row(ctx, 'coid-limit', 'working', 'limit')

    asyncio.run(broker._recover_in_flight_submissions())

    _assert_confirmed_contract(ctx, coid='coid-limit', deal_id='DEAL-LIMIT')
    store.close()
