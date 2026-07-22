"""Opt-in offline conformance scenarios for the Capital.com broker plugin."""

import asyncio
import threading
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pynecore.core.broker.exceptions import ExchangeConnectionError
from pynecore.core.broker.models import LegType, OrderStatus, OrderType
from pynecore.core.ohlcv_file import OHLCVReader
from pynecore.testing.broker_lab import Scenario, Step, pairwise_cases
from pynecore.testing.broker_lab.reference import ReferenceVenueProfile, VenueOrder
from pynecore_capitalcom import CapitalCom, CapitalComConfig
from pynecore_capitalcom.exceptions import CapitalComError

_RULES = {
    "dealingRules": {
        "minStepDistance": {"value": 0.01},
        "minSizeIncrement": {"value": 0.01},
        "minDealSize": {"value": 0.01},
        "minNormalStopOrLimitDistance": {"value": 0.0001},
    },
    "instrument": {"lotSize": 0.01},
}


class OfflineCapitalCom(CapitalCom):
    """Real Capital.com execution code with an in-memory REST transport."""

    def __init__(
        self, profile: "CapitalComProfile", run_name: str, store_ctx: Any
    ) -> None:
        super().__init__(
            symbol=profile.symbol,
            timeframe=profile.timeframe,
            config=CapitalComConfig(
                demo=True,
                user_email="offline@example.invalid",
                api_key="offline",
                api_password="offline",
            ),
        )
        self.profile = profile
        self.run_name = run_name
        self.store_ctx = store_ctx
        self.rest_calls: list[tuple[str, str, dict[str, Any] | None]] = []
        self._references: dict[str, str] = {}

    def _matches_fault(self, endpoint: str, method: str) -> bool:
        return (
            not self.profile.fault_consumed
            and self.profile.fault_endpoint == endpoint
            and self.profile.fault_method == method
        )

    def _raise_pre_write_fault(self, endpoint: str, method: str) -> None:
        if not self._matches_fault(endpoint, method):
            return
        if not self.profile.fault_before_write:
            return
        self.profile.fault_consumed = True
        self.profile.fault_barriers.append((endpoint, method, False))
        raise ExchangeConnectionError("offline pre-write control fault")

    def _raise_post_write_fault(self, endpoint: str, method: str) -> None:
        if not self._matches_fault(endpoint, method):
            return
        self.profile.fault_consumed = True
        self.profile.fault_barriers.append((endpoint, method, True))
        raise ExchangeConnectionError("offline post-write response suppression")

    async def _call(self, endpoint: str, *, data=None, method: str = "post"):
        payload = dict(data) if data is not None else None
        self.rest_calls.append((endpoint, method, payload))
        self.profile.transport_calls.append((endpoint, method, payload))
        self._raise_pre_write_fault(endpoint, method)
        if endpoint == f"markets/{self.profile.symbol}" and method == "get":
            return _RULES
        if endpoint == "workingorders" and method == "post":
            ref = f"ref-{self.profile.state.new_id()}"
            deal_id = self.profile.state.new_id()
            self._references[ref] = deal_id
            assert payload is not None
            self.profile.working_orders[deal_id] = {
                "workingOrderData": {
                    "dealId": deal_id,
                    "dealReference": ref,
                    "direction": payload["direction"],
                    "orderType": payload["type"],
                    "orderLevel": payload["level"],
                    "orderSize": payload["size"],
                    "createdDateUTC": "2026-07-21T00:00:00Z",
                },
                "market": {"epic": payload["epic"]},
            }
            now = datetime.now(UTC).isoformat()
            self.profile.polled_activities.append(
                {
                    "dateUTC": now,
                    "dealId": deal_id,
                    "dealReference": ref,
                    "epic": payload["epic"],
                    "direction": payload["direction"],
                    "size": payload["size"],
                    "source": "USER",
                    "status": "ACCEPTED",
                    "type": "WORKING_ORDER",
                }
            )
            self._raise_post_write_fault(endpoint, method)
            return {"dealReference": ref}
        if endpoint == "workingorders" and method == "get":
            return {"workingOrders": list(self.profile.working_orders.values())}
        if endpoint == "positions" and method == "post":
            ref = f"ref-{self.profile.state.new_id()}"
            deal_id = self.profile.state.new_id()
            self._references[ref] = deal_id
            if payload is not None:
                for raw in self.profile.positions.values():
                    position = raw["position"]
                    if position["direction"] == payload["direction"]:
                        continue
                    residual = float(position["size"]) - float(payload["size"])
                    if residual >= 0.0:
                        position["size"] = residual
                        break
            self._raise_post_write_fault(endpoint, method)
            return {"dealReference": ref}
        if endpoint == "positions" and method == "get":
            return {"positions": list(self.profile.positions.values())}
        if endpoint == "history/activity" and method == "get":
            return {"activities": list(self.profile.polled_activities)}
        if endpoint.startswith("positions/") and method == "delete":
            deal_id = endpoint.removeprefix("positions/")
            self.profile.positions.pop(deal_id, None)
            if self._matches_fault(endpoint, method):
                self.profile.state.position_owners[self.run_name] = 0.0
                self.profile.state.position = sum(
                    self.profile.state.position_owners.values()
                )
            self._raise_post_write_fault(endpoint, method)
            return {}
        if endpoint.startswith("positions/") and method == "put":
            deal_id = endpoint.removeprefix("positions/")
            raw = self.profile.positions.get(deal_id)
            if raw is not None and payload is not None:
                position = raw["position"]
                if "profitLevel" in payload:
                    position["profitLevel"] = payload["profitLevel"]
                if "stopLevel" in payload:
                    position["stopLevel"] = payload["stopLevel"]
                if "trailingStop" in payload:
                    position["trailingStop"] = payload["trailingStop"]
                if "stopDistance" in payload:
                    position["stopDistance"] = payload["stopDistance"]
            self._raise_post_write_fault(endpoint, method)
            return {"dealReference": "bracket-attach"}
        if endpoint.startswith("confirms/") and method == "get":
            ref = endpoint.removeprefix("confirms/")
            if ref == "bracket-attach":
                return {"dealStatus": "ACCEPTED"}
            return {
                "dealStatus": "ACCEPTED",
                "status": "OPEN",
                "dealId": self._references[ref],
            }
        raise AssertionError(
            f"unexpected offline Capital.com REST call: {method} {endpoint}"
        )

    async def execute_entry(self, envelope):
        orders = await super().execute_entry(envelope)
        for order in orders:
            self.profile.state.orders[order.id] = VenueOrder(
                order=order,
                run_name=self.run_name,
                pine_id=envelope.intent.pine_id,
                leg_type=LegType.ENTRY,
                intent_key=envelope.intent.intent_key,
            )
        self.profile.state.calls.append(
            (self.run_name, "entry", envelope.intent.intent_key)
        )
        return orders

    async def execute_exit(self, envelope):
        orders = await super().execute_exit(envelope)
        for order in orders:
            if order.order_type is OrderType.TRAILING_STOP:
                leg_type = LegType.TRAILING_STOP
            elif order.stop_price is not None:
                leg_type = LegType.STOP_LOSS
            else:
                leg_type = LegType.TAKE_PROFIT
            self.profile.state.orders[order.id] = VenueOrder(
                order=order,
                run_name=self.run_name,
                pine_id=envelope.intent.pine_id,
                leg_type=leg_type,
                intent_key=envelope.intent.intent_key,
                from_entry=envelope.intent.from_entry,
            )
        self.profile.state.calls.append(
            (self.run_name, "exit", envelope.intent.intent_key)
        )
        return orders

    async def execute_close(self, envelope):
        order = await super().execute_close(envelope)
        self.profile.state.orders[order.id] = VenueOrder(
            order=order,
            run_name=self.run_name,
            pine_id=envelope.intent.pine_id,
            leg_type=LegType.CLOSE,
            intent_key=envelope.intent.intent_key,
        )
        self.profile.state.calls.append(
            (self.run_name, "close", envelope.intent.intent_key)
        )
        return order

    async def execute_cancel(self, envelope):
        result = await super().execute_cancel(envelope)
        intent = envelope.intent
        for record in self.profile.state.orders.values():
            matches = record.run_name == self.run_name and (
                record.pine_id == intent.pine_id
                or (
                    getattr(intent, "from_entry", None) is not None
                    and record.from_entry == intent.from_entry
                )
            )
            if matches and record.order.status in (
                OrderStatus.OPEN,
                OrderStatus.PARTIALLY_FILLED,
            ):
                record.order = replace(
                    record.order,
                    status=OrderStatus.CANCELLED,
                    remaining_qty=0.0,
                )
        return result


class BrokenBracketClearCapitalCom(OfflineCapitalCom):
    """Control broker that acknowledges bracket clears without sending them."""

    async def _call(self, endpoint: str, *, data=None, method: str = "post"):
        payload = dict(data) if data is not None else None
        is_bracket_clear = (
            endpoint.startswith("positions/")
            and method == "put"
            and payload is not None
            and any(value is None for value in payload.values())
        )
        if is_bracket_clear:
            return {"dealReference": "bracket-attach"}
        return await super()._call(endpoint, data=data, method=method)


class CrossRunDeleteCapitalCom(OfflineCapitalCom):
    """Control broker that incorrectly deletes every run's position row."""

    async def _call(self, endpoint: str, *, data=None, method: str = "post"):
        response = await super()._call(endpoint, data=data, method=method)
        if endpoint.startswith("positions/") and method == "delete":
            self.profile.positions.clear()
        return response


class OfflinePriceCapitalCom(CapitalCom):
    """Real provider pagination over the shared lowest REST seam."""

    def __init__(
        self,
        *,
        bars: list[datetime],
        requested: list[datetime],
        ohlcv_dir: Path,
    ) -> None:
        super().__init__(
            symbol="EURUSD",
            timeframe="5",
            ohlcv_dir=ohlcv_dir,
            config=CapitalComConfig(
                demo=True,
                user_email="offline@example.invalid",
                api_key="offline",
                api_password="offline",
            ),
        )
        self._offline_bars = bars
        self._offline_requested = requested

    def __call__(
        self,
        endpoint: str,
        *,
        data: dict[str, Any] | None = None,
        method: str = "post",
        _level: int = 0,
    ) -> dict[str, Any]:
        del _level
        return asyncio.run(self._call(endpoint, data=data, method=method))

    async def _call(self, endpoint: str, *, data=None, method: str = "post"):
        if endpoint != "prices/EURUSD" or method != "get" or data is None:
            raise AssertionError(
                f"unexpected offline Capital.com REST call: {method} {endpoint}"
            )
        cursor = datetime.fromisoformat(str(data["from"]))
        self._offline_requested.append(cursor)
        selected = [bar for bar in self._offline_bars if bar >= cursor][
            : int(data["max"])
        ]
        return {"prices": [CapitalComProfile._price_row(bar) for bar in selected]}


class CapitalComProfile(ReferenceVenueProfile):
    """Netting profile around the real Capital.com broker implementation."""

    plugin_name = "capitalcom-offline-lab"
    symbol = "EURUSD"
    timeframe = "60"
    quantity_step = 0.01

    def __init__(self) -> None:
        super().__init__()
        self.working_orders: dict[str, dict[str, Any]] = {}
        self.positions: dict[str, dict[str, Any]] = {}
        self.activities: dict[str, dict[str, Any]] = {}
        self.polled_activities: list[dict[str, Any]] = []
        self.transport_calls: list[tuple[str, str, dict[str, Any] | None]] = []
        self.manual_close_events = 0
        self.fault_endpoint: str | None = None
        self.fault_method: str | None = None
        self.fault_consumed = False
        self.fault_before_write = False
        self.fault_barriers: list[tuple[str, str, bool]] = []

    def create_broker(self, run_name: str, store_ctx: Any) -> OfflineCapitalCom:
        return OfflineCapitalCom(self, run_name, store_ctx)

    def handle_step(self, runner: Any, step: Step) -> bool:
        if step.kind == "arm_capital_transport_fault":
            self.fault_endpoint = str(step.values["endpoint"])
            self.fault_method = str(step.values["method"])
            self.fault_consumed = False
            return True
        if step.kind == "expect_capital_post_write_barrier":
            endpoint = str(step.values["endpoint"])
            method = str(step.values["method"])
            matching = [
                barrier
                for barrier in self.fault_barriers
                if barrier[0] == endpoint and barrier[1] == method
            ]
            if len(matching) != 1:
                raise AssertionError(
                    f"expected one exact Capital.com transport barrier, got {matching}"
                )
            if not matching[0][2]:
                raise AssertionError(
                    "Capital.com post-write barrier fired before venue mutation"
                )
            return True
        if step.kind == "capital_recover_inflight":
            runtime = runner.runs[step.run]

            async def recover() -> None:
                await runtime.broker._recover_in_flight_submissions()
                async for event in runtime.broker._poll_once():
                    runtime.engine.on_order_event(event)

            asyncio.run(recover())
            runtime.engine.apply_async_events()
            return True
        if step.kind == "expect_capital_transport_attempts":
            endpoint = str(step.values["endpoint"])
            method = str(step.values["method"])
            expected = int(step.values.get("count", 1))
            actual = sum(
                call[0] == endpoint and call[1] == method
                for call in self.transport_calls
            )
            if actual != expected:
                raise AssertionError(
                    f"expected {expected} {method.upper()} {endpoint} attempts, got {actual}"
                )
            return True
        if step.kind == "capital_inject_transport_call":
            self.transport_calls.append(
                (
                    str(step.values["endpoint"]),
                    str(step.values["method"]),
                    dict(step.values.get("body") or {}),
                )
            )
            return True
        if step.kind == "expect_capital_recovered_working_order":
            runtime = runner.runs[step.run]
            if len(self.working_orders) != 1:
                raise AssertionError(
                    f"expected one venue working order, got {self.working_orders}"
                )
            rows = [
                row
                for row in runtime.store_ctx.iter_live_orders(symbol=self.symbol)
                if (row.extras or {}).get("kind") == "working"
            ]
            if len(rows) != 1 or rows[0].state != "confirmed":
                raise AssertionError(
                    f"Capital.com POST disposition did not recover: {rows}"
                )
            return True
        if step.kind == "expect_capital_run_position":
            activity = self.activities.get(step.run)
            if activity is None:
                raise AssertionError(
                    f"Capital.com run {step.run} has no owned entry activity"
                )
            deal_id = str(activity["dealId"])
            raw = self.positions.get(deal_id)
            expected_present = bool(step.values.get("present", True))
            if expected_present and raw is None:
                raise AssertionError(
                    f"Capital.com cross-run ownership violation: run {step.run} "
                    f"position {deal_id} disappeared"
                )
            if not expected_present and raw is not None:
                raise AssertionError(
                    f"Capital.com run {step.run} position {deal_id} still exists"
                )
            if raw is None:
                return True
            position = raw.get("position") or {}
            for key, value in step.values.items():
                if key == "present":
                    continue
                if position.get(key) != value:
                    raise AssertionError(
                        f"Capital.com run {step.run} expected {key}={value!r}, "
                        f"got {position.get(key)!r}"
                    )
            return True
        if step.kind == "expect_capital_run_store_deals":
            runtime = runner.runs[step.run]
            owner_runs = [str(value) for value in step.values["owners"]]
            expected = {
                str(self.activities[owner]["dealId"])
                for owner in owner_runs
            }
            actual = {
                str(row.exchange_order_id)
                for row in runtime.store_ctx.iter_live_orders(symbol=self.symbol)
                if row.exchange_order_id
                and (row.extras or {}).get("kind") == "position"
            }
            if actual != expected:
                raise AssertionError(
                    f"Capital.com cross-run store ownership violation: run "
                    f"{step.run} owns deals {sorted(actual)}, expected "
                    f"{sorted(expected)}"
                )
            return True
        if step.kind == "capital_inject_foreign_store_position":
            runtime = runner.runs[step.run]
            foreign_run = str(step.values["foreign_run"])
            activity = self.activities[foreign_run]
            deal_id = str(activity["dealId"])
            position = self.positions[deal_id]["position"]
            client_order_id = f"__control_foreign__{deal_id}"
            runtime.store_ctx.upsert_order(
                client_order_id,
                symbol=self.symbol,
                side=str(position["direction"]).lower(),
                qty=float(position["size"]),
                filled_qty=float(position["size"]),
                state="confirmed",
                exchange_order_id=deal_id,
                extras={"kind": "position", "control_foreign": True},
            )
            return True
        if step.kind == "capital_concurrent_session_bootstrap":
            import pynecore_capitalcom.rest as capital_rest

            class StubResponse:
                is_error = False

                def __init__(
                    self,
                    body: dict[str, Any],
                    headers: dict[str, str] | None = None,
                ) -> None:
                    self.body = body
                    self.headers = headers or {}

                def json(self) -> dict[str, Any]:
                    return self.body

            original_get = capital_rest.httpx.get
            original_post = capital_rest.httpx.post
            original_encrypt = capital_rest.encrypt_password
            post_started: list[float] = []
            transport_lock = threading.Lock()

            def fake_get(url: str, **kwargs: Any) -> StubResponse:
                del url, kwargs
                return StubResponse({"encryptionKey": "offline", "timeStamp": 1})

            def fake_post(url: str, **kwargs: Any) -> StubResponse:
                del url, kwargs
                with transport_lock:
                    started = time.monotonic()
                    if (
                        post_started
                        and started - post_started[-1]
                        < capital_rest._SESSION_CREATE_MIN_INTERVAL_S
                    ):
                        raise CapitalComError(
                            "API error occured: error.too-many.requests"
                        )
                    post_started.append(started)
                    number = len(post_started)
                return StubResponse(
                    {"currentAccountId": "offline-account"},
                    {
                        "CST": f"cst-{number}",
                        "X-SECURITY-TOKEN": f"security-{number}",
                    },
                )

            config = CapitalComConfig(
                demo=True,
                user_email="offline@example.invalid",
                api_key="shared-offline-key",
                api_password="offline",
            )
            brokers = [CapitalCom(config=config), CapitalCom(config=config)]
            errors: list[BaseException] = []

            def login(broker: CapitalCom) -> None:
                try:
                    broker.create_session()
                except BaseException as exc:
                    errors.append(exc)

            capital_rest.httpx.get = fake_get
            capital_rest.httpx.post = fake_post
            capital_rest.encrypt_password = lambda *_args, **_kwargs: "encrypted"
            try:
                workers = [
                    threading.Thread(target=login, args=(broker,)) for broker in brokers
                ]
                for worker in workers:
                    worker.start()
                for worker in workers:
                    worker.join(timeout=3.0)
            finally:
                capital_rest.httpx.get = original_get
                capital_rest.httpx.post = original_post
                capital_rest.encrypt_password = original_encrypt

            if errors:
                raise AssertionError(
                    f"concurrent Capital.com session bootstrap failed: {errors}"
                )
            if any(worker.is_alive() for worker in workers):
                raise AssertionError("concurrent Capital.com session bootstrap stalled")
            if len(post_started) != 2:
                raise AssertionError(
                    f"expected two session POSTs, got {len(post_started)}"
                )
            spacing = post_started[1] - post_started[0]
            if spacing < capital_rest._SESSION_CREATE_MIN_INTERVAL_S:
                raise AssertionError(
                    f"session POST spacing {spacing:.6f}s violated venue limit"
                )
            if {broker.account_id for broker in brokers} != {
                "capitalcom-demo-offline-account"
            }:
                raise AssertionError(
                    "concurrent sessions did not preserve account identity"
                )
            return True
        if step.kind == "expect_capital_request":
            broker = runner.runs[step.run].broker
            requests = [
                call for call in broker.rest_calls if call[0] == "workingorders"
            ]
            if not requests:
                raise AssertionError(
                    "Capital.com did not issue a working-order request"
                )
            body = requests[-1][2] or {}
            for key, value in step.values.items():
                if body.get(key) != value:
                    raise AssertionError(
                        f"expected Capital.com request {key}={value!r}, got {body.get(key)!r}"
                    )
            return True
        if step.kind == "expect_capital_position_post":
            broker = runner.runs[step.run].broker
            requests = [
                call
                for call in broker.rest_calls
                if call[0] == "positions" and call[1] == "post"
            ]
            expected_count = int(step.values.get("count", 1))
            if len(requests) != expected_count:
                raise AssertionError(
                    f"expected {expected_count} Capital.com position POSTs, got {len(requests)}"
                )
            body = requests[-1][2] or {}
            for key, value in step.values.items():
                if key != "count" and body.get(key) != value:
                    raise AssertionError(
                        f"expected Capital.com position POST {key}={value!r}, "
                        f"got {body.get(key)!r}"
                    )
            return True
        if step.kind == "capital_activity_fill":
            runtime = runner.runs[step.run]
            entries = [
                record
                for record in self.state.orders.values()
                if record.run_name == step.run and record.leg_type is LegType.ENTRY
            ]
            if not entries:
                raise AssertionError("Capital.com activity requires a dispatched entry")
            record = entries[-1]
            deal_id = record.order.id
            direction = "BUY" if record.order.side == "buy" else "SELL"
            qty = record.order.qty
            price = float(step.values.get("price", 1.10))
            activity = {
                "dateUTC": "2026-07-21T10:00:00.000",
                "dealId": deal_id,
                "type": "POSITION",
                "status": "EXECUTED",
                "source": "DEALER",
                "details": {"direction": direction, "size": qty, "level": price},
            }
            previous_activity = self.activities.get(step.run)
            self.activities[step.run] = activity

            async def decode() -> tuple[list[Any], list[Any]]:
                first = [
                    event
                    async for event in runtime.broker._process_activity([activity])
                ]
                second = [
                    event
                    async for event in runtime.broker._process_activity([activity])
                ]
                return first, second

            first, second = asyncio.run(decode())
            if len(first) != 1 or second:
                raise AssertionError(
                    "Capital.com activity fingerprint did not deduplicate replay"
                )
            event = first[0]
            runtime.engine.on_order_event(event)
            runtime.engine.apply_async_events()
            record.order = event.order
            signed_qty = qty if direction == "BUY" else -qty
            current = self.state.position_owners.get(step.run, 0.0)
            updated = current + signed_qty
            self.state.position_owners[step.run] = updated
            self.state.position = sum(self.state.position_owners.values())
            if previous_activity is not None and current * updated < 0.0:
                self.positions.pop(str(previous_activity["dealId"]), None)
            self.positions[deal_id] = {
                "position": {
                    "dealId": deal_id,
                    "direction": "BUY" if updated >= 0.0 else "SELL",
                    "size": abs(updated),
                    "level": price,
                    "upl": 0.0,
                    "createdDateUTC": "2026-07-21T10:00:00Z",
                },
                "market": {"epic": self.symbol},
            }
            return True
        if step.kind == "capital_replay_entry_activity":
            runtime = runner.runs[step.run]
            activity = self.activities.get(step.run)
            if activity is None:
                raise AssertionError(
                    "Capital.com replay requires a prior activity fill"
                )

            async def replay() -> list[Any]:
                return [
                    event
                    async for event in runtime.broker._process_activity([activity])
                ]

            events = asyncio.run(replay())
            if events:
                raise AssertionError(
                    "Capital.com durable activity replay changed position after restart"
                )
            return True
        if step.kind == "capital_snapshot_fill_without_activity":
            runtime = runner.runs[step.run]
            entries = [
                record
                for record in self.state.orders.values()
                if record.run_name == step.run and record.leg_type is LegType.ENTRY
            ]
            if len(entries) != 1:
                raise AssertionError(f"expected one Capital.com entry, got {entries}")
            record = entries[0]
            qty = record.order.qty
            direction = "BUY" if record.order.side == "buy" else "SELL"
            self.positions[record.order.id] = {
                "position": {
                    "dealId": record.order.id,
                    "direction": direction,
                    "size": qty,
                    "level": float(step.values.get("price", 1.10)),
                    "upl": 0.0,
                    "createdDateUTC": "2026-07-21T10:00:00Z",
                },
                "market": {"epic": self.symbol},
            }
            self.working_orders.pop(record.order.id, None)

            async def poll() -> list[Any]:
                return [event async for event in runtime.broker._poll_once()]

            events = asyncio.run(poll())
            fills = [
                event for event in events if event.event_type in ("filled", "partial")
            ]
            if len(fills) != 1:
                raise AssertionError(
                    "Capital.com REST snapshot did not recover exactly one missing fill: "
                    f"events={events}, rows={list(runtime.store_ctx.iter_live_orders())}"
                )
            for event in events:
                runtime.engine.on_order_event(event)
            runtime.engine.apply_async_events()
            signed = qty if direction == "BUY" else -qty
            self.state.position_owners[step.run] = signed
            self.state.position = sum(self.state.position_owners.values())
            record.order = fills[0].order
            return True
        if step.kind == "capital_price_pagination_probe":
            page_size = 3
            start = datetime(2025, 1, 6, 9, 0)
            bars = [start + timedelta(minutes=5 * index) for index in range(8)]
            pagination_dir = runner.workdir / "capital-pagination"
            pagination_dir.mkdir(parents=True, exist_ok=True)
            requested: list[datetime] = []
            provider = OfflinePriceCapitalCom(
                bars=bars,
                requested=requested,
                ohlcv_dir=pagination_dir,
            )
            with provider:
                provider.download_ohlcv(
                    start,
                    start + timedelta(minutes=5 * len(bars)),
                    limit=page_size,
                )
            with OHLCVReader(str(provider.ohlcv_path)) as reader:
                actual = [bar.timestamp for bar in reader]
            expected = [int(bar.replace(tzinfo=UTC).timestamp()) for bar in bars]
            if actual != expected:
                raise AssertionError(
                    f"Capital.com pagination gap/overlap: {actual} != {expected}"
                )
            expected_cursors = [start, bars[3], bars[6]]
            if requested != expected_cursors:
                raise AssertionError(
                    f"Capital.com pagination cursors moved incorrectly: {requested}"
                )
            return True
        if step.kind == "capital_shared_ohlcv_reader_gap":
            from pynecore.cli.commands.run import _atomic_ohlcv_download_target

            start = datetime(2025, 1, 6, 9, 0)
            bars = [start, start + timedelta(minutes=5)]
            shared_dir = runner.workdir / "capital-shared-ohlcv"
            shared_dir.mkdir(parents=True, exist_ok=True)
            first = OfflinePriceCapitalCom(
                bars=bars,
                requested=[],
                ohlcv_dir=shared_dir,
            )
            with _atomic_ohlcv_download_target(first):
                with first as writer:
                    writer.seek(0)
                    writer.truncate()
                    first.download_ohlcv(start, bars[-1] + timedelta(minutes=5))

            truncated = threading.Event()
            allow_finish = threading.Event()

            def rewrite() -> None:
                second = OfflinePriceCapitalCom(
                    bars=bars,
                    requested=[],
                    ohlcv_dir=shared_dir,
                )
                with _atomic_ohlcv_download_target(second):
                    with second as writer:
                        writer.seek(0)
                        writer.truncate()
                        truncated.set()
                        if not allow_finish.wait(2.0):
                            raise AssertionError("OHLCV gap control timed out")
                        second.download_ohlcv(
                            start, bars[-1] + timedelta(minutes=5)
                        )

            worker = threading.Thread(target=rewrite, daemon=True)
            worker.start()
            if not truncated.wait(2.0):
                raise AssertionError("concurrent OHLCV writer did not reach truncate")
            try:
                with OHLCVReader(str(first.ohlcv_path)) as reader:
                    if reader.end_timestamp is None:
                        raise AssertionError(
                            "shared OHLCV reader observed header-only file"
                        )
            finally:
                allow_finish.set()
                worker.join(2.0)
            if worker.is_alive():
                raise AssertionError("concurrent OHLCV writer did not finish")
            return True
        if step.kind == "capital_header_only_ohlcv_control":
            start = datetime(2025, 1, 6, 9, 0)
            control_dir = runner.workdir / "capital-header-only-control"
            control_dir.mkdir(parents=True, exist_ok=True)
            provider = OfflinePriceCapitalCom(
                bars=[start], requested=[], ohlcv_dir=control_dir
            )
            with provider as writer:
                writer.seek(0)
                writer.truncate()
            with OHLCVReader(str(provider.ohlcv_path)) as reader:
                if reader.end_timestamp is None:
                    raise AssertionError(
                        "shared OHLCV reader observed header-only file"
                    )
            return True
        if step.kind == "capital_reconnect_history_repair_probe":
            start = datetime(2025, 1, 6, 9, 0)
            bars = [
                start + timedelta(minutes=5),
                start + timedelta(minutes=15),
                start + timedelta(minutes=20),
            ]
            repair_dir = runner.workdir / "capital-reconnect-history"
            repair_dir.mkdir(parents=True, exist_ok=True)
            requested: list[datetime] = []
            provider = OfflinePriceCapitalCom(
                bars=bars,
                requested=requested,
                ohlcv_dir=repair_dir,
            )
            provider._last_bar_timestamp = int(start.replace(tzinfo=UTC).timestamp())
            current_open = int(
                (start + timedelta(minutes=20)).replace(tzinfo=UTC).timestamp()
            )
            payloads = provider._fetch_reconnect_gap_payloads(current_open)
            actual = [int(payload["t"] / 1000) for payload in payloads]
            expected = [int(bar.replace(tzinfo=UTC).timestamp()) for bar in bars[:2]]
            if actual != expected:
                raise AssertionError(
                    "Capital.com reconnect did not restore exact REST history "
                    f"without inventing the venue gap: {actual} != {expected}"
                )
            if requested != [start + timedelta(minutes=5)]:
                raise AssertionError(
                    f"Capital.com reconnect history cursor mismatch: {requested}"
                )
            return True
        if step.kind == "capital_activity_close":
            runtime = runner.runs[step.run]
            entry_activity = self.activities.get(step.run)
            if entry_activity is None:
                raise AssertionError("Capital.com close requires a prior activity fill")
            deal_id = str(entry_activity["dealId"])
            entry_direction = str(entry_activity["details"]["direction"])
            qty = float(step.values.get("qty", entry_activity["details"]["size"]))
            direction = "SELL" if entry_direction == "BUY" else "BUY"
            activity = {
                "dateUTC": "2026-07-21T10:01:00.000",
                "dealId": deal_id,
                "type": "POSITION",
                "status": "EXECUTED",
                "source": "USER",
                "epic": self.symbol,
                "size": qty,
                "level": float(step.values.get("price", 1.10)),
                "details": {"direction": direction},
            }

            async def decode_close() -> list[Any]:
                positions_by_deal = (
                    {deal_id: self.positions[deal_id]}
                    if deal_id in self.positions
                    else None
                )
                return [
                    event
                    async for event in runtime.broker._process_activity(
                        [activity], positions_by_deal
                    )
                ]

            events = asyncio.run(decode_close())
            if len(events) != 1 or events[0].leg_type is not LegType.CLOSE:
                raise AssertionError(
                    f"Capital.com close activity did not yield one CLOSE event: {events}"
                )
            runtime.engine.on_order_event(events[0])
            runtime.engine.apply_async_events()
            signed = -qty if entry_direction == "BUY" else qty
            current = self.state.position_owners.get(step.run, 0.0)
            residual = current + signed
            if current != 0.0 and current * residual < 0.0:
                residual = 0.0
            self.state.position_owners[step.run] = residual
            self.state.position = sum(self.state.position_owners.values())
            return True
        if step.kind == "capital_external_manual_close":
            runtime = runner.runs[step.run]
            entry_activity = self.activities.get(step.run)
            if entry_activity is None:
                raise AssertionError(
                    "Capital.com external close requires a prior activity fill"
                )
            deal_id = str(entry_activity["dealId"])
            entry_direction = str(entry_activity["details"]["direction"])
            qty = abs(self.state.position_owners.get(step.run, 0.0))
            if qty <= 0.0:
                raise AssertionError("Capital.com external close requires exposure")
            activity = {
                "dateUTC": "2026-07-21T10:02:00.000",
                "dealId": deal_id,
                "type": "POSITION",
                "status": "EXECUTED",
                "source": "USER",
                "details": {
                    "direction": "SELL" if entry_direction == "BUY" else "BUY",
                    "size": qty,
                    "level": float(step.values.get("price", 1.10)),
                },
            }
            self.positions.pop(deal_id, None)
            self.polled_activities = [activity]
            prior_intents = dict(runtime.engine._active_intents)

            async def poll() -> list[Any]:
                return [event async for event in runtime.broker._poll_once()]

            events = asyncio.run(poll())
            closes = [event for event in events if event.leg_type is LegType.CLOSE]
            if len(closes) != 1:
                raise AssertionError(
                    "Capital.com external close did not emit exactly one CLOSE event: "
                    f"{events}"
                )
            for event in events:
                runtime.engine.on_order_event(event)
            runtime.engine.apply_async_events()
            self.manual_close_events += len(closes)
            self.state.position_owners[step.run] = 0.0
            self.state.position = sum(self.state.position_owners.values())
            if getattr(self, "restore_retired_manual_close_intents", False):
                runtime.engine._active_intents.update(prior_intents)
            return True
        if step.kind == "expect_capital_external_close_retired":
            runtime = runner.runs[step.run]
            if self.manual_close_events != 1:
                raise AssertionError(
                    "Capital.com external close was not observed exactly once: "
                    f"{self.manual_close_events}"
                )
            if runtime.engine._active_intents:
                raise AssertionError(
                    "Capital.com external close did not retire strategy intents: "
                    f"{sorted(runtime.engine._active_intents)}"
                )
            position_posts = [
                call
                for call in self.transport_calls
                if call[0] == "positions" and call[1] == "post"
            ]
            if len(position_posts) != 1:
                raise AssertionError(
                    "Capital.com external close caused a defensive duplicate or reopen: "
                    f"{position_posts}"
                )
            if self.positions:
                raise AssertionError(
                    f"Capital.com venue was not flat after external close: {self.positions}"
                )
            return True
        if step.kind == "expect_capital_bracket_request":
            runtime = runner.runs[step.run]
            requests = [
                call
                for call in runtime.broker.rest_calls
                if call[0].startswith("positions/") and call[1] == "put"
            ]
            expected_count = int(step.values.get("count", 1))
            if len(requests) != expected_count:
                raise AssertionError(
                    f"expected {expected_count} Capital.com bracket PUTs, "
                    f"got {len(requests)}"
                )
            body = requests[-1][2] or {}
            for key, value in step.values.items():
                if key == "count":
                    continue
                if body.get(key) != value:
                    raise AssertionError(
                        f"expected Capital.com bracket {key}={value!r}, "
                        f"got {body.get(key)!r}"
                    )
            legs = [
                record.order
                for record in self.state.orders.values()
                if record.run_name == step.run
                and record.leg_type in (LegType.TAKE_PROFIT, LegType.STOP_LOSS)
                and record.order.status is OrderStatus.OPEN
            ]
            if len(legs) != 2 or not all(leg.reduce_only for leg in legs):
                raise AssertionError(
                    f"Capital.com bracket did not expose two reduce-only legs: {legs}"
                )
            return True
        if step.kind == "expect_capital_position_snapshot":
            runtime = runner.runs[step.run]
            snapshot = asyncio.run(runtime.broker._call("positions", method="get"))
            positions = snapshot.get("positions") or []
            if len(positions) != 1:
                raise AssertionError(
                    f"expected one Capital.com position, got {len(positions)}"
                )
            position = positions[0].get("position") or {}
            for key, value in step.values.items():
                if position.get(key) != value:
                    raise AssertionError(
                        f"expected Capital.com position {key}={value!r}, "
                        f"got {position.get(key)!r}"
                    )
            return True
        if step.kind == "expect_capital_bracket_clear_requests":
            requests = [
                call
                for call in self.transport_calls
                if call[0].startswith("positions/") and call[1] == "put"
            ]
            expected_bodies = [
                {"profitLevel": 1.20, "stopLevel": 1.05},
                {"profitLevel": None},
                {"stopLevel": None},
            ]
            actual_bodies = [call[2] for call in requests]
            if actual_bodies != expected_bodies:
                raise AssertionError(
                    "Capital.com bracket clear REST requests mismatch: "
                    f"expected {expected_bodies}, got {actual_bodies}"
                )
            return True
        if step.kind == "expect_capital_close_route":
            deletes = [
                call
                for call in self.transport_calls
                if call[0].startswith("positions/") and call[1] == "delete"
            ]
            position_posts = [
                call
                for call in self.transport_calls
                if call[0] == "positions" and call[1] == "post"
            ]
            bracket_puts = [
                call
                for call in self.transport_calls
                if call[0].startswith("positions/") and call[1] == "put"
            ]
            expected_deletes = int(step.values.get("deletes", 0))
            expected_posts = int(step.values.get("posts", 1))
            expected_bracket_puts = int(step.values.get("bracket_puts", 0))
            if len(deletes) != expected_deletes:
                raise AssertionError(
                    f"expected {expected_deletes} Capital.com position DELETEs, "
                    f"got {len(deletes)}"
                )
            if len(position_posts) != expected_posts:
                raise AssertionError(
                    f"expected {expected_posts} Capital.com position POSTs, "
                    f"got {len(position_posts)}"
                )
            if len(bracket_puts) != expected_bracket_puts:
                raise AssertionError(
                    f"expected {expected_bracket_puts} Capital.com bracket PUTs, "
                    f"got {len(bracket_puts)}: {bracket_puts}"
                )
            if "size" in step.values:
                body = position_posts[-1][2] or {}
                if body.get("size") != step.values["size"]:
                    raise AssertionError(
                        f"expected Capital.com close size={step.values['size']!r}, "
                        f"got {body.get('size')!r}"
                    )
            return True
        if step.kind == "expect_capital_trailing_request":
            runtime = runner.runs[step.run]
            requests = [
                call
                for call in runtime.broker.rest_calls
                if call[0].startswith("positions/") and call[1] == "put"
            ]
            expected_count = int(step.values["count"])
            if len(requests) != expected_count:
                raise AssertionError(
                    f"expected {expected_count} Capital.com trailing PUTs, got {len(requests)}"
                )
            if expected_count == 0:
                return True
            body = requests[-1][2] or {}
            if body.get("trailingStop") is not step.values["enabled"]:
                raise AssertionError(f"unexpected Capital.com trailing body: {body}")
            if (
                "distance" in step.values
                and body.get("stopDistance") != step.values["distance"]
            ):
                raise AssertionError(
                    f"expected trailing distance {step.values['distance']}, got {body.get('stopDistance')}"
                )
            return True
        if step.kind == "expect_capital_no_live_trailing":
            runtime = runner.runs[step.run]
            trailing_rows = [
                row
                for row in runtime.store_ctx.iter_live_orders(symbol=self.symbol)
                if (row.extras or {}).get("leg_kind") == "sl"
                and (
                    row.trailing_stop
                    or row.trailing_distance is not None
                    or (row.extras or {}).get("trail_offset") is not None
                )
            ]
            if trailing_rows:
                raise AssertionError(
                    f"Capital.com trailing protection resurrected: {trailing_rows}"
                )
            return True
        if step.kind == "capital_trailing_tick":
            runtime = runner.runs[step.run]
            for raw in self.positions.values():
                raw["market"].update(
                    {
                        "bid": float(step.values["bid"]),
                        "offer": float(step.values["offer"]),
                    }
                )
                if "active" in step.values:
                    raw["position"]["trailingStop"] = bool(step.values["active"])

            async def monitor() -> None:
                await runtime.broker._trailing_activation_monitor(self.positions)

            asyncio.run(monitor())
            return True
        return super().handle_step(runner, step)

    @staticmethod
    def _price_row(timestamp: datetime) -> dict[str, Any]:
        price = {"bid": 1.0, "ask": 1.0001}
        return {
            "snapshotTimeUTC": timestamp.isoformat(),
            "openPrice": price,
            "highPrice": price,
            "lowPrice": price,
            "closePrice": price,
            "lastTradedVolume": 1.0,
        }


class BrokenBracketClearProfile(CapitalComProfile):
    """Control profile proving that bracket-clear wire assertions are independent."""

    def create_broker(
        self, run_name: str, store_ctx: Any
    ) -> BrokenBracketClearCapitalCom:
        return BrokenBracketClearCapitalCom(self, run_name, store_ctx)


class CrossRunDeleteProfile(CapitalComProfile):
    """Control profile proving that a close cannot sweep another run."""

    def create_broker(
        self, run_name: str, store_ctx: Any
    ) -> CrossRunDeleteCapitalCom:
        return CrossRunDeleteCapitalCom(self, run_name, store_ctx)


class BrokenManualCloseRetireProfile(CapitalComProfile):
    """Control profile proving that stale intent retention is detected."""

    restore_retired_manual_close_intents = True


class PreWriteFaultControlProfile(CapitalComProfile):
    """Control profile proving that the exact barrier follows venue mutation."""

    def __init__(self) -> None:
        super().__init__()
        self.fault_before_write = True


def smoke_scenarios(seed: int = 0) -> list[Scenario]:
    return [
        Scenario(
            name="capitalcom-concurrent-session-bootstrap-respects-venue-rate-limit",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(Step("capital_concurrent_session_bootstrap"),),
        ),
        Scenario(
            name="capitalcom-limit-entry-uses-real-rest-shape",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(
                Step(
                    "entry",
                    values={"id": "L", "side": "buy", "qty": 1.0, "limit": 1.09},
                ),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_capital_request",
                    values={"epic": "EURUSD", "direction": "BUY", "size": 1.0},
                ),
            ),
        ),
        Scenario(
            name="capitalcom-restart-does-not-repeat-working-order",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(
                Step(
                    "entry",
                    values={"id": "L", "side": "sell", "qty": 1.0, "limit": 1.11},
                ),
                Step("sync", values={"last_price": 1.10}),
                Step("restart"),
                Step(
                    "entry",
                    values={"id": "L", "side": "sell", "qty": 1.0, "limit": 1.11},
                ),
                Step("sync", values={"last_price": 1.10}),
                Step("expect", values={"calls": 1}),
            ),
        ),
        Scenario(
            name="capitalcom-netting-reversal-activity-closes-and-opens-exactly-once",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "Long", "side": "buy", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step("entry", values={"id": "Short", "side": "sell", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_capital_position_post",
                    values={"count": 2, "direction": "SELL", "size": 2.0},
                ),
                Step("capital_activity_fill", values={"price": 1.09}),
                Step("expect", values={"position": -1.0, "engine_position": -1.0}),
                Step("restart", check_invariants=False),
                Step("capital_replay_entry_activity"),
                Step("expect", values={"position": -1.0, "engine_position": -1.0}),
            ),
        ),
        Scenario(
            name="capitalcom-missing-activity-falls-back-to-rest-position-snapshot",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(
                Step(
                    "entry",
                    values={"id": "Gap", "side": "buy", "qty": 1.0, "limit": 1.09},
                ),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_snapshot_fill_without_activity", values={"price": 1.10}),
                Step("expect", values={"position": 1.0, "engine_position": 1.0}),
            ),
        ),
        Scenario(
            name="capitalcom-price-pagination-has-no-overlap-or-gap",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(Step("capital_price_pagination_probe"),),
        ),
        Scenario(
            name="capitalcom-concurrent-download-reader-never-sees-header-only-file",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(Step("capital_shared_ohlcv_reader_gap"),),
        ),
        Scenario(
            name="control-capitalcom-header-only-ohlcv-is-detected",
            profile_factory=CapitalComProfile,
            seed=seed,
            expected_violation="shared OHLCV reader observed header-only file",
            steps=(Step("capital_header_only_ohlcv_control"),),
        ),
        Scenario(
            name="capitalcom-reconnect-restores-real-history-and-preserves-session-gap",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(Step("capital_reconnect_history_repair_probe"),),
        ),
        Scenario(
            name="capitalcom-position-bracket-attach-replace-shape",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "Long", "side": "buy", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step(
                    "exit",
                    values={
                        "id": "TP_SL",
                        "from_entry": "Long",
                        "side": "sell",
                        "qty": 1.0,
                        "limit": 1.20,
                        "stop": 1.05,
                    },
                ),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "exit",
                    values={
                        "id": "TP_SL",
                        "from_entry": "Long",
                        "side": "sell",
                        "qty": 1.0,
                        "limit": 1.21,
                        "stop": 1.04,
                    },
                ),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_capital_bracket_request",
                    values={"count": 2, "profitLevel": 1.21, "stopLevel": 1.04},
                ),
            ),
        ),
        Scenario(
            name="capitalcom-activity-replay-is-deduplicated",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(
                Step(
                    "entry",
                    values={"id": "Activity", "side": "buy", "qty": 1.0},
                ),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step("expect", values={"position": 1.0, "engine_position": 1.0}),
            ),
        ),
        Scenario(
            name="capitalcom-post-write-post-loss-recovers-without-duplicate",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(
                Step(
                    "entry",
                    values={"id": "Pending", "side": "buy", "qty": 1.0, "limit": 1.09},
                ),
                Step(
                    "arm_capital_transport_fault",
                    values={"endpoint": "workingorders", "method": "post"},
                ),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_capital_post_write_barrier",
                    values={"endpoint": "workingorders", "method": "post"},
                ),
                Step("capital_recover_inflight"),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_capital_transport_attempts",
                    values={"endpoint": "workingorders", "method": "post", "count": 1},
                ),
                Step("expect_capital_recovered_working_order"),
            ),
        ),
        Scenario(
            name="control-capitalcom-post-barrier-before-write-is-detected",
            profile_factory=PreWriteFaultControlProfile,
            seed=seed,
            expected_violation=(
                "Capital.com post-write barrier fired before venue mutation"
            ),
            steps=(
                Step(
                    "entry",
                    values={"id": "Pending", "side": "buy", "qty": 1.0, "limit": 1.09},
                ),
                Step(
                    "arm_capital_transport_fault",
                    values={"endpoint": "workingorders", "method": "post"},
                ),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_capital_post_write_barrier",
                    values={"endpoint": "workingorders", "method": "post"},
                ),
            ),
        ),
        Scenario(
            name="capitalcom-post-write-put-loss-reconciles-without-duplicate",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "Long", "side": "buy", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step(
                    "exit",
                    values={
                        "id": "Bracket",
                        "from_entry": "Long",
                        "side": "sell",
                        "qty": 1.0,
                        "limit": 1.20,
                        "stop": 1.05,
                    },
                ),
                Step(
                    "arm_capital_transport_fault",
                    values={"endpoint": "positions/lab-2", "method": "put"},
                ),
                Step("sync", values={"last_price": 1.10}, check_invariants=False),
                Step(
                    "expect_capital_post_write_barrier",
                    values={"endpoint": "positions/lab-2", "method": "put"},
                    check_invariants=False,
                ),
                Step("capital_recover_inflight", check_invariants=False),
                Step("sync", values={"last_price": 1.10}, check_invariants=False),
                Step(
                    "expect_capital_transport_attempts",
                    values={"endpoint": "positions/lab-2", "method": "put", "count": 1},
                    check_invariants=False,
                ),
                Step(
                    "expect_capital_position_snapshot",
                    values={"profitLevel": 1.20, "stopLevel": 1.05},
                    check_invariants=False,
                ),
            ),
        ),
        Scenario(
            name="control-capitalcom-put-barrier-before-write-is-detected",
            profile_factory=PreWriteFaultControlProfile,
            seed=seed,
            expected_violation=(
                "Capital.com post-write barrier fired before venue mutation"
            ),
            steps=(
                Step("entry", values={"id": "Long", "side": "buy", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step(
                    "exit",
                    values={
                        "id": "Bracket",
                        "from_entry": "Long",
                        "side": "sell",
                        "qty": 1.0,
                        "limit": 1.20,
                        "stop": 1.05,
                    },
                ),
                Step(
                    "arm_capital_transport_fault",
                    values={"endpoint": "positions/lab-2", "method": "put"},
                ),
                Step("sync", values={"last_price": 1.10}, check_invariants=False),
                Step(
                    "expect_capital_post_write_barrier",
                    values={"endpoint": "positions/lab-2", "method": "put"},
                ),
            ),
        ),
        Scenario(
            name="capitalcom-post-write-delete-loss-recovers-without-double-close",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "Long", "side": "buy", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step("close", values={"id": "Long", "qty": 1.0}),
                Step(
                    "arm_capital_transport_fault",
                    values={"endpoint": "positions/lab-2", "method": "delete"},
                ),
                Step("sync", values={"last_price": 1.10}, check_invariants=False),
                Step(
                    "expect_capital_post_write_barrier",
                    values={"endpoint": "positions/lab-2", "method": "delete"},
                    check_invariants=False,
                ),
                Step("restart", check_invariants=False),
                Step("capital_recover_inflight", check_invariants=False),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_capital_transport_attempts",
                    values={"endpoint": "positions/lab-2", "method": "delete", "count": 1},
                ),
                Step("expect", values={"position": 0.0, "engine_position": 0.0}),
            ),
        ),
        Scenario(
            name="control-capitalcom-delete-barrier-before-write-is-detected",
            profile_factory=PreWriteFaultControlProfile,
            seed=seed,
            expected_violation=(
                "Capital.com post-write barrier fired before venue mutation"
            ),
            steps=(
                Step("entry", values={"id": "Long", "side": "buy", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step("close", values={"id": "Long", "qty": 1.0}),
                Step(
                    "arm_capital_transport_fault",
                    values={"endpoint": "positions/lab-2", "method": "delete"},
                ),
                Step("sync", values={"last_price": 1.10}, check_invariants=False),
                Step(
                    "expect_capital_post_write_barrier",
                    values={"endpoint": "positions/lab-2", "method": "delete"},
                ),
            ),
        ),
        Scenario(
            name="capitalcom-external-manual-close-retires-intent-once",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "Long", "side": "buy", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step("capital_external_manual_close", values={"price": 1.11}),
                Step("expect", values={"position": 0.0, "engine_position": 0.0}),
                Step("sync", values={"last_price": 1.11}),
                Step("expect_capital_external_close_retired"),
                Step("restart", check_invariants=False),
                Step("sync", values={"last_price": 1.11}),
                Step("expect_capital_external_close_retired"),
            ),
        ),
        Scenario(
            name="control-capitalcom-external-close-stale-intent-is-detected",
            profile_factory=BrokenManualCloseRetireProfile,
            seed=seed,
            expected_violation=(
                "Capital.com external close did not retire strategy intents"
            ),
            steps=(
                Step("entry", values={"id": "Long", "side": "buy", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step("capital_external_manual_close", values={"price": 1.11}),
                Step("expect_capital_external_close_retired"),
            ),
        ),
        Scenario(
            name="capitalcom-marketable-limit-exit-uses-one-immediate-close",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "Long", "side": "buy", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step(
                    "exit",
                    values={
                        "id": "MarketableLimit",
                        "from_entry": "Long",
                        "side": "sell",
                        "qty": 1.0,
                        "limit": 1.09,
                    },
                ),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_capital_close_route",
                    values={"deletes": 1, "posts": 1},
                ),
                Step("capital_activity_close", values={"qty": 1.0, "price": 1.10}),
                Step("expect", values={"position": 0.0, "engine_position": 0.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("restart", check_invariants=False),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_capital_close_route",
                    values={"deletes": 1, "posts": 1},
                ),
            ),
        ),
        Scenario(
            name="capitalcom-marketable-stop-exit-uses-one-immediate-close",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "Long", "side": "buy", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step(
                    "exit",
                    values={
                        "id": "MarketableStop",
                        "from_entry": "Long",
                        "side": "sell",
                        "qty": 1.0,
                        "stop": 1.11,
                    },
                ),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_capital_close_route",
                    values={"deletes": 1, "posts": 1},
                ),
                Step("capital_activity_close", values={"qty": 1.0, "price": 1.10}),
                Step("expect", values={"position": 0.0, "engine_position": 0.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("restart", check_invariants=False),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_capital_close_route",
                    values={"deletes": 1, "posts": 1},
                ),
            ),
        ),
        Scenario(
            name="capitalcom-partial-marketable-exit-uses-exact-opposite-post",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "Long", "side": "buy", "qty": 2.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step(
                    "exit",
                    values={
                        "id": "Partial",
                        "from_entry": "Long",
                        "side": "sell",
                        "qty": 0.5,
                        "limit": 1.09,
                    },
                ),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_capital_close_route",
                    values={"deletes": 0, "posts": 2, "size": 0.5},
                ),
                Step("capital_activity_close", values={"qty": 0.5, "price": 1.10}),
                Step("expect", values={"position": 1.5, "engine_position": 1.5}),
            ),
        ),
        Scenario(
            name="capitalcom-protected-partial-close-preserves-venue-protection",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "Long", "side": "buy", "qty": 2.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step(
                    "exit",
                    values={
                        "id": "Bracket",
                        "from_entry": "Long",
                        "side": "sell",
                        "qty": 2.0,
                        "limit": 1.20,
                        "stop": 1.05,
                    },
                ),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_capital_position_snapshot",
                    values={"size": 2.0, "profitLevel": 1.20, "stopLevel": 1.05},
                ),
                Step("close", values={"id": "Long", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_capital_close_route",
                    values={
                        "deletes": 0,
                        "posts": 2,
                        "size": 1.0,
                        "bracket_puts": 1,
                    },
                ),
                Step(
                    "expect_capital_position_snapshot",
                    values={"size": 1.0, "profitLevel": 1.20, "stopLevel": 1.05},
                ),
                Step("capital_activity_close", values={"qty": 1.0, "price": 1.10}),
                Step("expect", values={"position": 1.0, "engine_position": 1.0}),
            ),
        ),
        Scenario(
            name="capitalcom-residual-full-close-uses-native-delete",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "Long", "side": "buy", "qty": 2.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step("close", values={"id": "Long", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_capital_close_route",
                    values={"deletes": 0, "posts": 2, "size": 1.0},
                ),
                Step("capital_activity_close", values={"qty": 1.0, "price": 1.10}),
                Step("expect", values={"position": 1.0, "engine_position": 1.0}),
                Step("close", values={"id": "Long", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}, check_invariants=False),
                Step(
                    "expect_capital_close_route",
                    values={"deletes": 1, "posts": 2},
                    check_invariants=False,
                ),
            ),
        ),
        Scenario(
            name="capitalcom-activity-replay-after-restart-is-durable",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "Durable", "side": "buy", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step("restart", check_invariants=False),
                Step("capital_replay_entry_activity"),
                Step("expect", values={"position": 1.0, "engine_position": 1.0}),
            ),
        ),
        Scenario(
            name="capitalcom-shared-netting-runs-adopt-and-close-only-own-ledger",
            profile_factory=CapitalComProfile,
            runs=("A", "B"),
            seed=seed,
            steps=(
                Step("entry", run="A", values={"id": "A", "side": "buy", "qty": 1.0}),
                Step("sync", run="A", values={"last_price": 1.10}),
                Step("capital_activity_fill", run="A", values={"price": 1.10}),
                Step("entry", run="B", values={"id": "B", "side": "buy", "qty": 1.0}),
                Step("sync", run="B", values={"last_price": 1.10}),
                Step("capital_activity_fill", run="B", values={"price": 1.10}),
                Step(
                    "exit",
                    run="A",
                    values={
                        "id": "A-Bracket",
                        "from_entry": "A",
                        "side": "sell",
                        "qty": 1.0,
                        "limit": 1.20,
                        "stop": 1.05,
                    },
                ),
                Step("sync", run="A", values={"last_price": 1.10}),
                Step(
                    "exit",
                    run="B",
                    values={
                        "id": "B-Bracket",
                        "from_entry": "B",
                        "side": "sell",
                        "qty": 1.0,
                        "limit": 1.21,
                        "stop": 1.04,
                    },
                ),
                Step("sync", run="B", values={"last_price": 1.10}),
                Step(
                    "exit",
                    run="A",
                    values={
                        "id": "A-Bracket",
                        "from_entry": "A",
                        "side": "sell",
                        "qty": 1.0,
                        "limit": 1.22,
                        "stop": 1.03,
                    },
                ),
                Step("sync", run="A", values={"last_price": 1.10}),
                Step(
                    "expect_capital_run_position",
                    run="A",
                    values={"size": 1.0, "profitLevel": 1.22, "stopLevel": 1.03},
                ),
                Step(
                    "expect_capital_run_position",
                    run="B",
                    values={"size": 1.0, "profitLevel": 1.21, "stopLevel": 1.04},
                ),
                Step("restart", run="A", check_invariants=False),
                Step("restart", run="B", check_invariants=False),
                Step(
                    "expect_capital_run_store_deals",
                    run="A",
                    values={"owners": ["A"]},
                ),
                Step(
                    "expect_capital_run_store_deals",
                    run="B",
                    values={"owners": ["B"]},
                ),
                Step(
                    "expect",
                    run="A",
                    values={
                        "position": 1.0,
                        "engine_position": 1.0,
                        "account_position": 2.0,
                    },
                ),
                Step(
                    "expect", run="B", values={"position": 1.0, "engine_position": 1.0}
                ),
                Step("close", run="A", values={"id": "A", "qty": 1.0}),
                Step("sync", run="A", values={"last_price": 1.10}),
                Step("capital_activity_close", run="A", values={"qty": 1.0}),
                Step(
                    "expect_capital_run_position",
                    run="A",
                    values={"present": False},
                    check_invariants=False,
                ),
                Step(
                    "expect_capital_run_position",
                    run="B",
                    values={"size": 1.0, "profitLevel": 1.21, "stopLevel": 1.04},
                    check_invariants=False,
                ),
                Step(
                    "expect",
                    run="A",
                    values={
                        "position": 0.0,
                        "engine_position": 0.0,
                        "account_position": 1.0,
                    },
                ),
                Step(
                    "expect", run="B", values={"position": 1.0, "engine_position": 1.0}
                ),
                Step("close", run="B", values={"id": "B", "qty": 1.0}),
                Step("sync", run="B", values={"last_price": 1.10}),
                Step("capital_activity_close", run="B", values={"qty": 1.0}),
                Step(
                    "expect",
                    run="B",
                    values={
                        "position": 0.0,
                        "engine_position": 0.0,
                        "account_position": 0.0,
                    },
                ),
            ),
        ),
        Scenario(
            name="capitalcom-restart-does-not-adopt-stopped-sibling-position",
            profile_factory=CapitalComProfile,
            runs=("A", "B"),
            seed=seed,
            steps=(
                Step("entry", run="A", values={"id": "A", "side": "buy", "qty": 1.0}),
                Step("sync", run="A", values={"last_price": 1.10}),
                Step("capital_activity_fill", run="A", values={"price": 1.10}),
                Step("entry", run="B", values={"id": "B", "side": "buy", "qty": 1.0}),
                Step("sync", run="B", values={"last_price": 1.10}),
                Step("capital_activity_fill", run="B", values={"price": 1.10}),
                Step("shutdown", run="A", check_invariants=False),
                Step("restart", run="B", check_invariants=False),
                Step("capital_recover_inflight", run="B", check_invariants=False),
                Step(
                    "expect_capital_run_store_deals",
                    run="B",
                    values={"owners": ["B"]},
                ),
            ),
        ),
        Scenario(
            name="capitalcom-filled-entry-recovery-does-not-post-again",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "B", "side": "buy", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step("restart", check_invariants=False),
                Step("capital_recover_inflight", check_invariants=False),
                Step("entry", values={"id": "B", "side": "buy", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_capital_transport_attempts",
                    values={"endpoint": "positions", "method": "post", "count": 1},
                ),
            ),
        ),
        Scenario(
            name="control-capitalcom-duplicate-recovery-post-is-detected",
            profile_factory=CapitalComProfile,
            seed=seed,
            expected_violation="expected 1 POST positions attempts, got 2",
            steps=(
                Step("entry", values={"id": "B", "side": "buy", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step(
                    "capital_inject_transport_call",
                    values={"endpoint": "positions", "method": "post"},
                    check_invariants=False,
                ),
                Step(
                    "expect_capital_transport_attempts",
                    values={"endpoint": "positions", "method": "post", "count": 1},
                ),
            ),
        ),
        Scenario(
            name="control-capitalcom-foreign-restart-adoption-is-detected",
            profile_factory=CapitalComProfile,
            runs=("A", "B"),
            seed=seed,
            expected_violation="Capital.com cross-run store ownership violation",
            steps=(
                Step("entry", run="A", values={"id": "A", "side": "buy", "qty": 1.0}),
                Step("sync", run="A", values={"last_price": 1.10}),
                Step("capital_activity_fill", run="A", values={"price": 1.10}),
                Step("entry", run="B", values={"id": "B", "side": "buy", "qty": 1.0}),
                Step("sync", run="B", values={"last_price": 1.10}),
                Step("capital_activity_fill", run="B", values={"price": 1.10}),
                Step("restart", run="B", check_invariants=False),
                Step("capital_recover_inflight", run="B", check_invariants=False),
                Step(
                    "capital_inject_foreign_store_position",
                    run="B",
                    values={"foreign_run": "A"},
                    check_invariants=False,
                ),
                Step(
                    "expect_capital_run_store_deals",
                    run="B",
                    values={"owners": ["B"]},
                ),
            ),
        ),
        Scenario(
            name="control-capitalcom-cross-run-delete-is-detected",
            profile_factory=CrossRunDeleteProfile,
            runs=("A", "B"),
            seed=seed,
            expected_violation="Capital.com cross-run ownership violation",
            steps=(
                Step("entry", run="A", values={"id": "A", "side": "buy", "qty": 1.0}),
                Step("sync", run="A", values={"last_price": 1.10}),
                Step("capital_activity_fill", run="A", values={"price": 1.10}),
                Step("entry", run="B", values={"id": "B", "side": "buy", "qty": 1.0}),
                Step("sync", run="B", values={"last_price": 1.10}),
                Step("capital_activity_fill", run="B", values={"price": 1.10}),
                Step("close", run="A", values={"id": "A", "qty": 1.0}),
                Step("sync", run="A", values={"last_price": 1.10}),
                Step("expect_capital_run_position", run="B", values={"size": 1.0}),
            ),
        ),
        Scenario(
            name="capitalcom-bracket-cancel-stays-cleared-after-restart",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "Long", "side": "buy", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step(
                    "exit",
                    values={
                        "id": "Bracket",
                        "from_entry": "Long",
                        "side": "sell",
                        "qty": 1.0,
                        "limit": 1.20,
                        "stop": 1.05,
                    },
                ),
                Step("sync", values={"last_price": 1.10}),
                Step("cancel", values={"id": "Bracket"}),
                Step("sync", values={"last_price": 1.10}),
                Step("expect_capital_bracket_clear_requests"),
                Step("expect", values={"open_orders": 0}),
                Step("restart", check_invariants=False),
                Step("sync", values={"last_price": 1.10}),
                Step("expect", values={"open_orders": 0}),
            ),
        ),
        Scenario(
            name="control-capitalcom-missing-bracket-clear-is-detected",
            profile_factory=BrokenBracketClearProfile,
            seed=seed,
            expected_violation="Capital.com bracket clear REST requests mismatch",
            steps=(
                Step("entry", values={"id": "Long", "side": "buy", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step(
                    "exit",
                    values={
                        "id": "Bracket",
                        "from_entry": "Long",
                        "side": "sell",
                        "qty": 1.0,
                        "limit": 1.20,
                        "stop": 1.05,
                    },
                ),
                Step("sync", values={"last_price": 1.10}),
                Step("cancel", values={"id": "Bracket"}),
                Step("sync", values={"last_price": 1.10}),
                Step("expect_capital_bracket_clear_requests"),
            ),
        ),
        Scenario(
            name="capitalcom-trailing-attach-amend-clear-survives-restart",
            profile_factory=CapitalComProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "Long", "side": "buy", "qty": 1.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("capital_activity_fill", values={"price": 1.10}),
                Step(
                    "exit",
                    values={
                        "id": "Trail",
                        "from_entry": "Long",
                        "side": "sell",
                        "qty": 1.0,
                        "trail_price": 1.10,
                        "trail_offset": 5.0,
                    },
                ),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "capital_trailing_tick",
                    values={"bid": 1.10, "offer": 1.10, "active": True},
                ),
                Step(
                    "expect_capital_trailing_request",
                    values={"count": 1, "enabled": True, "distance": 5.0},
                ),
                Step(
                    "amend_exit",
                    values={
                        "id": "Trail",
                        "from_entry": "Long",
                        "trail_offset": 7.0,
                    },
                ),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "capital_trailing_tick",
                    values={"bid": 1.10, "offer": 1.10, "active": True},
                ),
                Step(
                    "expect_capital_trailing_request",
                    values={"count": 2, "enabled": True, "distance": 7.0},
                ),
                Step(
                    "amend_exit",
                    values={
                        "id": "Trail",
                        "from_entry": "Long",
                        "trail_price": None,
                        "trail_offset": 0.0,
                    },
                ),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_capital_trailing_request",
                    values={"count": 3, "enabled": False},
                ),
                Step("restart", check_invariants=False),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_capital_trailing_request",
                    values={"count": 0},
                ),
                Step("expect_capital_no_live_trailing"),
            ),
        ),
    ]


def extended_scenarios(seed: int = 0) -> list[Scenario]:
    scenarios = smoke_scenarios(seed)
    axes = {
        "side": ("buy", "sell"),
        "order": ("limit", "stop"),
        "restart": (False, True),
    }
    for index, case in enumerate(pairwise_cases(axes, seed=seed)):
        values: dict[str, Any] = {"id": "E", "side": case["side"], "qty": 1.0}
        if case["order"] == "limit":
            values["limit"] = 1.09 if case["side"] == "buy" else 1.11
        else:
            values["stop"] = 1.11 if case["side"] == "buy" else 1.09
        steps = [
            Step("entry", values=values),
            Step("sync", values={"last_price": 1.10}),
        ]
        if case["restart"]:
            steps.extend(
                (
                    Step("restart"),
                    Step("entry", values=values),
                    Step("sync", values={"last_price": 1.10}),
                )
            )
        scenarios.append(
            Scenario(
                name=f"capitalcom-pairwise-{index:03d}",
                profile_factory=CapitalComProfile,
                seed=seed,
                tags=frozenset({"extended"}),
                steps=tuple(steps),
            )
        )
    return scenarios


def build_suite(*, mode: str, seed: int) -> list[Scenario]:
    return smoke_scenarios(seed) if mode == "smoke" else extended_scenarios(seed)
