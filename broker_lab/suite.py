"""Opt-in offline conformance scenarios for the Capital.com broker plugin."""

import asyncio
import threading
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

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

    async def _call(self, endpoint: str, *, data=None, method: str = "post"):
        payload = dict(data) if data is not None else None
        self.rest_calls.append((endpoint, method, payload))
        self.profile.transport_calls.append((endpoint, method, payload))
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
                    "direction": payload["direction"],
                    "orderType": payload["type"],
                    "orderLevel": payload["level"],
                    "orderSize": payload["size"],
                    "createdDateUTC": "2026-07-21T00:00:00Z",
                },
                "market": {"epic": payload["epic"]},
            }
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
            return {"dealReference": ref}
        if endpoint == "positions" and method == "get":
            return {"positions": list(self.profile.positions.values())}
        if endpoint == "history/activity" and method == "get":
            return {"activities": list(self.profile.polled_activities)}
        if endpoint.startswith("positions/") and method == "delete":
            deal_id = endpoint.removeprefix("positions/")
            self.profile.positions.pop(deal_id, None)
            return {}
        if endpoint.startswith("positions/") and method == "put":
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

    def create_broker(self, run_name: str, store_ctx: Any) -> OfflineCapitalCom:
        return OfflineCapitalCom(self, run_name, store_ctx)

    def handle_step(self, runner: Any, step: Step) -> bool:
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
            if bracket_puts:
                raise AssertionError(
                    "marketable Capital.com exit was incorrectly dispatched "
                    f"as native bracket PUT: {bracket_puts}"
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
                Step("restart", run="B", check_invariants=False),
                Step(
                    "expect", run="B", values={"position": 0.0, "engine_position": 0.0}
                ),
                Step("entry", run="B", values={"id": "B", "side": "buy", "qty": 1.0}),
                Step("sync", run="B", values={"last_price": 1.10}),
                Step("capital_activity_fill", run="B", values={"price": 1.10}),
                Step("restart", run="A", check_invariants=False),
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
