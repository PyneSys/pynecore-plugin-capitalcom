"""Opt-in offline conformance scenarios for the Capital.com broker plugin."""

import asyncio
from typing import Any

from pynecore.core.broker.models import LegType, OrderStatus
from pynecore.testing.broker_lab import Scenario, Step, pairwise_cases
from pynecore.testing.broker_lab.reference import ReferenceVenueProfile, VenueOrder
from pynecore_capitalcom import CapitalCom, CapitalComConfig

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
            return {"dealReference": ref}
        if endpoint == "positions" and method == "get":
            return {"positions": list(self.profile.positions.values())}
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
            leg_type = (
                LegType.STOP_LOSS
                if order.stop_price is not None
                else LegType.TAKE_PROFIT
            )
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

    def create_broker(self, run_name: str, store_ctx: Any) -> OfflineCapitalCom:
        return OfflineCapitalCom(self, run_name, store_ctx)

    def handle_step(self, runner: Any, step: Step) -> bool:
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
            self.state.positions[step.run] = qty if direction == "BUY" else -qty
            self.positions[deal_id] = {
                "position": {
                    "dealId": deal_id,
                    "direction": direction,
                    "size": qty,
                    "level": price,
                    "upl": 0.0,
                    "createdDateUTC": "2026-07-21T10:00:00Z",
                },
                "market": {"epic": self.symbol},
            }
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
        return super().handle_step(runner, step)


def smoke_scenarios(seed: int = 0) -> list[Scenario]:
    return [
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
