"""
@pyne

Deterministic fault injection for the Capital.com live feed: every closed bar
of an outage window must reach the runner, in order and exactly once.

The venue is faked at three seams — ``websockets.connect`` (a socket that
never speaks), ``create_session`` (no login) and ``get_historical_prices``
(a bar book) — so the real ``connect`` / ``disconnect`` / reconnect-replay /
``watch_ohlcv`` code runs. Live closed bars are placed on ``_update_queue``
in the exact ``("ohlc", payload)`` shape the volume worker forwards, and a
transport death as the plain ``None`` sentinel the listener posts.
"""
import asyncio
import json
from datetime import datetime, time, timezone

import websockets

from pynecore.core.syminfo import SymInfo, SymInfoInterval
from pynecore.types.ohlcv import OHLCV

import pynecore_capitalcom.streaming as streaming_module
from pynecore_capitalcom import CapitalCom, CapitalComConfig
from pynecore_capitalcom.exceptions import HistoricalPricesNotFoundError


def main():
    """
    Dummy main function to be a valid Pyne script
    """
    pass


# === Fixed grid ============================================================

#: One minute in seconds — every scenario runs on the ``1`` timeframe.
MIN = 60
#: Minute-aligned epoch anchor; the clock is frozen, so no wall-clock reads.
BASE = 1_800_000_000 // MIN * MIN


def _bar(k: int) -> int:
    """Opening second of the k-th minute bar of the scenario grid."""
    return BASE + k * MIN


def _calendar() -> SymInfo:
    """Always-open calendar so no watchdog can excuse a gap as a session."""
    return SymInfo(
        prefix="CAPITALCOM", description="EURUSD", ticker="EURUSD",
        currency="USD", basecurrency="EUR", period="1", type="forex",
        volumetype="tick", mintick=0.00001, pricescale=100000,
        pointvalue=1.0, mincontract=1000.0,
        opening_hours=[
            SymInfoInterval(day=day, start=time(0, 0), end=time(23, 59, 59))
            for day in range(7)
        ],
        session_starts=[], session_ends=[], timezone="UTC",
    )


# === Fake venue ============================================================

class _FakeSocket:
    """WebSocket stand-in that accepts sends and never pushes a frame."""

    def __init__(self):
        self.close_code: int | None = None
        self.sent: list[dict] = []
        self._dead = asyncio.Event()

    def __aiter__(self):
        return self

    async def __anext__(self):
        await self._dead.wait()
        raise StopAsyncIteration

    async def send(self, raw: str) -> None:
        self.sent.append(json.loads(raw))
        # A real send suspends; without a suspension point here the provider's
        # background tasks would never start before the test tore them down.
        await asyncio.sleep(0)

    async def close(self, code: int | None = None, reason: str | None = None) -> None:
        self.close_code = code if code is not None else 1000
        self._dead.set()
        # Let the listener observe the closure and post its sentinel, the way
        # a real close does while the socket unwinds.
        await asyncio.sleep(0)


class _Venue(CapitalCom):
    """Capital.com provider with transport, login and history faked out.

    ``book`` is every bar opening the venue can serve from ``/prices``;
    ``price_faults`` are raised (and consumed) one per history request.
    """

    def __init__(self):
        super().__init__(
            symbol="EURUSD", timeframe="1",
            config=CapitalComConfig(user_email="a@b.c", api_key="k",
                                    api_password="p"),
        )
        self.syminfo = _calendar()
        self.cst_token = "cst"
        self.security_token = "sec"
        #: Bar openings (seconds) the venue holds in its price history.
        self.book: set[int] = set()
        self.price_faults: list[Exception] = []
        self.sockets: list[_FakeSocket] = []
        self.price_calls: list[int] = []
        #: Frozen wall clock in seconds, advanced explicitly.
        self.now_s = float(_bar(0))

    # --- fake transport / login -------------------------------------------

    @property
    def socket(self) -> _FakeSocket:
        """The socket the provider is currently talking to."""
        return self.sockets[-1]

    def create_session(self) -> None:
        self.cst_token = "cst"
        self.security_token = "sec"

    async def open_socket(self, *_args, **_kwargs) -> _FakeSocket:
        socket = _FakeSocket()
        self.sockets.append(socket)
        return socket

    def advance_to(self, k: int, *, offset_s: float = 30.0) -> None:
        """Move the frozen clock into the k-th bar, ``offset_s`` past open."""
        self.now_s = float(_bar(k)) + offset_s

    def fill_book(self, upto: int) -> None:
        """Let the venue hold every scenario bar up to (excluding) ``upto``."""
        self.book = {_bar(k) for k in range(upto)}

    # --- fake REST ---------------------------------------------------------

    def get_historical_prices(self, time_from=None, time_to=None, limit=1000):
        start = int(time_from.replace(tzinfo=timezone.utc).timestamp())
        self.price_calls.append(start)
        if self.price_faults:
            raise self.price_faults.pop(0)
        stamps = sorted(ts for ts in self.book if ts >= start)[:limit]
        if not stamps:
            raise HistoricalPricesNotFoundError("no prices for this window")
        return {'prices': [{
            'snapshotTimeUTC': datetime.fromtimestamp(
                ts, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S'),
            'openPrice': {'bid': 1.10, 'ask': 1.11},
            'highPrice': {'bid': 1.12, 'ask': 1.13},
            'lowPrice': {'bid': 1.09, 'ask': 1.10},
            'closePrice': {'bid': 1.11, 'ask': 1.12},
            'lastTradedVolume': 5.0,
        } for ts in stamps]}


# === Runner-side simulation =================================================

class _Runner:
    """Mimics the core live loop's consumption of the provider stream.

    ``dropped`` collects the closed bars the core monotonicity guard had to
    throw away — a non-empty list means the PLUGIN re-served settled history.
    """

    def __init__(self):
        self.closed: list[int] = []
        self.dropped: list[int] = []
        self.last_confirmed: int | None = None

    def feed(self, bar: OHLCV) -> None:
        if not bar.is_closed:
            return
        if self.last_confirmed is not None and bar.timestamp <= self.last_confirmed:
            self.dropped.append(bar.timestamp)
            return
        self.last_confirmed = bar.timestamp
        self.closed.append(bar.timestamp)


def _payload(opening: int) -> dict:
    """One ``ohlc.event`` bid payload for the bar opening at ``opening``."""
    return {
        "priceType": "bid", "t": opening * 1000,
        "o": 1.10, "h": 1.12, "l": 1.09, "c": 1.11, "_volume": 5.0,
    }


async def _live_close(provider: _Venue, runner: _Runner, opening: int) -> None:
    """Deliver one live closed bar the way the volume worker does."""
    assert provider._update_queue is not None
    provider._update_queue.put_nowait(("ohlc", _payload(opening)))
    runner.feed(await provider.watch_ohlcv("EURUSD", "1"))


async def _take(provider: _Venue, runner: _Runner, count: int) -> None:
    """Consume exactly ``count`` updates already queued on the provider."""
    for _ in range(count):
        runner.feed(await provider.watch_ohlcv("EURUSD", "1"))


async def _drain(provider: _Venue, runner: _Runner) -> None:
    """Consume everything queued, terminated by the stream-death sentinel.

    The sentinel is what the listener itself posts when the transport dies,
    and it is the only deterministic end marker: a payload the plugin
    suppresses produces no update at all, so counting is not enough.
    """
    queue = provider._update_queue
    assert queue is not None
    queue.put_nowait(None)
    while True:
        try:
            runner.feed(await provider.watch_ohlcv("EURUSD", "1"))
        except ConnectionError:
            return


async def _reconnect(provider: _Venue, runner: _Runner,
                     *, attempts: int = 8) -> int:
    """Replay the live runner's reconnect loop verbatim.

    ``disconnect`` -> ``connect`` -> ``on_reconnect``, retried on any
    exception. Capital.com repairs the gap inside ``connect`` itself, so
    ``on_reconnect`` is the framework's no-op here.

    :return: The number of attempts the reconnect took.
    """
    for attempt in range(1, attempts + 1):
        await provider.on_disconnect()
        try:
            await provider.disconnect()
        except Exception:  # noqa: BLE001 - the runner logs and continues too
            pass
        try:
            await provider.connect()
            await provider.on_reconnect()
        except Exception:  # noqa: BLE001 - "Reconnect failed", next attempt
            continue
        queue = provider._update_queue
        assert queue is not None
        await _take(provider, runner, queue.qsize())
        return attempt
    raise AssertionError("reconnect never succeeded")


def _run(scenario, monkeypatch, *, before_thread=None) -> None:
    """Run ``scenario(provider, runner)`` against the faked venue.

    :param before_thread: Optional async hook awaited with the callable of
        every offloaded REST call before it executes. Scenarios that need a
        lookup to stall mid-pipeline park on their own event inside it.
    """
    provider = _Venue()
    runner = _Runner()
    monkeypatch.setattr(websockets, 'connect', provider.open_socket)
    monkeypatch.setattr(streaming_module, 'epoch_time', lambda: provider.now_s)

    async def _to_thread(func, /, *args, **kwargs):
        if before_thread is not None:
            await before_thread(func)
        return func(*args, **kwargs)

    monkeypatch.setattr(streaming_module.asyncio, 'to_thread', _to_thread)

    async def _main():
        try:
            await scenario(provider, runner)
        finally:
            await provider.disconnect()

    asyncio.run(_main())


async def _start_live(provider: _Venue, runner: _Runner) -> None:
    """Bring the feed up and let it close one bar, as a real session does."""
    provider.advance_to(1)
    provider.fill_book(1)
    # Warmup handoff: ``download_ohlcv`` leaves the cursor on its last bar.
    provider._last_bar_timestamp = _bar(0)
    await provider.connect()
    provider.advance_to(2)
    await _live_close(provider, runner, _bar(1))
    assert runner.closed == [_bar(1) * 1000]


# === (a) Outage bars arrive via the reconnect replay ========================

def __test_capitalcom_outage_bars_replayed_in_order__(monkeypatch):
    """Bars that close during an outage are delivered once, in order."""

    async def scenario(provider, runner):
        await _start_live(provider, runner)

        # The socket dies inside bar 2; bars 2..3 close while offline.
        provider.advance_to(4)
        provider.fill_book(5)
        assert await _reconnect(provider, runner) == 1
        assert runner.closed == [_bar(k) * 1000 for k in (1, 2, 3)]

        # Live streaming resumes with no seam gap and no repeat.
        provider.advance_to(5)
        await _live_close(provider, runner, _bar(4))

        assert runner.closed == [_bar(k) * 1000 for k in (1, 2, 3, 4)]
        assert runner.dropped == []

    _run(scenario, monkeypatch)


# === (b) Replay re-serving bars already delivered ===========================

def __test_capitalcom_startup_replay_not_double_emitted__(monkeypatch):
    """The startup gap is fetched twice but reaches the runner once.

    ``connect`` repairs the gap from REST on its own, and the framework then
    asks for the very same window through ``backfill_closed_bars``. Both
    read the same bars, so the plugin has to suppress its own queued copies
    rather than leave the core monotonicity guard to clean up after it.
    """

    async def scenario(provider, runner):
        provider.advance_to(3)
        provider.fill_book(3)
        provider._last_bar_timestamp = _bar(0)
        await provider.connect()
        # ``connect`` queued the gap bars; the framework now asks for the
        # same window and splices what it gets in ahead of the live stream.
        recovered = await provider.backfill_closed_bars(
            "EURUSD", "1", _bar(0) * 1000,
        )
        assert [bar.timestamp for bar in recovered] == [
            _bar(1) * 1000, _bar(2) * 1000,
        ]
        for bar in recovered:
            runner.feed(bar)
        await _drain(provider, runner)

        assert runner.closed == [_bar(1) * 1000, _bar(2) * 1000]
        assert runner.dropped == []

    _run(scenario, monkeypatch)


def __test_capitalcom_seam_bar_emitted_exactly_once__(monkeypatch):
    """The bar spanning the reconnect boundary is neither lost nor doubled.

    Bar 2 closes and is delivered live moments before the drop; the venue
    still holds it, so the replay window must start past it.
    """

    async def scenario(provider, runner):
        await _start_live(provider, runner)
        provider.advance_to(3, offset_s=0.0)
        await _live_close(provider, runner, _bar(2))

        provider.advance_to(4)
        provider.fill_book(5)
        await _reconnect(provider, runner)

        assert runner.closed == [_bar(k) * 1000 for k in (1, 2, 3)]
        assert runner.dropped == []
        # The already-delivered bar was never even requested.
        assert all(start > _bar(2) for start in provider.price_calls[-1:])

    _run(scenario, monkeypatch)


# === (c) Repeated failed reconnect attempts =================================

def __test_capitalcom_no_bar_lost_across_failed_reconnects__(monkeypatch):
    """Two dead dials and a dead history read later, the gap is intact."""

    async def scenario(provider, runner):
        await _start_live(provider, runner)
        provider.advance_to(5)
        provider.fill_book(6)

        dials = {'left': 2}
        original_open = provider.open_socket

        async def _refusing(*args, **kwargs):
            if dials['left'] > 0:
                dials['left'] -= 1
                raise OSError("connection refused")
            return await original_open(*args, **kwargs)

        monkeypatch.setattr(websockets, 'connect', _refusing)
        provider.price_faults = [ConnectionError("prices endpoint down")]

        assert await _reconnect(provider, runner) == 4
        assert runner.closed == [_bar(k) * 1000 for k in (1, 2, 3, 4)]
        assert runner.dropped == []

    _run(scenario, monkeypatch)


def __test_capitalcom_failed_replay_keeps_the_whole_gap__(monkeypatch):
    """A history failure mid-replay re-reads the entire gap on the retry.

    The replay only reaches the update queue once ``connect`` returns, and
    a failed ``connect`` throws that queue away — so the cursor must not
    have moved for any page that never reached the runner.
    """

    async def scenario(provider, runner):
        await _start_live(provider, runner)
        provider.advance_to(6)
        provider.fill_book(7)
        provider.price_faults = [ConnectionError("prices endpoint down")]

        assert await _reconnect(provider, runner) == 2
        assert runner.closed == [_bar(k) * 1000 for k in (1, 2, 3, 4, 5)]
        assert runner.dropped == []

    _run(scenario, monkeypatch)


# === (d) Drop exactly on a bar boundary =====================================

def __test_capitalcom_drop_at_bar_boundary_delivers_once__(monkeypatch):
    """A bar closing at the very moment of the drop arrives exactly once.

    Two variants of the same instant: the ``ohlc.event`` slips through just
    before the socket dies (the replay must not repeat it), and it does not
    (the replay must supply it).
    """

    async def event_arrived(provider, runner):
        await _start_live(provider, runner)
        provider.advance_to(3, offset_s=0.0)
        await _live_close(provider, runner, _bar(2))
        provider.fill_book(4)
        provider.advance_to(4, offset_s=0.0)
        await _reconnect(provider, runner)

        assert runner.closed == [_bar(k) * 1000 for k in (1, 2, 3)]
        assert runner.dropped == []

    async def event_lost(provider, runner):
        await _start_live(provider, runner)
        # Same instant, but the close event died with the socket.
        provider.fill_book(4)
        provider.advance_to(4, offset_s=0.0)
        await _reconnect(provider, runner)

        assert runner.closed == [_bar(k) * 1000 for k in (1, 2, 3)]
        assert runner.dropped == []

    _run(event_arrived, monkeypatch)
    _run(event_lost, monkeypatch)


# === (e) Watchdog recovery racing an in-flight volume lookup =================

#: Captured before any test can replace ``asyncio.sleep`` with a fast stand-in.
_REAL_SLEEP = asyncio.sleep


class _Stall:
    """Holds the REST volume lookup open until the scenario releases it."""

    def __init__(self):
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def hook(self, func) -> None:
        """``before_thread`` hook: park only the per-bar volume lookup."""
        if getattr(func, "__name__", "") != "_fetch_bar_volume":
            return
        self.entered.set()
        await self.release.wait()


async def _fast_sleep(_delay: float) -> None:
    """Stand-in for the watchdog's 1s poll: yield instead of waiting."""
    await _REAL_SLEEP(0)


async def _until(predicate, *, what: str, spins: int = 500) -> None:
    """Yield to the loop until ``predicate`` holds, or fail loudly."""
    for _ in range(spins):
        if predicate():
            return
        await _REAL_SLEEP(0)
    raise AssertionError(f"timed out waiting for {what}")


def _ws_payload(opening: int) -> dict:
    """A raw ``ohlc.event`` bid payload — no volume resolved yet."""
    payload = _payload(opening)
    del payload["_volume"]
    return payload


async def _arm_pipeline(provider: _Venue, *, emitted: int, anchor: int) -> None:
    """Start the live pipeline around hand-built queues, without ``connect``.

    ``connect`` is bypassed on purpose: its reconnect replay, keepalive and
    feed watchdog play no part in bar ordering, while their real waits would
    make the scenario depend on wall-clock timing. Everything ordering does
    depend on — the raw queue, the volume worker and the OHLC watchdog — is
    the production code, running as the production tasks.

    :param emitted: Opening second of the newest bar the runner already has.
    :param anchor: Opening second the watchdog probes the next slot from.
    """
    provider._ws = await provider.open_socket()  # type: ignore[assignment]
    provider._update_queue = asyncio.Queue()
    provider._raw_ohlc_queue = asyncio.Queue()
    provider._last_bar_timestamp = emitted
    provider._last_bar_open_ts = float(anchor)
    provider._last_ohlc_event_ts = provider.now_s
    provider._last_quote_event_ts = provider.now_s
    # Full WS coverage from the first scenario bar: the volume worker must
    # take the REST path because the quote bucket is empty, not because the
    # bar predates the subscription.
    provider._ws_coverage_started_at = float(_bar(0))
    provider._volume_backfill_task = asyncio.create_task(
        provider._volume_backfill_worker_loop()
    )
    provider._ohlc_watchdog_task = asyncio.create_task(
        provider._ohlc_watchdog_loop()
    )
    await _REAL_SLEEP(0)


async def _consume_until_death(provider: _Venue, runner: _Runner) -> None:
    """Feed the runner everything the pipeline still has to deliver."""
    while True:
        try:
            runner.feed(await provider.watch_ohlcv("EURUSD", "1"))
        except ConnectionError:
            return


def __test_capitalcom_watchdog_recovery_waits_for_in_flight_bar__(monkeypatch):
    """A stalled volume lookup must not let the watchdog's bar pass it.

    Bar 2 arrives on the WebSocket and parks in the volume worker on a REST
    lookup that outlives the watchdog's publish lag. Bar 3's ``ohlc.event``
    never arrives, so the watchdog recovers it from REST while bar 2 is still
    in flight. Both bars must reach the runner, in order, exactly once — with
    a second writer on the consumer queue bar 3 overtakes bar 2 and the
    plugin's own monotonicity filter then swallows bar 2 without a trace.
    """
    stall = _Stall()
    monkeypatch.setattr(streaming_module.asyncio, 'sleep', _fast_sleep)

    async def scenario(provider, runner):
        provider.fill_book(4)
        # Bar 2 closed a second ago and its ohlc.event just landed.
        provider.advance_to(3, offset_s=1.0)
        await _arm_pipeline(provider, emitted=_bar(1), anchor=_bar(2))
        raw = provider._raw_ohlc_queue
        queue = provider._update_queue
        assert raw is not None and queue is not None
        try:
            raw.put_nowait(("ohlc", _ws_payload(_bar(2))))
            await _until(lambda: stall.entered.is_set(),
                         what="the volume lookup of bar 2")

            # Bar 3's event is never pushed. The watchdog fires 10s past bar
            # 4's open, with bar 2 still parked on its volume lookup.
            provider.advance_to(4, offset_s=11.0)
            await _until(lambda: provider.price_calls == [_bar(3)],
                         what="the watchdog's REST read of bar 3")
            await _until(lambda: raw.qsize() >= 1 or not queue.empty(),
                         what="the recovered bar to be published")
            assert queue.empty(), (
                "the recovered bar overtook the bar still being enriched"
            )
        finally:
            # Unblock the pipeline even on failure, so the teardown does not
            # have to wait the lookup's real timeout out.
            stall.release.set()
        raw.put_nowait(("disconnect", None))
        await _consume_until_death(provider, runner)

        assert runner.closed == [_bar(2) * 1000, _bar(3) * 1000]
        assert runner.dropped == []
        # The recovery really did run while the volume lookup was parked.
        assert provider.price_calls == [_bar(3), _bar(2)]

    _run(scenario, monkeypatch, before_thread=stall.hook)


def __test_capitalcom_watchdog_recovery_delivered_without_in_flight_bar__(
        monkeypatch):
    """The plain recovery path still delivers when nothing is in flight."""
    monkeypatch.setattr(streaming_module.asyncio, 'sleep', _fast_sleep)

    async def scenario(provider, runner):
        provider.fill_book(4)
        provider.advance_to(4, offset_s=11.0)
        await _arm_pipeline(provider, emitted=_bar(2), anchor=_bar(2))
        raw = provider._raw_ohlc_queue
        queue = provider._update_queue
        assert raw is not None and queue is not None
        # The sentinel may only be queued once the recovery is in the
        # pipeline, otherwise the worker would exit ahead of it.
        await _until(lambda: raw.qsize() >= 1 or not queue.empty(),
                     what="the watchdog recovery of bar 3")

        raw.put_nowait(("disconnect", None))
        await _consume_until_death(provider, runner)

        assert runner.closed == [_bar(3) * 1000]
        assert runner.dropped == []
        # Recovered bars carry REST volume already; the worker adds no lookup.
        assert provider.price_calls == [_bar(3)]
        assert provider._last_bar_open_ts == float(_bar(3))

    _run(scenario, monkeypatch)


# === Sanity: no reconnect, no history reads =================================

def __test_capitalcom_clean_stream_never_replays__(monkeypatch):
    """Without a drop the subscription serves everything — no history reads."""

    async def scenario(provider, runner):
        await _start_live(provider, runner)
        for k in (2, 3, 4):
            provider.advance_to(k + 1)
            await _live_close(provider, runner, _bar(k))

        assert runner.closed == [_bar(k) * 1000 for k in (1, 2, 3, 4)]
        assert runner.dropped == []
        assert provider.price_calls == []

    _run(scenario, monkeypatch)
