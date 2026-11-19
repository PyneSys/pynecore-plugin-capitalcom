"""
@pyne

Deterministic fault injection for the Capital.com live feed under the
framework's stream recovery: the plugin streams only what the WebSocket
pushes, and every gap is filled through the pure ``backfill_closed_bars``
history query.

The venue is faked at three seams — ``websockets.connect`` (a socket that
never speaks), ``create_session`` (no login) and ``get_historical_prices``
(a bar book) — so the real ``connect`` / ``disconnect`` / ``watch_ohlcv`` /
``backfill_closed_bars`` code runs. Live closed bars are placed on
``_update_queue`` in the exact ``("ohlc", payload)`` shape the volume worker
forwards, and a transport death as the plain ``None`` sentinel the listener
posts.
"""
import asyncio
import json
from datetime import datetime, time, timezone

import pytest
import websockets

from pynecore.core.plugin.live_provider import LiveProviderPlugin
from pynecore.core.syminfo import SymInfo, SymInfoInterval
from pynecore.types.ohlcv import OHLCV

import pynecore_capitalcom.streaming as streaming_module
from pynecore_capitalcom import CapitalCom, CapitalComConfig, CapitalComError
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

    def state(self) -> dict[str, tuple[int, str]]:
        """Identity and contents of every plugin attribute, for purity checks.

        The fake's own bookkeeping of history requests is left out: it records
        the calls under test, it is not plugin state.
        """
        return {
            name: (id(value), repr(value))
            for name, value in vars(self).items()
            if name != "price_calls"
        }

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
    """Mimics the core live loop's closed-bar cursor over the provider stream.

    ``dropped`` collects the closed bars the core cursor threw away: bars the
    stream handed on although the runner already held that slot.
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
    exception, then the framework's reconnect fill: the bars after the
    runner's newest closed bar, read through ``backfill_closed_bars``.

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
        assert queue.empty(), "connect() must not queue bars of its own"
        assert runner.last_confirmed is not None
        try:
            filled = await provider.backfill_closed_bars(
                "EURUSD", "1", runner.last_confirmed,
            )
        except Exception:  # noqa: BLE001 - the framework reconnects again
            continue
        for bar in filled:
            runner.feed(bar)
        return attempt
    raise AssertionError("reconnect never succeeded")


def _run(scenario, monkeypatch) -> None:
    """Run ``scenario(provider, runner)`` against the faked venue."""
    provider = _Venue()
    runner = _Runner()
    monkeypatch.setattr(websockets, 'connect', provider.open_socket)
    monkeypatch.setattr(streaming_module, 'epoch_time', lambda: provider.now_s)

    async def _to_thread(func, /, *args, **kwargs):
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


# === Declared history properties ============================================

def __test_capitalcom_declares_its_history__():
    """Gap recovery is the framework's; the plugin declares its history."""
    assert issubclass(CapitalCom, LiveProviderPlugin)
    assert CapitalCom.history_has_idle_bars is False
    assert CapitalCom.history_publish_lag == 15.0
    # ``/prices`` revises a fresh bar, and the socket candle is not the history's
    assert CapitalCom.history_revises_before_lag is True
    assert CapitalCom.stream_closes_match_history is False


# === (a) Outage bars arrive through the framework's fill ====================

def __test_capitalcom_connect_reads_no_history__(monkeypatch):
    """A reconnect over an outage neither queues bars nor reads history."""

    async def scenario(provider, runner):
        await _start_live(provider, runner)
        provider.advance_to(4)
        provider.fill_book(5)
        await provider.disconnect()
        await provider.connect()

        assert provider.price_calls == []
        assert provider._update_queue is not None
        assert provider._update_queue.empty()

    _run(scenario, monkeypatch)


def __test_capitalcom_outage_bars_arrive_once_in_order__(monkeypatch):
    """Bars that close during an outage reach the runner once, in order."""

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


# === (b) The history query is pure ==========================================

def __test_capitalcom_backfill_is_a_pure_query__(monkeypatch):
    """Repeated queries, in any order, leave the plugin exactly as it was."""

    async def scenario(provider, runner):
        await _start_live(provider, runner)
        provider.advance_to(5)
        provider.fill_book(6)
        before = provider.state()

        first = await provider.backfill_closed_bars("EURUSD", "1", _bar(1) * 1000)
        older = await provider.backfill_closed_bars("EURUSD", "1", _bar(0) * 1000)
        again = await provider.backfill_closed_bars("EURUSD", "1", _bar(1) * 1000)

        assert provider.state() == before
        assert [bar.timestamp for bar in first] == [_bar(k) * 1000 for k in (2, 3, 4)]
        assert [bar.timestamp for bar in older] == [_bar(k) * 1000 for k in (1, 2, 3, 4)]
        assert again == first
        assert all(bar.is_closed for bar in first)

    _run(scenario, monkeypatch)


def __test_capitalcom_backfill_is_strictly_after_since_ascending_without_forming_bar__(
        monkeypatch):
    """Only closed bars after ``since_ms``, oldest first; the forming bar never."""

    async def scenario(provider, runner):
        await _start_live(provider, runner)
        # The venue already lists bar 6, the one forming right now.
        provider.advance_to(6)
        provider.fill_book(7)

        bars = await provider.backfill_closed_bars("EURUSD", "1", _bar(2) * 1000)

        assert [bar.timestamp for bar in bars] == [_bar(k) * 1000 for k in (3, 4, 5)]
        assert [bar.volume for bar in bars] == [5.0, 5.0, 5.0]
        assert all(bar.extra_fields is None for bar in bars)

    _run(scenario, monkeypatch)


def _no_prices_list(provider: _Venue) -> None:
    """Make the history answer with a body that has no ``prices`` list."""
    provider.get_historical_prices = lambda **_kwargs: {}  # type: ignore[method-assign]


@pytest.mark.parametrize("breaker", [
    lambda provider: provider.price_faults.append(ConnectionError("prices endpoint down")),
    lambda provider: provider.price_faults.append(CapitalComError("authentication unavailable")),
    _no_prices_list,
], ids=["transport", "venue-error", "schema"])
def __test_capitalcom_backfill_raises_on_an_unanswered_query__(monkeypatch, breaker):
    """A failed or unreadable history read raises, it is never an empty answer."""

    async def scenario(provider, runner):
        await _start_live(provider, runner)
        provider.advance_to(5)
        provider.fill_book(6)
        before = provider.state()
        breaker(provider)

        with pytest.raises((ConnectionError, CapitalComError, ValueError)):
            await provider.backfill_closed_bars("EURUSD", "1", _bar(1) * 1000)

        # The failure left nothing behind either
        state = provider.state()
        state.pop("get_historical_prices", None)
        assert state == before

    _run(scenario, monkeypatch)


def __test_capitalcom_backfill_empty_only_on_the_venues_own_answer__(monkeypatch):
    """``error.prices.not-found`` and an empty page are the venue saying "no bar"."""

    async def scenario(provider, runner):
        await _start_live(provider, runner)
        provider.advance_to(5)
        # The book holds nothing past bar 1: the fake answers not-found.
        assert await provider.backfill_closed_bars("EURUSD", "1", _bar(1) * 1000) == []

        provider.get_historical_prices = (  # type: ignore[method-assign]
            lambda **_kwargs: {'prices': []}
        )
        assert await provider.backfill_closed_bars("EURUSD", "1", _bar(1) * 1000) == []

    _run(scenario, monkeypatch)


def __test_capitalcom_seam_bar_published_late_is_returned_by_a_later_query__(monkeypatch):
    """A bar REST publishes only after the first query is there on the next one.

    Replays capitalcom cycle 174 (2026-09-18): the warmup tail was bar 0 and
    bar 1 closed while the subscription came up, but REST did not list it yet
    when the seam was first asked for. The framework asks again for a close
    the stream misses; the plugin must answer from the venue each time.
    """

    async def scenario(provider, _runner):
        provider.fill_book(1)
        provider.advance_to(2, offset_s=1.0)
        provider._last_bar_timestamp = _bar(0)
        await provider.connect()
        assert await provider.backfill_closed_bars("EURUSD", "1", _bar(0) * 1000) == []

        provider.fill_book(2)
        provider.advance_to(2, offset_s=11.0)
        recovered = await provider.backfill_closed_bars("EURUSD", "1", _bar(0) * 1000)

        assert [bar.timestamp for bar in recovered] == [_bar(1) * 1000]

    _run(scenario, monkeypatch)


# === (c) The socket hands on what the venue pushes ==========================

def __test_capitalcom_ws_after_reconnect_never_emits_an_older_close__(monkeypatch):
    """A new subscription re-sending settled closes cannot move the stream back."""

    async def scenario(provider, runner):
        await _start_live(provider, runner)
        provider.advance_to(3, offset_s=0.0)
        await _live_close(provider, runner, _bar(2))

        provider.advance_to(3)
        await provider.disconnect()
        await provider.connect()
        queue = provider._update_queue
        assert queue is not None
        for k in (1, 2, 3):
            queue.put_nowait(("ohlc", _payload(_bar(k))))
        await _drain(provider, runner)

        assert runner.closed == [_bar(k) * 1000 for k in (1, 2, 3)]
        assert runner.dropped == []

    _run(scenario, monkeypatch)


def __test_capitalcom_ws_close_after_history_recovery_is_handed_on__(monkeypatch):
    """The history query moves no plugin cursor: a late WS close still goes on.

    The framework took bar 2 from the history; the socket's own close of bar 2
    arrives afterwards. Dropping it is the framework cursor's job, so the
    plugin hands it on unchanged.
    """

    async def scenario(provider, runner):
        await _start_live(provider, runner)
        provider.advance_to(3, offset_s=15.0)
        provider.fill_book(3)
        assert runner.last_confirmed is not None
        for bar in await provider.backfill_closed_bars("EURUSD", "1", runner.last_confirmed):
            runner.feed(bar)
        assert runner.closed == [_bar(1) * 1000, _bar(2) * 1000]

        assert provider._update_queue is not None
        provider._update_queue.put_nowait(("ohlc", _payload(_bar(2))))
        late = await provider.watch_ohlcv("EURUSD", "1")
        assert late.is_closed and late.timestamp == _bar(2) * 1000
        runner.feed(late)

        assert runner.closed == [_bar(1) * 1000, _bar(2) * 1000]
        assert runner.dropped == [_bar(2) * 1000]

    _run(scenario, monkeypatch)


# === Sanity: no reconnect, no history reads =================================

def __test_capitalcom_clean_stream_never_reads_history__(monkeypatch):
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
