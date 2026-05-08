"""WebSocket streaming surface: connect lifecycle + 3-layer feed liveness.

Capital.com pushes ``ohlc.event`` (closed bars) and ``quote`` (tick
updates) over a single WS endpoint. This mix-in owns:

* The connection lifecycle (``connect`` / ``disconnect`` /
  ``is_connected``).
* Three concurrent watchdog tasks — TCP-level WS ping/pong (handled by
  the ``websockets`` library), application-level keepalive
  (``_ping_loop``), and a feed-staleness watchdog (``_feed_watchdog_loop``)
  that catches the worst case of a TCP-alive socket whose server
  stopped streaming market payloads.
* The listener (``_listen_loop``) that routes WS frames into an
  ``asyncio.Queue`` consumed by ``watch_ohlcv``.
* The intra-bar synth pipeline (``_synth_from_quote``) so the live
  spinner sees fresh bid/ask between closed-bar pushes.

State touched: ``_ws``, ``_update_queue``, ``_listen_task``,
``_ping_task``, ``_feed_watchdog_task``, ``_last_payload_ts``,
``_last_bar_timestamp``, ``_last_bar_ohlcv``, ``_last_bid``,
``_last_ask``, ``_tick_volume``.
"""
import asyncio
import json
from time import time as epoch_time

from websockets.exceptions import WebSocketException

from pynecore.core.plugin import override
from pynecore.types.ohlcv import OHLCV

from ._base import _CapitalComBase
from .helpers import WS_URL


class _StreamingMixin(_CapitalComBase):
    """WebSocket streaming mix-in: connect / listen / ping / watchdog / synth."""

    # --- LiveProviderPlugin (WebSocket) ------------------------------------

    async def _send(self, destination: str, payload: dict | None = None,
                    correlation_id: str = "1") -> None:
        """Send a JSON message over the WebSocket."""
        msg: dict[str, str | dict | None] = {
            "destination": destination,
            "correlationId": correlation_id,
            "cst": self.cst_token,
            "securityToken": self.security_token,
        }
        if payload:
            msg["payload"] = payload
        assert self._ws is not None
        await self._ws.send(json.dumps(msg))

    async def _listen_loop(self) -> None:
        """Background task: read WebSocket messages and route them.

        The Capital.com WS protocol distinguishes *subscribe-request* names
        from *event* names: the client sends ``OHLCMarketData.subscribe`` /
        ``marketData.subscribe`` but the server pushes OHLC bars on
        ``ohlc.event`` and tick quotes on ``quote`` (see the research
        dossier §6 and the official reference at
        https://open-api.capital.com/).  Matching on the subscribe name
        here would silently discard every update.

        Events are routed to ``_update_queue`` as ``("ohlc", payload)`` or
        ``("quote", None)`` tuples. The consumer (``watch_ohlcv``) turns a
        ``quote`` tick into a synthetic intra-bar OHLCV update so the
        live spinner and tick hooks see fresh bid/ask at every tick, not
        only on bar close.
        """
        assert self._ws is not None and self._update_queue is not None
        # Bind the queue locally so the sentinel-emit in ``finally``
        # always targets the queue THIS listener was started against.
        # If a disconnect/reconnect cycle replaces ``self._update_queue``
        # before this task's cancellation reaches ``finally``, the old
        # ``self._update_queue`` reference would point at the NEW
        # connection's queue and a stale ``None`` would land on it —
        # ``watch_ohlcv`` would raise ``ConnectionError`` on an
        # otherwise healthy reconnect. Reading ``self._update_queue``
        # in the body too keeps the producer/consumer pair pinned for
        # the lifetime of this listener.
        queue = self._update_queue
        # Initialise the stale-feed stamp at the first iteration of the
        # loop so the watchdog does not fire while we are still in the
        # subscribe handshake (no market payloads yet by definition).
        self._last_payload_ts = epoch_time()
        # noinspection PyBroadException
        try:
            async for raw in self._ws:
                self._last_payload_ts = epoch_time()
                data = json.loads(raw)
                dest = data.get("destination", "")
                if dest == "ohlc.event":
                    await queue.put(("ohlc", data.get("payload") or {}))
                elif dest == "quote":
                    payload = data.get("payload") or {}
                    bid: float | str | None = payload.get("bid")
                    ofr: float | str | None = payload.get("ofr")
                    if bid is not None:
                        self._last_bid = float(bid)
                    if ofr is not None:
                        self._last_ask = float(ofr)
                    self._tick_volume += 1
                    await queue.put(("quote", None))
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        finally:
            # Live runner reconnect is gated on a ``None`` sentinel in
            # the listener's queue. Emit it on EVERY exit reason — a
            # clean close from :meth:`_feed_watchdog_loop` forcing
            # ``_ws.close()`` ends the ``async for`` normally (no
            # Exception), so without the ``finally`` the consumer would
            # be silently stranded. The local ``queue`` binding ensures
            # the sentinel goes to THIS listener's queue, never to a
            # successor connection's queue established by a fast
            # disconnect/reconnect cycle.
            #
            # The consumer queue is unbounded, so ``put_nowait`` cannot
            # fail with ``QueueFull`` — no try/except needed.
            queue.put_nowait(None)

    async def _ping_loop(self) -> None:
        """Background task: heartbeat the WS session at exchange-grade cadence.

        Two roles in one loop:

        * Session keepalive — Capital.com closes the WS session after
          10 minutes of inactivity; any application-level ping resets
          that timer.
        * Feed-liveness probe — every server pong is routed back
          through :meth:`_listen_loop`, which stamps
          :attr:`_last_payload_ts`. With a 5s cadence the watchdog has
          three pong opportunities inside its 15s staleness window, so
          a half-open pipe (TCP alive, no market payloads) is forced
          to reconnect quickly even when the symbol has no fresh
          ticks.
        """
        try:
            while True:
                await asyncio.sleep(5)
                try:
                    await self._send("ping", correlation_id="ping")
                except (WebSocketException, OSError):
                    # A failed send already implies the socket is
                    # going down; let the listener's exit deliver the
                    # sentinel and the live runner own the reconnect.
                    return
        except asyncio.CancelledError:
            pass

    async def _feed_watchdog_loop(self) -> None:
        """Force-reconnect when the WS pipe is silent past the staleness window.

        WS-protocol ping/pong (5s/5s) catches TCP-level drops; the
        application-level ping/pong (5s in :meth:`_ping_loop`) covers
        Capital.com's session keepalive AND, more importantly, gives
        the listener a payload to stamp every 5s even on a quiet
        symbol. If :attr:`_last_payload_ts` ages beyond
        ``STALE_THRESHOLD_S`` despite both, the server is talking to
        us on a dead feed — close the WS so :meth:`_listen_loop`
        exits, the sentinel reaches ``watch_ohlcv``, and the live
        runner reconnects.
        """
        stale_threshold_s = 15.0
        try:
            while True:
                await asyncio.sleep(1.0)
                if self._last_payload_ts == 0.0:
                    continue
                silent_for = epoch_time() - self._last_payload_ts
                if silent_for <= stale_threshold_s:
                    continue
                if self._ws is None or self._ws.close_code is not None:
                    continue
                # Reset the stamp BEFORE awaiting the close so a
                # second pass through the loop doesn't double-fire
                # while the close is still in flight; the listener's
                # finally clause delivers the sentinel.
                self._last_payload_ts = 0.0
                try:
                    await self._ws.close(code=4000, reason="feed-stale")
                except (WebSocketException, OSError):
                    # Already-broken socket: the listener's finally is
                    # what guarantees the sentinel anyway.
                    pass
        except asyncio.CancelledError:
            pass

    @override
    async def connect(self) -> None:
        """Establish REST session, run broker-side recovery, and open WebSocket.

        The broker-side steps (cursor load + §5.1 recovery) must happen
        before the market-data WebSocket subscribes — the recovery fans
        out several ``GET`` calls whose results feed the BrokerStore
        under ``self.store_ctx``, and a live WS loop would race against
        those writes. Both steps are no-ops when ``store_ctx`` is
        ``None`` (data-only paths / tests), so the behaviour stays
        backward-compatible with pre-broker-layer usage.
        """
        try:
            import websockets
        except ImportError:
            raise ImportError(
                "websockets is required for live data. Install it with: "
                "pip install websockets"
            )

        if not self.cst_token or not self.security_token:
            self.create_session()

        if self.store_ctx is not None:
            await self._load_activity_cursor_from_events()
            await self._recover_in_flight_submissions()

        # WS ping/pong cadence: default `websockets` is 20s/20s = up to
        # 40s before a half-open connection is dropped. On a 1-minute
        # strategy that is too coarse — tighten to 5s/5s (≤10s detect)
        # so a TCP-level outage propagates inside one bar.
        self._ws = await websockets.connect(
            WS_URL, ping_interval=5, ping_timeout=5,
        )

        self._update_queue = asyncio.Queue()
        self._tick_volume = 0

        self._listen_task = asyncio.create_task(self._listen_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())
        self._feed_watchdog_task = asyncio.create_task(self._feed_watchdog_loop())

        assert self.timeframe is not None and self.symbol is not None
        xchg_tf = self.to_exchange_timeframe(self.timeframe)
        await self._send("OHLCMarketData.subscribe", {
            "epics": [self.symbol],
            "resolutions": [xchg_tf],
            "type": "classic",
        }, correlation_id="ohlc_sub")

        await self._send("marketData.subscribe", {
            "epics": [self.symbol],
        }, correlation_id="market_sub")

    @override
    async def disconnect(self) -> None:
        """Unsubscribe, cancel background tasks, and close the WebSocket."""
        if self._ws and self._ws.close_code is None and self.timeframe and self.symbol:
            try:
                xchg_tf = self.to_exchange_timeframe(self.timeframe)
                await self._send("OHLCMarketData.unsubscribe", {
                    "epics": [self.symbol],
                    "resolutions": [xchg_tf],
                }, correlation_id="ohlc_unsub")
                await self._send("marketData.unsubscribe", {
                    "epics": [self.symbol],
                }, correlation_id="market_unsub")
            except (ConnectionError, asyncio.CancelledError):
                pass

        if self._ping_task:
            self._ping_task.cancel()
            self._ping_task = None
        if self._feed_watchdog_task:
            self._feed_watchdog_task.cancel()
            self._feed_watchdog_task = None
        if self._listen_task:
            self._listen_task.cancel()
            self._listen_task = None
        if self._ws:
            await self._ws.close()
            self._ws = None

        self._update_queue = None
        self._last_bar_timestamp = None
        self._last_bar_ohlcv = None
        self._tick_volume = 0
        self._last_bid = None
        self._last_ask = None

    @property
    @override
    def is_connected(self) -> bool:
        """Whether the WebSocket connection is active."""
        return self._ws is not None and self._ws.close_code is None

    @override
    async def watch_ohlcv(self, symbol: str, timeframe: str) -> OHLCV:
        """Wait for the next OHLCV update from the WebSocket.

        Detects bar closure by tracking timestamp changes. Tick volume is
        accumulated from the ``marketData`` stream (each tick = +1
        volume).

        :param symbol: Symbol (epic) in Capital.com format (e.g. "EURUSD").
        :param timeframe: Timeframe in TradingView format (e.g. "1", "60", "1D").
        :return: OHLCV with ``is_closed=True`` for a final bar,
            ``False`` for intra-bar updates.
        """
        assert self._update_queue is not None
        while True:
            item = await self._update_queue.get()
            if item is None:
                raise ConnectionError("WebSocket listener disconnected")
            event_type, payload = item
            result: OHLCV | None
            if event_type == "ohlc":
                # ``_on_ohlc_event`` returns None for ask-side duplicates;
                # skip and wait for the next event in that case.
                result = self._on_ohlc_event(payload)
            else:
                # Quote arrived before any OHLC baseline yields None;
                # keep waiting until the first ``ohlc.event`` lands.
                result = self._synth_from_quote()
            if result is not None:
                return result

    def _extra_fields(self) -> dict[str, float] | None:
        """Build the bid/ask/spread snapshot for ``OHLCV.extra_fields``."""
        if self._last_bid is None or self._last_ask is None:
            return None
        return {
            "bid_close": self._last_bid,
            "ask_close": self._last_ask,
            "spread": self._last_ask - self._last_bid,
        }

    def _on_ohlc_event(self, payload: dict) -> OHLCV | None:
        """Turn an ``ohlc.event`` payload into a *closed* OHLCV.

        Capital.com pushes ``ohlc.event`` only at bar close: the official
        Java sample accumulates intra-bar price into a local ``OHLCBar``
        from the ``quote`` tick stream and never relies on the server for
        intra-bar OHLC. Every ``ohlc.event`` we receive therefore already
        represents a completed bar, so we flag ``is_closed=True`` on the
        first one — a timestamp-change detector would miss the first bar
        and delay the OHLCV log by a full period.

        With ``"type": "classic"`` the server emits *two* ``ohlc.event``
        payloads per close — one ``priceType="bid"`` and one
        ``priceType="ask"``. We only consume the bid candle (Pine OHLC is
        bid-side); the ask close is already exposed via the ``quote``
        stream in ``extra_fields["ask_close"]``. Returning ``None`` for
        ask payloads keeps :meth:`watch_ohlcv` looping for the next
        event instead of double-emitting the bar close.

        Intra-bar updates (spinner, tick hooks) come exclusively from
        :meth:`_synth_from_quote`.
        """
        price_type = str(payload.get("priceType", "bid")).lower()
        if price_type != "bid":
            return None
        timestamp = int(payload["t"] / 1000)
        closed = OHLCV(
            timestamp=timestamp,
            open=float(payload["o"]),
            high=float(payload["h"]),
            low=float(payload["l"]),
            close=float(payload["c"]),
            volume=float(self._tick_volume),
            extra_fields=self._extra_fields(),
            is_closed=True,
        )
        self._tick_volume = 0
        self._last_bar_timestamp = timestamp
        self._last_bar_ohlcv = closed
        return closed

    def _synth_from_quote(self) -> OHLCV | None:
        """Synthesize an intra-bar OHLCV from the latest tick quote.

        ``ohlc.event`` only pushes on bar close at some resolutions; the
        live spinner and tick hooks would then freeze between closes. To
        keep them fresh we emit a synthetic intra-bar OHLCV on each
        ``quote`` tick: O/H/L are kept from the current bar, ``close`` is
        the latest bid (Capital.com OHLC is bid-side for
        ``type: "classic"``), and H/L widen as the tick moves. The
        ``extra_fields`` snapshot carries the current bid/ask/spread so
        consumers can render real-time quotes.

        Returns ``None`` if no OHLC baseline has been received yet — the
        caller then waits for the next event.
        """
        if self._last_bar_ohlcv is None or self._last_bid is None:
            return None
        new_close = self._last_bid
        new_high = max(self._last_bar_ohlcv.high, new_close)
        new_low = min(self._last_bar_ohlcv.low, new_close)
        synth = self._last_bar_ohlcv._replace(
            high=new_high,
            low=new_low,
            close=new_close,
            volume=float(self._tick_volume),
            extra_fields=self._extra_fields(),
            is_closed=False,
        )
        self._last_bar_ohlcv = synth
        return synth
