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
* The closed-bar history query (``backfill_closed_bars``) the framework
  uses to fill every gap of the stream: at startup, after a reconnect and
  for a close the socket missed. The plugin itself recovers no bar; it
  declares the venue's history properties instead
  (:attr:`~_StreamingMixin.history_has_idle_bars`,
  :attr:`~_StreamingMixin.history_publish_lag`,
  :attr:`~_StreamingMixin.history_revises_before_lag`,
  :attr:`~_StreamingMixin.stream_closes_match_history`): every closed bar the
  strategy runs on comes from ``/prices``, the socket's close only stands in when
  the history has not answered by the close's deadline.

State touched: ``_ws``, ``_update_queue``, ``_listen_task``,
``_ping_task``, ``_feed_watchdog_task``, ``_last_payload_ts``,
``_last_bar_timestamp``, ``_last_bar_ohlcv``, ``_last_bid``,
``_last_ask``, ``_ws_quote_buckets``, ``_ws_coverage_started_at``,
``_ws_volume_baseline``, ``_ws_bad_bar_streak``.
"""
import asyncio
import collections
import json
import statistics
from abc import ABC
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from time import time as epoch_time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from httpx import HTTPError
from websockets.exceptions import WebSocketException

from pynecore.core.plugin import override
from pynecore.lib.log import broker_info, broker_warning
from pynecore.core.session import is_point_in_session
from pynecore.lib.timeframe import in_seconds
from pynecore.types.ohlcv import OHLCV

from ._base import _CapitalComBase, _QuoteSnapshot
from .exceptions import CapitalComError, HistoricalPricesNotFoundError
from .helpers import (
    _is_session_error,
    _WS_VOLUME_BAD_BAR_RECONNECT_THRESHOLD,
    _WS_VOLUME_BASELINE_BARS,
    _WS_VOLUME_LOW_RATIO,
    _WS_VOLUME_MIN_BASELINE_BARS,
    ws_url,
)


_OHLC_CLOSE_CLOCK_SKEW_TOLERANCE_S = 3.0


class StreamDisconnectOrigin(StrEnum):
    """Closed origin vocabulary for WebSocket disconnect evidence."""

    REMOTE = "remote"
    QUOTE_VOLUME_WATCHDOG = "quote-volume-watchdog"
    WORKER_LIVENESS = "worker-liveness"


class StreamDisconnectReason(StrEnum):
    """Closed reason vocabulary for WebSocket disconnect evidence."""

    LISTENER_ENDED = "listener-ended"
    LISTENER_ERROR = "listener-error"
    SESSION_INVALID = "session-invalid"
    FEED_STALE = "feed-stale"
    QUOTE_VOLUME_STALE = "quote-volume-stale"
    FRESH_STREAM_STALE = "fresh-stream-stale"


@dataclass(frozen=True, slots=True)
class StreamDisconnectEvidence:
    """Credential-free cause and connection generation for one disconnect."""

    origin: StreamDisconnectOrigin
    reason: StreamDisconnectReason
    connection_generation: int


class _StreamingMixin(_CapitalComBase, ABC):
    """WebSocket streaming mix-in: connect / listen / ping / watchdog / synth."""

    history_has_idle_bars = False
    """``/prices`` has no bar for a slot without a price update.

    The venue answers such a slot with ``error.prices.not-found`` or leaves it out
    of the page, also inside the session, so the framework emits no bar for it
    either.
    """

    history_publish_lag = 10.0
    """Seconds after a close until ``/prices`` returns the closed bar with its final values.

    Measured 2026-11-17 on 20 one-minute closes, polled every second: the revision
    came 7-8 s after the close every time, and the bar did not change again.
    """

    history_revises_before_lag = True
    """``/prices`` returns a slot right at its close, then revises it once.

    The bar answered at the close lacks the slot's last price updates; its final
    values arrive within :attr:`~_StreamingMixin.history_publish_lag`.
    """

    stream_closes_match_history = False
    """The ``ohlc.event`` candle is not the ``/prices`` bar.

    The venue builds the socket candle from the throttled ``quote`` stream (it equals
    the OHLC of the quotes the socket delivered), while ``/prices`` aggregates every
    price update: measured 2026-11-17, a fifth of the socket closes differed from
    ``/prices`` in a calm hour and every one in a volatile stretch, by up to hundreds
    of ticks. The framework therefore takes every closed bar from the history.
    """

    _connection_generation: int = 0
    _last_disconnect_evidence: StreamDisconnectEvidence | None = None

    @property
    def stream_connection_generation(self) -> int:
        """Return the generation of the current or most recent WebSocket."""
        return self._connection_generation

    def _record_stream_disconnect(
        self,
        origin: StreamDisconnectOrigin,
        reason: StreamDisconnectReason,
        *,
        generation: int | None = None,
    ) -> StreamDisconnectEvidence:
        """Record the first structured cause observed for one connection generation."""
        connection_generation = (
            self._connection_generation if generation is None else generation
        )
        current: StreamDisconnectEvidence | None = self._last_disconnect_evidence
        if current is not None and current.connection_generation == connection_generation:
            return current
        evidence = StreamDisconnectEvidence(
            origin=origin,
            reason=reason,
            connection_generation=connection_generation,
        )
        self._last_disconnect_evidence = evidence
        return evidence

    def take_stream_disconnect_evidence(self) -> StreamDisconnectEvidence:
        """Consume the structured cause for the most recent WebSocket disconnect."""
        evidence = self._last_disconnect_evidence
        if (
            evidence is None
            or evidence.connection_generation != self._connection_generation
        ):
            evidence = StreamDisconnectEvidence(
                origin=StreamDisconnectOrigin.REMOTE,
                reason=StreamDisconnectReason.LISTENER_ENDED,
                connection_generation=self._connection_generation,
            )
        self._last_disconnect_evidence = None
        return evidence

    async def request_stream_reconnect(self, reason: StreamDisconnectReason) -> bool:
        """Close the active WebSocket with structured worker-liveness evidence."""
        if reason is not StreamDisconnectReason.FRESH_STREAM_STALE:
            raise ValueError(f"unsupported worker reconnect reason: {reason}")
        websocket = self._ws
        if websocket is None or websocket.close_code is not None:
            return False
        self._record_stream_disconnect(
            StreamDisconnectOrigin.WORKER_LIVENESS,
            reason,
        )
        try:
            await websocket.close(code=4001, reason=reason.value)
        except (WebSocketException, OSError):
            pass
        return True

    # --- Session-calendar helper -----------------------------------------

    def _market_open_now(self) -> bool:
        """True iff ``self._sym_info`` says the market is currently open.

        Returns True when ``_sym_info`` is unset or its ``opening_hours``
        is empty — preserves legacy behaviour for 24/7 instruments and
        for the early-init window before ``update_symbol_info`` lands a
        ``SymInfo``. The feed-staleness watchdog consults this to suppress
        its active WS close during known-closed windows; if the WS dies on
        its own anyway (e.g. Capital.com's 10-minute inactivity timeout over
        a weekend) the framework reconnect loop is itself gated on the
        same calendar.

        Point-in-time semantics: does **not** extend wall-clock by one
        timeframe, so a higher TF (1h, 1D) cannot report the market as
        already open one TF before its real session start.
        """
        sym_info = self._sym_info
        if sym_info is None or not sym_info.opening_hours:
            return True
        try:
            tz = ZoneInfo(sym_info.timezone)
        except (ZoneInfoNotFoundError, ValueError):
            return True
        local_dt = datetime.fromtimestamp(epoch_time(), tz=tz)
        return is_point_in_session(sym_info.opening_hours, local_dt,
                                   sym_info.session_corrections)

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
        ``("quote", bar_open_s)`` tuples. The backfill worker snapshots the
        current bar-scoped quote count when forwarding each quote to the
        consumer, which turns it into a synthetic intra-bar OHLCV update. The
        spinner and tick hooks therefore see fresh bid/ask at every tick, not
        only on bar close, without REST or consumer lag erasing forming volume.
        """
        assert (self._ws is not None
                and self._update_queue is not None
                and self._raw_ohlc_queue is not None)
        # Bind the raw queue locally so every ``put`` in this listener
        # (including the disconnect signal in ``finally``) targets the
        # queue THIS listener was started against. If a fast
        # disconnect/reconnect cycle replaces ``self._raw_ohlc_queue``
        # before this task's cancellation reaches ``finally``,
        # re-reading the attribute would point at the NEW connection's
        # queue and a stale signal would land on it - the worker bound
        # to the new queue would then emit a spurious ``None`` and
        # ``watch_ohlcv`` would raise ``ConnectionError`` on an
        # otherwise healthy reconnect.
        raw_q = self._raw_ohlc_queue
        connection_generation = self._connection_generation
        pending_ohlc_tasks: set[asyncio.Task[None]] = set()

        async def forward_ohlc_at_close(
            ohlc_payload: dict,
            bar_timestamp_ms: int,
            close_boundary: float,
        ) -> None:
            delay = max(0.0, close_boundary - epoch_time())
            if delay > 0.0:
                loop = asyncio.get_running_loop()
                deadline_reached: asyncio.Future[None] = loop.create_future()

                def release_deadline(_timer_arg: object | None = None) -> None:
                    if not deadline_reached.done():
                        deadline_reached.set_result(None)

                timer = loop.call_later(delay, release_deadline, None)
                try:
                    await deadline_reached
                finally:
                    timer.cancel()
            if (
                self._connection_generation != connection_generation
                or self._raw_ohlc_queue is not raw_q
            ):
                return
            forwarded_at = epoch_time()
            max_live_age = max(
                300,
                3 * (
                    int(in_seconds(self.timeframe))
                    if self.timeframe is not None
                    else 60
                ),
            )
            bar_timestamp = bar_timestamp_ms // 1000
            if forwarded_at - bar_timestamp > max_live_age:
                broker_warning(
                    "dropping stale Capital.com WS OHLC event ts=%d age=%.1fs",
                    bar_timestamp,
                    forwarded_at - bar_timestamp,
                )
                return
            await raw_q.put(("ohlc", ohlc_payload))

        def task_completed(task: asyncio.Task[None]) -> None:
            pending_ohlc_tasks.discard(task)
            if task.cancelled():
                return
            error = task.exception()
            if error is not None:
                broker_warning(
                    "Capital.com deferred OHLC forwarding raised %s: %s",
                    type(error).__name__,
                    error,
                )

        # Initialise the stale-feed stamp at the first iteration of the
        # loop so the watchdog does not fire while we are still in the
        # subscribe handshake (no market payloads yet by definition).
        self._last_payload_ts = epoch_time()
        try:
            async for raw in self._ws:
                self._last_payload_ts = epoch_time()
                try:
                    data = json.loads(raw)
                    dest = data.get("destination", "")
                    if data.get("status") == "ERROR":
                        # Capital.com echoes the subscribe destination with a
                        # ``status: "ERROR"`` frame when a request is rejected.
                        # A session-invalid code means our tokens were rotated
                        # out (typically a second run on the same account): flag
                        # it so the next ``connect()`` re-authenticates, then
                        # break to trigger the reconnect. Non-session errors are
                        # logged but must not tear the loop down — the socket is
                        # otherwise healthy and later frames may be fine.
                        err_code = str(data.get("errorCode") or "")
                        if _is_session_error(err_code):
                            broker_warning(
                                "Capital.com WS rejected a frame with an "
                                "invalid session (%s); re-authenticating on "
                                "reconnect", err_code,
                            )
                            self._ws_session_invalid = True
                            self._record_stream_disconnect(
                                StreamDisconnectOrigin.REMOTE,
                                StreamDisconnectReason.SESSION_INVALID,
                                generation=connection_generation,
                            )
                            break
                        broker_warning(
                            "Capital.com WS error frame (%s); continuing",
                            err_code or data,
                        )
                        continue
                    if dest == "ohlc.event":
                        payload_dict = data.get("payload") or {}
                        price_type = str(
                            payload_dict.get("priceType", "bid")
                        ).lower()
                        if price_type != "bid":
                            # Ask-side duplicate (Pine OHLC is bid-side);
                            # drop entirely instead of routing through
                            # the backfill worker just to be filtered
                            # later.
                            continue
                        now = epoch_time()
                        t_ms: int | float | str | None = payload_dict.get("t")
                        timestamp_ms: int | None = None
                        if t_ms is not None:
                            parsed_timestamp_ms = int(t_ms)
                            timestamp_ms = parsed_timestamp_ms
                            timestamp = parsed_timestamp_ms // 1000
                            tf_seconds = (
                                int(in_seconds(self.timeframe))
                                if self.timeframe is not None
                                else 60
                            )
                            closes_at = timestamp + tf_seconds
                            closes_in = closes_at - now
                            if closes_in > _OHLC_CLOSE_CLOCK_SKEW_TOLERANCE_S:
                                broker_warning(
                                    "dropping premature Capital.com WS OHLC "
                                    "event ts=%d closes_in=%.1fs",
                                    timestamp,
                                    closes_in,
                                )
                                continue
                            if closes_in > 0.0:
                                pending_task = asyncio.create_task(
                                    forward_ohlc_at_close(
                                        dict(payload_dict),
                                        parsed_timestamp_ms,
                                        closes_at,
                                    )
                                )
                                pending_ohlc_tasks.add(pending_task)
                                pending_task.add_done_callback(task_completed)
                                continue
                        # The worker owns closed-bar volume resolution
                        # end-to-end: it consumes only this bar's quote
                        # bucket and decides REST vs. WS. Forming updates
                        # read their own bar-scoped bucket, so asynchronous
                        # REST backfill cannot reset or contaminate them.
                        if t_ms is None:
                            await raw_q.put(("ohlc", payload_dict))
                        elif timestamp_ms is not None:
                            await forward_ohlc_at_close(
                                payload_dict,
                                timestamp_ms,
                                now,
                            )
                    elif dest == "quote":
                        payload = data.get("payload") or {}
                        bid: float | str | None = payload.get("bid")
                        ofr: float | str | None = payload.get("ofr")
                        bid_snapshot = float(bid) if bid is not None else None
                        ask_snapshot = float(ofr) if ofr is not None else None
                        # Bucket the tick into the bar the quote
                        # actually belongs to (via its own
                        # ``timestamp``), NOT into whichever bar
                        # happens to be open when ``ohlc.event``
                        # arrives. A reconnect mid-bar would otherwise
                        # stamp one shared counter against the new
                        # connection's current bar, then close it as
                        # if it were the full count -> deterministic
                        # V=1 contamination. With bucketing, a partial
                        # bar (started before WS came up) gets routed
                        # through the REST fallback path by the worker
                        # because the bar's open predates
                        # ``_ws_coverage_started_at``.
                        ts_ms = payload.get("timestamp")
                        if ts_ms is None:
                            # Defensive: docs guarantee ``timestamp``;
                            # if a server build ever omits it, fall
                            # back to wallclock and warn once per
                            # connection so we notice the regression.
                            if not self._ws_quote_timestamp_warned:
                                broker_warning(
                                    "Capital.com quote payload missing "
                                    "'timestamp'; falling back to "
                                    "wallclock for bar bucketing "
                                    "(payload keys=%s)",
                                    sorted(payload.keys()),
                                )
                                self._ws_quote_timestamp_warned = True
                            ts_ms = int(epoch_time() * 1000)
                        tf_seconds = (int(in_seconds(self.timeframe))
                                      if self.timeframe is not None
                                      else 0)
                        bar_open_s: int | None
                        cumulative_volume: int
                        if tf_seconds > 0:
                            ts_s = int(ts_ms) // 1000
                            calculated_bar_open_s = ts_s - (ts_s % tf_seconds)
                            bar_open_s = calculated_bar_open_s
                            cumulative_volume = (
                                self._ws_quote_buckets.get(
                                    calculated_bar_open_s,
                                    0,
                                ) + 1
                            )
                            self._ws_quote_buckets[calculated_bar_open_s] = (
                                cumulative_volume
                            )
                        else:
                            bar_open_s = None
                            cumulative_volume = 0
                        # Route quotes through the same FIFO queue as
                        # bid bars so the volume backfill worker
                        # preserves stream order: a quote that arrives
                        # after a closed bar must not reach
                        # ``_update_queue`` ahead of that bar while
                        # REST volume resolution is in flight.
                        # Otherwise ``_synth_from_quote`` would
                        # mutate/emit the previous bar with next-bar
                        # prices before the closed bar lands.
                        await raw_q.put((
                            "quote",
                            _QuoteSnapshot(
                                bar_open_s=bar_open_s,
                                cumulative_volume=cumulative_volume,
                                bid=bid_snapshot,
                                ask=ask_snapshot,
                            ),
                        ))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # A single malformed frame must not bring the WS
                    # loop down. Until this hardening landed every
                    # exception in the per-frame body silently exited
                    # the listener, posted the disconnect sentinel,
                    # and forced a full reconnect cycle. Log the
                    # offender and keep reading: the next frame may be
                    # perfectly fine.
                    broker_warning(
                        "Capital.com WS frame handler raised %s: %s; "
                        "continuing",
                        type(exc).__name__, exc,
                    )
                    continue
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            # Loop-level failure (likely from ``async for raw in self._ws``
            # itself: socket reset, decode error past json.loads, etc.).
            # Surface it so the next reconnect is not a complete mystery.
            self._record_stream_disconnect(
                StreamDisconnectOrigin.REMOTE,
                StreamDisconnectReason.LISTENER_ERROR,
                generation=connection_generation,
            )
            broker_warning(
                "Capital.com WS listener loop ended with %s: %s",
                type(exc).__name__, exc,
            )
        finally:
            for pending_task in pending_ohlc_tasks:
                pending_task.cancel()
            if pending_ohlc_tasks:
                await asyncio.gather(*pending_ohlc_tasks, return_exceptions=True)
            self._record_stream_disconnect(
                StreamDisconnectOrigin.REMOTE,
                StreamDisconnectReason.LISTENER_ENDED,
                generation=connection_generation,
            )
            # Live runner reconnect is gated on a ``None`` sentinel in
            # the consumer queue. Route the disconnect signal through
            # ``raw_q`` so any bid bar still parked there (waiting for
            # its REST volume lookup) is forwarded BEFORE the sentinel
            # lands on ``_update_queue``. Putting ``None`` directly on
            # the consumer queue here would let ``watch_ohlcv`` raise
            # ``ConnectionError`` while a fully-received closed bar is
            # still mid-pipeline, losing that bar on reconnect.
            #
            # :meth:`_volume_backfill_worker_loop` recognises the
            # ``"disconnect"`` tag and forwards a single ``None`` to
            # ``_update_queue`` once every preceding raw frame has
            # been drained. The local ``raw_q`` binding ensures the
            # signal targets THIS listener's queue, never a successor
            # connection's queue established by a fast
            # disconnect/reconnect cycle.
            #
            # The queues are unbounded, so ``put_nowait`` cannot fail
            # with ``QueueFull`` - no try/except needed.
            raw_q.put_nowait(("disconnect", None))

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
                # Session-gate: market in a known-closed window, the
                # silence is expected. Do not close the socket — the
                # framework reconnect loop is itself gated and would
                # just bounce here indefinitely otherwise.
                if not self._market_open_now():
                    continue
                # Reset the stamp BEFORE awaiting the close so a
                # second pass through the loop doesn't double-fire
                # while the close is still in flight; the listener's
                # finally clause delivers the sentinel.
                self._last_payload_ts = 0.0
                self._record_stream_disconnect(
                    StreamDisconnectOrigin.REMOTE,
                    StreamDisconnectReason.FEED_STALE,
                )
                try:
                    await self._ws.close(
                        code=4000,
                        reason=StreamDisconnectReason.FEED_STALE.value,
                    )
                except (WebSocketException, OSError):
                    # Already-broken socket: the listener's finally is
                    # what guarantees the sentinel anyway.
                    pass
        except asyncio.CancelledError:
            pass

    async def _volume_backfill_worker_loop(self) -> None:
        """Resolve volume for each bid bar — WS-primary, REST fallback.

        Capital.com's WS ``ohlc.event`` carries no volume field. The
        per-quote ``timestamp`` is bucketed into
        :attr:`_ws_quote_buckets` by the listener (one bucket per
        bar OPEN), giving an authoritative tick-volume proxy *for full-
        coverage bars*. REST ``lastTradedVolume`` is the safety net
        for three suspect cases: partial coverage (bar started before
        the WS came up), zero quotes (the quote feed dropped silently),
        or a value far below the rolling baseline (silent throttling).

        Why WS-primary, not REST-primary as before: a REST roundtrip
        per closed bar burns the rate-limit budget and adds ~1s of
        publish-lag risk. The quote-bucket count is already a sound
        live-volume proxy; REST only earns its keep when the WS value
        is structurally suspect.

        Why per-bar buckets, not a global counter as before: a
        reconnect partway through a bar would otherwise stamp the new
        connection's tick count against the current bar at bar close,
        producing a deterministic V=1 contamination. With timestamp
        bucketing, the worker recognises the bar as partial (open
        predates :attr:`_ws_coverage_started_at`) and falls through to
        REST.

        Fallback decision matrix (full chain):

        =====================================  =============================
        Case                                   Emitted volume
        =====================================  =============================
        ws_vol > 0 AND full coverage AND       ws_vol (also appended to
        (no baseline OR ws_vol >= median*r)    rolling baseline)
        partial coverage OR ws_vol == 0 OR     REST if > 0; else median
        (baseline ready AND ws < median*r)     estimate if baseline ready;
                                               else raw ws_vol + warning
        =====================================  =============================

        REST-confirmed-bad streak: when REST fires for a full-coverage
        bar and reports materially more volume than the WS counted
        (``ws_vol < rest * _WS_VOLUME_LOW_RATIO``), the worker increments
        :attr:`_ws_bad_bar_streak`; a REST value that agrees with the WS
        count resets it. At :data:`_WS_VOLUME_BAD_BAR_RECONNECT_THRESHOLD`
        consecutive hits the WS is closed (``code=4001``,
        ``reason="quote-volume-stale"``) so the live_runner reconnects.
        Partial-coverage bars NEVER count toward this streak.

        Quote frames preserve their order relative to in-flight bid bars
        (single-consumer FIFO) and carry immutable bar-open, cumulative-volume,
        bid and ask snapshots. A slow REST lookup for the preceding close or a
        lagging consumer therefore cannot erase or replace queued tick state.

        REST stall bound: the underlying ``httpx`` client uses a 50s
        request timeout. Cap each REST lookup at
        ``backfill_volume_timeout_s`` (5s), further shrunk against the
        live_runner's per-bar synth deadline, so a slow roundtrip
        degrades to the median estimate (or raw WS) instead of holding
        the OHLC pipeline.
        """
        assert (self._raw_ohlc_queue is not None
                and self._update_queue is not None)
        raw_q = self._raw_ohlc_queue
        out_q = self._update_queue
        backfill_volume_timeout_s = 5.0
        synth_deadline_margin_s = 1.0
        tf_seconds = (float(in_seconds(self.timeframe))
                      if self.timeframe is not None else 0.0)
        # Mirror ``live_runner.py``'s ``bar_grace`` formula EXACTLY so the
        # deadline budget matches the framework's synth deadline for THIS
        # TF. For tf>=60s the actual grace is 30s (not 15s).
        framework_grace_s = max(15.0, min(tf_seconds * 0.5, 30.0))
        try:
            while True:
                event_type, payload = await raw_q.get()
                if event_type == "disconnect":
                    # Listener has exited and routed its sentinel
                    # through this queue so we drain any preceding
                    # frames first. Forward a single ``None`` to the
                    # consumer queue so ``watch_ohlcv`` returns the
                    # ``ConnectionError`` only AFTER every received
                    # bar has been delivered, then stop.
                    out_q.put_nowait(None)
                    return
                if event_type != "ohlc":
                    # The listener already froze this frame's bar, cumulative
                    # count and prices. Forward the immutable snapshot verbatim
                    # so neither REST nor consumer lag can substitute later
                    # mutable state.
                    await out_q.put((event_type, payload))
                    continue
                try:
                    await self._process_ohlc_payload(
                        payload,
                        out_q=out_q,
                        tf_seconds=tf_seconds,
                        framework_grace_s=framework_grace_s,
                        synth_deadline_margin_s=synth_deadline_margin_s,
                        backfill_volume_timeout_s=backfill_volume_timeout_s,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    broker_warning(
                        "Volume backfill worker dropped bar payload "
                        "(t=%s) due to %s: %s",
                        payload.get("t") if payload else None,
                        type(exc).__name__, exc,
                    )
        except asyncio.CancelledError:
            try:
                out_q.put_nowait(None)
            except asyncio.QueueFull:
                pass
            return
        except Exception as exc:
            # Unhandled exception in the outer loop: surface the WS
            # disconnect sentinel so the consumer wakes up and the
            # live_runner reconnects instead of waiting forever on the
            # dead worker.
            broker_warning(
                "Volume backfill worker died: %s: %s",
                type(exc).__name__, exc,
            )
            try:
                out_q.put_nowait(None)
            except asyncio.QueueFull:
                pass
            return

    async def _process_ohlc_payload(
        self,
        payload: dict,
        *,
        out_q: asyncio.Queue,
        tf_seconds: float,
        framework_grace_s: float,
        synth_deadline_margin_s: float,
        backfill_volume_timeout_s: float,
    ) -> None:
        """Body of :meth:`_volume_backfill_worker_loop` for a single bar.

        Extracted so the worker loop can catch per-bar exceptions and
        drop just the offending bar instead of tearing down the whole
        pipeline.
        """
        assert payload is not None
        bar_open_s_int = int(payload["t"]) // 1000
        bar_open_s = float(payload["t"]) / 1000.0
        # Pull this bar's quote bucket and drop any older orphans
        # (late quotes that never made it before the bar closed are
        # unrecoverable; Capital.com's throttled feed makes the
        # window vanishingly small).
        ws_vol = self._ws_quote_buckets.pop(bar_open_s_int, 0)
        for stale_ts in [k for k in self._ws_quote_buckets
                         if k < bar_open_s_int]:
            self._ws_quote_buckets.pop(stale_ts, None)

        is_partial = bar_open_s_int < self._ws_coverage_started_at
        baseline = self._ws_volume_baseline
        baseline_ready = (
            len(baseline) >= _WS_VOLUME_MIN_BASELINE_BARS
        )
        rolling_median: float = (
            float(statistics.median(baseline))
            if baseline_ready else 0.0
        )
        low_ratio = (
            baseline_ready
            and ws_vol < rolling_median * _WS_VOLUME_LOW_RATIO
        )
        need_rest = is_partial or ws_vol == 0 or low_ratio

        rest_vol: float = 0.0
        rest_attempted = False
        if need_rest:
            rest_vol = await self._fetch_bar_volume_with_budget(
                bar_open_s_int,
                bar_open_s=bar_open_s,
                tf_seconds=tf_seconds,
                framework_grace_s=framework_grace_s,
                synth_deadline_margin_s=synth_deadline_margin_s,
                backfill_volume_timeout_s=backfill_volume_timeout_s,
            )
            rest_attempted = True

        chosen: float
        reason: str
        if not need_rest:
            chosen = float(ws_vol)
            reason = "ws_primary"
            baseline.append(ws_vol)
        elif rest_vol > 0.0:
            chosen = rest_vol
            reason = (
                "partial" if is_partial
                else ("zero" if ws_vol == 0 else "low_ratio")
            )
        elif baseline_ready:
            chosen = rolling_median
            reason = "rest_failed_median_estimate"
        else:
            chosen = float(ws_vol)
            reason = "rest_failed_no_baseline"
            broker_warning(
                "WS volume fallback chain exhausted for ts=%d "
                "(ws=%d, no baseline); emitting raw count",
                bar_open_s_int, ws_vol,
            )

        if reason != "ws_primary":
            # The venue's REST volume is exact; only an estimate is worth a warning
            log_fallback = broker_info if rest_vol > 0.0 else broker_warning
            log_fallback(
                "WS volume fallback (ts=%d, reason=%s, "
                "ws=%d, rest=%.1f, median=%.1f)",
                bar_open_s_int, reason, ws_vol,
                rest_vol, rolling_median,
            )

        # REST-confirmed bad streak: bump only when the WS value
        # was demonstrably wrong on a *full-coverage* bar (REST
        # reports materially more than the WS counted). A low bar
        # that REST confirms is a quiet market, not a silent quote
        # stream, and resets the streak. Partial bars never count —
        # those are expected after every reconnect.
        if rest_attempted and not is_partial and rest_vol > 0.0:
            if ws_vol < rest_vol * _WS_VOLUME_LOW_RATIO:
                self._ws_bad_bar_streak += 1
            else:
                self._ws_bad_bar_streak = 0
        elif not need_rest:
            self._ws_bad_bar_streak = 0
        # `rest_failed_*` paths leave the streak unchanged: we
        # could not confirm whether the WS was wrong or REST was
        # simply unavailable.

        payload["_volume"] = chosen
        await out_q.put(("ohlc", payload))

        if (self._ws_bad_bar_streak
                >= _WS_VOLUME_BAD_BAR_RECONNECT_THRESHOLD):
            streak = self._ws_bad_bar_streak
            self._ws_bad_bar_streak = 0
            ws = self._ws
            if ws is not None and ws.close_code is None:
                broker_warning(
                    "WS quote-volume stale "
                    "(consecutive bad bars=%d), forcing reconnect",
                    streak,
                )
                self._record_stream_disconnect(
                    StreamDisconnectOrigin.QUOTE_VOLUME_WATCHDOG,
                    StreamDisconnectReason.QUOTE_VOLUME_STALE,
                )
                try:
                    await ws.close(
                        code=4001,
                        reason=StreamDisconnectReason.QUOTE_VOLUME_STALE.value,
                    )
                except (WebSocketException, OSError):
                    # Already-broken socket: the listener's finally
                    # clause delivers the sentinel anyway, so
                    # swallow and let the loop drain.
                    pass

    async def _fetch_bar_volume_with_budget(
        self,
        timestamp: int,
        *,
        bar_open_s: float,
        tf_seconds: float,
        framework_grace_s: float,
        synth_deadline_margin_s: float,
        backfill_volume_timeout_s: float,
    ) -> float:
        """REST lastTradedVolume with synth-deadline-aware timeout cap.

        Returns ``0.0`` on every kind of miss (WS down, no budget left,
        timeout, exception, or REST returns 0). Callers must treat 0
        as "REST did not deliver a usable value" — they may then fall
        through to a rolling-median estimate or raw WS count.

        Budget logic mirrors ``live_runner.py``'s synth deadline so a
        slow REST roundtrip degrades to the fallback path instead of
        holding the bar past the framework's V=0 synth window.
        """
        budget_s = (
            bar_open_s + tf_seconds + framework_grace_s
            - epoch_time() - synth_deadline_margin_s
        )
        rest_timeout_s = min(
            backfill_volume_timeout_s, max(0.0, budget_s),
        )
        ws = self._ws
        ws_down = ws is None or ws.close_code is not None
        if ws_down:
            broker_warning(
                "REST volume lookup skipped for ts=%d "
                "(WS disconnecting)",
                timestamp,
            )
            return 0.0
        if rest_timeout_s <= 0.0:
            broker_warning(
                "REST volume lookup skipped for ts=%d "
                "(bar age %.1fs, no budget vs framework synth)",
                timestamp, epoch_time() - bar_open_s - tf_seconds,
            )
            return 0.0
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self._fetch_bar_volume, timestamp),
                timeout=rest_timeout_s,
            )
        except asyncio.TimeoutError:
            broker_warning(
                "REST volume lookup timed out for ts=%d after %.1fs",
                timestamp, rest_timeout_s,
            )
            return 0.0
        except (CapitalComError, HTTPError, KeyError, OSError, TypeError, ValueError):
            return 0.0

    def _fetch_closed_bar_payloads(
        self,
        since: int,
        current_bar_open: int | None = None,
    ) -> list[dict]:
        """Read the venue's closed bars that opened after ``since``.

        Capital.com's ``/prices`` response is the source of truth here: a slot
        the venue returns no row for stays absent, and every returned row
        between ``since`` and the forming bar is handed back. Pages advance by
        one full timeframe past the last returned row, matching the inclusive
        ``from`` semantics used by historical downloads.

        A pure query: it reads no plugin cursor and writes no plugin state, so
        any number of calls in any order leave the plugin as it was. The
        venue's explicit ``error.prices.not-found`` answer and a successful
        empty page end the walk as "no further bar"; every other failure —
        transport, authentication, a response whose shape or order cannot be
        trusted — raises, so an unanswered query is never mistaken for an
        empty history.

        :param since: Opening second of the newest bar the caller holds; only
            later bars are returned.
        :param current_bar_open: Opening second of the bar still forming, which
            is never returned; defaults to the current wall-clock slot.
        :return: ``ohlc.event``-shaped payloads with a resolved ``_volume``,
            ascending by opening time.
        :raises CapitalComError: The plugin has no timeframe to query with.
        :raises ValueError: A history page whose shape or order is unusable.
        """
        if self.timeframe is None:
            raise CapitalComError("closed-bar history needs the plugin timeframe")
        tf_seconds = int(in_seconds(self.timeframe))
        if current_bar_open is None:
            current_bar_open = int(epoch_time() // tf_seconds * tf_seconds)
        cursor = since + tf_seconds

        payloads: dict[int, dict] = {}
        while cursor < current_bar_open:
            time_from = datetime.fromtimestamp(
                cursor, tz=timezone.utc,
            ).replace(tzinfo=None)
            try:
                res = self.get_historical_prices(
                    time_from=time_from,
                    limit=1000,
                )
            except HistoricalPricesNotFoundError:
                break
            prices = res.get('prices')
            if not isinstance(prices, list):
                raise ValueError("Capital.com closed-bar history prices schema changed")
            if not prices:
                break
            last_returned_ts: int | None = None
            for bar in prices:
                if not isinstance(bar, dict):
                    raise ValueError("Capital.com closed-bar history bar schema changed")
                snap = bar.get('snapshotTimeUTC')
                if not isinstance(snap, str):
                    raise ValueError("Capital.com closed-bar history timestamp is missing")
                timestamp = int(
                    datetime.fromisoformat(snap).replace(
                        tzinfo=timezone.utc,
                    ).timestamp()
                )
                if timestamp < cursor:
                    raise ValueError(
                        "Capital.com closed-bar history page did not reach its cursor"
                    )
                if last_returned_ts is not None and timestamp <= last_returned_ts:
                    raise ValueError(
                        "Capital.com closed-bar history page is not strictly ordered"
                    )
                last_returned_ts = timestamp
                if not since < timestamp < current_bar_open:
                    continue
                payloads[timestamp] = {
                    "priceType": "bid",
                    "t": int(timestamp * 1000),
                    "o": float(bar['openPrice']['bid']),
                    "h": float(bar['highPrice']['bid']),
                    "l": float(bar['lowPrice']['bid']),
                    "c": float(bar['closePrice']['bid']),
                    "_volume": float(bar.get('lastTradedVolume') or 0.0),
                }
            if last_returned_ts is None:
                raise ValueError("Capital.com closed-bar history page was not consumed")
            cursor = last_returned_ts + tf_seconds
        return [payloads[timestamp] for timestamp in sorted(payloads)]

    @override
    async def backfill_closed_bars(
        self, symbol: str, timeframe: str, since_ms: int,
    ) -> list[OHLCV]:
        """Return the venue's closed bars that opened after ``since_ms``.

        The framework calls this for every gap of the stream — the startup
        seam, the outage of a reconnect, a close the socket missed — any
        number of times while connected. It is a pure ``/prices`` query (see
        :meth:`_fetch_closed_bar_payloads`): an empty list is the venue's
        answer that it has no such bar, and a query the venue did not answer
        raises. ``extra_fields`` is left empty: the bid/ask snapshot describes
        the current instant, not the instant these bars closed at.

        :param symbol: Provider-format symbol (unused; the plugin is bound to
            its own instrument).
        :param timeframe: Timeframe in TradingView format (unused; the plugin
            is bound to its own resolution).
        :param since_ms: Opening of the last bar the framework already holds.
        :return: Closed bars strictly after ``since_ms``, ascending; the bar
            still forming is never included.
        """
        payloads = await asyncio.to_thread(
            self._fetch_closed_bar_payloads, since_ms // 1000,
        )
        return [
            OHLCV(
                timestamp=int(payload["t"]),
                open=float(payload["o"]),
                high=float(payload["h"]),
                low=float(payload["l"]),
                close=float(payload["c"]),
                volume=float(payload["_volume"]),
                is_closed=True,
            )
            for payload in payloads
        ]

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

        if not self.cst_token or not self.security_token or self._ws_session_invalid:
            # ``create_session()`` runs two blocking httpx calls (50s
            # timeout each) — offload to a thread so the event loop
            # (watchdogs, WS ping) keeps running during login. Also forced
            # when the previous connection died on a session-invalid WS error:
            # a reconnect with the same stale tokens would loop forever, so we
            # re-authenticate first. The retry/backoff cadence between
            # reconnects is owned by the live runner (event-driven, no sleep).
            if self._ws_session_invalid:
                broker_info(
                    "Re-authenticating Capital.com session after a WS "
                    "session-invalid rejection"
                )
                self._ws_session_invalid = False
            await asyncio.to_thread(self.create_session)

        if self.store_ctx is not None:
            # Account-mode probe BEFORE the recovery passes: a hedging-mode
            # account opts into core one-way emulation (``position_port =
            # self``) and the recovery + the engine's restart replay must
            # already see the wired port. See ``_detect_account_mode``.
            await self._detect_account_mode()
            await self._load_activity_cursor_from_events()
            self._send_pending_manual_close_reports()
            self._reload_netting_pending()
            self._reload_held_edit_verdicts()
            await self._recover_in_flight_submissions()

        assert self.timeframe is not None and self.symbol is not None

        # WS ping/pong cadence: default `websockets` is 20s/20s = up to
        # 40s before a half-open connection is dropped. On a 1-minute
        # strategy that is too coarse — tighten to 5s/5s (≤10s detect)
        # so a TCP-level outage propagates inside one bar.
        self._ws = await websockets.connect(
            ws_url(self._capital_config.ws_url), ping_interval=5, ping_timeout=5,
        )
        self._connection_generation += 1
        self._last_disconnect_evidence = None

        self._update_queue = asyncio.Queue()
        self._raw_ohlc_queue = asyncio.Queue()
        # WS-primary volume state. Buckets restart per-connect because
        # they key on the THIS connection's quote stream; baseline is
        # also fresh — the rolling median is only useful between bars
        # from the same WS coverage window, so a quick reconnect would
        # otherwise mix pre- and post-reconnect counts that came from
        # different feed-health epochs.
        self._ws_quote_buckets = {}
        self._ws_coverage_started_at = epoch_time()
        self._ws_volume_baseline = collections.deque(
            maxlen=_WS_VOLUME_BASELINE_BARS,
        )
        self._ws_bad_bar_streak = 0
        self._ws_quote_timestamp_warned = False

        self._listen_task = asyncio.create_task(self._listen_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())
        self._feed_watchdog_task = asyncio.create_task(self._feed_watchdog_loop())
        self._volume_backfill_task = asyncio.create_task(
            self._volume_backfill_worker_loop()
        )

        # The four background tasks above are already running; if the
        # subscribe handshake fails, the exception propagates to the
        # live runner's retry loop and its next ``connect()`` would
        # overwrite the task references — the old tasks would keep
        # running orphaned (a second WS reader, doubled watchdogs) for
        # the rest of the process. Roll the partial init back before
        # re-raising so every retry starts from a clean slate.
        try:
            xchg_tf = self.to_exchange_timeframe(self.timeframe)
            await self._send("OHLCMarketData.subscribe", {
                "epics": [self.symbol],
                "resolutions": [xchg_tf],
                "type": "classic",
            }, correlation_id="ohlc_sub")

            await self._send("marketData.subscribe", {
                "epics": [self.symbol],
            }, correlation_id="market_sub")
        except BaseException:
            await self._abort_partial_connect()
            raise

    async def _abort_partial_connect(self) -> None:
        """Tear down a partially initialised connection.

        Only used by :meth:`connect` when the subscribe handshake fails
        after the background tasks were started. Cancels all four tasks
        and clears their references first (so nothing keeps consuming
        the dying WS), then closes the WebSocket and drops the queues.
        No unsubscribe frames are sent — the subscription never
        completed and the socket is likely broken anyway.
        """
        for attr in ("_listen_task", "_ping_task", "_feed_watchdog_task",
                     "_volume_backfill_task"):
            task: asyncio.Task | None = getattr(self, attr)
            if task is not None:
                task.cancel()
                setattr(self, attr, None)
        if self._ws is not None:
            try:
                await self._ws.close()
            except (WebSocketException, OSError):
                pass
            self._ws = None
        self._update_queue = None
        self._raw_ohlc_queue = None

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

        # Cancel the watchdog/keepalive tasks first — they only consume
        # state and never produce data into the consumer pipeline, so
        # their cancellation order is irrelevant to bar-loss safety.
        if self._ping_task:
            self._ping_task.cancel()
            self._ping_task = None
        if self._feed_watchdog_task:
            self._feed_watchdog_task.cancel()
            self._feed_watchdog_task = None

        # Close the WS before cancelling the listener so its ``async
        # for`` exits naturally; the listener's ``finally`` then posts
        # the ``("disconnect", None)`` sentinel onto ``_raw_ohlc_queue``
        # behind any closed bar still parked there mid-REST-resolution.
        if self._ws:
            await self._ws.close()
            self._ws = None

        # Defensive cancel: if the listener was already mid-``await``
        # on a frame at the moment ``ws.close()`` returned, the natural
        # exit may not yet have happened. Cancelling here forces the
        # ``finally`` to run regardless — it always runs, both on
        # natural exit and on ``CancelledError``.
        if self._listen_task:
            self._listen_task.cancel()
            self._listen_task = None

        # Drain the volume-backfill worker instead of cancelling it
        # straight away: it is the sole consumer of ``_raw_ohlc_queue``
        # and the only path that can move a closed bar (already
        # received from WS, currently in REST-volume resolution) into
        # ``_update_queue``. The worker's per-bar REST cap is 5s; budget
        # 6s to cover that plus the listener-sentinel handoff. When the
        # worker sees ``("disconnect", None)`` it forwards a single
        # ``None`` to ``_update_queue`` and exits — that is what wakes
        # any consumer parked on ``watch_ohlcv``.
        worker_drained = False
        if self._volume_backfill_task:
            try:
                await asyncio.wait_for(
                    self._volume_backfill_task, timeout=6.0,
                )
                worker_drained = True
            except asyncio.TimeoutError:
                # REST stuck past the cap, or the task is otherwise
                # wedged. Fall through to the safety-net ``None`` below
                # so the consumer still wakes; the bar in flight is
                # lost in this corner case, but holding ``disconnect``
                # open indefinitely would be worse.
                self._volume_backfill_task.cancel()
            except asyncio.CancelledError:
                # Worker was already cancelled by another code path;
                # the consumer-wake safety net below covers us.
                pass
            self._volume_backfill_task = None

        # Safety net: if the worker drained cleanly it already posted
        # ``None`` for us; an extra sentinel here would only matter if
        # the worker timed out without emitting (so the consumer is
        # still parked). Skip it on clean drain to avoid landing a
        # spurious second ``None`` on a queue a future reconnect might
        # not yet have replaced.
        if not worker_drained and self._update_queue is not None:
            self._update_queue.put_nowait(None)

        self._update_queue = None
        self._raw_ohlc_queue = None
        # ``_last_bar_timestamp`` is intentionally NOT cleared on
        # disconnect: it is the open of the newest closed bar this plugin
        # handed on, and keeping it across the reconnect is what stops the
        # new subscription from emitting a closed bar at or before one the
        # previous connection already emitted.
        self._last_bar_ohlcv = None
        self._last_bid = None
        self._last_ask = None
        # Quote buckets are per-connection only — they cannot survive
        # because the next ``connect()`` resets ``_ws_coverage_started_at``
        # and any retained bucket would be misclassified as full-coverage.
        self._ws_quote_buckets.clear()

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
                # skip and wait for the next event in that case. Bars
                # reach this point already enriched with volume by
                # :meth:`_volume_backfill_worker_loop`, so no REST call
                # happens on the consumer path and ``live_runner``'s
                # ``wait_for(timeout=2.0)`` cannot trip on backfill latency.
                result = self._on_ohlc_event(payload)
            else:
                # Quote arrived before any OHLC baseline yields None;
                # keep waiting until the first ``ohlc.event`` lands.
                assert isinstance(payload, _QuoteSnapshot)
                result = self._synth_from_quote(payload)
            if result is not None:
                return result

    def _fetch_bar_volume(self, timestamp: int) -> float:
        """Fetch ``lastTradedVolume`` for a single closed bar via REST.

        Used by the volume backfill worker to override a suspect WS quote
        bucket with Capital.com's authoritative per-bar
        ``lastTradedVolume``. Returns ``0.0`` if the bar is not yet
        published, the request fails, or the returned bar doesn't match
        our timestamp — callers keep the local tick count in that case.

        Capital.com's ``prices/{epic}`` ``from`` parameter is parsed as
        UTC when the ISO timestamp has **no offset suffix**. Passing an
        aware datetime via ``.isoformat()`` produces ``...+00:00`` which
        the API does not honour correctly (returns the wrong bar or an
        empty list). The warmup path in ``provider.py`` strips ``tzinfo``
        for the same reason — mirror that here.
        """
        try:
            time_from = datetime.fromtimestamp(
                timestamp, tz=timezone.utc,
            ).replace(tzinfo=None)
            res = self.get_historical_prices(time_from=time_from, limit=1)
            prices = res.get('prices') or []
            if not prices:
                return 0.0
            bar = prices[0]
            # Confirm we got the exact bar we asked for; if Capital.com
            # returned a neighbour (e.g. because the requested bar is not
            # yet published), fall through to the local tick count.
            snap = bar.get('snapshotTimeUTC')
            if snap:
                returned_ts = int(
                    datetime.fromisoformat(snap).replace(
                        tzinfo=timezone.utc,
                    ).timestamp()
                )
                if returned_ts != timestamp:
                    return 0.0
            return float(bar.get('lastTradedVolume') or 0.0)
        except (CapitalComError, HTTPError, KeyError, OSError, TypeError, ValueError):
            return 0.0

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

        A bar at or before :attr:`~_CapitalComBase._last_bar_timestamp` (the
        newest closed bar this plugin emitted, or the warmup tail) is
        dropped, so the stream never goes backwards, a reconnect included.
        Only this method and the warmup download move that cursor.

        A close that lands after the next slot's quotes already opened that
        slot's forming bar (the venue published it late, or the framework
        settled the slot from the history meanwhile) only moves the cursor: the
        later slot's forming bar stays the base its next tick extends.

        Intra-bar updates (spinner, tick hooks) come exclusively from
        :meth:`_synth_from_quote`.
        """
        price_type = str(payload.get("priceType", "bid")).lower()
        if price_type != "bid":
            return None
        timestamp_ms = int(payload["t"])
        timestamp = timestamp_ms // 1000
        if (
            self._last_bar_timestamp is not None
            and timestamp <= self._last_bar_timestamp
        ):
            return None
        # The payload is pre-enriched with ``_volume`` by
        # :meth:`_volume_backfill_worker_loop`. Quote volume is bar-scoped,
        # so this method must not mutate any later forming bar's bucket.
        volume = float(payload.get("_volume", 0.0))
        closed = OHLCV(
            timestamp=timestamp_ms,
            open=float(payload["o"]),
            high=float(payload["h"]),
            low=float(payload["l"]),
            close=float(payload["c"]),
            volume=volume,
            extra_fields=self._extra_fields(),
            is_closed=True,
        )
        self._last_bar_timestamp = timestamp
        if (self._last_bar_ohlcv is None
                or self._last_bar_ohlcv.timestamp <= timestamp_ms):
            self._last_bar_ohlcv = closed
        return closed

    def _synth_from_quote(self, quote: _QuoteSnapshot) -> OHLCV | None:
        """Synthesize an intra-bar OHLCV from one immutable tick snapshot.

        ``ohlc.event`` only pushes on bar close at some resolutions; the
        live spinner and tick hooks would then freeze between closes. To
        keep them fresh we emit a synthetic intra-bar OHLCV on each
        ``quote`` tick: O/H/L are kept from the current bar, ``close`` is
        the latest bid (Capital.com OHLC is bid-side for
        ``type: "classic"``), and H/L widen as the tick moves. The
        ``extra_fields`` snapshot carries the current bid/ask/spread so
        consumers can render real-time quotes.

        The listener freezes bar-open timestamp, cumulative volume, bid and ask
        when the frame arrives. The consumer applies exactly that snapshot, so
        queued ticks preserve spikes, retracements and volume even when REST or
        consumer work delays them.

        The synth is stamped with the quote's OWN bar slot
        (``_QuoteSnapshot.bar_open_s``, bucketed from the frame timestamp),
        never with the last closed bar's. Once a slot has closed, the ticks
        that follow belong to the next period: emitting them under the closed
        bar's timestamp would re-execute that settled bar and overwrite its
        committed OHLC, so every series derived from it — an EMA, an ATR, the
        ``close[1]`` a crossover compares against — would be computed from a
        price belonging to the following bar. The first tick of a fresh slot
        therefore OPENS that bar (O=H=L=C=bid) instead of inheriting the
        previous bar's levels; later ticks in the same slot widen H/L.

        A tick of a later slot opens that slot even while the close of the slot
        before it has not arrived: at every boundary the first tick of the new
        period races the old period's close, and a close the socket never
        delivers is settled by the framework from the history or from its
        ticks. Ordering the forming bar behind the unsettled slot is the
        framework's job (it holds updates of later slots until the awaited one
        is settled), so the feed never loses a slot's intra-bar updates here.

        Returns ``None`` if no OHLC baseline or bid has been received yet, if
        the quote belongs to a slot the feed has already closed, or if it is a
        late frame of a slot older than the bar already forming — the caller
        then waits for the next event. The snapshot still updates the latest
        quote state before those gates so the first later close can expose
        bid/ask fields even if no forming baseline existed yet.
        """
        if quote.bid is not None:
            self._last_bid = quote.bid
        if quote.ask is not None:
            self._last_ask = quote.ask
        if self._last_bar_ohlcv is None or quote.bid is None:
            return None
        bar_open_s = quote.bar_open_s
        if bar_open_s is None:
            # No timeframe to bucket by: the slot this tick belongs to is
            # unknowable, and guessing it is what corrupts closed bars.
            return None
        if (self._last_bar_timestamp is not None
                and bar_open_s <= self._last_bar_timestamp):
            # ``_last_bar_timestamp`` holds the open time of the most recent
            # CLOSED bar — this tick is a late frame for a settled slot.
            return None
        if bar_open_s * 1000 < self._last_bar_ohlcv.timestamp:
            # A later slot is already forming: reopening this older one would
            # make the next tick of the later slot open it afresh, losing the
            # open, high and low it accumulated.
            return None
        new_close = quote.bid
        if bar_open_s * 1000 != self._last_bar_ohlcv.timestamp:
            synth = OHLCV(
                timestamp=bar_open_s * 1000,
                open=new_close,
                high=new_close,
                low=new_close,
                close=new_close,
                volume=float(quote.cumulative_volume),
                extra_fields=self._extra_fields(),
                is_closed=False,
            )
        else:
            synth = self._last_bar_ohlcv._replace(
                high=max(self._last_bar_ohlcv.high, new_close),
                low=min(self._last_bar_ohlcv.low, new_close),
                close=new_close,
                volume=float(quote.cumulative_volume),
                extra_fields=self._extra_fields(),
                is_closed=False,
            )
        self._last_bar_ohlcv = synth
        return synth
