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
``_last_ask``, ``_tick_volume``, ``_ws_quote_buckets``,
``_ws_coverage_started_at``, ``_ws_volume_baseline``, ``_ws_bad_bar_streak``.
"""
import asyncio
import collections
import json
import statistics
from datetime import datetime, timezone
from time import time as epoch_time
from zoneinfo import ZoneInfo

from websockets.exceptions import WebSocketException

from pynecore.core.plugin import override
from pynecore.lib.log import broker_info, broker_warning
from pynecore.lib.session import _is_in_session, _is_point_in_session
from pynecore.lib.timeframe import in_seconds
from pynecore.types.ohlcv import OHLCV

from ._base import _CapitalComBase
from .helpers import (
    WS_URL,
    _WS_VOLUME_BAD_BAR_RECONNECT_THRESHOLD,
    _WS_VOLUME_BASELINE_BARS,
    _WS_VOLUME_LOW_RATIO,
    _WS_VOLUME_MIN_BASELINE_BARS,
)


class _StreamingMixin(_CapitalComBase):
    """WebSocket streaming mix-in: connect / listen / ping / watchdog / synth."""

    # --- Session-calendar helper -----------------------------------------

    def _market_open_now(self) -> bool:
        """True iff ``self._sym_info`` says the market is currently open.

        Returns True when ``_sym_info`` is unset or its ``opening_hours``
        is empty — preserves legacy behaviour for 24/7 instruments and
        for the early-init window before ``update_symbol_info`` lands a
        ``SymInfo``. The streaming watchdogs consult this to suppress
        active WS-close calls (REST recovery + ohlc-stale reconnect)
        during known-closed windows; if the WS dies on its own anyway
        (e.g. Capital.com's 10-minute inactivity timeout over a
        weekend) the framework reconnect loop is itself gated on the
        same calendar.

        Point-in-time semantics: does **not** extend wall-clock by one
        timeframe, so a higher TF (1h, 1D) cannot report the market as
        already open one TF before its real session start. The slot-
        aware variant for bar decisions is :meth:`_market_open_at`.
        """
        sym_info = self._sym_info
        if sym_info is None or not sym_info.opening_hours:
            return True
        try:
            tz = ZoneInfo(sym_info.timezone)
        except Exception:  # noqa: BLE001
            return True
        local_dt = datetime.fromtimestamp(epoch_time(), tz=tz)
        return _is_point_in_session(sym_info.opening_hours, local_dt)

    def _market_open_at(self, epoch_ts: float) -> bool:
        """True iff the symbol's opening_hours mark ``epoch_ts`` as open.

        Returns True for the 24/7 fallback (no calendar) so callers can
        delegate the open/closed decision unconditionally. The slot-
        aware variant of :meth:`_market_open_now`; the OHLC watchdog
        consults it for the *missing slot's* calendar state rather than
        wall-clock now, so a bar dropped just before the session edge
        still gets REST recovery instead of being treated as
        out-of-session.
        """
        sym_info = self._sym_info
        if sym_info is None or not sym_info.opening_hours:
            return True
        try:
            tz = ZoneInfo(sym_info.timezone)
        except Exception:  # noqa: BLE001
            return True
        assert self.timeframe is not None
        tf_seconds = max(1, int(in_seconds(self.timeframe)))
        local_dt = datetime.fromtimestamp(epoch_ts, tz=tz)
        return _is_in_session(sym_info.opening_hours, local_dt, tf_seconds)

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
                    if dest == "ohlc.event":
                        payload_dict = data.get("payload") or {}
                        price_type = str(
                            payload_dict.get("priceType", "bid")
                        ).lower()
                        if price_type != "bid":
                            # Ask-side duplicate (Pine OHLC is bid-side);
                            # drop entirely instead of routing through
                            # the backfill worker just to be filtered
                            # later. Do NOT advance liveness markers
                            # from an ask payload: if the bid candle
                            # was dropped server-side but the ask
                            # duplicate still arrived, advancing here
                            # would mask the missing bid bar from the
                            # OHLC watchdog.
                            continue
                        self._last_ohlc_event_ts = epoch_time()
                        t_ms = payload_dict.get("t")
                        if t_ms is not None:
                            # Stamp the bar OPEN of the latest forwarded
                            # event so the OHLC watchdog can anchor on
                            # the same axis as live_runner's synth
                            # deadline (bar-time, not arrival-wallclock).
                            self._last_bar_open_ts = float(t_ms) / 1000.0
                        # The worker owns volume resolution end-to-end:
                        # it reads ``_ws_quote_buckets[bar_open_s]``
                        # (filled by the quote branch using per-quote
                        # ``timestamp``), decides REST vs. WS, and
                        # resets ``_tick_volume`` for the synth
                        # spinner. Doing the reset here would race a
                        # late quote whose bucket the worker has not
                        # yet pulled.
                        await raw_q.put(("ohlc", payload_dict))
                    elif dest == "quote":
                        payload = data.get("payload") or {}
                        bid: float | str | None = payload.get("bid")
                        ofr: float | str | None = payload.get("ofr")
                        if bid is not None:
                            self._last_bid = float(bid)
                        if ofr is not None:
                            self._last_ask = float(ofr)
                        # Bucket the tick into the bar the quote
                        # actually belongs to (via its own
                        # ``timestamp``), NOT into whichever bar
                        # happens to be open when ``ohlc.event``
                        # arrives. A reconnect mid-bar would otherwise
                        # stamp ``_tick_volume`` against the new
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
                        if tf_seconds > 0:
                            ts_s = int(ts_ms) // 1000
                            bar_open_s = ts_s - (ts_s % tf_seconds)
                            self._ws_quote_buckets[bar_open_s] = (
                                self._ws_quote_buckets.get(bar_open_s, 0)
                                + 1
                            )
                        # Global counter remains the source for the
                        # intra-bar synth spinner
                        # (``_synth_from_quote``). The worker resets it
                        # when the bid bar closes so the next bar's
                        # spinner starts from 0, matching the legacy
                        # behaviour.
                        self._tick_volume += 1
                        # Stamp the quote-liveness anchor so the OHLC
                        # watchdog can tell "quotes still flowing =>
                        # token-bound OHLC failure, must reconnect"
                        # from "quotes also idle => expected
                        # market-closed gap, do not reconnect".
                        self._last_quote_event_ts = epoch_time()
                        # Route quotes through the same FIFO queue as
                        # bid bars so the volume backfill worker
                        # preserves stream order: a quote that arrives
                        # after a closed bar must not reach
                        # ``_update_queue`` ahead of that bar while
                        # REST volume resolution is in flight.
                        # Otherwise ``_synth_from_quote`` would
                        # mutate/emit the previous bar with next-bar
                        # prices before the closed bar lands.
                        await raw_q.put(("quote", None))
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
            broker_warning(
                "Capital.com WS listener loop ended with %s: %s",
                type(exc).__name__, exc,
            )
        finally:
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
                try:
                    await self._ws.close(code=4000, reason="feed-stale")
                except (WebSocketException, OSError):
                    # Already-broken socket: the listener's finally is
                    # what guarantees the sentinel anyway.
                    pass
        except asyncio.CancelledError:
            pass

    async def _ohlc_watchdog_loop(self) -> None:
        """Recover missing OHLC bars via REST before the framework synthesises.

        Capital.com occasionally drops a single ``ohlc.event`` payload
        while the underlying subscription remains alive (next bar
        arrives normally). It also has the token-bound death mode where
        ``OHLCMarketData.subscribe`` stops emitting while the parallel
        ``marketData.subscribe`` quote stream keeps the TCP pipe alive.
        Both produce the same observable: ``live_runner`` times out on
        the missing bar's TF slot, synthesises a V=0 zero-volume bar
        (which contaminates volume and TP/SL math), and the
        ``<= last_closed_bar.timestamp`` dedup filter drops the late
        real bar.

        The deadline is anchored on the *expected close* of the next
        bar — ``last_bar_open + 2*tf`` — plus a small REST publish-lag
        margin. Concretely we fire at
        ``last_bar_open + 2*tf + REST_PUBLISH_LAG_S``; the framework's
        synth deadline (``+ bar_grace``, where bar_grace >= 15s) sits
        comfortably further out, so REST query + inject finish before
        the framework would synth. Anchoring on next-close (not on
        ``framework_synth_at - lead``) keeps the watchdog independent
        of the framework's grace formula and removes the narrow timing
        window where the previous version could miss firing.

        Flow each iteration:

        1. Wait until ``REST_PUBLISH_LAG_S`` past the expected close.
        2. Query Capital.com REST for the missing bar (capped by
           ``REST_FETCH_TIMEOUT_S`` so a stalled httpx roundtrip cannot
           block the watchdog past the framework's synth grace).
        3. If REST returns the bar - inject it into ``_update_queue``
           as if it had come from WS; ``watch_ohlcv`` then returns a
           real closed bar and the framework's ``wait_for`` never
           times out.
        4. If REST hasn't published the bar yet (or timed out) - retry
           on the next iteration. Only after ``MAX_REST_WAIT_S`` has
           elapsed for the same slot do we escalate to ``ws.close``
           and force a reconnect (publish lag alone must not trigger
           a reconnect storm).
        5. If multiple consecutive bars are recovered from REST, the
           OHLC subscription itself is stale even though REST can hide
           the gap. Emit the recovered bar first, then close the WS so
           the live runner reconnects with a fresh subscription.
        """
        assert self.timeframe is not None
        tf_seconds = float(in_seconds(self.timeframe))
        rest_publish_lag_s = 10.0
        max_rest_wait_s = 20.0
        # Cap the REST roundtrip well below the framework's 15s synth
        # grace floor. The underlying httpx client uses a 50s request
        # timeout; without this cap a single stalled fetch would block
        # the watchdog past the synth deadline, defeating its purpose.
        rest_fetch_timeout_s = 5.0
        rest_recovery_reconnect_threshold = 2
        broker_info(
            "ohlc watchdog started: tf=%.0fs publish_lag=%.0fs max_wait=%.0fs",
            tf_seconds, rest_publish_lag_s, max_rest_wait_s,
        )
        # Tracks the slot currently considered missing and when we first
        # noticed it; reset whenever we either inject a bar or step past
        # the slot. Prevents an indefinite retry loop on publish lag.
        missing_slot_ts: int | None = None
        missing_slot_first_seen_at: float = 0.0
        consecutive_rest_recoveries = 0
        last_real_ohlc_event_ts = self._last_ohlc_event_ts
        try:
            while True:
                try:
                    await asyncio.sleep(1.0)
                    if self._ws is None or self._ws.close_code is not None:
                        continue
                    # Session-gate: gated on the *missing slot's*
                    # calendar state, not wall-clock now. The slot in
                    # question is the one immediately after the last
                    # known bar open (``_last_bar_open_ts + tf_seconds``).
                    # Using wall-clock would drop REST recovery for a
                    # valid closing-session bar whose ohlc.event was
                    # missed: the watchdog deadline lands a moment past
                    # session close, ``_market_open_now()`` returns
                    # false, the slot never gets fetched, and
                    # ``live_runner`` then synthesises a zero-volume bar
                    # for that still-in-session slot.
                    #
                    # Also disarm the anchor here: if we leave
                    # ``_last_bar_open_ts`` pointing at the pre-close
                    # bar, on session reopen the quote stream lights up
                    # before the first ``ohlc.event`` and the watchdog
                    # probes REST for stale weekend slots, potentially
                    # forcing a reconnect on an otherwise healthy
                    # subscription. The next real ``ohlc.event`` after
                    # reopen re-arms the watchdog via ``_listen_loop``.
                    #
                    # No-baseline case (``_last_bar_open_ts == 0.0``)
                    # falls through to wall-clock: there is no slot to
                    # gate on, so we use ``_market_open_now`` to keep
                    # the pre-baseline branch out of REST until quotes
                    # arrive in-session.
                    if self._last_bar_open_ts > 0.0:
                        candidate_missing_ts = (
                            self._last_bar_open_ts + tf_seconds
                        )
                        slot_in_session = self._market_open_at(
                            candidate_missing_ts
                        )
                    else:
                        slot_in_session = self._market_open_now()
                    if not slot_in_session:
                        missing_slot_ts = None
                        missing_slot_first_seen_at = 0.0
                        consecutive_rest_recoveries = 0
                        self._last_bar_open_ts = 0.0
                        continue
                    now = epoch_time()
                    if self._last_ohlc_event_ts > last_real_ohlc_event_ts:
                        last_real_ohlc_event_ts = self._last_ohlc_event_ts
                        consecutive_rest_recoveries = 0
                    if self._last_bar_open_ts == 0.0:
                        # Watchdog is disarmed — either we just connected
                        # without a baseline, or the session-gap branch
                        # below cleared the anchor. If quotes are flowing
                        # (within one TF), re-arm so a quote-alive /
                        # OHLC-dead state (token-bound subscription
                        # failure that survives the session edge) still
                        # escalates to REST inject or reconnect instead
                        # of letting ``_synth_from_quote`` emit forever
                        # off the last known closed bar. Anchor one TF
                        # before the current bar's open so ``missing_ts``
                        # below resolves to the current in-progress bar
                        # rather than the next one — otherwise the very
                        # first missing slot is silently skipped.
                        if (self._last_quote_event_ts > 0.0
                                and now - self._last_quote_event_ts < tf_seconds):
                            current_bar_open = (
                                (now // tf_seconds) * tf_seconds
                            )
                            self._last_bar_open_ts = (
                                current_bar_open - tf_seconds
                            )
                            self._last_ohlc_event_ts = now
                            last_real_ohlc_event_ts = now
                            consecutive_rest_recoveries = 0
                            missing_slot_ts = None
                            missing_slot_first_seen_at = 0.0
                        continue
                    next_bar_close_at = (
                        self._last_bar_open_ts + 2.0 * tf_seconds
                    )
                    if now < next_bar_close_at + rest_publish_lag_s:
                        # Reset slot tracking - we're still inside the
                        # normal arrival window for the next bar.
                        missing_slot_ts = None
                        missing_slot_first_seen_at = 0.0
                        continue

                    missing_ts = int(self._last_bar_open_ts + tf_seconds)
                    if missing_slot_ts != missing_ts:
                        missing_slot_ts = missing_ts
                        missing_slot_first_seen_at = now
                    ohlc_silent_for = now - self._last_ohlc_event_ts

                    try:
                        payload = await asyncio.wait_for(
                            asyncio.to_thread(
                                self._fetch_bar_payload, missing_ts,
                            ),
                            timeout=rest_fetch_timeout_s,
                        )
                    except asyncio.TimeoutError:
                        # REST roundtrip stalled past our short cap.
                        # The orphaned ``to_thread`` blocks one worker
                        # thread until httpx times out at 50s, but the
                        # watchdog stays responsive: treat this iteration
                        # as "no bar yet" and let ``slot_wait`` below
                        # decide between retry and reconnect escalation.
                        broker_warning(
                            "ohlc watchdog REST fetch timed out for ts=%d "
                            "after %.1fs - retrying",
                            missing_ts, rest_fetch_timeout_s,
                        )
                        payload = None
                    if payload is not None:
                        broker_warning(
                            "ohlc.event missed for ts=%d (silent %.1fs) "
                            "- injecting bar from REST",
                            missing_ts, ohlc_silent_for,
                        )
                        # Bump the bar-time anchor first so a slow
                        # consumer cannot keep the watchdog re-firing
                        # on the same slot while the injected bar is
                        # still in the queue.
                        self._last_bar_open_ts = float(missing_ts)
                        # Reset the tick-volume accumulator at the
                        # recovered bar boundary. ``_listen_loop``
                        # normally snapshots-and-resets ``_tick_volume``
                        # in lockstep with every bid bar (so each bar's
                        # synth/REST-fallback volume counts only its own
                        # ticks); because the bid event was missed, no
                        # such reset happened. Without this, the next
                        # ``_synth_from_quote`` or the REST-volume
                        # fallback would include quote ticks accumulated
                        # across the missed slot AND the current slot,
                        # inflating volume after every recovered bar.
                        # We do not need the missed-slot count: REST's
                        # ``lastTradedVolume`` is the authoritative volume
                        # for the recovered bar.
                        self._tick_volume = 0
                        consecutive_rest_recoveries += 1
                        missing_slot_ts = None
                        missing_slot_first_seen_at = 0.0
                        assert self._update_queue is not None
                        await self._update_queue.put(("ohlc", payload))
                        if (consecutive_rest_recoveries
                                >= rest_recovery_reconnect_threshold):
                            quote_silent_for = (
                                now - self._last_quote_event_ts
                                if self._last_quote_event_ts > 0.0 else 0.0
                            )
                            broker_warning(
                                "ohlc.event recovered via REST for %d "
                                "consecutive slots (last ts=%d, "
                                "silent %.1fs, quotes silent %.1fs) - "
                                "forcing WS reconnect to refresh "
                                "OHLC subscription",
                                consecutive_rest_recoveries, missing_ts,
                                ohlc_silent_for, quote_silent_for,
                            )
                            self._last_bar_open_ts = 0.0
                            consecutive_rest_recoveries = 0
                            try:
                                await self._ws.close(
                                    code=4001,
                                    reason="ohlc-rest-recovered-stale",
                                )
                            except (WebSocketException, OSError):
                                pass
                        continue

                    slot_wait = now - missing_slot_first_seen_at
                    if slot_wait < max_rest_wait_s:
                        # REST hasn't published the bar yet; retry on
                        # the next iteration before escalating.
                        continue

                    # Distinguish a token-bound OHLC failure (quotes
                    # still streaming, OHLC subscription dead — must
                    # reconnect to revive bars) from an expected
                    # market-closed/session gap (REST has no bar
                    # because no trading happened; the quote stream is
                    # also idle). Reconnecting in the session-gap case
                    # produces a tight loop: every reconnect re-seeds
                    # ``_last_bar_open_ts`` from ``_last_bar_timestamp``
                    # and the watchdog fires again on the same missing
                    # slot ~30s later.
                    quote_silent_for = now - self._last_quote_event_ts
                    quote_idle_threshold_s = max(tf_seconds, 30.0)
                    if (self._last_quote_event_ts == 0.0
                            or quote_silent_for >= quote_idle_threshold_s):
                        # Quote stream is also idle - treat as expected
                        # no-bar period. Disarm the watchdog so it
                        # stops re-firing on the same slot; the next
                        # live ``ohlc.event`` will re-seed
                        # ``_last_bar_open_ts`` via the listener.
                        broker_info(
                            "ohlc.event missed for ts=%d but quotes "
                            "also idle (%.1fs) and REST has no bar - "
                            "treating as session gap, disarming watchdog",
                            missing_ts, quote_silent_for,
                        )
                        self._last_bar_open_ts = 0.0
                        missing_slot_ts = None
                        missing_slot_first_seen_at = 0.0
                        continue

                    broker_warning(
                        "ohlc.event missed for ts=%d (silent %.1fs, "
                        "quotes alive %.1fs ago) and REST has no bar "
                        "after %.1fs - forcing WS reconnect",
                        missing_ts, ohlc_silent_for,
                        quote_silent_for, slot_wait,
                    )
                    # Reset the anchor so a second pass doesn't double-fire
                    # while the close + reconnect is in flight.
                    self._last_bar_open_ts = 0.0
                    missing_slot_ts = None
                    missing_slot_first_seen_at = 0.0
                    try:
                        await self._ws.close(code=4001, reason="ohlc-stale")
                    except (WebSocketException, OSError):
                        pass
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # Log and continue - a single bad iteration must not
                    # kill the watchdog task silently and leave the
                    # framework-synth path uncovered.
                    broker_warning(
                        "ohlc watchdog iteration failed: %s",
                        repr(exc),
                    )
                    await asyncio.sleep(2.0)
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
        bar and returns a real number, the worker increments
        :attr:`_ws_bad_bar_streak`. At
        :data:`_WS_VOLUME_BAD_BAR_RECONNECT_THRESHOLD`
        consecutive hits the WS is closed (``code=4001``,
        ``reason="quote-volume-stale"``) so the live_runner reconnects
        — mirrors the OHLC watchdog's stale-subscription recovery.
        Partial-coverage bars NEVER count toward this streak.

        Quote frames are forwarded verbatim and preserve their order
        relative to in-flight bid bars (single-consumer FIFO).

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
                    # Quote frame - forward verbatim, preserves order
                    # relative to any in-flight bid bar.
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
            broker_warning(
                "WS volume fallback (ts=%d, reason=%s, "
                "ws=%d, rest=%.1f, median=%.1f)",
                bar_open_s_int, reason, ws_vol,
                rest_vol, rolling_median,
            )

        # REST-confirmed bad streak: bump only when the WS value
        # was demonstrably wrong on a *full-coverage* bar (REST
        # returned a real number and we trusted it over WS).
        # Partial bars never count — those are expected after
        # every reconnect.
        if (rest_attempted and not is_partial
                and rest_vol > 0.0):
            self._ws_bad_bar_streak += 1
        elif not need_rest:
            self._ws_bad_bar_streak = 0
        # `rest_failed_*` paths leave the streak unchanged: we
        # could not confirm whether the WS was wrong or REST was
        # simply unavailable.

        payload["_volume"] = chosen
        await out_q.put(("ohlc", payload))

        # Reset the global tick counter at bar close so
        # ``_synth_from_quote`` starts the next bar's intra-bar
        # spinner from 0 (matches legacy spinner behaviour). Doing
        # this here, not in the listener, avoids a race against a
        # late quote whose bucket the worker has yet to consume.
        self._tick_volume = 0

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
                try:
                    await ws.close(
                        code=4001,
                        reason="quote-volume-stale",
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
        except Exception:  # noqa: BLE001
            return 0.0

    def _fetch_bar_payload(self, timestamp: int) -> dict | None:
        """Fetch a single closed bar from REST as an ohlc.event payload.

        Returns a dict in the same shape as ``ohlc.event.payload``
        (``priceType, t, o, h, l, c``) plus a pre-resolved ``_volume``
        field, so the existing :meth:`_on_ohlc_event` path consumes it
        identically to a real WS event that has already been enriched
        by :meth:`_volume_backfill_worker_loop`. The watchdog inject
        path is allowed to bypass the worker (and the FIFO ordering of
        :attr:`_raw_ohlc_queue`) because it puts directly into
        :attr:`_update_queue`; pre-resolving volume here keeps the
        consumer path REST-free in both code paths.

        Returns ``None`` if the bar is not yet published, the request
        fails, or the returned bar's ``snapshotTimeUTC`` does not
        match ``timestamp``.
        """
        try:
            time_from = datetime.fromtimestamp(
                timestamp, tz=timezone.utc,
            ).replace(tzinfo=None)
            res = self.get_historical_prices(time_from=time_from, limit=1)
            prices = res.get('prices') or []
            if not prices:
                return None
            bar = prices[0]
            snap = bar.get('snapshotTimeUTC')
            if not snap:
                return None
            returned_ts = int(
                datetime.fromisoformat(snap).replace(
                    tzinfo=timezone.utc,
                ).timestamp()
            )
            if returned_ts != timestamp:
                return None
            return {
                "priceType": "bid",
                "t": int(timestamp * 1000),
                "o": float(bar['openPrice']['bid']),
                "h": float(bar['highPrice']['bid']),
                "l": float(bar['lowPrice']['bid']),
                "c": float(bar['closePrice']['bid']),
                "_volume": float(bar.get('lastTradedVolume') or 0.0),
            }
        except Exception:
            return None

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
            # ``create_session()`` runs two blocking httpx calls (50s
            # timeout each) — offload to a thread so the event loop
            # (watchdogs, WS ping) keeps running during login.
            await asyncio.to_thread(self.create_session)

        if self.store_ctx is not None:
            # Account-mode probe BEFORE the recovery passes: a hedging-mode
            # account opts into core one-way emulation (``position_port =
            # self``) and the recovery + the engine's restart replay must
            # already see the wired port. See ``_detect_account_mode``.
            await self._detect_account_mode()
            await self._load_activity_cursor_from_events()
            await self._recover_in_flight_submissions()

        # WS ping/pong cadence: default `websockets` is 20s/20s = up to
        # 40s before a half-open connection is dropped. On a 1-minute
        # strategy that is too coarse — tighten to 5s/5s (≤10s detect)
        # so a TCP-level outage propagates inside one bar.
        self._ws = await websockets.connect(
            WS_URL, ping_interval=5, ping_timeout=5,
        )

        assert self.timeframe is not None and self.symbol is not None

        self._update_queue = asyncio.Queue()
        self._raw_ohlc_queue = asyncio.Queue()
        self._tick_volume = 0
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
        # Seed the bar-time anchor from the warmup baseline if the
        # provider already loaded one. Without this, the OHLC
        # watchdog stays disarmed (``_last_bar_open_ts == 0.0`` short-
        # circuits its loop) until the first live ``ohlc.event``
        # arrives - and if ``OHLCMarketData.subscribe`` is dead from
        # the start while the parallel quote stream keeps the feed
        # watchdog quiet, the watchdog would never probe REST and the
        # live stream would stall indefinitely. Seeding here arms the
        # watchdog on the very next expected bar boundary so a
        # subscribe-side failure is caught even before any real OHLC
        # event lands. ``_last_ohlc_event_ts`` is paired-seeded with
        # the current wallclock so the watchdog's "silent for" log
        # line measures from connect rather than from the Unix epoch.
        #
        # Stale-baseline guard: after a market/session gap the cached
        # ``_last_bar_timestamp`` can be hours old. Seeding from it
        # would point the watchdog at a non-trading slot inside the
        # gap; REST has no bar for that slot, and if quotes have
        # already resumed (so the session-gap disarm branch does not
        # trigger) the watchdog would force a reconnect on a healthy
        # subscription before the first new OHLC event can arrive.
        #
        # ``_last_bar_timestamp`` stores the OPEN time of the most
        # recent closed bar, so even a perfectly fresh
        # warmup-to-live handoff has ``now - baseline ~= tf_seconds``
        # (the bar opened one TF ago and just closed). Accept up to
        # two TFs of age so the normal startup path arms the
        # watchdog, while a session gap (hours old on any reasonable
        # TF) still falls through to the disarmed branch. The
        # watchdog's own session-gap detection (quote silence) is the
        # second line of defence if a "fresh" baseline still happens
        # to point inside an unexpected gap.
        now_ts = epoch_time()
        tf_seconds = float(in_seconds(self.timeframe))
        if (self._last_bar_timestamp is not None
                and now_ts - float(self._last_bar_timestamp)
                < 2.0 * tf_seconds):
            self._last_bar_open_ts = float(self._last_bar_timestamp)
            self._last_ohlc_event_ts = now_ts
        else:
            self._last_bar_open_ts = 0.0
            self._last_ohlc_event_ts = 0.0

        self._listen_task = asyncio.create_task(self._listen_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())
        self._feed_watchdog_task = asyncio.create_task(self._feed_watchdog_loop())
        self._ohlc_watchdog_task = asyncio.create_task(self._ohlc_watchdog_loop())
        self._volume_backfill_task = asyncio.create_task(
            self._volume_backfill_worker_loop()
        )

        # The five background tasks above are already running; if the
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
        after the background tasks were started. Cancels all five tasks
        and clears their references first (so nothing keeps consuming
        the dying WS), then closes the WebSocket and drops the queues.
        No unsubscribe frames are sent — the subscription never
        completed and the socket is likely broken anyway.
        """
        for attr in ("_listen_task", "_ping_task", "_feed_watchdog_task",
                     "_ohlc_watchdog_task", "_volume_backfill_task"):
            task: asyncio.Task | None = getattr(self, attr)
            if task is not None:
                task.cancel()
                setattr(self, attr, None)
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
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
        if self._ohlc_watchdog_task:
            self._ohlc_watchdog_task.cancel()
            self._ohlc_watchdog_task = None

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
        # disconnect: ``connect()`` seeds ``_last_bar_open_ts`` from it
        # (when it is still fresh — within one TF of wallclock) so the
        # OHLC watchdog is armed on the very next bar boundary after a
        # quick reconnect, even if the new subscription is dead from the
        # start. Clearing it here would force the watchdog to wait for
        # the first live ``ohlc.event`` before arming, which is exactly
        # the failure mode the seed branch exists to prevent. When the
        # baseline has gone stale (reconnect after a market/session
        # gap), ``connect()`` discards it on its own and the quote-alive
        # re-arm branch inside ``_ohlc_watchdog_loop`` takes over.
        self._last_bar_ohlcv = None
        self._tick_volume = 0
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
                # reach this point already enriched with REST volume by
                # :meth:`_volume_backfill_worker_loop` (or by
                # :meth:`_fetch_bar_payload` on the watchdog inject
                # path), so no REST call happens on the consumer path
                # and ``live_runner``'s ``wait_for(timeout=2.0)``
                # cannot trip on backfill latency.
                result = self._on_ohlc_event(payload)
            else:
                # Quote arrived before any OHLC baseline yields None;
                # keep waiting until the first ``ohlc.event`` lands.
                result = self._synth_from_quote()
            if result is not None:
                return result

    def _fetch_bar_volume(self, timestamp: int) -> float:
        """Fetch ``lastTradedVolume`` for a single closed bar via REST.

        Used by :meth:`watch_ohlcv` to override the throttled local
        ``_tick_volume`` with Capital.com's authoritative per-bar
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
        except Exception:
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

        Intra-bar updates (spinner, tick hooks) come exclusively from
        :meth:`_synth_from_quote`.
        """
        price_type = str(payload.get("priceType", "bid")).lower()
        if price_type != "bid":
            return None
        timestamp = int(payload["t"] / 1000)
        # The payload is pre-enriched with ``_volume`` by either
        # :meth:`_volume_backfill_worker_loop` (regular WS path) or
        # :meth:`_fetch_bar_payload` (watchdog REST-inject path). The
        # ``_tick_volume`` snapshot/reset for the consumed bar already
        # happened in :meth:`_listen_loop` synchronously with the bar
        # boundary, so this method must not touch ``self._tick_volume``
        # - doing so would corrupt the next bar's accumulator.
        volume = float(payload.get("_volume", 0.0))
        closed = OHLCV(
            timestamp=timestamp,
            open=float(payload["o"]),
            high=float(payload["h"]),
            low=float(payload["l"]),
            close=float(payload["c"]),
            volume=volume,
            extra_fields=self._extra_fields(),
            is_closed=True,
        )
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
