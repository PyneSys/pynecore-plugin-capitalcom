"""Provider-side surface: timeframe converters, symbol metadata, REST OHLCV.

Carries everything that produces *historical* market data + symbol info
through the synchronous REST API. Live WebSocket streaming lives in
``streaming.py``; the order-execution path lives in ``execution.py``.

State touched: ``_instrument_rules_cache`` (TTL-bounded per-epic cache),
``_last_bar_ohlcv`` / ``_last_bar_timestamp`` (seeding the live synth
pipeline at the end of warm-up so the very first quote can produce an
intra-bar update).
"""
from datetime import UTC, datetime, time, timedelta
from time import time as epoch_time
from typing import Callable
from zoneinfo import ZoneInfo

from pynecore.core.broker.models import (
    CapabilityLevel,
    ExchangeCapabilities,
)
from pynecore.core.plugin import override
from pynecore.core.syminfo import SymInfo, SymInfoInterval, SymInfoSession
from pynecore.lib.timeframe import in_seconds
from pynecore.types.ohlcv import OHLCV

from ._base import _CapitalComBase
from .exceptions import CapitalComError
from .helpers import (
    TIMEFRAMES,
    TIMEFRAMES_INV,
    TYPES,
    _INSTRUMENT_RULES_TTL_S,
    _parse_opening_hours_segment,
)
from .models import _InstrumentRules


class _ProviderMixin(_CapitalComBase):
    """Provider mix-in: timeframe converters, symbol metadata, REST OHLCV."""

    # --- timeframe helpers --------------------------------------------------

    @classmethod
    @override
    def to_tradingview_timeframe(cls, timeframe: str) -> str:
        """Convert Capital.com timeframe format to TradingView format."""
        try:
            return TIMEFRAMES_INV[timeframe.upper()]
        except KeyError:
            raise ValueError(f"Invalid Capital.com timeframe format: {timeframe}")

    @classmethod
    @override
    def to_exchange_timeframe(cls, timeframe: str) -> str:
        """Convert TradingView timeframe format to Capital.com format."""
        try:
            return TIMEFRAMES[timeframe]
        except KeyError:
            raise ValueError(f"Unsupported timeframe for Capital.com: {timeframe}")

    # --- market-data helpers ------------------------------------------------

    def get_market_details(self, search_term: str = None, symbols: list[str] = None) -> dict:
        """Get and search market details."""
        data = {}
        if search_term:
            data['searchTerm'] = search_term
        if symbols:
            data['epics'] = ','.join(symbols)
        res: dict = self('markets', data=data, method='get')
        return res

    def get_single_market_details(self) -> dict:
        """Get market details of the plugin's current symbol."""
        assert self.symbol is not None
        return self('markets/' + self.symbol, method='get')

    def get_historical_prices(self, time_from: datetime = None, time_to: datetime = None,
                              limit=1000) -> dict:
        """Get historical prices of the plugin's current symbol.

        :param time_from: The start time (interpreted as UTC).
        :param time_to: The end time (interpreted as UTC).
        :param limit: The maximum number of candles to return.
        """
        assert self.symbol is not None
        assert self.xchg_timeframe is not None
        params = {'resolution': self.xchg_timeframe, 'max': limit}
        if time_from is not None:
            params['from'] = time_from.isoformat()
        if time_to is not None:
            params['to'] = time_to.isoformat()
        res: dict = self('prices/' + self.symbol, data=params, method='get')
        return res

    @override
    def get_list_of_symbols(self, *args, search_term: str = None) -> list[str]:
        """Get list of symbols, optionally filtered by ``search_term``."""
        res: dict = self.get_market_details(search_term=search_term)
        markets = [m['epic'] for m in res['markets']]
        markets.sort()
        return markets

    @override
    def update_symbol_info(self) -> SymInfo:
        """Update symbol info, including opening hours and sessions."""
        assert self.timeframe is not None
        market_details = self.get_single_market_details()
        instrument = market_details['instrument']

        opening_hours_data = instrument['openingHours']

        from pynecore.types.weekdays import Weekdays
        tz = opening_hours_data['zone']
        opening_hours = []
        session_starts = []
        session_ends = []

        # Anchor a (source_day, source_time) pair on a real date in the
        # source tz so the conversion captures BOTH the time-of-day and
        # the day shift (e.g. Tokyo Mon 09:00 → US/Eastern Sun 19:00).
        # Returning only ``.time()`` like the previous ``timetz`` helper
        # silently dropped the day shift and made sessions fire on the
        # wrong local weekday for any market whose opening hours zone
        # crosses local midnight against ``self.timezone``.
        # ``datetime.combine`` with ``ZoneInfo`` resolves DST on the
        # target date itself, not on ``now()``.
        def to_local_dt(_t: time, _tz: str, _day_val: int) -> datetime:
            now_src = datetime.now(ZoneInfo(_tz))
            target_date = (now_src + timedelta(
                days=(_day_val - now_src.weekday()) % 7,
            )).date()
            src_dt = datetime.combine(target_date, _t, tzinfo=ZoneInfo(_tz))
            return src_dt.astimezone(ZoneInfo(self.timezone))

        midnight = time(hour=0, minute=0)
        for day in ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']:
            ohs = opening_hours_data[day]
            day_val = Weekdays[day.capitalize()]
            for oh in ohs:
                parsed = _parse_opening_hours_segment(oh)
                if parsed is None:
                    continue
                start_raw, end_raw = parsed
                if start_raw == midnight and end_raw == midnight:
                    # 24h source-day ("00:00 - 00:00"). PyneCore's
                    # ``_check_session`` treats ``start == end`` as a zero
                    # length session and reads the symbol as closed for
                    # the day, so encode the source day as
                    # ``00:00 - 23:59:59`` and run it through the same
                    # TZ-shift + local-midnight split path as a normal
                    # segment — for crypto/forex the per-source-day
                    # pieces stitch back into a continuous local week.
                    start_for_tz = time(0, 0, 0)
                    end_for_tz = time(23, 59, 59)
                    emit_start_marker = False
                    emit_end_marker = False
                else:
                    start_for_tz = start_raw
                    # Midnight in the source zone is the day-boundary
                    # sentinel: ``"17:00 - 00:00"`` means "open from 17:00
                    # to end-of-source-day". Treating that 00:00 literally
                    # would either build a zero-length interval (if the
                    # sentinel converts to local midnight too) or stretch
                    # the interval an extra day past the boundary. Encode
                    # the boundary as ``23:59:59`` of the same source day
                    # before TZ conversion so the local split at midnight
                    # behaves identically to a non-sentinel end.
                    end_for_tz = (time(23, 59, 59) if end_raw == midnight
                                  else end_raw)
                    # Markers fire on every real boundary — midnight in
                    # the source zone is NOT a real boundary (it is a
                    # day-roll sentinel). A non-midnight source that
                    # happens to convert to local midnight is still a
                    # real boundary and emits.
                    emit_start_marker = start_raw != midnight
                    emit_end_marker = end_raw != midnight

                start_dt = to_local_dt(start_for_tz, tz, day_val.value)
                end_dt = to_local_dt(end_for_tz, tz, day_val.value)
                # Source-side overnight (end <= start in source) — bump
                # the end into the next source-day before conversion.
                # Capital.com's per-day openingHours doesn't list this
                # shape today, but the guard avoids a malformed empty /
                # negative interval if the encoding ever changes.
                if end_dt <= start_dt:
                    end_dt = end_dt + timedelta(days=1)

                if start_dt.date() == end_dt.date():
                    opening_hours.append(SymInfoInterval(
                        day=start_dt.weekday(),
                        start=start_dt.time(),
                        end=end_dt.time(),
                    ))
                else:
                    # The local interval crosses local midnight — split
                    # so ``_check_session`` can match candles on both
                    # local weekdays. Its overnight handling only catches
                    # post-midnight candles when there is a separate
                    # ``(next_day, 00:00, …)`` entry to match against.
                    opening_hours.append(SymInfoInterval(
                        day=start_dt.weekday(),
                        start=start_dt.time(),
                        end=time(23, 59, 59),
                    ))
                    opening_hours.append(SymInfoInterval(
                        day=end_dt.weekday(),
                        start=time(0, 0, 0),
                        end=end_dt.time(),
                    ))

                if emit_start_marker:
                    session_starts.append(SymInfoSession(
                        day=start_dt.weekday(), time=start_dt.time(),
                    ))
                if emit_end_marker:
                    session_ends.append(SymInfoSession(
                        day=end_dt.weekday(), time=end_dt.time(),
                    ))

        dealing_rules = market_details['dealingRules']
        mintick = dealing_rules['minStepDistance']["value"]
        minmove = mintick
        pricescale = 1
        while minmove < 1.0:
            pricescale *= 10
            minmove *= 10

        # minDealSize is the smallest tradable amount — TV's mincontract
        # definition. 0.0 lets the provider chain fall back to volume
        # analysis / heuristics when the API omits it.
        try:
            mincontract = float(dealing_rules['minDealSize']['value'])
        except (KeyError, TypeError, ValueError):
            mincontract = 0.0

        res = self.get_historical_prices()
        avg_spred_summ = 0.0
        for p in res['prices']:
            spread = abs(p['closePrice']['bid'] - p['closePrice']['ask'])
            avg_spred_summ += spread
        avg_spred = avg_spred_summ / len(res['prices'])

        sym_info = SymInfo(
            prefix='CAPITALCOM',
            description=instrument['name'],
            ticker=instrument['epic'],
            currency=instrument['currency'],
            basecurrency=instrument['symbol'].split('/')[0] if '/' in instrument['symbol'] else None,
            period=self.timeframe,
            type=TYPES[instrument['type']] if instrument['type'] in TYPES else 'other',
            mintick=mintick,
            pricescale=pricescale,
            minmove=minmove,
            pointvalue=instrument['lotSize'],
            mincontract=mincontract,
            timezone=self.timezone,
            opening_hours=opening_hours,
            session_starts=session_starts,
            session_ends=session_ends,
            avg_spread=avg_spred,
        )
        # Cache for the streaming watchdogs to consult: they suppress
        # REST recovery and ohlc-stale WS reconnect calls while the
        # market is in a known-closed window.
        self._sym_info = sym_info
        return sym_info

    @override
    def get_symbol_info(self, force_update=False) -> SymInfo:
        """Wrap the base implementation so the streaming watchdogs always
        see a populated ``_sym_info``.

        The base ``get_symbol_info`` short-circuits to ``SymInfo.load_toml``
        when the cached TOML exists, bypassing ``update_symbol_info`` and
        therefore the ``self._sym_info = sym_info`` write above. On any
        subsequent run with a cached symbol file ``_market_open_now``
        would then see ``_sym_info is None`` and treat every period as
        open, disabling the session gates on the OHLC watchdog and the
        live-runner reconnect loop.
        """
        sym_info = super().get_symbol_info(force_update=force_update)
        self._sym_info = sym_info
        return sym_info

    @override
    def download_ohlcv(self, time_from: datetime, time_to: datetime,
                       on_progress: Callable[[datetime], None] | None = None,
                       limit: int | None = None, with_extra: bool = False):
        """Download OHLCV data between ``time_from`` and ``time_to``.

        The Capital.com REST ``/prices`` endpoint includes the currently-
        forming open bar as the last entry of the response. We drop it
        here so warmup gets fully-closed bars only — otherwise the very
        next WS push (the close of that same bar) would arrive with the
        same timestamp as the last warmup bar and the script_runner
        would have to treat it as an in-place refinement, which makes
        the logged bar_index appear stuck at the warmup boundary.

        The response already carries bid and ask OHLC, so ``with_extra``
        only gates whether the ask/spread fields are written to the
        ``.extra.csv`` sidecar (no extra request either way).
        """
        tf = time_from.replace(tzinfo=None)
        tt = (time_to if time_to is not None else datetime.now(UTC)).replace(tzinfo=None)

        # Bars that close *after* this cutoff are still forming — drop them.
        assert self.timeframe is not None
        interval = timedelta(seconds=in_seconds(self.timeframe))
        now_naive = datetime.now(UTC).replace(tzinfo=None)

        try:
            previous_tf: datetime | None = None
            while tf < tt:
                if on_progress:
                    on_progress(tf)

                # Stuck-cursor guard: Capital occasionally returns the
                # same forming bar (or empty payload) and ``tf`` would
                # oscillate at the same position forever. Bail out
                # rather than spin — the caller (``run.py`` retry loop /
                # ``data.py`` bar-count check) decides what to do next.
                if previous_tf is not None and tf <= previous_tf:
                    break
                previous_tf = tf

                try:
                    res: dict = self.get_historical_prices(time_from=tf, limit=limit or 1000)
                except CapitalComError as exc:
                    # Capital.com returns HTTP 404 with
                    # ``error.prices.not-found`` (rather than an empty
                    # ``prices`` list) when the requested window has no
                    # bars at all — typically when ``tf`` has advanced
                    # past the Friday close on a weekend or into any
                    # other market-closed gap. Treat it identically to
                    # an empty response: stop downloading, keep what we
                    # already wrote. All other error codes propagate.
                    if 'error.prices.not-found' in str(exc):
                        break
                    raise
                if not res or not res['prices']:
                    break
                ps = res['prices']

                for p in ps:
                    t = datetime.fromisoformat(p['snapshotTimeUTC'])

                    if t > tt:
                        raise StopIteration
                    if t + interval > now_naive:
                        # Still-forming bar — its close has not happened
                        # yet, so it is not historical data. Capital.com
                        # places the open bar at the END of its REST
                        # response, so any subsequent entry would also be
                        # in the future: stop the outer download here and
                        # let the WS feed deliver the close on the bar
                        # boundary. Using ``continue`` would risk an
                        # infinite loop if ``tf`` failed to advance past
                        # the partial bar on a re-request.
                        raise StopIteration
                    bid_o = float(p['openPrice']['bid'])
                    bid_h = float(p['highPrice']['bid'])
                    bid_l = float(p['lowPrice']['bid'])
                    bid_c = float(p['closePrice']['bid'])
                    ask_o = float(p['openPrice']['ask'])
                    ask_h = float(p['highPrice']['ask'])
                    ask_l = float(p['lowPrice']['ask'])
                    ask_c = float(p['closePrice']['ask'])

                    ohlcv = OHLCV(
                        timestamp=int(t.replace(tzinfo=UTC).timestamp()),
                        open=bid_o, high=bid_h, low=bid_l, close=bid_c,
                        volume=float(p['lastTradedVolume']),
                        extra_fields={
                            'ask_open': ask_o,
                            'ask_high': ask_h,
                            'ask_low': ask_l,
                            'ask_close': ask_c,
                            'spread': abs(ask_c - bid_c),
                        } if with_extra else None,
                    )

                    self.save_ohlcv_data(ohlcv)
                    # Seed the live synth pipeline so the very first WS
                    # quote can produce an intra-bar update — without this
                    # the spinner would stay empty until the first
                    # ``ohlc.event`` (bar close) populates a baseline.
                    # ``_last_bar_ohlcv``'s timestamp matches the last
                    # warmup bar, so script_runner sees the synth as an
                    # intra-bar update (``is_new_bar=False``) and does
                    # not advance ``bar_index``.
                    self._last_bar_ohlcv = ohlcv
                    self._last_bar_timestamp = ohlcv.timestamp
                    tf = t + timedelta(minutes=1)

        except StopIteration:
            pass

        if on_progress:
            on_progress(tt)

    # --- BrokerPlugin: capabilities + read-only state ----------------------

    def get_capabilities(self) -> ExchangeCapabilities:
        """Declared capabilities per the research dossier §3.

        See :class:`~pynecore.core.broker.models.CapabilityLevel` for the
        semantics of each level. Capital.com specifics:

        - ``tp_sl_bracket = NATIVE`` — a single ``POST /positions`` call
          atomically attaches the TP/SL as *position attributes* (full-row,
          see ``partial_qty_bracket_exit``).
        - ``partial_qty_bracket_exit = SOFTWARE`` — Capital.com has no
          native multi-leg bracket inside a single position (empirically
          validated, see ``partial-qty-bracket-exit-plan.md`` §9 #18:
          opposite-direction WO trigger nets the parent but the WO's
          TP/SL are NOT inherited onto the remainder; §9 #16/#17:
          orphaned WO opens a fresh reverse position once the parent is
          flat — never safe to park). The sync engine arms an in-memory
          leg state machine per ``strategy.exit`` call and dispatches a
          partial close through ``execute_close`` when the price crosses
          the trigger level. The §2.6 native fail-safe stop manager
          publishes the worst remaining SL onto the parent's position
          attributes via ``PUT /positions/{dealId}`` so engine downtime
          is bounded to the worst-SL drawdown.
        - ``partial_qty_bracket_exit_pyramiding = SOFTWARE`` — multi-row
          reduction routing is empirically resolved (§9 #13): an
          opposite-direction ``POST`` nets the aggregated position and the
          server reduces the oldest ``dealId`` first (FIFO), so the engine
          drives a single delta close against the summed parent with no
          client-side per-``dealId`` routing. Each pyramiding parent keeps
          its own §2.6 fail-safe state on its own ``dealId``.
        - ``trailing_stop = NATIVE`` — server-side, points-only via
          ``trailingStop=true, stopDistance``. Mutually exclusive with
          ``guaranteedStop``.
        - ``oca_cancel = SOFTWARE`` — Capital.com has no exchange-side OCA
          group between independent working orders; the sync engine emits
          cancels itself when one leg fills. The position-attribute bracket
          is a separate capability covered by ``tp_sl_bracket``, not OCA.
        - ``amend_order = PARTIAL_NATIVE`` — ``PUT /workingorders/{id}``
          and ``PUT /positions/{id}`` amend level / SL / TP / risk flags
          in-place, but ``size`` is **not** amendable on either endpoint.
          Size changes need cancel+recreate.
        - ``cancel_all = SOFTWARE`` — no batch endpoint on Capital.com,
          but Pine ``strategy.cancel_all()`` is delivered end-to-end: the
          position drops every tracked entry/exit dict, and the sync
          engine's diff loop dispatches one ``DELETE /workingorders/{id}``
          per previously active intent on the next ``sync()``.
        - ``reduce_only = SOFTWARE`` — upheld via the one-way netting
          model: an opposite-side ``POST /positions`` reduces / closes the
          existing row instead of opening a counter-leg. Declaring
          UNSUPPORTED here would break the validator and prevent any
          script that touches ``strategy.exit`` / ``strategy.close``.
        - ``watch_orders = SOFTWARE`` — no WebSocket order channel; the
          plugin emulates one with an AsyncIterator that fuses
          ``GET /positions`` + ``GET /workingorders`` +
          ``GET /history/activity`` snapshots. Cadence and backoff are
          internal module constants in ``helpers.py`` — deliberately NOT
          user-configurable (see the ``config.py`` module docstring).
        - ``fetch_position = NATIVE`` — ``GET /positions`` returns the
          live position(s) directly.
        - ``idempotency = SOFTWARE`` — the server generates the
          ``dealReference``; the plugin dedups locally using its SQLite
          store keyed by :attr:`DispatchEnvelope.client_order_id`.
          Restart-safe recovery is intact; the exchange does not enforce
          dedup at the API.
        """
        return ExchangeCapabilities(
            stop_order=CapabilityLevel.NATIVE,
            trailing_stop=CapabilityLevel.NATIVE,
            tp_sl_bracket=CapabilityLevel.NATIVE,
            partial_qty_bracket_exit=CapabilityLevel.SOFTWARE,
            partial_qty_bracket_exit_pyramiding=CapabilityLevel.SOFTWARE,
            oca_cancel=CapabilityLevel.SOFTWARE,
            amend_order=CapabilityLevel.PARTIAL_NATIVE,
            cancel_all=CapabilityLevel.SOFTWARE,
            reduce_only=CapabilityLevel.SOFTWARE,
            watch_orders=CapabilityLevel.SOFTWARE,
            fetch_position=CapabilityLevel.NATIVE,
            idempotency=CapabilityLevel.SOFTWARE,
        )

    async def _fetch_market(
            self, epic: str,
    ) -> tuple[_InstrumentRules, float | None]:
        """Pull ``/markets/{epic}`` once and parse out everything callers
        need.

        Returns the parsed :class:`_InstrumentRules` (also stored in the
        TTL cache under ``epic``) plus the snapshot mid quote derived from
        ``snapshot.bid`` / ``snapshot.offer`` — ``None`` when the response
        omits a tradable snapshot. The two consumers split here:
        :meth:`_get_instrument_rules` only takes the rules, while
        :meth:`_get_current_mid_price` only takes the mid; sharing this
        single call keeps the bracket-distance pre-check from doubling
        the network round-trip when both are needed in the same flow.

        The bracket-distance minimum is quoted under venue-dependent field
        names and units. Every market observed live quotes
        ``minStopOrProfitDistance`` with a ``PERCENTAGE`` unit (BTCUSD /
        EURUSD / GOLD / US500 measured 2026-07-11); the IG-style
        ``minNormalStopOrLimitDistance`` is preferred where present since
        it is specific to the normal (non-guaranteed) bracket this plugin
        issues. The guaranteed-stop minimums
        (``minControlledRiskStopDistance`` / ``minGuaranteedStopDistance``)
        are much wider and only apply to guaranteed stops, which the
        plugin never requests — falling back to them would pre-reject
        perfectly valid normal-bracket orders, so they are deliberately
        NOT read. The unit is stored alongside the value; conversion to a
        price-space distance happens at validation time against the live
        mid (:meth:`._InstrumentRules.min_bracket_distance_at`).
        """
        now = epoch_time()
        details = await self._call('markets/' + epic, method='get')
        dealing = details.get('dealingRules') or {}
        snapshot = details.get('snapshot') or {}
        # The SIZE grid is ``minSizeIncrement`` — ``minStepDistance`` is the
        # PRICE step (order-level distance) and must not be used here: on
        # e.g. BTCUSD it is 0.05 while the real size increment is 0.0001,
        # so quantizing a small crypto size against it collapses to 0.
        # ``minDealSize`` doubles as the fallback grid (the increment
        # equals the minimum on every venue-quoted instrument observed).
        lot_step = float(
            (dealing.get('minSizeIncrement') or {}).get('value', 0.0)
            or (dealing.get('minDealSize') or {}).get('value', 0.0)
            or 0.01
        )
        min_size = float((dealing.get('minDealSize') or {}).get('value', lot_step))
        max_size = float((dealing.get('maxDealSize') or {}).get('value', 0.0))
        min_distance = 0.0
        min_distance_unit = 'POINTS'
        for distance_field in (
                'minNormalStopOrLimitDistance', 'minStopOrProfitDistance',
        ):
            rule = dealing.get(distance_field) or {}
            value = float(rule.get('value') or 0.0)
            if value > 0.0:
                min_distance = value
                min_distance_unit = str(rule.get('unit') or 'POINTS').upper()
                break
        rules = _InstrumentRules(
            epic=epic,
            lot_step=lot_step if lot_step > 0.0 else 0.01,
            min_size=min_size,
            max_size=max_size,
            min_stop_or_limit_distance=min_distance,
            min_stop_or_limit_distance_unit=min_distance_unit,
            fetched_at=now,
        )
        self._instrument_rules_cache[epic] = rules
        bid_raw = snapshot.get('bid')
        offer_raw = snapshot.get('offer')
        mid: float | None = None
        if isinstance(bid_raw, (int, float)) and isinstance(offer_raw, (int, float)):
            mid = (float(bid_raw) + float(offer_raw)) / 2.0
        return rules, mid

    async def _get_instrument_rules(self, epic: str) -> _InstrumentRules:
        """Return cached dealing rules for ``epic``, fetching on first use
        or after the TTL window elapses.

        The plugin rounds every ``size`` field through ``lot_step`` before
        hitting the exchange: Capital.com rejects non-multiples with
        ``error.invalid.size``, and an implicit rounding at the REST
        boundary is a silent data-loss bug waiting to happen. The cache
        honours the internal :data:`_INSTRUMENT_RULES_TTL_S` so the
        bracket-distance minimum widening during volatile sessions
        does not get masked by a stale entry.

        :param epic: Capital.com market identifier (e.g. ``"EURUSD"``).
        :return: A :class:`_InstrumentRules` with the four fields the
            execute path actually uses, plus the freshness timestamp.
        """
        now = epoch_time()
        cached = self._instrument_rules_cache.get(epic)
        if cached is not None and now - cached.fetched_at < _INSTRUMENT_RULES_TTL_S:
            return cached
        rules, _ = await self._fetch_market(epic)
        return rules

    async def _get_current_mid_price(self, epic: str) -> float | None:
        """Always-fresh ``snapshot.bid + snapshot.offer`` mid for the
        proactive bracket-distance pre-check.

        Capital.com validates ``stopLevel``/``profitLevel`` against the
        live quote rather than the entry's fill price, so the pre-check
        must read a fresh snapshot every time. ``None`` propagates to the
        validators as "no anchor — let the broker decide", keeping the
        REST round-trip authoritative when the snapshot block is missing.

        Side effect: the underlying ``/markets/{epic}`` call also refreshes
        the :class:`_InstrumentRules` cache, so a subsequent
        :meth:`_get_instrument_rules` in the same flow is a cache hit.
        """
        _, mid = await self._fetch_market(epic)
        return mid
