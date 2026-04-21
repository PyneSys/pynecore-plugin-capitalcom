"""Capital.com integration for PyneCore over the REST v1 API.

Historical and live market data, plus live order execution, share one
REST session and one credential block. The execute path is not yet
implemented; stubs raise :class:`NotImplementedError` pointing at the
open questions tracked in
``docs/pynecore/plugin-system/broker/capitalcom-broker-research.md``.
"""
import asyncio
import json
from base64 import standard_b64decode, standard_b64encode
from dataclasses import dataclass, field
from datetime import UTC, datetime, time, timedelta
from functools import lru_cache
from pathlib import Path
from time import time as epoch_time
from typing import TYPE_CHECKING, Callable
from zoneinfo import ZoneInfo

import httpx
from Crypto.Cipher import PKCS1_v1_5
from Crypto.PublicKey import RSA

from pynecore.core.broker.exceptions import (
    AuthenticationError,
    BrokerError,
    ExchangeCapabilityError,
    ExchangeConnectionError,
    ExchangeOrderRejectedError,
    ExchangeRateLimitError,
    InsufficientMarginError,
)
from pynecore.core.broker.models import (
    ExchangeCapabilities,
    ExchangeOrder,
    ExchangePosition,
    OrderStatus,
    OrderType,
)
from pynecore.core.plugin import override
from pynecore.core.plugin.broker import BrokerPlugin
from pynecore.core.syminfo import SymInfo, SymInfoInterval, SymInfoSession
from pynecore.types.ohlcv import OHLCV

if TYPE_CHECKING:
    from pynecore.core.broker.models import DispatchEnvelope

__all__ = [
    'CapitalCom',
    'CapitalComConfig',
    'CapitalComError',
]

URL = 'https://api-capital.backend-capital.com'
URL_DEMO = 'https://demo-api-capital.backend-capital.com'

WS_URL = 'wss://api-streaming-capital.backend-capital.com/connect'
WS_URL_DEMO = 'wss://demo-api-streaming-capital.backend-capital.com/connect'

ENDPOINT_PREFIX = '/api/v1/'

TIMEFRAMES = {
    '1': 'MINUTE',
    '5': 'MINUTE_5',
    '15': 'MINUTE_15',
    '30': 'MINUTE_30',
    '60': 'HOUR',
    '240': 'HOUR_4',
    '1D': 'DAY',
    '1W': 'WEEK'
}

TIMEFRAMES_INV = {v: k for k, v in TIMEFRAMES.items()}

TYPES = {
    'CURRENCIES': 'forex',
    'CRYTOCURRENCIES': 'crypto',
    'SHARES': 'stock',
    'INDICES': 'index',
}

_RETRYABLE_CODES = frozenset({
    'error.too-many.requests',
    'error.security.client-token-missing',
    'error.null.client.token',
})

_AUTH_ERROR_CODES = frozenset({
    'error.null.api.key',
    'error.invalid.details',
    'error.invalid.accountId',
})


def encrypt_password(password: str, encryption_key: str, timestamp: int | None = None):
    if timestamp is None:
        timestamp = int(epoch_time())
    payload = password + '|' + str(timestamp)
    payload = standard_b64encode(payload.encode('ascii'))
    public_key = RSA.importKey(standard_b64decode(encryption_key.encode('ascii')))
    cipher = PKCS1_v1_5.new(public_key)
    ciphertext = standard_b64encode(cipher.encrypt(payload)).decode()
    return ciphertext


class CapitalComError(ValueError):
    ...


@dataclass
class CapitalComConfig:
    """Capital.com plugin configuration.

    Covers both the data-ingest and the order-execution side — one
    credential block serves both. Broker-only tunables
    (``poll_interval_seconds``, ``require_one_way_mode``) are inert when
    the plugin is used for data ingest only.
    """

    demo: bool = False
    """Use the demo account/host instead of live."""

    user_email: str = ""
    """Your Capital.com account email."""

    api_key: str = ""
    """API key from Capital.com settings."""

    api_password: str = ""
    """API password for authentication."""

    poll_interval_seconds: float = 1.5
    """Seconds between ``GET /positions`` + ``GET /workingorders`` polls.

    The 10 req/s budget easily accommodates 1 s cadence, but 1.5 s is a
    safer default that leaves headroom for the snapshot + activity tail +
    any ad-hoc reconcile calls the sync engine issues.
    """

    require_one_way_mode: bool = True
    """When True (the default), the startup probe fails closed if the
    account has hedging mode enabled. The base :class:`BrokerPlugin`
    semantics are one-way Pine — hedging mode belongs on a future
    ``HedgeBrokerPlugin`` subclass."""


@dataclass
class _ActivityCursor:
    """Cursor state for ``GET /history/activity`` polling.

    ``last_date_utc`` is the ISO-8601 UTC timestamp of the most recent
    activity row already processed. ``seen_keys`` dedups the current
    window — Capital.com does not expose a stable ``activityId`` (§9 #16),
    so the plugin matches on the composite
    ``(dateUTC, dealId, type, status, source)`` tuple inside the window.
    """
    last_date_utc: str | None = None
    seen_keys: set[tuple] = field(default_factory=set)


class CapitalCom(BrokerPlugin[CapitalComConfig]):
    """Capital.com plugin for PyneCore.

    Provides historical OHLCV and live WebSocket market data, and live
    order execution over the Capital.com REST v1 API. Data ingest and
    trading share a single REST session, so one credential block serves
    both roles.

    Only one-way position mode is supported; hedging-mode accounts are
    rejected at startup. Stop orders and server-side trailing stops are
    supported; stop-limit is not. Partial closes are emulated via
    opposite-direction posts (netting), and size changes require
    cancel-and-recreate — order size is not amendable.

    Idempotency is software-upheld via the unified
    :class:`~pynecore.core.broker.storage.BrokerStore` — the generic
    ``order_refs`` table indexes each server-side ``dealReference`` /
    ``dealId`` so the plugin can resolve an activity tail entry back to
    the originating ``client_order_id`` with a single indexed SELECT.
    Execution events are polled from REST (``/positions``,
    ``/workingorders``, ``/history/activity``) since there is no
    WebSocket order channel.
    """

    plugin_name = "Capital.com"
    Config = CapitalComConfig

    timezone = 'US/Eastern'

    # Narrow the base ``ProviderPlugin.config: ConfigT | None`` — the
    # runtime ``__init__`` asserts the value is a ``CapitalComConfig``,
    # so every method can treat it as non-``None``.
    config: CapitalComConfig

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

    # --- construction -------------------------------------------------------

    def __init__(self, *, symbol: str | None = None, timeframe: str | None = None,
                 ohlcv_dir: Path | None = None, config: object | None = None):
        """
        :param symbol: The symbol to get data for.
        :param timeframe: The timeframe in TradingView format.
        :param ohlcv_dir: The directory to save OHLCV data.
        :param config: Pre-loaded :class:`CapitalComConfig` instance.
        """
        super().__init__(symbol=symbol, timeframe=timeframe,
                         ohlcv_dir=ohlcv_dir, config=config)
        assert isinstance(self.config, CapitalComConfig), "CapitalComConfig is required"

        # REST session state
        self.security_token: str | None = None
        self.cst_token: str | None = None
        self.session_data: dict = {}

        # Live WebSocket streaming state
        self._ws = None
        self._last_bar_timestamp: int | None = None
        self._last_bar_ohlcv: OHLCV | None = None
        self._update_queue: asyncio.Queue | None = None
        self._listen_task: asyncio.Task | None = None
        self._ping_task: asyncio.Task | None = None
        self._tick_volume: int = 0

        # Broker state
        self._account_preferences: dict | None = None
        self._activity_cursor = _ActivityCursor()
        self._last_auth_probe_ts: float = 0.0

    # --- REST core ----------------------------------------------------------

    def __call__(self, endpoint: str, *, data: dict | None = None,
                 method: str = 'post', _level: int = 0) -> dict:
        """Call a General REST API endpoint (synchronous).

        The broker side wraps every call in :meth:`_call` which offloads
        this method to a worker thread via :func:`asyncio.to_thread`.
        """
        from json import JSONDecodeError

        headers = {'X-CAP-API-KEY': self.config.api_key}
        if self.security_token:
            headers['X-SECURITY-TOKEN'] = self.security_token
        if self.cst_token:
            headers['CST'] = self.cst_token

        method = method.lower()
        params: dict[str, dict | float | None] = dict(headers=headers, timeout=50.0)
        if method == 'get':
            params['params'] = data
        elif method in ('post', 'put'):
            params['json'] = data

        url = URL_DEMO if self.config.demo else URL
        url += ENDPOINT_PREFIX + endpoint

        res: httpx.Response = getattr(httpx, method)(url, **params)
        try:
            dict_res = res.json()
        except JSONDecodeError:
            raise CapitalComError(f"JSON Error: {res.text}")

        if res.is_error:
            if dict_res['errorCode'] in ('error.security.client-token-missing',
                                         'error.null.client.token') \
                    and self.config.user_email and self.config.api_password and _level < 3:
                self.create_session()
                return self(endpoint=endpoint, data=data, method=method, _level=_level + 1)
            raise CapitalComError(f"API error occured: {dict_res['errorCode']}")

        try:
            self.security_token = res.headers['X-SECURITY-TOKEN']
        except KeyError:
            pass
        try:
            self.cst_token = res.headers['CST']
        except KeyError:
            pass

        return dict_res

    def create_session(self):
        """Create a session with the Capital.com API (login)."""
        res: dict = self('session/encryptionKey', method='get')
        encryption_key = res['encryptionKey']
        timestamp = res['timeStamp']
        user = self.config.user_email
        api_password = self.config.api_password
        password = encrypt_password(api_password, encryption_key, timestamp)
        self.session_data = self('session', data=dict(
            encryptedPassword=True,
            identifier=user,
            password=password
        ))

    async def _call(self, endpoint: str, *, data: dict | None = None,
                    method: str = 'post') -> dict:
        """Async wrapper around the sync REST helper.

        ``httpx`` exposes a separate async client, but wiring it in would
        duplicate the auth / re-session logic that :meth:`__call__`
        already handles. Offloading to a thread keeps the network call
        off the event loop without that duplication — the REST request
        rate from a single broker is orders of magnitude below what
        :func:`asyncio.to_thread` can sustain.
        """
        try:
            return await asyncio.to_thread(
                self, endpoint, data=data, method=method,
            )
        except CapitalComError as e:
            mapped = self._map_exception(e)
            if mapped is not None:
                raise mapped from e
            raise

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

    @lru_cache(maxsize=1)
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

        def timetz(_t: time, _tz: str) -> time:
            dt = datetime.now(ZoneInfo(_tz))
            dt = dt.replace(hour=_t.hour, minute=_t.minute,
                            second=_t.second, microsecond=_t.microsecond)
            dt = dt.astimezone(ZoneInfo(self.timezone))
            return dt.time()

        from pynecore.types.weekdays import Weekdays
        tz = opening_hours_data['zone']
        opening_hours = []
        session_starts = []
        session_ends = []

        for day in ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']:
            ohs = opening_hours_data[day]
            day_val = Weekdays[day.capitalize()]
            for oh in ohs:
                oh = oh.replace('00:00', '').strip()
                if oh.startswith('-'):
                    t = timetz(time.fromisoformat(oh[2:]), tz)
                    opening_hours.append(
                        SymInfoInterval(day=day_val.value, start=time(hour=0, minute=0), end=t))
                    session_ends.append(SymInfoSession(day=day_val.value, time=t))
                elif oh.endswith('-'):
                    t = timetz(time.fromisoformat(oh[:-2]), tz)
                    opening_hours.append(
                        SymInfoInterval(day=day_val.value, start=t, end=time(hour=0, minute=0)))
                    session_starts.append(SymInfoSession(day=day_val.value, time=t))

        dealing_rules = market_details['dealingRules']
        mintick = dealing_rules['minStepDistance']["value"]
        minmove = mintick
        pricescale = 1
        while minmove < 1.0:
            pricescale *= 10
            minmove *= 10

        res = self.get_historical_prices()
        avg_spred_summ = 0.0
        for p in res['prices']:
            spread = abs(p['closePrice']['bid'] - p['closePrice']['ask'])
            avg_spred_summ += spread
        avg_spred = avg_spred_summ / len(res['prices'])

        return SymInfo(
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
            timezone=self.timezone,
            opening_hours=opening_hours,
            session_starts=session_starts,
            session_ends=session_ends,
            avg_spread=avg_spred,
        )

    @override
    def download_ohlcv(self, time_from: datetime, time_to: datetime,
                       on_progress: Callable[[datetime], None] | None = None,
                       limit: int | None = None):
        """Download OHLCV data between ``time_from`` and ``time_to``."""
        tf = time_from.replace(tzinfo=None)
        tt = (time_to if time_to is not None else datetime.now(UTC)).replace(tzinfo=None)

        try:
            d = None
            while tf < tt:
                if on_progress:
                    on_progress(tf)

                res: dict = self.get_historical_prices(time_from=tf, limit=limit or 1000)
                if not res or not res['prices']:
                    break
                ps = res['prices']
                if len(ps) == 1 and d is not None:
                    break

                for p in ps:
                    t = datetime.fromisoformat(p['snapshotTimeUTC'])

                    if p['lastTradedVolume'] <= 1.0:
                        tf = t + timedelta(minutes=1)
                        continue

                    if t > tt:
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
                        },
                    )

                    self.save_ohlcv_data(ohlcv)
                    tf = t + timedelta(minutes=1)

        except CapitalComError:
            pass

        except StopIteration:
            pass

        if on_progress:
            on_progress(tt)

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
        """Background task: read WebSocket messages and route them."""
        assert self._ws is not None and self._update_queue is not None
        # noinspection PyBroadException
        try:
            async for raw in self._ws:
                data = json.loads(raw)
                dest = data.get("destination", "")
                if dest == "OHLCMarketData":
                    await self._update_queue.put(data.get("payload", {}))
                elif dest == "marketData":
                    self._tick_volume += 1
        except asyncio.CancelledError:
            pass
        except Exception:
            # Any listener failure (protocol error, JSON decode, socket
            # drop, ...) must unblock the consumer via a sentinel.
            if self._update_queue is not None:
                await self._update_queue.put(None)

    async def _ping_loop(self) -> None:
        """Background task: keep the WebSocket session alive."""
        try:
            while True:
                await asyncio.sleep(540)  # 9 min (session timeout is 10 min)
                await self._send("ping", correlation_id="ping")
        except asyncio.CancelledError:
            pass

    @override
    async def connect(self) -> None:
        """Establish REST session and WebSocket connection."""
        try:
            import websockets
        except ImportError:
            raise ImportError(
                "websockets is required for live data. Install it with: "
                "pip install websockets"
            )

        if not self.cst_token or not self.security_token:
            self.create_session()

        url = WS_URL_DEMO if self.config.demo else WS_URL
        self._ws = await websockets.connect(url)

        self._update_queue = asyncio.Queue()
        self._tick_volume = 0

        self._listen_task = asyncio.create_task(self._listen_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())

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
        payload = await self._update_queue.get()

        if payload is None:
            raise ConnectionError("WebSocket listener disconnected")

        timestamp = int(payload["t"] / 1000)
        current_ohlcv = OHLCV(
            timestamp=timestamp,
            open=float(payload["o"]),
            high=float(payload["h"]),
            low=float(payload["l"]),
            close=float(payload["c"]),
            volume=float(self._tick_volume),
            is_closed=False,
        )

        if (self._last_bar_timestamp is not None
                and timestamp != self._last_bar_timestamp):
            assert self._last_bar_ohlcv is not None
            closed_bar = self._last_bar_ohlcv._replace(
                volume=float(self._tick_volume),
                is_closed=True,
            )
            self._tick_volume = 0
            self._last_bar_timestamp = timestamp
            self._last_bar_ohlcv = current_ohlcv
            return closed_bar

        self._last_bar_timestamp = timestamp
        self._last_bar_ohlcv = current_ohlcv
        return current_ohlcv

    # --- BrokerPlugin: capabilities + read-only state ----------------------

    def get_capabilities(self) -> ExchangeCapabilities:
        """Declared capabilities per the research dossier §3.

        ``tp_sl_bracket_native=True`` is a *full-row* bracket: Capital.com
        attaches the SL/TP as attributes of the entire position. A Pine
        ``strategy.exit(qty=N, from_entry=...)`` with ``N`` less than the
        total entry qty cannot be expressed natively and must either be
        rejected at startup or handled through a future
        ``ScriptRequirements.partial_qty_exit`` gate. The bracket flag
        alone is therefore NOT a promise that every Pine exit shape fits.

        ``reduce_only=True`` is software-upheld via the netting execution
        model — in one-way mode an opposite ``POST /positions`` reduces
        or closes the current exposure instead of opening a counter-leg.
        Declaring ``False`` here would break
        :mod:`pynecore.core.broker.validation` and the plugin could not
        run any script that touches ``strategy.exit`` / ``strategy.close``.
        """
        return ExchangeCapabilities(
            stop_order=True,
            stop_limit_order=False,
            trailing_stop=True,
            tp_sl_bracket=True,
            tp_sl_bracket_native=True,
            oca_cancel_native=False,
            amend_order=True,
            cancel_all=False,
            reduce_only=True,
            watch_orders=False,
            fetch_position=True,
            client_id_echo=False,
            idempotency_native=False,
        )

    async def get_balance(self) -> dict[str, float]:
        """Return ``{currency: available}`` for the active account.

        The :class:`~pynecore.core.plugin.broker.BrokerPlugin` contract
        uses this method as the startup auth probe — a failed call
        surfaces :class:`AuthenticationError` (terminal) or
        :class:`ExchangeConnectionError` (retryable).
        """
        res = await self._call('accounts', method='get')
        accounts = res.get('accounts') or []
        for acc in accounts:
            if acc.get('preferred'):
                balance = acc.get('balance') or {}
                currency = acc.get('currency') or balance.get('currency') or 'USD'
                available = float(balance.get('available', 0.0))
                # Latch the plugin-qualified account_id for the unified
                # broker storage.  ``mode`` is sticky per host (demo vs live);
                # the preferred accountId is the stable durable id.
                account_id = acc.get('accountId')
                if account_id:
                    mode = 'demo' if self.config.demo else 'live'
                    self._account_id = f"capitalcom-{mode}-{account_id}"
                self._last_auth_probe_ts = epoch_time()
                return {currency: available}
        raise BrokerError("No preferred account returned by GET /accounts")

    async def get_preferences(self) -> dict:
        """Fetch ``GET /accounts/preferences``.

        Result is cached on the instance because the hedging-mode check
        only needs to re-run on account switch (``PUT /session``), which
        the plugin forbids at runtime.
        """
        prefs = self._account_preferences
        if prefs is None:
            prefs = await self._call('accounts/preferences', method='get')
            self._account_preferences = prefs
        return prefs

    async def assert_one_way_mode(self) -> None:
        """Fail-closed if the active account is in hedging mode.

        :raises ExchangeCapabilityError: The account has hedging enabled
            and the plugin was instantiated with ``require_one_way_mode``.
        """
        if not self.config.require_one_way_mode:
            return
        prefs = await self.get_preferences()
        if prefs.get('hedgingMode'):
            raise ExchangeCapabilityError(
                "Capital.com account is in hedging mode — this plugin "
                "only supports one-way mode. Disable hedging on the account "
                "or await the future HedgeBrokerPlugin subclass.",
            )

    async def get_position(self, symbol: str) -> ExchangePosition | None:
        """Return the aggregate position across all rows for ``symbol``.

        Capital.com netting opens a fresh row per same-direction entry
        (confirmed empirically after §9 #5 is closed) — aggregation is
        therefore mandatory for Pine's one-way model. Returns ``None``
        when no row exists for the symbol.
        """
        res = await self._call('positions', method='get')
        rows = res.get('positions') or []
        long_size = 0.0
        short_size = 0.0
        long_notional = 0.0
        short_notional = 0.0
        unrealized_pnl = 0.0
        leverage = 0.0
        margin_mode = 'cross'
        matched_any = False

        for row in rows:
            market = row.get('market') or {}
            if market.get('epic') != symbol:
                continue
            position = row.get('position') or {}
            direction = (position.get('direction') or '').upper()
            size = float(position.get('size', 0.0))
            open_level = float(position.get('level', 0.0))
            upl = float(position.get('upl', 0.0))
            if direction == 'BUY':
                long_size += size
                long_notional += size * open_level
            elif direction == 'SELL':
                short_size += size
                short_notional += size * open_level
            else:
                continue
            unrealized_pnl += upl
            row_leverage = position.get('leverage')
            if isinstance(row_leverage, (int, float)):
                leverage = max(leverage, float(row_leverage))
            matched_any = True

        if not matched_any:
            return None

        net = long_size - short_size
        if abs(net) < 1e-12:
            side = 'flat'
            entry_price = 0.0
        elif net > 0:
            side = 'long'
            entry_price = long_notional / long_size if long_size else 0.0
        else:
            side = 'short'
            entry_price = short_notional / short_size if short_size else 0.0

        return ExchangePosition(
            symbol=symbol,
            side=side,
            size=abs(net),
            entry_price=entry_price,
            unrealized_pnl=unrealized_pnl,
            liquidation_price=None,
            leverage=leverage,
            margin_mode=margin_mode,
        )

    async def get_open_orders(
            self, symbol: str | None = None,
    ) -> list[ExchangeOrder]:
        """Return all pending working orders, optionally filtered by symbol.

        Capital.com's ``GET /workingorders`` returns server-generated
        ``dealId`` values and no user-side reference. To restore
        ``client_order_id`` echo semantics (required by the sync engine
        for dispatch recovery), the plugin queries the unified broker
        storage's generic alias index (``order_refs`` table) by
        ``ref_type='deal_id'`` — a single indexed SELECT.  When no matching
        row exists (or when the plugin runs without a ``store_ctx``, e.g.
        tests), ``client_order_id`` is left ``None`` and the sync engine
        treats the order as externally owned.
        """
        res = await self._call('workingorders', method='get')
        orders = res.get('workingOrders') or []
        result: list[ExchangeOrder] = []
        for wo in orders:
            data = wo.get('workingOrderData') or {}
            market = wo.get('market') or {}
            epic = market.get('epic') or data.get('epic')
            if symbol is not None and epic != symbol:
                continue
            deal_id = str(data.get('dealId') or '')
            direction = (data.get('direction') or '').lower()
            side = 'buy' if direction == 'buy' else 'sell'
            order_type_raw = (data.get('orderType') or '').upper()
            if order_type_raw == 'LIMIT':
                order_type = OrderType.LIMIT
                price = float(data.get('orderLevel', 0.0))
                stop_price = None
            elif order_type_raw == 'STOP':
                order_type = OrderType.STOP
                price = None
                stop_price = float(data.get('orderLevel', 0.0))
            else:
                continue
            qty = float(data.get('orderSize', 0.0))

            coid: str | None = None
            if deal_id and self.store_ctx is not None:
                if order_row := self.store_ctx.find_by_ref('deal_id', deal_id):
                    coid = order_row.client_order_id

            created_raw = str(data.get('createdDateUTC')
                              or data.get('createdDate') or '')
            timestamp = _parse_iso_timestamp(created_raw)

            result.append(ExchangeOrder(
                id=deal_id,
                symbol=epic or '',
                side=side,
                order_type=order_type,
                qty=qty,
                filled_qty=0.0,
                remaining_qty=qty,
                price=price,
                stop_price=stop_price,
                average_fill_price=None,
                status=OrderStatus.OPEN,
                timestamp=timestamp,
                fee=0.0,
                fee_currency='',
                reduce_only=False,
                client_order_id=coid,
            ))
        return result

    # --- BrokerPlugin: execute path (step-2 stubs) -------------------------

    async def execute_entry(
            self, envelope: 'DispatchEnvelope',
    ) -> list[ExchangeOrder]:
        raise NotImplementedError(
            "CapitalCom.execute_entry is step-2 work — blocked on research "
            "dossier §9 #4 (partial fill live) and #5 (pyramiding row "
            "semantics). See docs/pynecore/plugin-system/broker/"
            "capitalcom-broker-research.md §10 for the implementation order.",
        )

    async def execute_exit(
            self, envelope: 'DispatchEnvelope',
    ) -> list[ExchangeOrder]:
        raise NotImplementedError(
            "CapitalCom.execute_exit is step-2 work — blocked on research "
            "dossier §9 #14 (partial close SL/TP retention) and "
            "#15 (trailing activation semantics).",
        )

    async def execute_close(
            self, envelope: 'DispatchEnvelope',
    ) -> ExchangeOrder:
        raise NotImplementedError(
            "CapitalCom.execute_close is step-2 work — blocked on research "
            "dossier §9 #13 (row reduction order) and §8 partial-close "
            "safety policy.",
        )

    async def execute_cancel(
            self, envelope: 'DispatchEnvelope',
    ) -> bool:
        raise NotImplementedError(
            "CapitalCom.execute_cancel is step-2 work — depends on "
            "working-order missing_pending grace window (§8 buktatók).",
        )

    def watch_orders(self):
        """Capital.com has no WebSocket order channel.

        Returning :class:`NotImplementedError` is the contract described
        in :meth:`BrokerPlugin.watch_orders` — the framework then polls
        :meth:`get_open_orders` on each bar. A proper event stream will
        land as a plugin-internal polling coroutine driven by
        ``GET /positions`` + ``GET /workingorders`` + ``GET /history/activity``
        (research dossier §6 design B).
        """
        raise NotImplementedError(
            "Capital.com has no WS order channel — execution event stream "
            "will be a polling coroutine (dossier §6 design B).",
        )

    def _map_exception(self, raw: Exception) -> BrokerError | None:
        """Translate Capital.com / stdlib exceptions into the broker taxonomy.

        The parent class handles :class:`ConnectionError`; here we
        classify :class:`CapitalComError` by error code, leaning on the
        terminal-vs-retryable partition from the research dossier §8.
        """
        if isinstance(raw, CapitalComError):
            code = _extract_error_code(raw)
            if code in _AUTH_ERROR_CODES:
                return AuthenticationError(str(raw), reason=code)
            if code == 'error.too-many.requests':
                return ExchangeRateLimitError(str(raw), retry_after=1.0)
            if code and code.startswith('error.invalid.') and 'margin' in code:
                return InsufficientMarginError(str(raw))
            if code and code.startswith('error.invalid.'):
                return ExchangeOrderRejectedError(str(raw))
            if code in _RETRYABLE_CODES:
                return ExchangeConnectionError(str(raw))
        return super()._map_exception(raw)


def _extract_error_code(exc: CapitalComError) -> str:
    """Pull the ``error.*`` code out of a :class:`CapitalComError` message.

    The provider formats errors as ``"API error occured: <code>"`` — we
    split on the prefix so callers get the raw code back. Returns the
    empty string when the format does not match (defensive, since the
    formatter could change).
    """
    message = str(exc)
    prefix = "API error occured: "
    if message.startswith(prefix):
        return message[len(prefix):].split(':', 1)[0].strip()
    return message.split(':', 1)[0].strip()


def _parse_iso_timestamp(value: str) -> float:
    """Best-effort ISO-8601 → unix-seconds parser.

    Capital.com stamps ``createdDateUTC`` as ``YYYY-MM-DDTHH:MM:SS.sss``
    without a trailing ``Z``; :func:`datetime.fromisoformat` accepts that
    directly on Python 3.11+. Returns ``0.0`` on parse failure so the
    caller gets a defined value without a raise — timestamps here are
    advisory and never load-bearing for order identity.
    """
    if not value:
        return 0.0
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.timestamp()
    except ValueError:
        return 0.0
