from typing import Callable
from dataclasses import dataclass
from time import time as epoch
from datetime import datetime, time, UTC, timedelta
from base64 import standard_b64encode, standard_b64decode
from zoneinfo import ZoneInfo
from pathlib import Path
from functools import lru_cache
import asyncio
import json

import httpx
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5

from pynecore.core.plugin import LiveProviderPlugin, override
from pynecore.core.syminfo import SymInfo, SymInfoInterval, SymInfoSession
from pynecore.types.ohlcv import OHLCV

__all__ = ['CapitalComProvider']

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


def encrypt_password(password: str, encryption_key: str, timestamp: int | None = None):
    if timestamp is None:
        timestamp = int(epoch())
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
    """Capital.com provider configuration"""

    demo: bool = False
    """Use demo account instead of live"""

    user_email: str = ""
    """Your Capital.com account email"""

    api_key: str = ""
    """API key from Capital.com settings"""

    api_password: str = ""
    """API password for authentication"""


class CapitalComProvider(LiveProviderPlugin[CapitalComConfig]):
    """
    Capital.com data provider plugin with live streaming support.
    """

    plugin_name = "Capital.com"
    Config = CapitalComConfig

    timezone = 'US/Eastern'

    @classmethod
    @override
    def to_tradingview_timeframe(cls, timeframe: str) -> str:
        """
        Convert Capital.com timeframe format to TradingView format.

        :param timeframe: Timeframe in Capital.com format.
        :return: Timeframe in TradingView format.
        :raises ValueError: If timeframe format is invalid.
        """
        try:
            return TIMEFRAMES_INV[timeframe.upper()]
        except KeyError:
            raise ValueError(f"Invalid Capital.com timeframe format: {timeframe}")

    @classmethod
    @override
    def to_exchange_timeframe(cls, timeframe: str) -> str:
        """
        Convert TradingView timeframe format to Capital.com format.

        :param timeframe: Timeframe in TradingView format.
        :return: Timeframe in Capital.com format.
        :raises ValueError: If timeframe format is invalid.
        """
        try:
            return TIMEFRAMES[timeframe]
        except KeyError:
            raise ValueError(f"Unsupported timeframe for Capital.com: {timeframe}")

    def __init__(self, *, symbol: str | None = None, timeframe: str | None = None,
                 ohlcv_dir: Path | None = None, config: object | None = None):
        """
        :param symbol: The symbol to get data for.
        :param timeframe: The timeframe in TradingView format.
        :param ohlcv_dir: The directory to save OHLCV data.
        :param config: Pre-loaded CapitalComConfig instance.
        """
        super().__init__(symbol=symbol, timeframe=timeframe, ohlcv_dir=ohlcv_dir, config=config)
        assert self.config is not None, "CapitalComConfig is required"
        self.config: CapitalComConfig = self.config
        self.security_token: str | None = None
        self.cst_token: str | None = None
        self.session_data: dict = {}

        # Live streaming state
        self._ws = None
        self._last_bar_timestamp: int | None = None
        self._last_bar_ohlcv: OHLCV | None = None
        self._update_queue: asyncio.Queue | None = None
        self._listen_task: asyncio.Task | None = None
        self._ping_task: asyncio.Task | None = None
        self._tick_volume: int = 0

    # Basic API calls

    def __call__(self, endpoint: str, *, data: dict | None = None, method: str = 'post', _level: int = 0) -> dict:
        """
        Call General API endpoints.
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
            if dict_res['errorCode'] in ('error.security.client-token-missing', 'error.null.client.token') \
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
        """
        Create session with Capital.com API.
        """
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

    ###

    def get_market_details(self, search_term: str = None, symbols: list[str] = None) -> dict:
        """
        Get and search market details.
        """
        data = {}
        if search_term:
            data['searchTerm'] = search_term
        if symbols:
            data['epics'] = ','.join(symbols)
        res: dict = self('markets', data=data, method='get')
        return res

    @lru_cache(maxsize=1)
    def get_single_market_details(self) -> dict:
        """
        Get market details of a symbol.
        """
        assert self.symbol is not None
        return self('markets/' + self.symbol, method='get')

    def get_historical_prices(self, time_from: datetime = None, time_to: datetime = None,
                              limit=1000) -> dict:
        """
        Get historical prices of market.

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
        """
        Get list of symbols.

        :param search_term: Search term.
        """
        res: dict = self.get_market_details(search_term=search_term)
        markets = [m['epic'] for m in res['markets']]
        markets.sort()
        return markets

    @override
    def update_symbol_info(self) -> SymInfo:
        """
        Update symbol info from the exchange, including opening hours and sessions.
        """
        assert self.timeframe is not None
        market_details = self.get_single_market_details()
        instrument = market_details['instrument']

        # Get opening hours and sessions
        opening_hours_data = instrument['openingHours']

        def timetz(_t: time, _tz: str) -> time:
            dt = datetime.now(ZoneInfo(_tz))
            dt = dt.replace(hour=_t.hour, minute=_t.minute, second=_t.second, microsecond=_t.microsecond)
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
                    opening_hours.append(SymInfoInterval(day=day_val.value, start=time(hour=0, minute=0), end=t))
                    session_ends.append(SymInfoSession(day=day_val.value, time=t))
                elif oh.endswith('-'):
                    t = timetz(time.fromisoformat(oh[:-2]), tz)
                    opening_hours.append(SymInfoInterval(day=day_val.value, start=t, end=time(hour=0, minute=0)))
                    session_starts.append(SymInfoSession(day=day_val.value, time=t))

        dealing_rules = market_details['dealingRules']
        mintick = dealing_rules['minStepDistance']["value"]
        minmove = mintick
        pricescale = 1
        while minmove < 1.0:
            pricescale *= 10
            minmove *= 10

        # Download some data to get the average spread
        res = self.get_historical_prices()
        avg_spred_summ = 0.0
        for p in res['prices']:
            spread = abs(p['closePrice']['bid'] - p['closePrice']['ask'])
            avg_spred_summ += spread
        avg_spred = avg_spred_summ / len(res['prices'])

        return SymInfo(
            prefix=self.__class__.__name__.replace('Provider', '').upper(),
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
        """
        Download OHLCV data.

        :param time_from: The start time.
        :param time_to: The end time.
        :param on_progress: Optional callback to call on progress.
        :param limit: Override the automatic chunk size.
        """
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

    # --- LiveProviderPlugin methods ---

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
        """Background task: read WebSocket messages and route them to the queue."""
        assert self._ws is not None and self._update_queue is not None
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
        """Establish REST session and WebSocket connection for live streaming."""
        try:
            import websockets
        except ImportError:
            raise ImportError(
                "websockets is required for live data. Install it with: pip install websockets"
            )

        # Ensure REST session tokens exist
        if not self.cst_token or not self.security_token:
            self.create_session()

        url = WS_URL_DEMO if self.config.demo else WS_URL
        self._ws = await websockets.connect(url)

        self._update_queue = asyncio.Queue()
        self._tick_volume = 0

        # Start background tasks
        self._listen_task = asyncio.create_task(self._listen_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())

        # Subscribe to OHLC candles and market data (for tick volume)
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
        """
        Wait for the next OHLCV update from Capital.com WebSocket.

        Detects bar closure by tracking timestamp changes. Tick volume is
        accumulated from the ``marketData`` stream (each tick = +1 volume).

        :param symbol: Symbol (epic) in Capital.com format (e.g. "EURUSD").
        :param timeframe: Timeframe in TradingView format (e.g. "1", "60", "1D").
        :return: OHLCV with ``is_closed=True`` for a final bar, ``False`` for intra-bar updates.
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
