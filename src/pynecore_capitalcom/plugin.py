"""Capital.com integration for PyneCore over the REST v1 API.

Historical and live market data, plus live order execution, share one
REST session and one credential block. The execute path follows the
defensive-reconcile architecture described in
``docs/pynecore/plugin-system/broker/capitalcom-broker-research.md``:
every state transition is PERSIST-FIRST (BrokerStore write before the
REST call), so a process crash mid-dispatch leaves an auditable trail
that ``_recover_in_flight_submissions`` can replay on restart.
"""
import asyncio
import hashlib
import json
from base64 import standard_b64decode, standard_b64encode
from dataclasses import dataclass, field
from datetime import UTC, datetime, time, timedelta
from functools import lru_cache
from pathlib import Path
from time import time as epoch_time
from typing import TYPE_CHECKING, AsyncIterator, Callable
from zoneinfo import ZoneInfo

import httpx
from Crypto.Cipher import PKCS1_v1_5
from Crypto.PublicKey import RSA

from pynecore.core.broker.exceptions import (
    AuthenticationError,
    BrokerError,
    BrokerManualInterventionError,
    ExchangeCapabilityError,
    ExchangeConnectionError,
    ExchangeOrderRejectedError,
    ExchangeRateLimitError,
    InsufficientMarginError,
    OrderDispositionUnknownError,
    OrderSkippedByPlugin,
    UnexpectedCancelError,
)
from pynecore.core.broker.idempotency import (
    KIND_CANCEL,
    KIND_CLOSE,
    KIND_ENTRY,
    KIND_EXIT_SL,
    KIND_EXIT_TP,
)
from pynecore.core.broker.models import (
    CancelIntent,
    CloseIntent,
    DispatchEnvelope,
    EntryIntent,
    ExchangeCapabilities,
    ExchangeOrder,
    ExchangePosition,
    ExitIntent,
    LegType,
    OrderEvent,
    OrderStatus,
    OrderType,
)
from pynecore.core.plugin import override
from pynecore.core.plugin.broker import BrokerPlugin
from pynecore.core.syminfo import SymInfo, SymInfoInterval, SymInfoSession
from pynecore.types.ohlcv import OHLCV

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow

__all__ = [
    'CapitalCom',
    'CapitalComConfig',
    'CapitalComError',
    'InvalidStopDistanceError',
    'InvalidTakeProfitDistanceError',
    'OrderNotFoundError',
]

URL = 'https://api-capital.backend-capital.com'
URL_DEMO = 'https://demo-api-capital.backend-capital.com'

WS_URL = 'wss://api-streaming-capital.backend-capital.com/connect'

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

_NOT_FOUND_DEAL_ID_CODE = 'error.not-found.dealId'
_NOT_FOUND_DEAL_REF_CODE = 'error.not-found.dealReference'
_INVALID_STOP_PREFIX = 'error.invalid.stoploss.minvalue'
_INVALID_TP_PREFIX = 'error.invalid.takeprofit.minvalue'
_INVALID_LEVERAGE_CODE = 'error.invalid.leverage.value'

# Valid ``on_unexpected_cancel`` policy values — keep in sync with the base
# :class:`BrokerPlugin` docstring. Anything else is a config error raised at
# construction so the live-trading loop never starts against a typo.
_VALID_UNEXPECTED_CANCEL_POLICIES = frozenset({
    'stop', 'stop_and_cancel', 're_place', 'ignore',
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


class InvalidStopDistanceError(ExchangeOrderRejectedError):
    """Capital.com rejected an order because the requested stop distance is
    below the instrument's current minimum.

    The exchange returns the minimum in the error message; the plugin
    surfaces it on the exception so the risk layer can size the fallback
    bracket without re-fetching dealing rules.

    :ivar min_distance: The minimum stop distance (in price units) reported
        by Capital.com for this instrument at the time of the rejection.
        Dynamic — may widen during volatile sessions.
    """

    def __init__(self, message: str, *, min_distance: float) -> None:
        super().__init__(message)
        self.min_distance = min_distance


class InvalidTakeProfitDistanceError(ExchangeOrderRejectedError):
    """Capital.com rejected an order because the requested take-profit
    distance is below the instrument's current minimum.

    Twin of :class:`InvalidStopDistanceError` for the profit leg.

    :ivar min_distance: The minimum TP distance (in price units) reported
        by Capital.com at rejection time.
    """

    def __init__(self, message: str, *, min_distance: float) -> None:
        super().__init__(message)
        self.min_distance = min_distance


class OrderNotFoundError(ExchangeOrderRejectedError):
    """Capital.com does not know the ``dealId`` / ``dealReference`` the plugin
    referenced — the order was cancelled, filled and purged, or never
    successfully reached the exchange.

    The error is definitive (the order is *known* not to exist), which lets
    the plugin treat it differently from a transient connection error.
    Cancel / modify paths downgrade to a noop; recovery paths flip to the
    snapshot-fallback branch.

    :ivar ref_type: ``'deal_id'`` or ``'deal_reference'`` — tells the caller
        which identifier was not found so recovery can decide whether to
        widen the confirm lookup or fall back to the snapshot.
    """

    def __init__(self, message: str, *, ref_type: str) -> None:
        super().__init__(message)
        self.ref_type = ref_type


@dataclass(frozen=True)
class _InstrumentRules:
    """Cached dealing rules for a single instrument.

    Capital.com returns these on ``GET /markets/{epic}`` as the
    ``dealingRules`` + ``instrument`` blocks. Three values are load-bearing
    for order sizing: ``lot_step`` (granularity of the ``size`` param),
    ``min_size`` (the smallest accepted ``size``), and ``min_sl_distance``
    (dynamic, used to pre-filter obviously-rejectable SL/TP levels before
    the round-trip). Everything else is currently unused by the plugin.
    """
    epic: str
    lot_step: float
    min_size: float
    min_sl_distance: float


def _bracket_leg_id(deal_id: str, kind: str) -> str:
    """Canonical composite id for a bracket TP/SL leg.

    The entry side of a Capital.com bracket is a *single* ``dealId`` on the
    position row — the TP and SL legs are position attributes, not distinct
    exchange orders. PyneCore's event model, by contrast, expects one
    :class:`ExchangeOrder` per leg. This helper produces a stable
    synthetic id (``{deal_id}:tp`` / ``{deal_id}:sl``) that the poller and
    tests can use to disambiguate the two halves without the plugin
    inventing ad-hoc strings at every emission site.

    :param deal_id: The Capital.com position ``dealId`` the bracket attaches to.
    :param kind: Either ``'tp'`` or ``'sl'``; other values raise.
    :raises AssertionError: On an invalid ``kind``.
    """
    assert kind in ('tp', 'sl'), f"invalid bracket leg kind: {kind!r}"
    return f"{deal_id}:{kind}"


def _parse_bracket_leg_id(composite: str) -> tuple[str, str | None]:
    """Inverse of :func:`_bracket_leg_id`.

    :param composite: An id that may have been produced by
        :func:`_bracket_leg_id`. Plain ``dealId`` (no leg suffix) is also
        accepted — returned verbatim with ``None`` as the leg kind.
    :return: ``(deal_id, leg_kind)`` where ``leg_kind`` is ``'tp'``, ``'sl'``,
        or ``None`` for raw ids.
    """
    if ':' not in composite:
        return composite, None
    deal_id, _, kind = composite.rpartition(':')
    if kind in ('tp', 'sl'):
        return deal_id, kind
    return composite, None


def _compute_cumulative_fill(row_qty: float, current_exchange_size: float) -> float:
    """Single source of truth for cumulative fill computation.

    Capital.com reports the *remaining* ``size`` on a position snapshot
    (``/positions``) — there is no per-fill delta. The plugin therefore
    derives the fill as ``row_qty - current_exchange_size`` in several
    places (snapshot reconcile, partial-fill emission, recovery). Keeping
    the formula in one place prevents drift when a future code path
    changes the sign convention.

    :param row_qty: The total qty the plugin submitted for the order.
    :param current_exchange_size: The remaining size reported by Capital
        now. Clamped at zero (snapshot can trail by a poll cycle and
        briefly overshoot the submitted total).
    :return: Cumulative filled qty. Monotonically non-decreasing *per
        position row* over the row's lifetime.
    """
    filled = row_qty - current_exchange_size
    return filled if filled > 0.0 else 0.0


def _activity_fingerprint(activity: dict) -> str:
    """Deterministic fingerprint for a Capital.com activity row.

    Capital.com's ``/history/activity`` does not expose a stable
    ``activityId`` (see research dossier §9 #16), so cross-restart dedup
    must be content-addressed. SHA-1 over the concatenation of the fields
    that uniquely identify a row in practice yields a 16-char hex prefix
    (~64 bits of entropy), which is dedup-safe well beyond any realistic
    strategy lifetime.

    The value is stored on :class:`_ActivityCursor.seen_fingerprints` for
    the in-session dedup set and persisted in the ``events`` table
    (``activity_processed`` kind) so the next process incarnation can
    rebuild the set from the BrokerStore.
    """
    key = "|".join(str(activity.get(k, '')) for k in (
        'dateUTC', 'dealId', 'type', 'status', 'source', 'level', 'size',
    ))
    return hashlib.sha1(key.encode('utf-8')).hexdigest()[:16]


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

    on_unexpected_cancel: str = "stop"
    """Policy when a bot-owned order disappears without the bot cancelling it.

    ``"stop"`` (default) — graceful stop; surfaced to the runner via the
    event sink so observability can page.
    ``"stop_and_cancel"`` — stop plus a best-effort cancel pass over the
    remaining bot-owned orders.
    ``"re_place"`` — no-op on the cancel; the sync engine re-dispatches
    the protective order on the next diff cycle.
    ``"ignore"`` — silently continue. Only safe when manual external
    cancellations are an expected part of the operational workflow.
    """


@dataclass
class _ActivityCursor:
    """Cursor state for ``GET /history/activity`` polling.

    ``last_date_utc`` is the ISO-8601 UTC timestamp of the most recent
    activity row already processed. ``seen_fingerprints`` dedups the
    current window *and* is rebuildable across restarts: every processed
    row gets an ``activity_processed`` event written to the BrokerStore
    whose payload carries the same SHA-1 fingerprint, so
    :meth:`CapitalCom._load_activity_cursor_from_events` can replay the
    last-24h window at ``connect()`` time.
    """
    last_date_utc: str | None = None
    seen_fingerprints: set[str] = field(default_factory=set)


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

        if self.config.on_unexpected_cancel not in _VALID_UNEXPECTED_CANCEL_POLICIES:
            raise ValueError(
                f"CapitalComConfig.on_unexpected_cancel must be one of "
                f"{sorted(_VALID_UNEXPECTED_CANCEL_POLICIES)}, got "
                f"{self.config.on_unexpected_cancel!r}",
            )
        # Promote to instance attribute so the base-class handler sees the
        # user-selected policy. The class-level default is ``"stop"``.
        self.on_unexpected_cancel = self.config.on_unexpected_cancel

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
        # Latest tick quote snapshot, updated by each ``quote`` event and
        # attached to every OHLCV emitted by ``watch_ohlcv`` so the spinner
        # and the per-bar OHLCV log can show ``bid``, ``ask`` and ``spread``.
        self._last_bid: float | None = None
        self._last_ask: float | None = None

        # Broker state
        self._account_preferences: dict | None = None
        self._activity_cursor = _ActivityCursor()
        self._last_auth_probe_ts: float = 0.0
        # Per-epic dealing-rules cache.  Capital.com rules are effectively
        # static during a trading session (lot step, min size); the plugin
        # refetches on explicit cache invalidation only.
        self._instrument_rules_cache: dict[str, _InstrumentRules] = {}

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
        # noinspection PyBroadException
        try:
            async for raw in self._ws:
                data = json.loads(raw)
                dest = data.get("destination", "")
                if dest == "ohlc.event":
                    await self._update_queue.put(("ohlc", data.get("payload") or {}))
                elif dest == "quote":
                    payload = data.get("payload") or {}
                    bid = payload.get("bid")
                    ofr = payload.get("ofr")
                    if bid is not None:
                        self._last_bid = float(bid)
                    if ofr is not None:
                        self._last_ask = float(ofr)
                    self._tick_volume += 1
                    await self._update_queue.put(("quote", None))
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

        self._ws = await websockets.connect(WS_URL)

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
            if event_type == "ohlc":
                return self._on_ohlc_event(payload)
            synth = self._synth_from_quote()
            if synth is not None:
                return synth
            # Quote arrived before any OHLC baseline — keep waiting.

    def _extra_fields(self) -> dict[str, float] | None:
        """Build the bid/ask/spread snapshot for ``OHLCV.extra_fields``."""
        if self._last_bid is None or self._last_ask is None:
            return None
        return {
            "bid_close": self._last_bid,
            "ask_close": self._last_ask,
            "spread": self._last_ask - self._last_bid,
        }

    def _on_ohlc_event(self, payload: dict) -> OHLCV:
        """Turn an ``ohlc.event`` payload into a *closed* OHLCV.

        Capital.com pushes ``ohlc.event`` only at bar close: the official
        Java sample accumulates intra-bar price into a local ``OHLCBar``
        from the ``quote`` tick stream and never relies on the server for
        intra-bar OHLC. Every ``ohlc.event`` we receive therefore already
        represents a completed bar, so we flag ``is_closed=True`` on the
        first one — a timestamp-change detector would miss the first bar
        and delay the OHLCV log by a full period.

        Intra-bar updates (spinner, tick hooks) come exclusively from
        :meth:`_synth_from_quote`.
        """
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

    # --- BrokerPlugin: capabilities + read-only state ----------------------

    def get_capabilities(self) -> ExchangeCapabilities:
        """Declared capabilities per the research dossier §3.

        ``tp_sl_bracket_native=True`` is a *full-row* bracket: Capital.com
        attaches the SL/TP as attributes of the entire position. A Pine
        ``strategy.exit(qty=N, from_entry=...)`` with ``N`` less than the
        total entry qty cannot be expressed — ``partial_qty_bracket_exit``
        is therefore declared ``False`` and the core validator rejects
        such scripts at startup (see :mod:`pynecore.core.broker.validation`).

        ``watch_orders=True`` advertises the polling event stream
        implemented by :meth:`watch_orders`: Capital.com has no WebSocket
        order channel, so the plugin emulates one with an AsyncIterator
        that fuses ``GET /positions`` + ``GET /workingorders`` +
        ``GET /history/activity`` snapshots. Cadence and backoff live on
        :class:`CapitalComConfig`.

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
            partial_qty_bracket_exit=False,
            oca_cancel_native=False,
            amend_order=True,
            cancel_all=False,
            reduce_only=True,
            watch_orders=True,
            fetch_position=True,
            client_id_echo=False,
            idempotency_native=False,
        )

    async def _get_instrument_rules(self, epic: str) -> _InstrumentRules:
        """Return cached dealing rules for ``epic``, fetching on first use.

        The plugin rounds every ``size`` field through ``lot_step`` before
        hitting the exchange: Capital.com rejects non-multiples with
        ``error.invalid.size``, and an implicit rounding at the REST
        boundary is a silent data-loss bug waiting to happen. Centralising
        the lookup also keeps the ``min_sl_distance`` pre-check in one
        place — the research dossier §6 describes how the value widens in
        volatile sessions and must be re-read, not hard-coded.

        :param epic: Capital.com market identifier (e.g. ``"EURUSD"``).
        :return: A :class:`_InstrumentRules` with the three fields the
            execute path actually uses.
        """
        cached = self._instrument_rules_cache.get(epic)
        if cached is not None:
            return cached
        details = await self._call('markets/' + epic, method='get')
        dealing = details.get('dealingRules') or {}
        instrument = details.get('instrument') or {}
        lot_step = float(
            (dealing.get('minStepDistance') or {}).get('value', 0.01)
            or instrument.get('lotSize', 0.01)
        )
        min_size = float((dealing.get('minDealSize') or {}).get('value', lot_step))
        min_sl_distance = float(
            (dealing.get('minControlledRiskStopDistance') or {}).get('value', 0.0)
            or (dealing.get('minNormalStopOrLimitDistance') or {}).get('value', 0.0)
        )
        rules = _InstrumentRules(
            epic=epic,
            lot_step=lot_step if lot_step > 0.0 else 0.01,
            min_size=min_size,
            min_sl_distance=min_sl_distance,
        )
        self._instrument_rules_cache[epic] = rules
        return rules

    @staticmethod
    def _quantize_size(qty: float, rules: _InstrumentRules) -> float:
        """Snap ``qty`` to the nearest multiple of ``rules.lot_step``.

        Uses :func:`round` (banker-free here — lot steps are decimal) on
        the step count, then multiplies back out. Below-min-size is the
        caller's concern: silently inflating to ``rules.min_size`` would
        make the exchange's executed size diverge from the strategy's
        ``intent.qty`` and corrupt every downstream sizing assumption
        (bracket full-row check, position accounting, P&L). The entry
        path raises :class:`OrderSkippedByPlugin` *before* calling this
        helper, so a runtime sizing model that yields a too-small qty
        becomes a logged skip rather than silent up-inflation.
        """
        if rules.lot_step <= 0.0:
            return qty
        units = round(qty / rules.lot_step)
        return units * rules.lot_step

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

    # --- BrokerPlugin: execute path ---------------------------------------

    async def execute_entry(
            self, envelope: DispatchEnvelope,
    ) -> list[ExchangeOrder]:
        """Open a position (MARKET) or place a working order (LIMIT/STOP).

        The flow follows the defensive-reconcile contract — every state
        transition is **PERSIST-FIRST** so a crash between the REST round
        trips leaves enough audit trail for
        :meth:`_recover_in_flight_submissions` to resolve the ambiguity on
        restart. The six-point crash matrix from the research dossier
        §5.1 is the truth table this method implements.
        """
        intent = envelope.intent
        assert isinstance(intent, EntryIntent)
        coid = envelope.client_order_id(KIND_ENTRY)

        if intent.order_type == OrderType.STOP_LIMIT:
            raise ExchangeCapabilityError(
                "Capital.com does not support STOP_LIMIT orders — core "
                "validation should have caught this at startup.",
            )

        rules = await self._get_instrument_rules(intent.symbol)
        if rules.min_size > 0 and intent.qty < rules.min_size:
            raise OrderSkippedByPlugin(
                f"Skipping {intent.symbol} {intent.side.upper()} entry "
                f"id={intent.pine_id!r}: qty={intent.qty} below Capital.com "
                f"minimum size {rules.min_size}. No order sent.",
                intent_key=intent.intent_key,
                reason="below_min_size",
                context={
                    'symbol': intent.symbol,
                    'side': intent.side,
                    'qty': intent.qty,
                    'min_size': rules.min_size,
                },
            )
        quantized_qty = self._quantize_size(intent.qty, rules)
        direction = "BUY" if intent.side == 'buy' else "SELL"

        if intent.order_type == OrderType.MARKET:
            endpoint = 'positions'
            body: dict = {
                'epic': intent.symbol,
                'direction': direction,
                'size': quantized_qty,
            }
            kind = 'position'
        else:
            endpoint = 'workingorders'
            if intent.order_type == OrderType.LIMIT:
                level = intent.limit
                wo_type = 'LIMIT'
            else:  # STOP
                level = intent.stop
                wo_type = 'STOP'
            if level is None:
                raise ExchangeOrderRejectedError(
                    f"Capital execute_entry: {wo_type} order requires a level, "
                    f"got None (pine_id={intent.pine_id!r})",
                )
            body = {
                'epic': intent.symbol,
                'direction': direction,
                'size': quantized_qty,
                'level': float(level),
                'type': wo_type,
            }
            kind = 'working'

        # === (1) PERSIST-FIRST ===
        if self.store_ctx is not None:
            self.store_ctx.upsert_order(
                coid,
                symbol=intent.symbol,
                side=intent.side,
                qty=quantized_qty,
                state='submitted',
                intent_key=intent.intent_key,
                pine_entry_id=intent.pine_id,
                extras={'kind': kind, 'order_type': intent.order_type.value},
            )
            self.store_ctx.log_event(
                'dispatch_submitted',
                client_order_id=coid,
                intent_key=intent.intent_key,
                payload={'endpoint': endpoint, 'body': body},
            )

        # === (2) POST — network timeout = ambiguous disposition ===
        try:
            resp = await self._call(endpoint, data=body, method='post')
        except (httpx.TimeoutException, httpx.RequestError,
                ConnectionError, ExchangeConnectionError) as net:
            if self.store_ctx is not None:
                self.store_ctx.set_order_state(coid, 'disposition_unknown')
                self.store_ctx.log_event(
                    'disposition_unknown', client_order_id=coid,
                    intent_key=intent.intent_key,
                    payload={'error': str(net), 'endpoint': endpoint},
                )
            raise OrderDispositionUnknownError(
                f"Capital POST {endpoint} ambiguous: {net}",
                client_order_id=coid,
                cause=net if isinstance(net, Exception) else None,
            ) from net

        deal_ref = resp.get('dealReference')
        if not deal_ref:
            if self.store_ctx is not None:
                self.store_ctx.set_order_state(coid, 'disposition_unknown')
                self.store_ctx.log_event(
                    'disposition_unknown', client_order_id=coid,
                    intent_key=intent.intent_key,
                    payload={'reason': 'no_deal_reference', 'response': resp},
                )
            raise OrderDispositionUnknownError(
                f"Capital POST {endpoint}: no dealReference in response",
                client_order_id=coid,
            )

        # === (3) PERSIST dealReference ===
        if self.store_ctx is not None:
            self.store_ctx.add_ref(coid, 'deal_reference', deal_ref)
            # Mirror into extras so recovery can cross-check without a join
            # when the order is still in ``submitted`` state (pre-ref add).
            self.store_ctx.upsert_order(
                coid,
                extras={
                    'kind': kind,
                    'order_type': intent.order_type.value,
                    'deal_reference': deal_ref,
                },
            )
            self.store_ctx.set_order_state(coid, 'server_ref_seen')
            self.store_ctx.log_event(
                'deal_reference_seen', client_order_id=coid,
                payload={'deal_reference': deal_ref},
            )

        # === (4) CONFIRM ===
        confirm = await self._call(f'confirms/{deal_ref}', method='get')
        deal_status = (confirm.get('dealStatus') or '').upper()

        if deal_status == 'REJECTED':
            reason = confirm.get('reason') or 'unknown'
            if self.store_ctx is not None:
                self.store_ctx.set_order_state(coid, 'rejected')
                self.store_ctx.log_event(
                    'rejected', client_order_id=coid,
                    intent_key=intent.intent_key,
                    payload={'confirm': confirm},
                )
            reason_lc = reason.lower()
            if 'margin' in reason_lc or 'leverage' in reason_lc:
                raise InsufficientMarginError(f"Capital reject: {reason}")
            raise ExchangeOrderRejectedError(f"Capital confirm REJECTED: {reason}")

        # === (5) PERSIST dealId ===
        deal_id: str | None = None
        affected = confirm.get('affectedDeals') or []
        if affected:
            deal_id = affected[0].get('dealId')
        if not deal_id:
            deal_id = confirm.get('dealId')

        level_confirmed = float(confirm.get('level') or 0.0)
        filled_size = float(confirm.get('size') or quantized_qty)
        confirm_status = (confirm.get('status') or '').upper()
        is_filled_market = (
            intent.order_type == OrderType.MARKET and confirm_status == 'OPEN'
        )

        if self.store_ctx is not None:
            if deal_id:
                self.store_ctx.add_ref(coid, 'deal_id', deal_id)
                self.store_ctx.set_exchange_id(coid, deal_id)
            self.store_ctx.set_order_state(coid, 'confirmed')
            if is_filled_market:
                self.store_ctx.set_filled(coid, filled_size)
            self.store_ctx.log_event(
                'confirmed', client_order_id=coid,
                exchange_order_id=deal_id, intent_key=intent.intent_key,
                payload={'confirm': confirm},
            )

        return [ExchangeOrder(
            id=deal_id or '',
            symbol=intent.symbol,
            side=intent.side,
            order_type=intent.order_type,
            qty=quantized_qty,
            filled_qty=filled_size if is_filled_market else 0.0,
            remaining_qty=(
                max(0.0, quantized_qty - filled_size)
                if is_filled_market else quantized_qty
            ),
            price=intent.limit,
            stop_price=intent.stop,
            average_fill_price=level_confirmed if is_filled_market else None,
            status=OrderStatus.FILLED if is_filled_market else OrderStatus.OPEN,
            timestamp=epoch_time(),
            fee=0.0,
            fee_currency='',
            reduce_only=False,
            client_order_id=coid,
        )]

    async def execute_exit(
            self, envelope: DispatchEnvelope,
    ) -> list[ExchangeOrder]:
        """Attach a TP/SL/trailing bracket to an open position.

        Capital.com's bracket is a *full-row* attribute on the position
        (``PUT /positions/{dealId}`` with ``profitLevel`` / ``stopLevel``
        / ``trailingStop``) — there is no separate exit order. The plugin
        synthesises two :class:`ExchangeOrder` objects with composite
        ``dealId:tp`` / ``dealId:sl`` ids so the sync engine's leg
        accounting still works.

        ``partial_qty_bracket_exit`` is declared ``False`` in
        :meth:`get_capabilities`, so core validation rejects
        ``strategy.exit(qty=N, from_entry='L')`` with ``N < row qty`` at
        startup. A runtime-safety-net assertion here stays as a
        belt-and-braces check — the unit count comparison uses the
        instrument's ``lot_step`` so rounding noise does not falsely trip
        it.

        Trailing with an activation threshold (Pine ``trail_price``) is
        *deferred*: the plugin sets an ``activating`` state and lets the
        activation monitor flip the exchange to native ``trailingStop``
        when the mid-price crosses the threshold. Immediate trailing
        (``trail_offset`` only, no ``trail_price``) goes out on the PUT
        directly.
        """
        intent = envelope.intent
        assert isinstance(intent, ExitIntent)
        rules = await self._get_instrument_rules(intent.symbol)

        # --- Resolve target row ---
        # The parent entry row carries ``pine_entry_id == intent.from_entry``
        # and ``from_entry IS NULL`` (entries have no from_entry). Filtering
        # the SQL by ``from_entry`` would wrongly exclude it — filter by
        # ``symbol`` only and disambiguate in Python.
        target_row: 'OrderRow | None' = None
        if self.store_ctx is not None:
            for row in self.store_ctx.iter_live_orders(symbol=intent.symbol):
                if (row.state == 'confirmed'
                        and row.pine_entry_id == intent.from_entry
                        and row.exchange_order_id
                        and (row.extras or {}).get('kind') == 'position'):
                    target_row = row
                    break
        if target_row is None or not target_row.exchange_order_id:
            raise ExchangeOrderRejectedError(
                f"Capital execute_exit: no confirmed entry row for "
                f"from_entry={intent.from_entry!r} symbol={intent.symbol!r}",
            )
        deal_id = target_row.exchange_order_id

        # Partial-qty runtime safety net (core validation is authoritative).
        row_units = round(target_row.qty / rules.lot_step) if rules.lot_step > 0 else 0
        intent_units = round(intent.qty / rules.lot_step) if rules.lot_step > 0 else 0
        if row_units and intent_units and row_units != intent_units:
            raise ExchangeCapabilityError(
                f"Capital.com bracket is full-row only (row_qty={target_row.qty}, "
                f"intent_qty={intent.qty}). Core validation should have "
                f"rejected this script at startup.",
            )

        # --- Build body + decide trailing deferral ---
        trail_pending = (
            intent.trail_offset is not None and intent.trail_price is not None
        )
        body: dict = {}
        if intent.tp_price is not None:
            body['profitLevel'] = float(intent.tp_price)
        if intent.sl_price is not None:
            body['stopLevel'] = float(intent.sl_price)
        if intent.trail_offset is not None and not trail_pending:
            body['trailingStop'] = True
            body['stopDistance'] = float(intent.trail_offset)

        tp_coid = envelope.client_order_id(KIND_EXIT_TP)
        sl_coid = envelope.client_order_id(KIND_EXIT_SL)

        # --- PERSIST-FIRST the TP leg row ---
        if intent.tp_price is not None and self.store_ctx is not None:
            self.store_ctx.upsert_order(
                tp_coid,
                symbol=intent.symbol,
                side=intent.side,
                qty=target_row.qty,
                state='submitted',
                intent_key=intent.intent_key + '\0TP',
                from_entry=intent.from_entry,
                pine_entry_id=intent.from_entry,
                tp_level=intent.tp_price,
                extras={'leg_kind': 'tp', 'parent_deal_id': deal_id},
            )
            self.store_ctx.add_ref(tp_coid, 'bracket_deal_id', deal_id)

        has_sl_leg = (intent.sl_price is not None or intent.trail_offset is not None)
        if has_sl_leg and self.store_ctx is not None:
            sl_extras: dict = {'leg_kind': 'sl', 'parent_deal_id': deal_id}
            if trail_pending:
                sl_extras['trail_activation_price'] = intent.trail_price
                sl_extras['trail_offset'] = intent.trail_offset
                sl_extras['trail_state'] = 'pending'
            self.store_ctx.upsert_order(
                sl_coid,
                symbol=intent.symbol,
                side=intent.side,
                qty=target_row.qty,
                state='submitted',
                intent_key=intent.intent_key + '\0SL',
                from_entry=intent.from_entry,
                pine_entry_id=intent.from_entry,
                sl_level=intent.sl_price,
                trailing_distance=intent.trail_offset,
                trailing_stop=(intent.trail_offset is not None and not trail_pending),
                extras=sl_extras,
            )
            self.store_ctx.add_ref(sl_coid, 'bracket_deal_id', deal_id)

        # --- PUT the bracket (skipped entirely when body is empty, e.g.
        # trailing-with-activation only — the activation monitor issues
        # the PUT on the crossing tick) ---
        if body:
            resp = await self._call(
                f'positions/{deal_id}', data=body, method='put',
            )
            new_ref = resp.get('dealReference')
            if new_ref:
                attach_confirm = await self._call(
                    f'confirms/{new_ref}', method='get',
                )
                if (attach_confirm.get('dealStatus') or '').upper() == 'REJECTED':
                    reason = attach_confirm.get('reason') or 'unknown'
                    raise ExchangeOrderRejectedError(
                        f"Capital bracket attach REJECTED: {reason}",
                    )

        # --- Mark legs confirmed + persist risk on entry row ---
        if self.store_ctx is not None:
            if intent.tp_price is not None:
                self.store_ctx.set_order_state(tp_coid, 'confirmed')
            if has_sl_leg:
                # ``confirmed`` even when trailing is pending — the row
                # exists; the activation monitor promotes it to active.
                self.store_ctx.set_order_state(sl_coid, 'confirmed')
            self.store_ctx.set_risk(
                target_row.client_order_id,
                sl=intent.sl_price, tp=intent.tp_price,
                trailing_distance=intent.trail_offset,
                trailing_stop=(intent.trail_offset is not None and not trail_pending),
            )
            self.store_ctx.log_event(
                'bracket_attached',
                client_order_id=target_row.client_order_id,
                exchange_order_id=deal_id,
                intent_key=intent.intent_key,
                payload={
                    'tp': intent.tp_price, 'sl': intent.sl_price,
                    'trail_offset': intent.trail_offset,
                    'trail_activation': intent.trail_price,
                    'trail_pending': trail_pending,
                },
            )

        # --- Build canonical ExchangeOrder legs ---
        legs: list[ExchangeOrder] = []
        now_ts = epoch_time()
        if intent.tp_price is not None:
            legs.append(ExchangeOrder(
                id=_bracket_leg_id(deal_id, 'tp'),
                symbol=intent.symbol,
                side=intent.side,
                order_type=OrderType.LIMIT,
                qty=target_row.qty,
                filled_qty=0.0,
                remaining_qty=target_row.qty,
                price=intent.tp_price,
                stop_price=None,
                average_fill_price=None,
                status=OrderStatus.OPEN,
                timestamp=now_ts,
                fee=0.0,
                fee_currency='',
                reduce_only=True,
                client_order_id=tp_coid,
            ))
        if has_sl_leg:
            legs.append(ExchangeOrder(
                id=_bracket_leg_id(deal_id, 'sl'),
                symbol=intent.symbol,
                side=intent.side,
                order_type=(
                    OrderType.TRAILING_STOP
                    if intent.trail_offset is not None else OrderType.STOP
                ),
                qty=target_row.qty,
                filled_qty=0.0,
                remaining_qty=target_row.qty,
                price=None,
                stop_price=intent.sl_price,
                average_fill_price=None,
                status=OrderStatus.OPEN,
                timestamp=now_ts,
                fee=0.0,
                fee_currency='',
                reduce_only=True,
                client_order_id=sl_coid,
            ))
        return legs

    async def execute_close(
            self, envelope: DispatchEnvelope,
    ) -> ExchangeOrder:
        """Close (full) or reduce (partial) a position.

        *Full close* uses ``DELETE /positions/{dealId}`` — preferred, no
        race window. *Partial close* is emulated via an opposite-direction
        ``POST /positions`` because Capital.com has no partial-close
        endpoint. The opposite POST is inherently racy against another
        REST session opening a fresh opposite row in the same instant;
        the plugin protects with a pre- + post-snapshot comparison and a
        corrective ``DELETE`` *only* when the fresh row's
        ``createdDateUTC`` falls within a ±3 s window of our POST. If the
        race cannot be confidently resolved, the plugin raises
        :class:`BrokerManualInterventionError` — the sync engine halts.
        """
        intent = envelope.intent
        assert isinstance(intent, CloseIntent)
        coid = envelope.client_order_id(KIND_CLOSE)
        rules = await self._get_instrument_rules(intent.symbol)

        targets: list['OrderRow'] = []
        if self.store_ctx is not None:
            for row in self.store_ctx.iter_live_orders(symbol=intent.symbol):
                if (row.state == 'confirmed'
                        and row.exchange_order_id
                        and (row.extras or {}).get('kind') == 'position'):
                    targets.append(row)
        if not targets:
            raise ExchangeOrderRejectedError(
                f"Capital execute_close: no confirmed position rows for "
                f"symbol={intent.symbol!r}",
            )

        total_live_units = sum(
            round(max(0.0, row.qty - row.filled_qty) / rules.lot_step)
            if rules.lot_step > 0 else 0
            for row in targets
        )
        intent_units = (
            round(intent.qty / rules.lot_step) if rules.lot_step > 0 else 0
        )

        # --- Full close branch (safe: single DELETE per live row) ---
        if intent_units == total_live_units:
            if self.store_ctx is not None:
                self.store_ctx.upsert_order(
                    coid,
                    symbol=intent.symbol,
                    side=intent.side,
                    qty=intent.qty,
                    state='submitted',
                    intent_key=intent.intent_key,
                    extras={'kind': 'full_close'},
                )
                self.store_ctx.log_event(
                    'dispatch_submitted',
                    client_order_id=coid,
                    intent_key=intent.intent_key,
                    payload={'mode': 'full_close',
                             'targets': [r.exchange_order_id for r in targets]},
                )
            for row in targets:
                await self._call(
                    f'positions/{row.exchange_order_id}', method='delete',
                )
                if self.store_ctx is not None:
                    self.store_ctx.set_order_state(row.client_order_id, 'closing')
                    self.store_ctx.log_event(
                        'close_dispatched',
                        client_order_id=row.client_order_id,
                        exchange_order_id=row.exchange_order_id,
                        intent_key=intent.intent_key,
                    )
            primary = targets[0]
            return ExchangeOrder(
                id=primary.exchange_order_id or '',
                symbol=intent.symbol,
                side=intent.side,
                order_type=OrderType.MARKET,
                qty=intent.qty,
                filled_qty=intent.qty,
                remaining_qty=0.0,
                price=None,
                stop_price=None,
                average_fill_price=None,
                status=OrderStatus.FILLED,
                timestamp=epoch_time(),
                fee=0.0,
                fee_currency='',
                reduce_only=True,
                client_order_id=coid,
            )

        # --- Partial close branch (emulated opposite POST) ---
        return await self._execute_close_partial(envelope, coid, intent, rules)

    async def _execute_close_partial(
            self,
            envelope: DispatchEnvelope,
            coid: str,
            intent: CloseIntent,
            rules: _InstrumentRules,
    ) -> ExchangeOrder:
        """Emulated partial close — see :meth:`execute_close` docstring."""
        # Pre-snapshot
        pre_snap = await self._call('positions', method='get')
        pre_rows = [
            r for r in (pre_snap.get('positions') or [])
            if (r.get('market') or {}).get('epic') == intent.symbol
        ]
        pre_deal_ids = {
            (r.get('position') or {}).get('dealId') for r in pre_rows
        }
        pre_total_units = sum(
            round(float((r.get('position') or {}).get('size', 0.0))
                  / rules.lot_step) if rules.lot_step > 0 else 0
            for r in pre_rows
        )
        intent_units = (
            round(intent.qty / rules.lot_step) if rules.lot_step > 0 else 0
        )

        opposite_dir = 'SELL' if intent.side == 'sell' else 'BUY'
        quantized_qty = self._quantize_size(intent.qty, rules)
        body = {
            'epic': intent.symbol,
            'direction': opposite_dir,
            'size': quantized_qty,
        }

        if self.store_ctx is not None:
            self.store_ctx.upsert_order(
                coid,
                symbol=intent.symbol,
                side=intent.side,
                qty=quantized_qty,
                state='submitted',
                intent_key=intent.intent_key,
                extras={'kind': 'partial_close_emulated',
                        'pre_total_units': pre_total_units,
                        'intent_units': intent_units},
            )
            self.store_ctx.log_event(
                'dispatch_submitted',
                client_order_id=coid,
                intent_key=intent.intent_key,
                payload={'mode': 'partial_close', 'body': body},
            )

        # POST — disposition_unknown on a timeout is *not* auto-retried:
        # a retry could open a second opposite leg and blow the hedge.
        try:
            post_resp = await self._call('positions', data=body, method='post')
        except (httpx.TimeoutException, httpx.RequestError,
                ConnectionError, ExchangeConnectionError) as net:
            if self.store_ctx is not None:
                self.store_ctx.set_order_state(coid, 'disposition_unknown')
                self.store_ctx.log_event(
                    'partial_close_disposition_unknown',
                    client_order_id=coid,
                    intent_key=intent.intent_key,
                    payload={'error': str(net)},
                )
            raise BrokerManualInterventionError(
                f"Partial close POST disposition unknown: {net}",
                intent_key=intent.intent_key,
                context={'coid': coid, 'symbol': intent.symbol,
                         'qty': intent.qty, 'error': str(net)},
            ) from net

        deal_ref = post_resp.get('dealReference')
        if deal_ref and self.store_ctx is not None:
            self.store_ctx.add_ref(coid, 'deal_reference', deal_ref)
            self.store_ctx.set_order_state(coid, 'server_ref_seen')
        if deal_ref:
            await self._call(f'confirms/{deal_ref}', method='get')

        # Post-snapshot — detect the race
        post_snap = await self._call('positions', method='get')
        post_rows = [
            r for r in (post_snap.get('positions') or [])
            if (r.get('market') or {}).get('epic') == intent.symbol
        ]
        post_total_units = sum(
            round(float((r.get('position') or {}).get('size', 0.0))
                  / rules.lot_step) if rules.lot_step > 0 else 0
            for r in post_rows
        )
        expected_post_units = pre_total_units - intent_units

        if post_total_units > expected_post_units:
            # Race: our opposite POST opened a fresh row instead of netting.
            # Only correct when we can pin the fresh row to our POST via
            # createdDateUTC window; otherwise halt.
            fresh_opposite: dict | None = None
            now_ts = epoch_time()
            for r in post_rows:
                pos_data = r.get('position') or {}
                if pos_data.get('dealId') in pre_deal_ids:
                    continue
                if (pos_data.get('direction') or '').upper() != opposite_dir:
                    continue
                created_at = _parse_iso_timestamp(
                    pos_data.get('createdDateUTC') or '',
                )
                if created_at and abs(created_at - now_ts) <= 3.0:
                    fresh_opposite = pos_data
                    break
            if fresh_opposite is not None:
                fresh_id = fresh_opposite.get('dealId')
                await self._call(
                    f'positions/{fresh_id}', method='delete',
                )
                if self.store_ctx is not None:
                    self.store_ctx.log_event(
                        'partial_close_corrective_delete',
                        client_order_id=coid,
                        exchange_order_id=fresh_id,
                        intent_key=intent.intent_key,
                        payload={'reason': 'race_reverse_leg_corrected'},
                    )
            else:
                raise BrokerManualInterventionError(
                    f"Partial close race detected but reverse leg cannot be "
                    f"confidently identified (expected {expected_post_units} "
                    f"units, have {post_total_units})",
                    intent_key=intent.intent_key,
                    context={
                        'coid': coid,
                        'pre_total_units': pre_total_units,
                        'post_total_units': post_total_units,
                        'intent_units': intent_units,
                        'symbol': intent.symbol,
                    },
                )

        if self.store_ctx is not None:
            self.store_ctx.set_order_state(coid, 'confirmed')
            self.store_ctx.log_event(
                'partial_close_completed',
                client_order_id=coid,
                intent_key=intent.intent_key,
                payload={'pre_total_units': pre_total_units,
                         'post_total_units': post_total_units,
                         'intent_units': intent_units},
            )

        return ExchangeOrder(
            id=deal_ref or '',
            symbol=intent.symbol,
            side=intent.side,
            order_type=OrderType.MARKET,
            qty=quantized_qty,
            filled_qty=quantized_qty,
            remaining_qty=0.0,
            price=None,
            stop_price=None,
            average_fill_price=None,
            status=OrderStatus.FILLED,
            timestamp=epoch_time(),
            fee=0.0,
            fee_currency='',
            reduce_only=True,
            client_order_id=coid,
        )

    async def execute_cancel(
            self, envelope: DispatchEnvelope,
    ) -> bool:
        """Cancel pending working orders and/or bracket legs matching the intent.

        Idempotent: if no live row matches the intent, returns ``True``
        with a ``cancel_noop`` audit event. If the exchange reports
        ``error.not-found.dealId`` / ``.dealReference`` on the DELETE,
        the plugin treats it as already-gone (benign race), logs a
        ``cancel_already_gone`` event, and continues.
        """
        intent = envelope.intent
        assert isinstance(intent, CancelIntent)
        coid = envelope.client_order_id(KIND_CANCEL)

        targets: list['OrderRow'] = []
        if self.store_ctx is not None:
            for row in self.store_ctx.iter_live_orders(
                    symbol=intent.symbol,
                    from_entry=intent.from_entry,
            ):
                matches_pine = (
                    row.pine_entry_id == intent.pine_id
                    or row.from_entry == intent.from_entry
                )
                if matches_pine:
                    targets.append(row)

        if not targets:
            if self.store_ctx is not None:
                self.store_ctx.log_event(
                    'cancel_noop',
                    client_order_id=coid,
                    intent_key=intent.intent_key,
                    payload={'reason': 'no_matching_live_row',
                             'pine_id': intent.pine_id,
                             'from_entry': intent.from_entry},
                )
            return True

        for row in targets:
            if row.state not in ('submitted', 'server_ref_seen', 'confirmed'):
                continue
            if not row.exchange_order_id:
                # No exchange id yet — recovery will clear this on the next
                # reconcile; marking ``cancel_pending`` prevents a duplicate
                # DELETE once the id finally lands.
                if self.store_ctx is not None:
                    self.store_ctx.set_order_state(
                        row.client_order_id, 'cancel_pending',
                    )
                continue

            kind = (row.extras or {}).get('kind', 'working')
            endpoint = (
                f'workingorders/{row.exchange_order_id}' if kind == 'working'
                else f'positions/{row.exchange_order_id}'
            )
            try:
                await self._call(endpoint, method='delete')
            except OrderNotFoundError:
                if self.store_ctx is not None:
                    self.store_ctx.log_event(
                        'cancel_already_gone',
                        client_order_id=row.client_order_id,
                        exchange_order_id=row.exchange_order_id,
                        intent_key=intent.intent_key,
                    )
            if self.store_ctx is not None:
                self.store_ctx.close_order(row.client_order_id)
                self.store_ctx.log_event(
                    'cancelled',
                    client_order_id=row.client_order_id,
                    exchange_order_id=row.exchange_order_id,
                    intent_key=intent.intent_key,
                )
        return True

    # --- BrokerPlugin: modify overrides -----------------------------------

    @override
    async def modify_entry(
            self, old: DispatchEnvelope, new: DispatchEnvelope,
    ) -> list[ExchangeOrder]:
        """Amend a pending working order's level, or fall back to cancel+create.

        Capital.com lets us ``PUT /workingorders/{dealId}`` with a new
        ``level`` — cheaper and more atomic than cancel+create. But
        ``size`` is **not** amendable, and switching between LIMIT and
        STOP is not either. Both of those fall through to the base
        class's cancel+execute path so the canonical CO-ID formula (same
        bar, same pine_id → same id) keeps idempotency intact.
        """
        old_i = old.intent
        new_i = new.intent
        assert isinstance(old_i, EntryIntent)
        assert isinstance(new_i, EntryIntent)

        rules = await self._get_instrument_rules(new_i.symbol)
        old_units = (
            round(old_i.qty / rules.lot_step) if rules.lot_step > 0 else 0
        )
        new_units = (
            round(new_i.qty / rules.lot_step) if rules.lot_step > 0 else 0
        )
        if old_units != new_units or old_i.order_type != new_i.order_type:
            return await super().modify_entry(old, new)

        old_coid = old.client_order_id(KIND_ENTRY)
        row: 'OrderRow | None' = (
            self.store_ctx.get_order(old_coid) if self.store_ctx is not None else None
        )
        if row is None or not row.exchange_order_id:
            return await super().modify_entry(old, new)

        body: dict = {}
        if new_i.order_type == OrderType.LIMIT and new_i.limit is not None:
            body['level'] = float(new_i.limit)
        elif new_i.order_type == OrderType.STOP and new_i.stop is not None:
            body['level'] = float(new_i.stop)
        if not body:
            # Nothing to amend (both levels None) — tell the sync engine
            # nothing changed by returning the existing row as-is.
            return [self._row_to_exchange_order(row, new_i)]

        resp = await self._call(
            f'workingorders/{row.exchange_order_id}', data=body, method='put',
        )
        new_ref = resp.get('dealReference')
        if new_ref:
            await self._call(f'confirms/{new_ref}', method='get')
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'modify_entry',
                client_order_id=old_coid,
                exchange_order_id=row.exchange_order_id,
                intent_key=new_i.intent_key,
                payload={'new_level': body['level'],
                         'order_type': new_i.order_type.value},
            )
        return [self._row_to_exchange_order(row, new_i)]

    @override
    async def modify_exit(
            self, old: DispatchEnvelope, new: DispatchEnvelope,
    ) -> list[ExchangeOrder]:
        """Amend an open position's TP/SL levels in place.

        ``PUT /positions/{dealId}`` re-writes ``profitLevel`` /
        ``stopLevel`` atomically — no cancel window where the position
        sits unprotected. Size / from_entry changes are semantic, not
        level changes; those fall through to cancel+create so the
        bracket row is rebuilt from scratch.
        """
        old_i = old.intent
        new_i = new.intent
        assert isinstance(old_i, ExitIntent)
        assert isinstance(new_i, ExitIntent)

        if (old_i.from_entry != new_i.from_entry
                or abs(old_i.qty - new_i.qty) > 1e-9):
            return await super().modify_exit(old, new)

        # Locate the parent position row via its confirmed entry. Same
        # NULL-vs-from_entry subtlety as :meth:`execute_exit` — filter on
        # symbol only, disambiguate in Python.
        target_row: 'OrderRow | None' = None
        if self.store_ctx is not None:
            for row in self.store_ctx.iter_live_orders(symbol=new_i.symbol):
                if (row.state == 'confirmed'
                        and row.pine_entry_id == new_i.from_entry
                        and row.exchange_order_id
                        and (row.extras or {}).get('kind') == 'position'):
                    target_row = row
                    break
        if target_row is None or not target_row.exchange_order_id:
            return await super().modify_exit(old, new)

        body: dict = {}
        if new_i.tp_price is not None:
            body['profitLevel'] = float(new_i.tp_price)
        if new_i.sl_price is not None:
            body['stopLevel'] = float(new_i.sl_price)
        if new_i.trail_offset is not None and new_i.trail_price is None:
            body['trailingStop'] = True
            body['stopDistance'] = float(new_i.trail_offset)
        if not body:
            return []

        resp = await self._call(
            f'positions/{target_row.exchange_order_id}',
            data=body, method='put',
        )
        new_ref = resp.get('dealReference')
        if new_ref:
            await self._call(f'confirms/{new_ref}', method='get')

        if self.store_ctx is not None:
            self.store_ctx.set_risk(
                target_row.client_order_id,
                sl=new_i.sl_price, tp=new_i.tp_price,
                trailing_distance=new_i.trail_offset,
            )
            self.store_ctx.log_event(
                'modify_exit',
                client_order_id=target_row.client_order_id,
                exchange_order_id=target_row.exchange_order_id,
                intent_key=new_i.intent_key,
                payload={'tp': new_i.tp_price, 'sl': new_i.sl_price,
                         'trail_offset': new_i.trail_offset},
            )

        legs: list[ExchangeOrder] = []
        now_ts = epoch_time()
        deal_id = target_row.exchange_order_id
        tp_coid = new.client_order_id(KIND_EXIT_TP)
        sl_coid = new.client_order_id(KIND_EXIT_SL)
        if new_i.tp_price is not None:
            legs.append(ExchangeOrder(
                id=_bracket_leg_id(deal_id, 'tp'),
                symbol=new_i.symbol, side=new_i.side,
                order_type=OrderType.LIMIT,
                qty=target_row.qty, filled_qty=0.0,
                remaining_qty=target_row.qty,
                price=new_i.tp_price, stop_price=None,
                average_fill_price=None, status=OrderStatus.OPEN,
                timestamp=now_ts, fee=0.0, fee_currency='',
                reduce_only=True, client_order_id=tp_coid,
            ))
        if new_i.sl_price is not None or new_i.trail_offset is not None:
            legs.append(ExchangeOrder(
                id=_bracket_leg_id(deal_id, 'sl'),
                symbol=new_i.symbol, side=new_i.side,
                order_type=(
                    OrderType.TRAILING_STOP
                    if new_i.trail_offset is not None else OrderType.STOP
                ),
                qty=target_row.qty, filled_qty=0.0,
                remaining_qty=target_row.qty,
                price=None, stop_price=new_i.sl_price,
                average_fill_price=None, status=OrderStatus.OPEN,
                timestamp=now_ts, fee=0.0, fee_currency='',
                reduce_only=True, client_order_id=sl_coid,
            ))
        return legs

    @staticmethod
    def _row_to_exchange_order(
            row: 'OrderRow', intent: EntryIntent,
    ) -> ExchangeOrder:
        """Synthesize an :class:`ExchangeOrder` from a stored ``OrderRow``.

        Used by :meth:`modify_entry` to return the post-amend representation
        to the sync engine without making another REST call — the stored
        row already carries the authoritative state after the PUT + confirm.
        """
        return ExchangeOrder(
            id=row.exchange_order_id or '',
            symbol=row.symbol,
            side=row.side,
            order_type=intent.order_type,
            qty=row.qty,
            filled_qty=row.filled_qty,
            remaining_qty=max(0.0, row.qty - row.filled_qty),
            price=intent.limit,
            stop_price=intent.stop,
            average_fill_price=None,
            status=OrderStatus.OPEN,
            timestamp=row.updated_ts_ms / 1000.0,
            fee=0.0,
            fee_currency='',
            reduce_only=False,
            client_order_id=row.client_order_id,
        )

    @override
    async def watch_orders(self) -> AsyncIterator[OrderEvent]:
        """Emulated order event stream (Capital.com has no WebSocket channel).

        The outer loop polls three REST endpoints per cadence tick and
        fuses them into a single event stream:

        1. ``GET /history/activity`` — causal: every executed fill /
           cancel / expiry is reported here.  Cross-restart-safe dedup
           via SHA-1 content fingerprint.
        2. ``GET /positions`` / ``GET /workingorders`` — authoritative
           snapshots. Reconcile detects entry fills (working → position
           transition), partial fills (cumulative size decrease), and
           bot-owned orders that silently vanished (``missing_pending``
           grace window + ``on_unexpected_cancel`` policy).
        3. Trailing activation monitor — client-side state machine
           (``pending → activating → active``) that implements Pine
           ``trail_price`` activation threshold on top of Capital's
           always-on native trailing.

        Rate limits (3 GET per tick @ 1.5 s default cadence = 2 req/s)
        sit comfortably below Capital's 10 req/s account budget; 429s
        still trigger an exponential backoff.
        """
        cadence = max(0.5, float(self.config.poll_interval_seconds))
        consecutive_429 = 0
        while True:
            try:
                async for ev in self._poll_once():
                    yield ev
                consecutive_429 = 0
            except asyncio.CancelledError:
                raise
            except ExchangeRateLimitError:
                consecutive_429 += 1
                backoff = min(30.0, cadence * (2 ** min(consecutive_429, 5)))
                await asyncio.sleep(backoff)
                continue
            except (ExchangeConnectionError,
                    httpx.TimeoutException, httpx.RequestError,
                    ConnectionError):
                # Single-cycle hiccup — retry next tick.
                pass
            except CapitalComError as e:
                mapped = self._map_exception(e)
                if mapped is not None and not isinstance(
                        mapped, (ExchangeConnectionError, ExchangeRateLimitError)):
                    raise mapped from e
            await asyncio.sleep(cadence)

    async def _poll_once(self) -> AsyncIterator[OrderEvent]:
        """One poll cycle: fetch the three endpoints and emit fused events.

        The order of the sub-generators matters.  Activity comes first
        because it is *causal* — when an order fills, the fill hits
        ``/history/activity`` before ``/positions`` updates.  Snapshot
        reconcile then covers the cases activity alone cannot pin down
        (working → position transitions, missing pending, partial fill).
        The trailing monitor runs last since it depends on the current
        positions snapshot bid/offer.
        """
        positions_resp = await self._call('positions', method='get')
        working_resp = await self._call('workingorders', method='get')
        last_period = max(60, int(self.config.poll_interval_seconds * 10))
        activity_resp = await self._call(
            'history/activity',
            data={'lastPeriod': last_period, 'detailed': 'true'},
            method='get',
        )

        positions_by_deal: dict[str, dict] = {}
        for row in positions_resp.get('positions') or []:
            pos = row.get('position') or {}
            did = pos.get('dealId')
            if did:
                positions_by_deal[str(did)] = row

        working_by_deal: dict[str, dict] = {}
        for wo in working_resp.get('workingOrders') or []:
            data = wo.get('workingOrderData') or {}
            did = data.get('dealId')
            if did:
                working_by_deal[str(did)] = wo

        async for ev in self._process_activity(
                activity_resp.get('activities') or []):
            yield ev
        async for ev in self._reconcile_snapshot(
                positions_by_deal, working_by_deal):
            yield ev
        async for ev in self._trailing_activation_monitor(positions_by_deal):
            yield ev
        async for ev in self._missing_pending_tracker(
                working_by_deal, positions_by_deal):
            yield ev

    async def _process_activity(
            self, activities: list[dict],
    ) -> AsyncIterator[OrderEvent]:
        """Convert activity rows to events, with fingerprint-based dedup.

        External activity (i.e. ``dealId`` not owned by this bot) is
        logged as ``external_activity_ignored`` and **not** emitted —
        see the reference-plugin external-order policy in the broker
        plugin plan. The sync engine would otherwise try to reconcile
        orders it never placed, which is an invariant violation.
        """
        cursor = self._activity_cursor
        activities_sorted = sorted(
            activities, key=lambda a: a.get('dateUTC') or '',
        )
        for a in activities_sorted:
            date_utc = a.get('dateUTC') or ''
            deal_id = str(a.get('dealId') or '')
            fingerprint = _activity_fingerprint(a)

            if cursor.last_date_utc and date_utc < cursor.last_date_utc:
                continue
            if fingerprint in cursor.seen_fingerprints:
                continue
            cursor.seen_fingerprints.add(fingerprint)
            if not cursor.last_date_utc or date_utc > cursor.last_date_utc:
                cursor.last_date_utc = date_utc

            row: 'OrderRow | None' = None
            if deal_id and self.store_ctx is not None:
                row = self.store_ctx.find_by_ref('deal_id', deal_id)

            if row is None:
                # Log-only for external activity; do not emit.
                if self.store_ctx is not None:
                    self.store_ctx.log_event(
                        'external_activity_ignored',
                        exchange_order_id=deal_id or None,
                        payload={'activity': a, 'fingerprint': fingerprint},
                    )
                continue

            if self.store_ctx is not None:
                self.store_ctx.log_event(
                    'activity_processed',
                    exchange_order_id=deal_id,
                    client_order_id=row.client_order_id,
                    payload={'fingerprint': fingerprint, 'dateUTC': date_utc,
                             'type': a.get('type'), 'status': a.get('status'),
                             'source': a.get('source')},
                )

            event = self._activity_to_event(a, row)
            if event is not None:
                yield event

    def _activity_to_event(
            self, activity: dict, row: 'OrderRow',
    ) -> OrderEvent | None:
        """Map a Capital.com activity row to an :class:`OrderEvent`.

        Returns ``None`` when the row does not correspond to an order-
        lifecycle transition the sync engine needs (e.g. a ``UPDATE``
        without size change).
        """
        activity_type = (activity.get('type') or '').upper()
        status = (activity.get('status') or '').upper()
        source = (activity.get('source') or '').upper()
        size = float(activity.get('size') or row.qty)
        level = float(activity.get('level') or 0.0)
        now_ts = epoch_time()

        side = row.side
        order_type = _order_type_from_row(row, activity_type)
        kind = (row.extras or {}).get('kind', 'position')
        leg_kind = (row.extras or {}).get('leg_kind')

        leg_type: LegType | None = None
        event_type: str | None = None

        if activity_type == 'POSITION':
            if status in ('EXECUTED', 'ACCEPTED'):
                if source == 'TP' or leg_kind == 'tp':
                    leg_type = LegType.TAKE_PROFIT
                    event_type = 'filled'
                elif source == 'SL' or leg_kind == 'sl':
                    leg_type = (LegType.TRAILING_STOP if row.trailing_stop
                                else LegType.STOP_LOSS)
                    event_type = 'filled'
                elif source in ('CLOSE_OUT', 'CLOSE', 'USER', 'DEALER'):
                    leg_type = LegType.CLOSE if kind != 'position' else LegType.ENTRY
                    event_type = 'filled'
                else:
                    leg_type = LegType.ENTRY
                    event_type = 'filled'
            elif status in ('REJECTED',):
                event_type = 'rejected'
            elif status in ('CANCELLED', 'DELETED', 'EXPIRED'):
                event_type = 'cancelled'
        elif activity_type == 'WORKING_ORDER':
            if status in ('ACCEPTED', 'CREATED'):
                leg_type = LegType.ENTRY
                event_type = 'created'
            elif status == 'EXECUTED':
                leg_type = LegType.ENTRY
                event_type = 'filled'
            elif status in ('EXPIRED', 'REJECTED', 'DELETED', 'CANCELLED'):
                event_type = 'cancelled' if status != 'REJECTED' else 'rejected'

        if event_type is None:
            return None

        exch_order = ExchangeOrder(
            id=row.exchange_order_id or '',
            symbol=row.symbol,
            side=side,
            order_type=order_type,
            qty=row.qty,
            filled_qty=size if event_type == 'filled' else row.filled_qty,
            remaining_qty=max(0.0, row.qty - size) if event_type == 'filled' else row.qty,
            price=None,
            stop_price=None,
            average_fill_price=level if event_type == 'filled' else None,
            status=(OrderStatus.FILLED if event_type == 'filled'
                    else OrderStatus.CANCELLED if event_type == 'cancelled'
                    else OrderStatus.REJECTED if event_type == 'rejected'
                    else OrderStatus.OPEN),
            timestamp=now_ts,
            fee=0.0,
            fee_currency='',
            reduce_only=(leg_kind in ('tp', 'sl')),
            client_order_id=row.client_order_id,
        )
        return OrderEvent(
            order=exch_order,
            event_type=event_type,
            fill_price=level if event_type == 'filled' else None,
            fill_qty=size if event_type == 'filled' else None,
            timestamp=now_ts,
            pine_id=row.pine_entry_id,
            from_entry=row.from_entry,
            leg_type=leg_type,
        )

    async def _reconcile_snapshot(
            self,
            positions_by_deal: dict[str, dict],
            working_by_deal: dict[str, dict],
    ) -> AsyncIterator[OrderEvent]:
        """Authoritative snapshot reconcile.

        Walks every live order row in the BrokerStore and emits events
        for state transitions that ``_process_activity`` alone may miss
        (working → position flip on fill, partial fills via cumulative
        size decrease). Rows whose exchange counterpart has disappeared
        get a ``missing_pending_since`` stamp so the grace tracker can
        close them out after the grace window.
        """
        if self.store_ctx is None:
            return
        now_ts = epoch_time()
        for row in list(self.store_ctx.iter_live_orders()):
            did = row.exchange_order_id
            if not did:
                continue
            pos = positions_by_deal.get(did)
            work = working_by_deal.get(did)

            if pos is None and work is None:
                extras = dict(row.extras or {})
                if 'missing_pending_since' not in extras:
                    extras['missing_pending_since'] = now_ts
                    self.store_ctx.upsert_order(
                        row.client_order_id, extras=extras,
                    )
                continue

            # Clear any stale missing_pending stamp — it came back.
            if 'missing_pending_since' in (row.extras or {}):
                extras = {k: v for k, v in (row.extras or {}).items()
                          if k != 'missing_pending_since'}
                self.store_ctx.upsert_order(
                    row.client_order_id, extras=extras,
                )

            # Working → position transition (entry fill)
            if (row.state == 'server_ref_seen' and pos is not None
                    and (row.extras or {}).get('kind') == 'working'):
                pos_data = pos.get('position') or {}
                filled = float(pos_data.get('size') or row.qty)
                self.store_ctx.set_order_state(row.client_order_id, 'confirmed')
                self.store_ctx.set_filled(row.client_order_id, filled)
                self.store_ctx.log_event(
                    'working_to_position',
                    client_order_id=row.client_order_id,
                    exchange_order_id=did,
                    payload={'filled': filled},
                )
                yield OrderEvent(
                    order=ExchangeOrder(
                        id=did, symbol=row.symbol, side=row.side,
                        order_type=OrderType.MARKET,
                        qty=row.qty, filled_qty=filled,
                        remaining_qty=max(0.0, row.qty - filled),
                        price=None, stop_price=None,
                        average_fill_price=float(pos_data.get('level') or 0.0),
                        status=OrderStatus.FILLED,
                        timestamp=now_ts, fee=0.0, fee_currency='',
                        reduce_only=False, client_order_id=row.client_order_id,
                    ),
                    event_type='filled',
                    fill_price=float(pos_data.get('level') or 0.0),
                    fill_qty=filled,
                    timestamp=now_ts,
                    pine_id=row.pine_entry_id,
                    from_entry=row.from_entry,
                    leg_type=LegType.ENTRY,
                )

            # Partial fill detection: position size < row.qty - row.filled_qty.
            if pos is not None and (row.extras or {}).get('kind') == 'position':
                pos_data = pos.get('position') or {}
                current_size = float(pos_data.get('size') or 0.0)
                cumulative = _compute_cumulative_fill(row.qty, current_size)
                if cumulative > row.filled_qty + 1e-9 and cumulative < row.qty:
                    self.store_ctx.set_filled(row.client_order_id, cumulative)
                    self.store_ctx.log_event(
                        'partial_fill_detected',
                        client_order_id=row.client_order_id,
                        exchange_order_id=did,
                        payload={'cumulative': cumulative,
                                 'previous': row.filled_qty},
                    )
                    yield OrderEvent(
                        order=ExchangeOrder(
                            id=did, symbol=row.symbol, side=row.side,
                            order_type=OrderType.MARKET,
                            qty=row.qty, filled_qty=cumulative,
                            remaining_qty=row.qty - cumulative,
                            price=None, stop_price=None,
                            average_fill_price=float(pos_data.get('level') or 0.0),
                            status=OrderStatus.PARTIALLY_FILLED,
                            timestamp=now_ts, fee=0.0, fee_currency='',
                            reduce_only=False,
                            client_order_id=row.client_order_id,
                        ),
                        event_type='partial',
                        fill_price=float(pos_data.get('level') or 0.0),
                        fill_qty=cumulative - row.filled_qty,
                        timestamp=now_ts,
                        pine_id=row.pine_entry_id,
                        from_entry=row.from_entry,
                        leg_type=LegType.ENTRY,
                    )

    async def _missing_pending_tracker(
            self,
            working_by_deal: dict[str, dict],
            positions_by_deal: dict[str, dict],
    ) -> AsyncIterator[OrderEvent]:
        """Emit cancelled events for rows missing past the grace window.

        A row may temporarily disappear between polls (a fill in flight
        shows neither in working nor in positions for an instant). The
        grace window (``5 × cadence``, min 5 s) absorbs that noise.
        Rows missing past the window are treated as cancelled and fed
        into the :meth:`_maybe_raise_unexpected_cancel` policy branch.
        """
        if self.store_ctx is None:
            return
        grace = max(5.0, self.config.poll_interval_seconds * 5.0)
        now_ts = epoch_time()
        for row in list(self.store_ctx.iter_live_orders()):
            since = (row.extras or {}).get('missing_pending_since')
            if since is None:
                continue
            if (now_ts - float(since)) < grace:
                continue
            did = row.exchange_order_id
            if did and (did in working_by_deal or did in positions_by_deal):
                # Came back — already cleared in reconcile, skip.
                continue
            self.store_ctx.close_order(row.client_order_id)
            self.store_ctx.log_event(
                'unexpected_cancel',
                client_order_id=row.client_order_id,
                exchange_order_id=did,
                payload={'missing_since': since, 'grace': grace},
            )
            yield OrderEvent(
                order=ExchangeOrder(
                    id=did or '', symbol=row.symbol, side=row.side,
                    order_type=OrderType.MARKET,
                    qty=row.qty, filled_qty=row.filled_qty,
                    remaining_qty=max(0.0, row.qty - row.filled_qty),
                    price=None, stop_price=None,
                    average_fill_price=None,
                    status=OrderStatus.CANCELLED,
                    timestamp=now_ts, fee=0.0, fee_currency='',
                    reduce_only=False, client_order_id=row.client_order_id,
                ),
                event_type='cancelled',
                fill_price=None, fill_qty=None, timestamp=now_ts,
                pine_id=row.pine_entry_id,
                from_entry=row.from_entry,
            )
            await self._maybe_raise_unexpected_cancel(row)

    async def _maybe_raise_unexpected_cancel(self, row: 'OrderRow') -> None:
        """Apply the configured ``on_unexpected_cancel`` policy.

        - ``stop`` (default): raise :class:`UnexpectedCancelError` — the
          sync engine halts via its normal graceful-stop path.
        - ``stop_and_cancel``: best-effort cancel pass over the other
          bot-owned orders in the same symbol, then raise.
        - ``re_place``: no-op — the sync engine re-dispatches the
          protective order on the next diff cycle.
        - ``ignore``: no-op with an audit log.
        """
        policy = self.on_unexpected_cancel
        if policy == 'ignore':
            if self.store_ctx is not None:
                self.store_ctx.log_event(
                    'unexpected_cancel_ignored',
                    client_order_id=row.client_order_id,
                    exchange_order_id=row.exchange_order_id,
                )
            return
        if policy == 're_place':
            if self.store_ctx is not None:
                self.store_ctx.log_event(
                    'unexpected_cancel_re_place',
                    client_order_id=row.client_order_id,
                    exchange_order_id=row.exchange_order_id,
                )
            return
        if policy == 'stop_and_cancel' and self.store_ctx is not None:
            for other in list(self.store_ctx.iter_live_orders(symbol=row.symbol)):
                if (other.client_order_id == row.client_order_id
                        or not other.exchange_order_id):
                    continue
                kind = (other.extras or {}).get('kind', 'working')
                endpoint = (f'workingorders/{other.exchange_order_id}'
                            if kind == 'working'
                            else f'positions/{other.exchange_order_id}')
                try:
                    await self._call(endpoint, method='delete')
                except (OrderNotFoundError, BrokerError):
                    pass
                self.store_ctx.close_order(other.client_order_id)
        raise UnexpectedCancelError(
            f"Bot-owned order disappeared unexpectedly: "
            f"coid={row.client_order_id!r} deal_id={row.exchange_order_id!r}"
        )

    # --- Trailing activation monitor --------------------------------------

    async def _trailing_activation_monitor(
            self, positions_by_deal: dict[str, dict],
    ) -> AsyncIterator[OrderEvent]:
        """3-state client-side trailing activation machine.

        Pine's ``trail_price`` + ``trail_offset`` semantics: the trailing
        stop only *activates* once the market crosses ``trail_price``.
        Capital.com's native ``trailingStop`` has no activation price
        — it starts tracking immediately. The gap is closed by this
        monitor:

        - ``pending``: row carries ``trail_activation_price``; the
          exchange is not yet trailing.  On the first tick where the
          mid-price crosses the threshold, transition to ``activating``
          and PUT ``trailingStop=true``.
        - ``activating``: idempotent PUT retry every tick until the
          snapshot confirms the exchange flipped to native trailing.
          Then transition to ``active`` and clear the activation fields.
        - ``active``: managed by the exchange; the monitor is a no-op.
        """
        if self.store_ctx is None:
            return
        for row in list(self.store_ctx.iter_live_orders()):
            state = (row.extras or {}).get('trail_state')
            if state not in ('pending', 'activating'):
                continue
            parent_deal_id = (row.extras or {}).get('parent_deal_id')
            if not parent_deal_id:
                continue
            pos = positions_by_deal.get(str(parent_deal_id))
            if pos is None:
                continue
            market = pos.get('market') or {}
            bid = float(market.get('bid') or 0.0)
            offer = float(market.get('offer') or 0.0)
            if not (bid and offer):
                continue
            mid = (bid + offer) / 2.0
            act_price = float((row.extras or {}).get('trail_activation_price') or 0.0)
            # ``row.side`` is the *exit* side. A long-entry SL has side='sell'
            # and activates when the mid rises to the activation price; a
            # short-entry SL has side='buy' and activates when the mid falls
            # to it. Pine's ``trail_price`` is always "the price at which the
            # trailing begins to follow the market", regardless of direction.
            if row.side == 'sell':
                crossed = mid >= act_price
            else:  # row.side == 'buy'
                crossed = mid <= act_price

            if state == 'pending':
                if not crossed:
                    continue
                new_extras = dict(row.extras or {})
                new_extras['trail_state'] = 'activating'
                self.store_ctx.upsert_order(
                    row.client_order_id, extras=new_extras,
                )
                self.store_ctx.log_event(
                    'trailing_activation_triggered',
                    client_order_id=row.client_order_id,
                    exchange_order_id=row.exchange_order_id,
                    payload={'mid': mid, 'activation_price': act_price},
                )
                body = {'trailingStop': True,
                        'stopDistance': float(row.trailing_distance or 0.0)}
                try:
                    await self._call(
                        f'positions/{parent_deal_id}', data=body, method='put',
                    )
                except BrokerError:
                    # Stay in activating; next tick re-tries.
                    pass

            # state 'activating' (possibly just transitioned) — verify
            # from the snapshot and promote to active on confirmation.
            pos_data = pos.get('position') or {}
            if pos_data.get('trailingStop') is True:
                new_extras = {
                    k: v for k, v in (row.extras or {}).items()
                    if k not in ('trail_state', 'trail_activation_price')
                }
                self.store_ctx.set_risk(
                    row.client_order_id, trailing_stop=True,
                )
                self.store_ctx.upsert_order(
                    row.client_order_id, extras=new_extras,
                )
                self.store_ctx.log_event(
                    'trailing_activated',
                    client_order_id=row.client_order_id,
                    exchange_order_id=row.exchange_order_id,
                    payload={'distance': row.trailing_distance},
                )
        # Nothing to yield — the monitor only mutates state.
        return
        yield  # pragma: no cover — keep the generator signature

    # --- Connect / recovery -----------------------------------------------

    async def _load_activity_cursor_from_events(self) -> None:
        """Rebuild :attr:`_activity_cursor` from persisted ``activity_processed``
        events written during previous process incarnations.

        Capital.com's ``/history/activity`` returns a rolling window;
        without cross-restart fingerprint awareness, a restart within
        the window would re-emit every activity row as a duplicate
        event. The plugin side-steps the issue by writing the
        fingerprint to the BrokerStore on every processed row and
        reloading the last 24 h on connect.
        """
        if self.store_ctx is None:
            return
        cutoff_ms = int((epoch_time() - 86400.0) * 1000)
        # Direct SQL is unavoidable here — RunContext has no event query
        # API, and adding one is outside this plugin's scope. Access is
        # intra-package, read-only, and the query is simple enough that
        # the coupling risk is low.
        conn = self.store_ctx._store._conn
        rows = conn.execute(
            "SELECT payload FROM events "
            "WHERE run_instance_id = ? AND kind = 'activity_processed' "
            "AND ts_ms >= ? ORDER BY ts_ms ASC",
            (self.store_ctx.run_instance_id, cutoff_ms),
        ).fetchall()
        cursor = self._activity_cursor
        for row in rows:
            raw = row['payload']
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except ValueError:
                continue
            fp = payload.get('fingerprint')
            if fp:
                cursor.seen_fingerprints.add(fp)
            date_utc = payload.get('dateUTC')
            if date_utc and (cursor.last_date_utc is None
                             or date_utc > cursor.last_date_utc):
                cursor.last_date_utc = date_utc

    async def _recover_in_flight_submissions(self) -> None:
        """Replay §5.1 crash-matrix recovery on connect.

        Batch-fetches the three REST snapshots once, then walks every
        live order row whose state is ``submitted`` or ``server_ref_seen``
        and resolves it against the batch. Three outcomes per row:

        - Stored ``deal_reference`` matches a position/working row →
          promote to ``confirmed`` with the exchange's ``dealId``.
        - ``submitted`` row with no stored ref → confidence-ranked match
          against recent activity (single match → promote; multiple →
          :class:`BrokerManualInterventionError`; none → leave
          submitted, sync engine re-dispatches with same CO-ID).
        - ``server_ref_seen`` row → call ``/confirms/{ref}`` directly;
          on TTL expiry fall back to the snapshot match.
        """
        if self.store_ctx is None:
            return
        positions_resp = await self._call('positions', method='get')
        working_resp = await self._call('workingorders', method='get')
        activity_resp = await self._call(
            'history/activity',
            data={'lastPeriod': 86400, 'detailed': 'true'},
            method='get',
        )

        pos_by_ref: dict[str, dict] = {}
        for row in positions_resp.get('positions') or []:
            pos_data = row.get('position') or {}
            ref = pos_data.get('dealReference')
            if ref:
                pos_by_ref[str(ref)] = row
        wo_by_ref: dict[str, dict] = {}
        for wo in working_resp.get('workingOrders') or []:
            data = wo.get('workingOrderData') or {}
            ref = data.get('dealReference')
            if ref:
                wo_by_ref[str(ref)] = wo
        activities = activity_resp.get('activities') or []

        for row in list(self.store_ctx.iter_live_orders()):
            # ``disposition_unknown`` rows are a superset of ``submitted``
            # for recovery purposes — the POST either never went out or
            # went out without a response. Same resolution strategy: try
            # a stored ref first, then fall back to a confidence-ranked
            # activity match.
            if row.state in ('submitted', 'disposition_unknown'):
                await self._recover_submitted_row(
                    row, activities, pos_by_ref, wo_by_ref,
                )
            elif row.state == 'server_ref_seen':
                await self._recover_server_ref_seen_row(
                    row, pos_by_ref, wo_by_ref,
                )

    async def _recover_submitted_row(
            self, row: 'OrderRow',
            activities: list[dict],
            pos_by_ref: dict[str, dict],
            wo_by_ref: dict[str, dict],
    ) -> None:
        """Resolve a ``submitted`` row with multi-factor confidence."""
        assert self.store_ctx is not None
        stored_ref = (row.extras or {}).get('deal_reference')
        if stored_ref:
            hit = pos_by_ref.get(stored_ref) or wo_by_ref.get(stored_ref)
            if hit is not None:
                data = hit.get('position') or hit.get('workingOrderData') or {}
                deal_id = data.get('dealId')
                if deal_id:
                    self.store_ctx.add_ref(
                        row.client_order_id, 'deal_id', str(deal_id),
                    )
                    self.store_ctx.set_exchange_id(
                        row.client_order_id, str(deal_id),
                    )
                self.store_ctx.set_order_state(
                    row.client_order_id, 'confirmed',
                )
                self.store_ctx.log_event(
                    'recovery_promoted_stored_ref',
                    client_order_id=row.client_order_id,
                    exchange_order_id=deal_id,
                )
                return

        # Activity heuristic: epic + direction + size + ±3 s createdDateUTC.
        row_side = 'BUY' if row.side == 'buy' else 'SELL'
        row_created = row.created_ts_ms / 1000.0 if row.created_ts_ms else 0.0
        candidates: list[dict] = []
        for a in activities:
            if (a.get('epic') or '') != row.symbol:
                continue
            if (a.get('direction') or '').upper() != row_side:
                continue
            if abs(float(a.get('size') or 0.0) - row.qty) > 1e-9:
                continue
            when = _parse_iso_timestamp(a.get('dateUTC') or '')
            if row_created and abs(when - row_created) > 3.0:
                continue
            candidates.append(a)

        if not candidates:
            self.store_ctx.log_event(
                'recovery_no_match',
                client_order_id=row.client_order_id,
            )
            return
        if len(candidates) == 1:
            deal_id = candidates[0].get('dealId')
            if deal_id:
                self.store_ctx.add_ref(
                    row.client_order_id, 'deal_id', str(deal_id),
                )
                self.store_ctx.set_exchange_id(
                    row.client_order_id, str(deal_id),
                )
            self.store_ctx.set_order_state(row.client_order_id, 'confirmed')
            self.store_ctx.log_event(
                'recovery_promoted_single_match',
                client_order_id=row.client_order_id,
                exchange_order_id=deal_id,
            )
            return

        # Multiple matches — cannot auto-resolve.
        self.store_ctx.set_order_state(
            row.client_order_id, 'submission_ambiguous',
        )
        raise BrokerManualInterventionError(
            f"Recovery ambiguous: {len(candidates)} activity matches for "
            f"coid={row.client_order_id!r}",
            intent_key=row.intent_key,
            context={'symbol': row.symbol, 'qty': row.qty, 'side': row.side,
                     'candidate_deal_ids': [c.get('dealId') for c in candidates]},
        )

    async def _recover_server_ref_seen_row(
            self, row: 'OrderRow',
            pos_by_ref: dict[str, dict],
            wo_by_ref: dict[str, dict],
    ) -> None:
        """Resolve a ``server_ref_seen`` row — confirm first, snapshot fallback."""
        assert self.store_ctx is not None
        ref = (row.extras or {}).get('deal_reference')
        if not ref:
            return
        try:
            confirm = await self._call(f'confirms/{ref}', method='get')
        except OrderNotFoundError:
            # TTL expired — fall back to snapshot match on the ref.
            hit = pos_by_ref.get(ref) or wo_by_ref.get(ref)
            if hit is None:
                return
            data = hit.get('position') or hit.get('workingOrderData') or {}
            deal_id = data.get('dealId')
            if deal_id:
                self.store_ctx.add_ref(
                    row.client_order_id, 'deal_id', str(deal_id),
                )
                self.store_ctx.set_exchange_id(
                    row.client_order_id, str(deal_id),
                )
                self.store_ctx.set_order_state(
                    row.client_order_id, 'confirmed',
                )
                self.store_ctx.log_event(
                    'recovery_snapshot_fallback',
                    client_order_id=row.client_order_id,
                    exchange_order_id=deal_id,
                )
            return

        deal_id = None
        affected = confirm.get('affectedDeals') or []
        if affected:
            deal_id = affected[0].get('dealId')
        if not deal_id:
            deal_id = confirm.get('dealId')
        if deal_id:
            self.store_ctx.add_ref(
                row.client_order_id, 'deal_id', str(deal_id),
            )
            self.store_ctx.set_exchange_id(
                row.client_order_id, str(deal_id),
            )
            self.store_ctx.set_order_state(row.client_order_id, 'confirmed')
            self.store_ctx.log_event(
                'recovery_confirm_resolved',
                client_order_id=row.client_order_id,
                exchange_order_id=deal_id,
            )

    def _map_exception(self, raw: Exception) -> BrokerError | None:
        """Translate Capital.com / stdlib exceptions into the broker taxonomy.

        The classification follows the research dossier §8 terminal-vs-
        retryable partition, layered with specific subtypes so the risk
        layer can pattern-match without string-parsing:

        * ``error.not-found.dealId`` / ``.dealReference`` →
          :class:`OrderNotFoundError` — definitive not-exists, cancel/
          modify downgrade to noop, recovery falls back to snapshot.
        * ``error.invalid.stoploss.minvalue: X`` /
          ``error.invalid.takeprofit.minvalue: X`` →
          :class:`InvalidStopDistanceError` /
          :class:`InvalidTakeProfitDistanceError` carrying the dynamic
          minimum so the strategy can re-size without another round-trip.
        * ``error.invalid.leverage.value`` → :class:`InsufficientMarginError`
          (margin/leverage are semantically the same from the script's
          viewpoint — both mean "can't afford this size").
        * Network-layer timeouts / request errors from ``httpx`` →
          :class:`ExchangeConnectionError`; the Order Sync Engine reconnects
          and re-reconciles. Intentionally *not*
          :class:`OrderDispositionUnknownError`: dispatch-site code maps
          a mid-flight POST timeout to that one after persist-first,
          where we actually know a dispatch was in-flight.
        """
        if isinstance(raw, (httpx.TimeoutException, httpx.RequestError)):
            return ExchangeConnectionError(str(raw) or "Capital.com HTTP transport error")

        if isinstance(raw, CapitalComError):
            code = _extract_error_code(raw)
            if code in _AUTH_ERROR_CODES:
                return AuthenticationError(str(raw), reason=code)
            if code == 'error.too-many.requests':
                return ExchangeRateLimitError(str(raw), retry_after=1.0)
            if code == _NOT_FOUND_DEAL_ID_CODE:
                return OrderNotFoundError(str(raw), ref_type='deal_id')
            if code == _NOT_FOUND_DEAL_REF_CODE:
                return OrderNotFoundError(str(raw), ref_type='deal_reference')
            if code.startswith(_INVALID_STOP_PREFIX):
                return InvalidStopDistanceError(
                    str(raw), min_distance=_extract_min_value(raw),
                )
            if code.startswith(_INVALID_TP_PREFIX):
                return InvalidTakeProfitDistanceError(
                    str(raw), min_distance=_extract_min_value(raw),
                )
            if code == _INVALID_LEVERAGE_CODE:
                return InsufficientMarginError(str(raw))
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


def _order_type_from_row(row: 'OrderRow', activity_type: str) -> OrderType:
    """Pick an :class:`OrderType` for an event based on row + activity context.

    Bracket legs carry their intended type through ``row.extras.leg_kind``
    (``'tp'`` → LIMIT, ``'sl'`` → STOP / TRAILING_STOP). Working-order
    rows store the type under ``extras.order_type``. Everything else
    falls back to MARKET — the default for a position fill.
    """
    extras = row.extras or {}
    leg_kind = extras.get('leg_kind')
    if leg_kind == 'tp':
        return OrderType.LIMIT
    if leg_kind == 'sl':
        return OrderType.TRAILING_STOP if row.trailing_stop else OrderType.STOP
    stored = extras.get('order_type')
    if stored:
        try:
            return OrderType(stored)
        except ValueError:
            pass
    if activity_type == 'WORKING_ORDER':
        return OrderType.LIMIT
    return OrderType.MARKET


def _extract_min_value(exc: CapitalComError) -> float:
    """Pull the ``X`` out of a Capital.com ``...minvalue: X`` error message.

    Stop-distance / take-profit-distance rejects follow the pattern
    ``error.invalid.stoploss.minvalue: 0.00050``. The trailing numeric
    value is the dynamic minimum the exchange currently enforces for the
    instrument. Returns ``0.0`` on parse failure so the caller gets a
    defined value without a raise — the typed exception is already the
    signal; the numeric payload is advisory.
    """
    message = str(exc)
    _, _, tail = message.rpartition(':')
    try:
        return float(tail.strip())
    except ValueError:
        return 0.0


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
