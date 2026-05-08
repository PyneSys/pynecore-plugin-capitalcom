"""Synchronous + async REST core for the Capital.com plugin.

Carries authentication state (``security_token``, ``cst_token``,
``session_data``), the request dispatcher (``__call__`` runs the actual
HTTP request, ``_call`` offloads it to a worker thread for async
callers), the session creator (``create_session``), the startup auth
probe (``get_balance``), the cached preferences fetch
(``get_preferences``), the one-way-mode assertion, and the exception
mapper that classifies Capital.com error codes into the broker
taxonomy.

State touched: ``security_token``, ``cst_token``, ``session_data``,
``_account_id``, ``_account_preferences``, ``_last_auth_probe_ts``.
"""
import asyncio
from json import JSONDecodeError
from time import time as epoch_time

import httpx

from pynecore.core.broker.exceptions import (
    AuthenticationError,
    BrokerError,
    ExchangeCapabilityError,
    ExchangeConnectionError,
    ExchangeOrderRejectedError,
    ExchangeRateLimitError,
    InsufficientMarginError,
)

from ._base import _CapitalComBase
from .exceptions import (
    CapitalComError,
    InvalidStopDistanceError,
    InvalidStopMaxValueError,
    InvalidTakeProfitDistanceError,
    InvalidTakeProfitMaxValueError,
    OrderNotFoundError,
)
from .helpers import (
    ENDPOINT_PREFIX,
    URL,
    URL_DEMO,
    _AUTH_ERROR_CODES,
    _INVALID_LEVERAGE_CODE,
    _INVALID_STOP_MAX_PREFIX,
    _INVALID_STOP_MIN_PREFIX,
    _INVALID_TP_MAX_PREFIX,
    _INVALID_TP_MIN_PREFIX,
    _NOT_FOUND_DEAL_ID_CODE,
    _NOT_FOUND_DEAL_REF_CODE,
    _RETRYABLE_CODES,
    _extract_error_code,
    _extract_numeric_value,
    encrypt_password,
)


class _RestSessionMixin(_CapitalComBase):
    """REST/auth surface: dispatcher, session, balance probe, exception mapper."""

    # --- REST core ----------------------------------------------------------

    def __call__(self, endpoint: str, *, data: dict | None = None,
                 method: str = 'post', _level: int = 0) -> dict:
        """Call a General REST API endpoint (synchronous).

        The broker side wraps every call in :meth:`_call` which offloads
        this method to a worker thread via :func:`asyncio.to_thread`.
        """
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

    # --- balance / preferences --------------------------------------------

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

    # --- exception mapping ------------------------------------------------

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
        * ``error.invalid.stoploss.maxvalue: X`` /
          ``error.invalid.takeprofit.maxvalue: X`` →
          :class:`InvalidStopMaxValueError` /
          :class:`InvalidTakeProfitMaxValueError` carrying the boundary
          level (price, not distance) the exchange would have accepted.
          Triggered when a fresh fill has already moved the spread past
          the requested SL/TP level by the time the bracket attach hits.
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
            if code.startswith(_INVALID_STOP_MIN_PREFIX):
                return InvalidStopDistanceError(
                    str(raw), min_distance=_extract_numeric_value(raw),
                )
            if code.startswith(_INVALID_STOP_MAX_PREFIX):
                return InvalidStopMaxValueError(
                    str(raw), max_value=_extract_numeric_value(raw),
                )
            if code.startswith(_INVALID_TP_MIN_PREFIX):
                return InvalidTakeProfitDistanceError(
                    str(raw), min_distance=_extract_numeric_value(raw),
                )
            if code.startswith(_INVALID_TP_MAX_PREFIX):
                return InvalidTakeProfitMaxValueError(
                    str(raw), max_value=_extract_numeric_value(raw),
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
