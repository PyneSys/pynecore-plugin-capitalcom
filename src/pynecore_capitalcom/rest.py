"""Synchronous + async REST core for the Capital.com plugin.

Carries authentication state (``security_token``, ``cst_token``,
``session_data``), the request dispatcher (``__call__`` runs the actual
HTTP request, ``_call`` offloads it to a worker thread for async
callers), the session creator (``create_session``), the startup auth
probe (``get_balance``), the cached preferences fetch
(``get_preferences``), the account-mode probe
(``_detect_account_mode``), and the exception mapper that classifies
Capital.com error codes into the broker taxonomy.

State touched: ``security_token``, ``cst_token``, ``session_data``,
``_account_id``, ``_account_preferences``, ``_hedging_enabled``.
"""
import asyncio
import hashlib
import os
import threading
from abc import ABC
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
from json import JSONDecodeError
from time import monotonic, sleep, time as epoch_time

import httpx

from pynecore.core.broker.exceptions import (
    AuthenticationError,
    BrokerError,
    ExchangeConnectionError,
    ExchangeOrderRejectedError,
    ExchangeRateLimitError,
    InsufficientMarginError,
)

from ._base import _CapitalComBase
from .exceptions import (
    CapitalComError,
    HistoricalPricesNotFoundError,
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
    _extract_jwt_expiry,
    _extract_numeric_value,
    encrypt_password,
)

# Capital.com session tokens expire ~1h after creation despite the
# documented 10-min "inactivity" TTL — the cap is undocumented but
# observed in practice. We refresh proactively before the cap so the
# trading loop never sees ``error.invalid.session.token``.
_SESSION_REFRESH_FALLBACK_S = 50 * 60
_SESSION_REFRESH_SAFETY_S = 5 * 60
# Capital.com permits one ``POST /session`` per second per API key. Broker and
# provider instances can start concurrently in one process, so their instance
# locks are insufficient: coordinate bootstrap POSTs across all instances that
# share a key. The transport still gets one bounded retry because another
# process may have consumed the venue slot immediately before this process.
_SESSION_CREATE_MIN_INTERVAL_S = 1.0
_SESSION_CREATE_RETRY_COUNT = 1
_SESSION_CREATE_GUARD = threading.Lock()
_SESSION_CREATE_LOCKS: dict[bytes, threading.Lock] = {}
_SESSION_CREATE_LAST_POST: dict[bytes, float] = {}
_ACCOUNT_WRITE_GUARD = threading.Lock()
_ACCOUNT_WRITE_LOCKS: dict[bytes, threading.Lock] = {}


@contextmanager
def _exclusive_account_file_lock(path: Path) -> Iterator[None]:
    """Hold one process-shared account write lock on POSIX."""
    import fcntl

    descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)
# Bootstrap requests — must NOT trigger a proactive refresh (would
# recurse into ``create_session`` itself), MUST go out without auth
# headers, and MUST NOT trigger reactive retry on
# ``error.invalid.session.token``. Keyed on (endpoint, method) so an
# authenticated ``PUT session`` (account switch) is NOT mistaken for
# the unauthenticated bootstrap login — only the encryption-key GET
# and the login POST form the bootstrap pair.
_SESSION_BOOTSTRAP_REQUESTS = frozenset({
    ('session/encryptionKey', 'get'),
    ('session', 'post'),
})
# Codes that mean "your session is gone" — re-create and retry.
_SESSION_RECREATE_CODES = frozenset({
    'error.security.client-token-missing',
    'error.null.client.token',
    'error.invalid.session.token',
})


class _RestSessionMixin(_CapitalComBase, ABC):
    """REST/auth surface: dispatcher, session, balance probe, exception mapper."""

    # --- REST core ----------------------------------------------------------

    def __call__(self, endpoint: str, *, data: dict | None = None,
                 method: str = 'post', _level: int = 0) -> dict:
        """Call a General REST API endpoint (synchronous).

        The broker side wraps every call in :meth:`_call` which offloads
        this method to a worker thread via :func:`asyncio.to_thread`.
        """
        method_lc = method.lower()
        is_bootstrap = (endpoint, method_lc) in _SESSION_BOOTSTRAP_REQUESTS
        # Proactive session refresh: Capital.com tokens have an
        # undocumented hard cap (~1h). Refresh ahead of the deadline so
        # ``watch_orders`` never observes ``error.invalid.session.token``.
        # Skipped during the bootstrap calls themselves to avoid
        # re-entering ``create_session`` from ``__call__``.
        if (_level == 0
                and not is_bootstrap
                and self._capital_config.user_email and self._capital_config.api_password):
            self._refresh_session_if_stale()

        # Snapshot the tokens used for THIS request. Two purposes:
        #   1. Bootstrap endpoints (``session/encryptionKey``/``session``)
        #      must go out without auth headers — sending stale CST/
        #      X-SECURITY-TOKEN there can itself trigger
        #      ``error.invalid.session.token``. We skip auth here rather
        #      than wiping the class state, so a concurrent WebSocket
        #      ``_send`` does not see ``None`` mid-refresh.
        #   2. The post-response rotation handler compares against this
        #      snapshot, not the current attribute, so a slow response
        #      that returns AFTER another worker has refreshed the
        #      session does not roll the freshly-issued tokens back to
        #      the stale pair the server echoed.
        # Atomic snapshot: tokens AND generation must be read together
        # under the session lock — otherwise a ``create_session`` that
        # completes between the token reads and the generation read
        # would tag THIS request (already pointed at the OLD session
        # through its token snapshot) with the NEW generation, and the
        # rotation handler would later accept the response's old-session
        # rotation header as current. Holding the lock here also blocks
        # against an in-progress refresh: the snapshot is consistent in
        # both cases (pre-refresh: old tokens + old gen; post-refresh:
        # new tokens + new gen). The lock is only held across attribute
        # reads, so HTTP I/O is unaffected.
        with self._session_lock:
            req_security_token = self.security_token
            req_cst_token = self.cst_token
            req_generation = self._session_generation

        headers = {'X-CAP-API-KEY': self._capital_config.api_key}
        if not is_bootstrap:
            if req_security_token:
                headers['X-SECURITY-TOKEN'] = req_security_token
            if req_cst_token:
                headers['CST'] = req_cst_token

        params: dict[str, dict | float | None] = dict(headers=headers, timeout=50.0)
        if method_lc == 'get':
            params['params'] = data
        elif method_lc in ('post', 'put'):
            params['json'] = data

        url = URL_DEMO if self._capital_config.demo else URL
        url += ENDPOINT_PREFIX + endpoint

        res: httpx.Response = getattr(httpx, method_lc)(url, **params)
        try:
            dict_res = res.json()
        except JSONDecodeError:
            raise CapitalComError(f"JSON Error: {res.text}")
        if not isinstance(dict_res, dict):
            raise CapitalComError("REST response root schema changed")

        if res.is_error:
            error_code = dict_res.get('errorCode')
            if error_code == 'error.prices.not-found':
                raise HistoricalPricesNotFoundError(
                    'Capital.com historical price range is empty'
                )
            # Bootstrap endpoints must NOT trigger a re-login retry —
            # ``create_session`` calls them with the default ``_level=0``,
            # so a session-token error there would recurse into another
            # ``create_session`` until ``RecursionError``. Surface the
            # error directly; bootstrap requests already go out without
            # stale auth headers (see snapshot above), so a real auth
            # failure here is terminal and needs operator attention.
            if error_code in _SESSION_RECREATE_CODES \
                    and not is_bootstrap \
                    and self._capital_config.user_email and self._capital_config.api_password and _level < 3:
                # Coordinate re-creation without holding ``_session_lock``
                # across bootstrap I/O. Workers that lose the race observe the
                # advanced generation and skip a redundant login.
                self._create_session_coordinated(
                    expected_generation=req_generation,
                )
                return self(endpoint=endpoint, data=data, method=method, _level=_level + 1)
            raise CapitalComError(f"API error occured: {error_code or 'unknown-error'}")

        # Apply rotation under the session lock so a concurrent refresh
        # cannot interleave with this update. If a fresh session was
        # installed since we snapshotted (generation advanced), the
        # rotation header on our response belongs to the OLD session —
        # discarding it keeps the freshly-issued tokens intact. Bootstrap
        # responses are exempt: they ARE the new session being installed,
        # so they always apply and bump the generation. Concurrent
        # *non-bootstrap* responses with the same generation are allowed
        # to apply (last-writer-wins) — the snapshot tokens are NOT
        # compared because two normal requests both rotating from the
        # same starting tokens are equally valid signals to update.
        new_security = res.headers.get('X-SECURITY-TOKEN', req_security_token)
        new_cst = res.headers.get('CST', req_cst_token)
        if new_security == req_security_token and new_cst == req_cst_token:
            return dict_res
        with self._session_lock:
            if not is_bootstrap and self._session_generation != req_generation:
                return dict_res
            now = epoch_time()
            if new_security != req_security_token:
                self.security_token = new_security
                self._security_token_deadline = self._token_deadline(
                    new_security, now,
                )
            if new_cst != req_cst_token:
                self.cst_token = new_cst
                self._cst_token_deadline = self._token_deadline(
                    new_cst, now,
                )
            self._session_token_expiry_ts = self._compute_token_expiry()
            if is_bootstrap:
                self._session_generation += 1

        return dict_res

    @staticmethod
    def _token_deadline(tok: str | None, now: float) -> float:
        """Per-token expiry: JWT ``exp`` if present, else ``now + fallback``.

        Returns ``0.0`` for an empty token so :meth:`_compute_token_expiry`
        can ignore it. Called at issue time so opaque tokens get their
        hard cap anchored to the actual issue moment, not a later recompute.
        """
        if not tok:
            return 0.0
        exp = _extract_jwt_expiry(tok)
        return exp if exp is not None else now + _SESSION_REFRESH_FALLBACK_S

    def _compute_token_expiry(self) -> float:
        """Return the earliest deadline across the two session tokens.

        Reads the per-token deadlines maintained by :meth:`__call__`; the
        session is only as fresh as the earlier-expiring half. Falls back
        to ``epoch_time() + _SESSION_REFRESH_FALLBACK_S`` only when no
        deadline has been registered yet (e.g. tests bypassing the
        rotation handler).
        """
        deadlines = [
            d for d in (self._security_token_deadline, self._cst_token_deadline)
            if d > 0.0
        ]
        if deadlines:
            return min(deadlines)
        return epoch_time() + _SESSION_REFRESH_FALLBACK_S

    def _refresh_session_if_stale(self) -> None:
        """Re-create the session when the current one is within the safety window.

        The fast state snapshot avoids coordinator traffic for fresh sessions.
        The coordinated path repeats the check after any active login finishes,
        without holding the session lock while it waits or performs network I/O.
        """
        with self._session_lock:
            expiry = self._session_token_expiry_ts
        if expiry == 0.0 or epoch_time() < expiry - _SESSION_REFRESH_SAFETY_S:
            return
        self._create_session_coordinated(stale_only=True)

    def create_session(self) -> None:
        """Create a session with the Capital.com API (login).

        Concurrent direct and refresh callers coalesce around one active login.
        If that owner fails, the next queued caller takes ownership and retries.

        Leaves the existing CST / X-SECURITY-TOKEN attributes intact.
        :meth:`__call__` skips auth headers on the bootstrap endpoints,
        so they never see stale CST/X-SECURITY-TOKEN values, and a
        concurrent WebSocket ``_send`` keeps observing valid tokens
        until the response of the ``session`` POST is applied through
        the rotation handler (which atomically swaps both halves).

        Latches the plugin-qualified ``_account_id`` from the session
        response. Session creation is the earliest authenticated call —
        it runs on the provider (OHLCV download) path too, well before
        the broker's ``get_balance`` startup probe. Deferring the latch
        to ``get_balance`` alone left ``account_id`` at the ``"default"``
        sentinel when the run-identity contract validation ran between
        provider authentication and broker startup, aborting the run.
        """
        self._create_session_coordinated()

    def _create_session_coordinated(
            self,
            *,
            expected_generation: int | None = None,
            stale_only: bool = False,
    ) -> None:
        """Run one login owner and coalesce concurrent callers around it."""
        with self._session_refresh_condition:
            observed_completion = self._session_login_completion
            while self._session_login_active:
                self._session_refresh_condition.wait()
            if self._session_login_completion != observed_completion:
                return
            with self._session_lock:
                if (
                    expected_generation is not None
                    and self._session_generation != expected_generation
                ):
                    return
                expiry = self._session_token_expiry_ts
            if (
                stale_only
                and (
                    expiry == 0.0
                    or epoch_time() < expiry - _SESSION_REFRESH_SAFETY_S
                )
            ):
                return
            self._session_login_active = True

        try:
            self._perform_session_login()
        except BaseException:
            with self._session_refresh_condition:
                self._session_login_active = False
                self._session_refresh_condition.notify_all()
            raise
        with self._session_refresh_condition:
            self._session_login_completion += 1
            self._session_login_active = False
            self._session_refresh_condition.notify_all()

    def _perform_session_login(self) -> None:
        """Authenticate and install one complete Capital.com session."""
        key_fingerprint = hashlib.sha256(
            self._capital_config.api_key.encode('utf-8')
        ).digest()
        with _SESSION_CREATE_GUARD:
            bootstrap_lock = _SESSION_CREATE_LOCKS.setdefault(
                key_fingerprint, threading.Lock(),
            )

        with bootstrap_lock:
            for attempt in range(_SESSION_CREATE_RETRY_COUNT + 1):
                elapsed = monotonic() - _SESSION_CREATE_LAST_POST.get(
                    key_fingerprint, float('-inf'),
                )
                delay = _SESSION_CREATE_MIN_INTERVAL_S - elapsed
                if delay > 0.0:
                    sleep(delay)

                res: dict = self('session/encryptionKey', method='get')
                encryption_key = res['encryptionKey']
                timestamp = res['timeStamp']
                password = encrypt_password(
                    self._capital_config.api_password, encryption_key, timestamp,
                )
                _SESSION_CREATE_LAST_POST[key_fingerprint] = monotonic()
                try:
                    session_data = self('session', data=dict(
                        encryptedPassword=True,
                        identifier=self._capital_config.user_email,
                        password=password,
                    ))
                except CapitalComError as exc:
                    if (_extract_error_code(exc) != 'error.too-many.requests'
                            or attempt >= _SESSION_CREATE_RETRY_COUNT):
                        raise
                    continue
                self.session_data = session_data
                break
        current_account_id = (self.session_data or {}).get('currentAccountId')
        if current_account_id:
            mode = 'demo' if self._capital_config.demo else 'live'
            self._account_id = f"capitalcom-{mode}-{current_account_id}"

    def _call_serialized_write(
            self, endpoint: str, *, data: dict | None = None,
            method: str = 'post',
    ) -> dict:
        """Call REST synchronously while the caller owns the account lock."""
        return self(endpoint, data=data, method=method)

    def _account_write_lock(self) -> tuple[threading.Lock, Path]:
        """Return the shared thread lock and cross-process lock path."""
        account_id = self._account_id
        if not account_id or account_id == 'default':
            raise AuthenticationError(
                "Capital.com account identity must be resolved before a position write",
                reason='account_identity_unresolved',
            )
        lock_key = hashlib.sha256(account_id.encode('utf-8')).digest()
        with _ACCOUNT_WRITE_GUARD:
            write_lock = _ACCOUNT_WRITE_LOCKS.setdefault(
                lock_key, threading.Lock(),
            )
        lock_path = Path.home() / '.pynecore' / 'locks' / (
            f"capitalcom-account-{lock_key.hex()}.lock"
        )
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        return write_lock, lock_path

    def _call_with_account_write_lock(
            self, endpoint: str, *, data: dict | None = None,
            method: str = 'post',
    ) -> dict:
        """Serialize one Pyne-controlled position mutation account-wide."""
        write_lock, lock_path = self._account_write_lock()
        with write_lock, _exclusive_account_file_lock(lock_path):
            return self._call_serialized_write(
                endpoint, data=data, method=method,
            )

    async def _submit_exclusive_partial_close(
            self, *, coid: str, symbol: str, expected_direction: str,
            target_deal_ids: frozenset[str], body: dict,
            intent_qty: float, lot_step: float,
    ) -> tuple[dict, list[dict]]:
        """Serialize the final positions snapshot and non-targetable reduce POST.

        The lock is keyed only by resolved account identity, so plugin instances
        using different API keys for the same account share one critical section.
        External manual/API writers remain outside this lock; callers therefore
        reconcile the exact target deal after the POST.
        """
        write_lock, lock_path = self._account_write_lock()

        def snapshot_and_post() -> tuple[dict, list[dict]]:
            with write_lock, _exclusive_account_file_lock(lock_path):
                snapshot = self._call_serialized_write('positions', method='get')
                rows = [
                    raw for raw in (snapshot.get('positions') or [])
                    if (raw.get('market') or {}).get('epic') == symbol
                ]
                positions = [raw.get('position') or {} for raw in rows]
                deal_ids = {
                    str(position['dealId'])
                    for position in positions
                    if position.get('dealId')
                }
                exclusive_target = (
                    len(target_deal_ids) == 1
                    and len(positions) == 1
                    and deal_ids == target_deal_ids
                    and (positions[0].get('direction') or '').upper()
                    == expected_direction
                )
                if not exclusive_target:
                    raise ExchangeOrderRejectedError(
                        "Capital partial close requires one exclusive owned "
                        "venue position immediately before dispatch because "
                        "the reduction POST cannot target a dealId"
                    )
                target = positions[0]
                target_deal_id = str(target['dealId'])
                target_direction = (target.get('direction') or '').upper()
                live_size = float(target.get('size', 0.0))
                intent_units = round(intent_qty / lot_step) if lot_step > 0 else 0
                live_units = round(live_size / lot_step) if lot_step > 0 else 0
                if intent_units <= 0 or intent_units >= live_units:
                    raise ExchangeOrderRejectedError(
                        "Capital partial close quantity must be positive and "
                        "smaller than the target's current venue quantity "
                        "immediately before dispatch"
                    )
                store_ctx = self.store_ctx
                if store_ctx is not None:
                    existing = store_ctx.get_order(coid)
                    merged_extras = dict((existing.extras or {}) if existing else {})
                    merged_extras.update({
                        'target_deal_id': target_deal_id,
                        'target_direction': target_direction,
                        'pre_target_units': live_units,
                        'intent_units': intent_units,
                    })
                    store_ctx.upsert_order(coid, extras=merged_extras)
                response = self._call_serialized_write(
                    'positions', data=body, method='post',
                )
                return response, rows

        try:
            return await asyncio.to_thread(snapshot_and_post)
        except Exception as exc:
            mapped = self._map_exception(exc)
            if mapped is not None:
                raise mapped from exc
            raise

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
            method_lc = method.lower()
            position_write = (
                endpoint == 'positions' and method_lc == 'post'
                or endpoint.startswith('positions/')
                and method_lc in {'put', 'delete'}
            )
            caller = (
                self._call_with_account_write_lock
                if position_write else self
            )
            return await asyncio.to_thread(
                caller, endpoint, data=data, method=method,
            )
        except Exception as e:
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
                    mode = 'demo' if self._capital_config.demo else 'live'
                    self._account_id = f"capitalcom-{mode}-{account_id}"
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

    async def _detect_account_mode(self) -> None:
        """Probe the account's position mode and wire the one-way emulation.

        A hedging-mode account opts into core one-way emulation: the Order
        Sync Engine reroutes close/reversal/bracket through the
        :class:`~pynecore.core.broker.one_way_emulator.OneWayEmulator` via
        this plugin's :class:`~pynecore.core.plugin.broker.PositionPort`
        primitives, presenting Pine one-way semantics over the multi-row
        book. A one-way (netting) account leaves ``position_port`` ``None``
        and keeps the direct execute path. ``get_preferences`` is
        instance-cached, so a reconnect re-entry is free and the
        assignment is idempotent.
        """
        prefs = await self.get_preferences()
        self._hedging_enabled = bool(prefs.get('hedgingMode'))
        if self._hedging_enabled:
            self.position_port = self

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
            # Must precede the generic ``error.invalid.`` branch so
            # ``error.invalid.session.token`` (a session-expiry signal,
            # not an order-rejection) routes here instead of falling
            # through to ``ExchangeOrderRejectedError``.
            if code in _RETRYABLE_CODES:
                return ExchangeConnectionError(str(raw))
            if code and code.startswith('error.invalid.') and 'margin' in code:
                return InsufficientMarginError(str(raw))
            if code and code.startswith('error.invalid.'):
                return ExchangeOrderRejectedError(str(raw))
        return super()._map_exception(raw)
