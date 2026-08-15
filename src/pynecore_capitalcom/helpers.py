"""Module-level constants and stateless helpers.

Bundles three loosely related concerns that share one trait — none of them
touch class state, all are pure functions / constants:

* Endpoint constants (live + demo REST hosts, WS URL, prefix).
* Lookup tables (timeframe map, asset-type label map).
* Error-code frozensets used by ``_RestSessionMixin._map_exception``.
* Pure helpers: password encryption, opening-hours parser, error-code +
  numeric-value extractors, ISO timestamp parser, order-type-from-row
  resolver.

Imported by ``_base.py`` and many mix-ins; depends only on stdlib and
``exceptions.py``.
"""
import json
import math
from base64 import standard_b64decode, standard_b64encode, urlsafe_b64decode
from collections.abc import Mapping
from datetime import UTC, datetime, time
from decimal import Decimal
from time import time as epoch_time
from typing import TYPE_CHECKING, Literal

from Crypto.Cipher import PKCS1_v1_5
from Crypto.PublicKey import RSA

from pynecore.core.broker.models import OrderType, PositionLeg

from .exceptions import CapitalComError

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow


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

TYPES: dict[str, Literal['forex', 'crypto', 'stock', 'index']] = {
    'CURRENCIES': 'forex',
    'CRYTOCURRENCIES': 'crypto',
    'SHARES': 'stock',
    'INDICES': 'index',
}

_RETRYABLE_CODES = frozenset({
    'error.too-many.requests',
    'error.security.client-token-missing',
    'error.null.client.token',
    'error.invalid.session.token',
})

_AUTH_ERROR_CODES = frozenset({
    'error.null.api.key',
    'error.invalid.details',
    'error.invalid.accountId',
})

_NOT_FOUND_DEAL_ID_CODE = 'error.not-found.dealId'
_NOT_FOUND_DEAL_REF_CODE = 'error.not-found.dealReference'
_INVALID_STOP_MIN_PREFIX = 'error.invalid.stoploss.minvalue'
_INVALID_STOP_MAX_PREFIX = 'error.invalid.stoploss.maxvalue'
_INVALID_TP_MIN_PREFIX = 'error.invalid.takeprofit.minvalue'
_INVALID_TP_MAX_PREFIX = 'error.invalid.takeprofit.maxvalue'
_INVALID_LEVERAGE_CODE = 'error.invalid.leverage.value'

# ``rejectReason`` tokens / substrings on a REJECTED confirm that mean the
# account cannot fund or risk-clear the order (insufficient balance, margin,
# leverage cap, or a generic risk-engine veto). These map to the typed
# :class:`InsufficientMarginError` — a non-terminal reject the runner survives
# (see its docstring) so the strategy can respond on a later bar.
_FUNDS_REJECT_REASON_SUBSTRINGS = ('MARGIN', 'LEVERAGE', 'FUND', 'RISK', 'BALANCE')


# --- Internal tuning constants ---------------------------------------------
#
# These are intentionally NOT exposed in :class:`CapitalComConfig` (and
# therefore not in ``workdir/config/plugins/capitalcom.toml``). They are
# rate-limit / cache / WS-feed heuristics tuned once for Capital.com's
# specific server behaviour and are not something an end user can
# meaningfully reason about. If real operator intent ever surfaces to
# override one, promote it through the config writer's hidden-field
# mechanism — not by reintroducing a docstring'd dataclass field.

# Seconds between ``GET /positions`` + ``GET /workingorders`` polls.
# Capital.com's 10 req/s budget easily accommodates 1s cadence, but 1.5s
# leaves headroom for the snapshot + activity tail + any ad-hoc reconcile
# calls the sync engine issues.
_POLL_INTERVAL_S = 1.5

# TTL on cached ``_InstrumentRules`` entries. Capital.com widens the
# bracket-distance minimum during volatile sessions; unbounded
# caching would silently disable the pre-check. Five minutes bounds the
# staleness to one news-event window without flooding the rate-limited
# markets endpoint.
_INSTRUMENT_RULES_TTL_S = 300.0

# Rolling window of full-coverage WS quote counts used by the volume
# sanity checks. Drives the low-ratio outlier trigger and the
# median-estimate emitted when REST also fails. 20 bars stays responsive
# to session-level liquidity changes without being swayed by a single
# quiet bar.
_WS_VOLUME_BASELINE_BARS = 20

# Minimum baseline samples required before low-ratio and median-estimate
# paths activate. During warmup the worker emits raw WS counts; REST
# still fires for ``ws_volume == 0`` and partial-coverage cases.
_WS_VOLUME_MIN_BASELINE_BARS = 5

# Multiplier applied to the rolling median when deciding whether a
# full-coverage WS volume is low enough to warrant a REST sanity check.
# 0.20 → a bar below 20% of the median triggers REST.
_WS_VOLUME_LOW_RATIO = 0.20

# Consecutive REST-confirmed bad full-coverage bars after which the
# worker forces a WS reconnect. Two in a row implies the quote feed has
# gone silent while the OHLC subscription still ticks.
_WS_VOLUME_BAD_BAR_RECONNECT_THRESHOLD = 2


def encrypt_password(password: str, encryption_key: str, timestamp: int | None = None):
    if timestamp is None:
        timestamp = int(epoch_time())
    payload = password + '|' + str(timestamp)
    payload = standard_b64encode(payload.encode('ascii'))
    public_key = RSA.importKey(standard_b64decode(encryption_key.encode('ascii')))
    cipher = PKCS1_v1_5.new(public_key)
    ciphertext = standard_b64encode(cipher.encrypt(payload)).decode()
    return ciphertext


def _parse_opening_hours_segment(oh: str) -> tuple[time, time] | None:
    """Parse one Capital.com ``openingHours`` segment into ``(start, end)``.

    Each per-day entry has the shape ``"HH:MM - HH:MM"`` where either side may
    be ``"00:00"`` to denote midnight (open from / closed at the day boundary).
    Whitespace around the dash and around the times is optional.

    The two times are returned as :class:`datetime.time` values in the *source*
    timezone reported by Capital — the caller is responsible for converting
    them to ``self.timezone``. ``time(0, 0)`` represents midnight on either
    side; the caller uses that to decide whether to emit a session_start /
    session_end marker (midnight is the day boundary, not a real transition).

    Returns ``None`` for empty input or any segment that cannot be parsed
    (closed-day markers, multi-segment lines with stray dashes, malformed
    time strings) — invalid segments are silently skipped by the caller.
    """
    parts = oh.split('-')
    if len(parts) != 2:
        return None
    raw_start = parts[0].strip()
    raw_end = parts[1].strip()
    # A bare ``"-"`` (or any segment where one side is empty after
    # stripping) is a closed-day marker, NOT a 24h session. Defaulting an
    # empty side to ``time(0, 0)`` would round-trip to ``(00:00, 00:00)``,
    # which the caller treats as a 24h shape — silently flipping the
    # market open all day. Both sides must be explicit times; reject
    # the segment otherwise so the caller's ``parsed is None`` skip
    # fires.
    if not raw_start or not raw_end:
        return None
    try:
        start = time.fromisoformat(raw_start)
        end = time.fromisoformat(raw_end)
    except ValueError:
        return None
    return start, end


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


def _extract_reject_reason(confirm: dict) -> str:
    """Pull the rejection reason out of a Capital.com confirm payload.

    Capital.com reports the reason under ``rejectReason`` (e.g.
    ``"RISK_CHECK"``, ``"INSUFFICIENT_FUNDS"``). The legacy ``reason`` key is
    kept as a fallback in case an alternate payload shape ever carries it.
    Returns ``"unknown"`` when neither is present.
    """
    reason = confirm.get('rejectReason') or confirm.get('reason')
    return str(reason) if reason else 'unknown'


def _is_funds_reject(reason: str) -> bool:
    """True when a Capital.com reject reason means the account cannot fund /
    risk-clear the order (insufficient balance, margin, leverage cap or a
    generic risk-engine veto), so the caller should raise the non-terminal
    :class:`InsufficientMarginError` rather than a plain reject."""
    reason_uc = reason.upper()
    return any(token in reason_uc for token in _FUNDS_REJECT_REASON_SUBSTRINGS)


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


def _wire_float(value: object, *, finite: bool = False) -> float | None:
    """Parse an untyped wire value as a float.

    Booleans are rejected even though :class:`bool` is an :class:`int`
    subclass. When ``finite`` is true, NaN and infinities are rejected as
    invalid monetary / price / size inputs.
    """
    if value is None or isinstance(value, bool):
        return None
    if not isinstance(value, (str, bytes, bytearray, int, float, Decimal)):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if finite and not math.isfinite(parsed):
        return None
    return parsed


def _wire_int(value: object) -> int | None:
    """Parse an untyped wire value as an exact integer."""
    parsed = _wire_float(value, finite=True)
    if parsed is None or not parsed.is_integer():
        return None
    return int(parsed)


def _wire_id(value: object) -> str | None:
    """Return a non-empty string identifier from an untyped wire value."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (str, int)):
        parsed = str(value).strip()
        return parsed or None
    return None


def _extract_numeric_value(exc: CapitalComError) -> float:
    """Pull the trailing numeric ``X`` out of Capital.com's
    ``error.invalid.<leg>.<bound>: X`` messages.

    Stop-distance / take-profit-distance rejects (``minvalue``) follow
    the pattern ``error.invalid.stoploss.minvalue: 0.00050`` — the
    trailing value is the dynamic *distance* minimum. Maxvalue rejects
    (``maxvalue``) use the same shape but the trailing value is an
    *absolute price level* the bracket level cannot cross. The parser
    is identical for both — the *meaning* of the value differs by
    error code, which the typed exception captures.

    Returns ``0.0`` on parse failure so the caller gets a defined
    value without a raise — the typed exception is already the
    signal; the numeric payload is advisory.
    """
    message = str(exc)
    _, _, tail = message.rpartition(':')
    try:
        return float(tail.strip())
    except ValueError:
        return 0.0


def _extract_jwt_expiry(token: str | None) -> float | None:
    """Extract the ``exp`` claim from a JWT, or ``None`` if not parseable.

    Capital.com session tokens (``CST`` / ``X-SECURITY-TOKEN``) MAY be
    JWTs carrying their lifetime in the payload; the format is
    ``header.payload.signature`` where each segment is URL-safe base64
    without padding, and the ``exp`` claim is unix seconds.

    Returns ``None`` when the token is empty, not three dot-separated
    segments, the payload is not valid base64+JSON, or no numeric
    ``exp`` claim is present — the caller then falls back to a
    hardcoded refresh interval.
    """
    if not token:
        return None
    parts = token.split('.')
    if len(parts) != 3:
        return None
    payload_b64 = parts[1]
    payload_b64 += '=' * (-len(payload_b64) % 4)
    try:
        payload_bytes = urlsafe_b64decode(payload_b64)
        payload = json.loads(payload_bytes)
    except (ValueError, json.JSONDecodeError):
        return None
    exp = payload.get('exp') if isinstance(payload, dict) else None
    if isinstance(exp, (int, float)) and not isinstance(exp, bool):
        return float(exp)
    return None


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


def _position_legs_from_payload(
        payload: Mapping, symbol: str,
) -> list[PositionLeg]:
    """Parse a ``/positions`` payload into symbol-scoped :class:`PositionLeg`\\ s.

    Shared by ``fetch_raw_positions`` (fresh GET) and the startup
    adoption pass (already-fetched snapshot): one leg per Capital.com
    position row on ``symbol`` with ZERO aggregation. Rows on any other
    epic are dropped — the endpoint is account-wide but a run trades ONE
    epic, and a foreign instrument's leg must never reach a store-row
    seeding path. Sorted by ``(open_time, leg_id)`` so the FIFO close
    order is deterministic and replay-stable across polls
    (``createdDateUTC`` has millisecond resolution; the dealId tiebreak
    covers same-millisecond fills).
    """
    legs: list[PositionLeg] = []
    for row in payload.get('positions') or []:
        market = row.get('market') or {}
        if market.get('epic') != symbol:
            continue
        position = row.get('position') or {}
        direction = (position.get('direction') or '').upper()
        if direction not in ('BUY', 'SELL'):
            continue
        deal_id = position.get('dealId')
        if not deal_id:
            continue
        legs.append(PositionLeg(
            leg_id=str(deal_id),
            # Post-filter the epic IS ``symbol``; stamping the leg from the
            # wire value (not the parameter) keeps the core seeding
            # helper's foreign-symbol guard armed should the filter above
            # ever regress.
            symbol=str(market.get('epic')),
            side='buy' if direction == 'BUY' else 'sell',
            qty=float(position.get('size', 0.0)),
            entry_price=float(position.get('level', 0.0)),
            open_time=_parse_iso_timestamp(
                position.get('createdDateUTC') or '',
            ),
            unrealized_pnl=float(position.get('upl', 0.0)),
        ))
    legs.sort(key=lambda leg: (leg.open_time, leg.leg_id))
    return legs


def _size_from_units(units: int, lot_step: float) -> float:
    """Convert a lot-step unit count back to a JSON-safe ``size`` value.

    ``units * lot_step`` in binary floats produces artifacts
    (``7 * 0.01 == 0.07000000000000001``) that would be serialized
    verbatim into the request body. Rounding to the lot step's own
    decimal precision restores the exact grid value.
    """
    exponent = Decimal(str(lot_step)).normalize().as_tuple().exponent
    decimals = max(0, -int(exponent))
    return round(units * lot_step, decimals)
