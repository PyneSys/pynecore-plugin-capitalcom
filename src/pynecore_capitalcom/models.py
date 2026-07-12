"""Internal data models + bracket-leg id helpers + activity fingerprint.

Sits at the bottom of the dependency graph: depended on by every mix-in
file but does not import from any of them. Bundles the small dataclasses
the plugin uses internally (instrument-rules cache row, activity-poll
cursor) with the bracket-leg id helpers and the activity fingerprint —
all stateless, all required cross-mixin.
"""
import hashlib
from dataclasses import dataclass, field


@dataclass(frozen=True)
class _InstrumentRules:
    """Cached dealing rules for a single instrument.

    Capital.com returns these on ``GET /markets/{epic}`` as the
    ``dealingRules`` + ``instrument`` blocks. Four values are load-bearing
    for order sizing: ``lot_step`` (granularity of the ``size`` param),
    ``min_size`` / ``max_size`` (the smallest / largest accepted ``size``;
    ``maxDealSize`` rejects oversized orders with
    ``error.invalid.size.maxvalue``), and ``min_stop_or_limit_distance`` —
    the dynamic bracket-distance minimum; it applies symmetrically to both
    stop level and limit (take-profit) level, used to pre-filter
    obviously-rejectable bracket levels before the round-trip.

    The distance minimum is quoted with a unit: every market observed on
    the venue quotes ``minStopOrProfitDistance`` with ``PERCENTAGE``
    (e.g. BTCUSD 0.01, i.e. 0.01% of the price), while the IG-style
    ``minNormalStopOrLimitDistance`` — where present — carries a plain
    price distance (``POINTS``). ``min_stop_or_limit_distance`` stores the
    raw quoted value; :meth:`min_bracket_distance_at` converts it to a
    price-space distance against a reference price.

    ``max_size`` is ``0.0`` when the venue does not quote a ``maxDealSize``
    (or the field is unavailable); the execute path treats ``<= 0`` as
    "no ceiling known" and skips the pre-check.

    ``fetched_at`` is the epoch second the entry was cached at; the lookup
    re-fetches once it ages past
    :data:`pynecore_capitalcom.helpers._INSTRUMENT_RULES_TTL_S`. Capital.com
    widens the minimum during volatile sessions, so unbounded caching
    would silently drift out of date.
    """
    epic: str
    lot_step: float
    min_size: float
    min_stop_or_limit_distance: float
    fetched_at: float
    max_size: float = 0.0
    min_stop_or_limit_distance_unit: str = 'POINTS'

    def min_bracket_distance_at(self, reference_price: float) -> float:
        """Minimum SL/TP distance in price units at ``reference_price``.

        ``PERCENTAGE`` minimums are relative to the current quote, so the
        price-space threshold can only be computed against a concrete
        reference (the live mid the pre-check anchors on). Any other unit
        (``POINTS`` — observed to be plain price units on this venue, or a
        missing unit) is returned as-is.

        :param reference_price: Positive anchor price the distance is
            measured from.
        :return: Price-space minimum distance; ``0.0`` when the venue did
            not quote a usable minimum (pre-check no-op).
        """
        if self.min_stop_or_limit_distance_unit == 'PERCENTAGE':
            return reference_price * self.min_stop_or_limit_distance / 100.0
        return self.min_stop_or_limit_distance


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

    ``external_logged_fingerprints`` is a session-only set tracking
    activities that we observed *without* a matching bot-owned row. These
    are NOT dedup-cached (because the row may attach on a later poll once
    ``add_ref('deal_id', …)`` lands — a real race in the entry path
    between ``execute_entry``'s POST→/confirms and the watch_orders
    poller). The set just keeps the ``external_activity_ignored`` log
    one-shot per row within the session; truly external activities age
    out of Capital.com's 60-second rolling window without further noise.
    """
    last_date_utc: str | None = None
    seen_fingerprints: set[str] = field(default_factory=set)
    external_logged_fingerprints: set[str] = field(default_factory=set)


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
    rebuild the set from the BrokerStore. It is also surfaced as the
    ``OrderEvent.fill_id`` the sync engine's duplicate-fill gate keys on.

    The field set is FROZEN: it must not change without a stored-fingerprint
    migration. Adding or removing a field (e.g. ``epic`` / ``direction``)
    re-hashes every row, so a fill recorded before the change would no longer
    match its persisted fingerprint after a restart and would be applied a
    second time. ``dealId`` plus the timestamp already discriminate rows in
    practice; the extra fields only guard against same-instant collisions.
    """
    key = "|".join(str(activity.get(k, '')) for k in (
        'dateUTC', 'dealId', 'type', 'status', 'source', 'level', 'size',
    ))
    return hashlib.sha1(key.encode('utf-8')).hexdigest()[:16]
