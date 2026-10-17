"""Capital.com–specific exception types.

Layered on top of :mod:`pynecore.core.broker.exceptions` so the risk layer
can pattern-match on type instead of parsing message strings. The four
``Invalid*`` types carry the numeric value the exchange returned (a
distance for ``minvalue``, an absolute price level for ``maxvalue``) so
callers can re-size or re-place without another round-trip.
"""
from pynecore.core.broker.exceptions import ExchangeOrderRejectedError
from pynecore.core.plugin import ProviderError


class CapitalComError(ProviderError):
    """Base class for Capital.com REST / connection failures.

    Subclasses :class:`~pynecore.core.plugin.ProviderError` so credential,
    auth and connection problems surface as a clean one-line ``pyne data``
    error rather than a traceback, while staying catchable as
    ``CapitalComError`` on the broker and risk paths.
    """


class CapitalComTransportError(CapitalComError):
    """The request never reached a Capital.com API verdict.

    A 5xx from the venue's gateway or a body that is not JSON (the nginx
    ``504 Gateway Time-out`` page, a CDN interstitial) — the transport
    failed between the client and the API, nothing about the request was
    judged, and the same call can succeed a moment later. Retryable on the
    provider path (``pyne run`` waits and re-downloads) and mapped to
    :class:`~pynecore.core.broker.exceptions.ExchangeConnectionError` on
    the broker path, where the sync engine parks the cycle and re-reads.
    """

    retryable: bool = True


class CapitalComApiError(CapitalComError):
    """The venue received the request and answered with an API error code.

    Raised only for a parsed error RESPONSE (``errorCode`` present), never
    for transport trouble or an unparseable body — so on a write path it is
    a DEFINITIVE refusal of that request: nothing landed on the venue. The
    dispatch layers rely on this to classify a synchronous entry reject
    (e.g. ``error.validation.stop.price``) as
    :class:`~pynecore.core.broker.exceptions.ExchangeOrderRejectedError`
    instead of an ambiguous disposition.

    :ivar error_code: The ``errorCode`` string the venue returned.
    """

    def __init__(self, message: str, *, error_code: str) -> None:
        super().__init__(message)
        self.error_code = error_code


class HistoricalPricesNotFoundError(CapitalComError):
    """The venue confirmed that a requested historical price range is empty."""


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


class InvalidStopMaxValueError(ExchangeOrderRejectedError):
    """Capital.com rejected an order because the requested stop level is
    on the wrong side of the maximum the instrument's current spread
    permits.

    Side-aware: a long position's SL must be strictly below the current
    bid, a short position's SL strictly above the current ask. When the
    requested level violates that bound the exchange returns
    ``error.invalid.stoploss.maxvalue: <X>`` where ``X`` is the boundary
    level (NOT a distance — the absolute price beyond which the SL
    cannot be placed).

    Distinct from :class:`InvalidStopDistanceError` (which carries a
    *distance* below the instrument's minimum) — semantically a
    different problem and a different correction strategy.

    :ivar max_value: The maximum allowed stop level (price, not
        distance) reported by Capital.com at rejection time.
    """

    def __init__(self, message: str, *, max_value: float) -> None:
        super().__init__(message)
        self.max_value = max_value


class InvalidTakeProfitMaxValueError(ExchangeOrderRejectedError):
    """Capital.com rejected an order because the requested take-profit
    level violates the maximum the instrument's current spread permits.

    Twin of :class:`InvalidStopMaxValueError` for the profit leg.

    :ivar max_value: The maximum allowed TP level (price, not distance)
        reported by Capital.com at rejection time.
    """

    def __init__(self, message: str, *, max_value: float) -> None:
        super().__init__(message)
        self.max_value = max_value


class ForeignDealBlocksWriteError(ExchangeOrderRejectedError):
    """A netting ``POST /positions`` was refused before it left: it could net a deal
    this run does not own.

    The reduce POST and an opposite market entry cannot name a ``dealId``: the
    netting account applies them FIFO to the oldest deal on the side they
    reduce. While a deal someone else opened sits on that side, the POST could
    consume it, so the plugin refuses it instead of touching another party's
    exposure. Raised inside the dispatch journal (the command row is marked
    rejected and stays as the audit trail) and turned into a non-halting skip at
    the ``execute_*`` boundary.

    :ivar reason: Stable token: ``'foreign_deal_blocks_partial_close'`` or
        ``'foreign_deal_blocks_opposite_entry'``.
    :ivar foreign_deals: The blocking deals, one dict per deal with ``deal_id``,
        ``direction``, ``size`` and ``created``.
    """

    def __init__(self, message: str, *, reason: str, foreign_deals: list[dict]) -> None:
        super().__init__(message)
        self.reason = reason
        self.foreign_deals = foreign_deals


class PositionSnapshotUnavailableError(ExchangeOrderRejectedError):
    """The ``GET /positions`` read a netting POST needs first did not complete.

    Nothing was sent: the POST is only issued after the snapshot proved it
    cannot net a foreign deal. Classified as a refusal so the entry is dropped
    for this bar and re-evaluated on the next one, never parked as ambiguous.
    """


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
