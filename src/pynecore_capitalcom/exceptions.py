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
