"""Configuration dataclass for the Capital.com plugin.

Covers both the data-ingest and the order-execution side — one credential
block serves both. Broker-only tunables (``poll_interval_seconds``,
``require_one_way_mode``) are inert when the plugin is used for data ingest
only.
"""
from dataclasses import dataclass


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

    instrument_rules_ttl_seconds: float = 300.0
    """Seconds before a cached :class:`_InstrumentRules` entry must be
    re-fetched.

    Capital.com widens ``minNormalStopOrLimitDistance`` during volatile
    sessions; an indefinitely-cached value would silently disable the
    pre-check. Five minutes is conservative — it bounds the staleness to
    one news-event window without flooding the rate-limited markets endpoint.
    """

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
