"""Configuration dataclass for the Capital.com plugin.

Covers both the data-ingest and the order-execution side — one credential
block serves both. The broker-only tunable ``poll_interval_seconds`` is
inert when the plugin is used for data ingest only.
"""
from dataclasses import dataclass

from pynecore.core.plugin import LiveProviderConfig


@dataclass
class CapitalComConfig(LiveProviderConfig):
    """Capital.com plugin configuration.

    Covers both the data-ingest and the order-execution side — one
    credential block serves both. The broker-only tunable
    ``poll_interval_seconds`` is inert when the plugin is used for data
    ingest only.
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

    instrument_rules_ttl_seconds: float = 300.0
    """Seconds before a cached :class:`_InstrumentRules` entry must be
    re-fetched.

    Capital.com widens ``minNormalStopOrLimitDistance`` during volatile
    sessions; an indefinitely-cached value would silently disable the
    pre-check. Five minutes is conservative — it bounds the staleness to
    one news-event window without flooding the rate-limited markets endpoint.
    """

    ws_volume_baseline_bars: int = 20
    """How many recent full-coverage WS quote counts to remember as the
    rolling baseline for the volume sanity checks.

    The baseline drives two decisions: the low-ratio outlier trigger
    (``ws_volume < median * ws_volume_low_ratio`` → REST fallback) and the
    median-estimate emitted when REST also fails. A 20-bar window keeps
    the median responsive to session-level liquidity changes without being
    swayed by a single quiet bar.
    """

    ws_volume_min_baseline_bars: int = 5
    """Minimum baseline samples required before low-ratio and median-estimate
    paths activate.

    During the warmup window the worker emits raw WS counts even when they
    look suspicious (no reliable median yet). REST still fires for the
    ``ws_volume == 0`` and partial-coverage cases — those do not depend on
    the baseline.
    """

    ws_volume_low_ratio: float = 0.20
    """Multiplier applied to the rolling median when deciding whether a
    full-coverage WS volume is low enough to warrant a REST sanity check.

    With the default 0.20 a bar must come in below 20% of the median to
    trigger REST. Tighter (smaller) values silence the trigger; looser
    (larger) values burn REST budget on normal lulls.
    """

    ws_volume_bad_bar_reconnect_threshold: int = 2
    """Consecutive REST-confirmed bad full-coverage bars after which the
    worker forces a WS reconnect.

    "Bad" means REST returned a real volume that disagreed materially with
    the WS count (zero or low-ratio). Two in a row implies the quote feed
    has gone silent while the OHLC subscription still ticks — the same
    failure mode the OHLC watchdog handles for the other direction.
    Partial-coverage bars never count toward this streak.
    """
