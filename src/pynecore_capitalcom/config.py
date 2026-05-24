"""Configuration dataclass for the Capital.com plugin.

Covers both the data-ingest and the order-execution side — one credential
block serves both. Internal heuristics (poll cadence, cache TTLs, WS
volume sanity thresholds, …) live as module-level constants in
:mod:`pynecore_capitalcom.helpers`, deliberately NOT in this dataclass:
they have no user-facing reason to be touched, and exposing them as
config fields balloons the user TOML with knobs the user does not
understand.
"""
from dataclasses import dataclass

from pynecore.core.plugin import LiveProviderConfig


@dataclass
class CapitalComConfig(LiveProviderConfig):
    """Capital.com plugin configuration.

    Covers both the data-ingest and the order-execution side — one
    credential block serves both.
    """

    demo: bool = False
    """Use the demo account/host instead of live."""

    user_email: str = ""
    """Your Capital.com account email."""

    api_key: str = ""
    """API key from Capital.com settings."""

    api_password: str = ""
    """API password for authentication."""
