"""Shared fixtures of the Capital.com plugin tests."""

import pytest

from pynecore_capitalcom import rest


@pytest.fixture(autouse=True)
def _unpaced_requests(monkeypatch):
    """Send mocked REST calls without the per-user request pacing.

    The pacing waits on the clock and keeps its slot file in the user's home
    directory; ``test_capitalcom_request_pacing.py`` covers it on its own.
    """
    monkeypatch.setattr(rest, '_pace_request', lambda _pace_key: None)
