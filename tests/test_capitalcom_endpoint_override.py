"""Per-process endpoint overrides must win over the config file.

A proxy placed in front of the venue for one bot process (the live lab's
chaos cycles) cannot edit the plugin config shared by every other process on
the machine, so the environment has to take precedence over the config
override, which in turn wins over the venue hosts.
"""

import asyncio

import httpx

from pynecore_capitalcom import CapitalCom, CapitalComConfig
from pynecore_capitalcom.helpers import URL, URL_DEMO, WS_URL, rest_url, ws_url


def __test_override_order_is_environment_then_config_then_venue__(monkeypatch):
    monkeypatch.delenv("PYNE_CAPITALCOM_REST_URL", raising=False)
    monkeypatch.delenv("PYNE_CAPITALCOM_WS_URL", raising=False)
    assert rest_url(True) == URL_DEMO
    assert rest_url(False) == URL
    assert rest_url(True, "https://proxy.example/") == "https://proxy.example"
    assert ws_url() == WS_URL
    assert ws_url("wss://proxy.example/connect") == "wss://proxy.example/connect"
    monkeypatch.setenv("PYNE_CAPITALCOM_REST_URL", "https://localhost:47011")
    monkeypatch.setenv("PYNE_CAPITALCOM_WS_URL", "wss://localhost:47012/connect")
    assert rest_url(True, "https://proxy.example") == "https://localhost:47011"
    assert ws_url("wss://proxy.example/connect") == "wss://localhost:47012/connect"


def __test_rest_calls_dial_the_override__(monkeypatch):
    monkeypatch.setenv("PYNE_CAPITALCOM_REST_URL", "https://localhost:47011")
    broker = CapitalCom(config=CapitalComConfig(
        demo=True, user_email="u@example.com", api_key="k", api_password="p"))
    dialed: list[str] = []

    def fake_get(url, **_kwargs):
        dialed.append(url)
        raise httpx.ConnectError("refused")

    monkeypatch.setattr(httpx, "get", fake_get)
    try:
        asyncio.run(broker._call("positions", method="get"))
    except Exception:  # the mapped connection error is not the point here
        pass
    assert dialed and dialed[0].startswith("https://localhost:47011/api/v1/")
