"""
@pyne

The plugin passes the PyneCore broker plugin contract probe: the checks a live
run makes at startup, and the port probe the order sync engine makes once the
plugin has wired its ports.
"""
from pynecore.core.broker.validation import validate_plugin_contract, validate_plugin_ports

from pynecore_capitalcom import CapitalCom, CapitalComConfig


def main():
    """
    Dummy main function to be a valid Pyne script
    """
    pass


def __test_helper_plugin() -> CapitalCom:
    return CapitalCom(config=CapitalComConfig(
        demo=True, user_email="test@example.com", api_key="test-key",
        api_password="test-password"))


def __test_plugin_passes_the_contract_probe__():
    errors, _warnings = validate_plugin_contract(__test_helper_plugin())
    assert errors == []


def __test_hedging_account_port_passes_the_port_probe__():
    # A hedging-mode account opts into core one-way emulation with the plugin
    # itself as the port (the account-mode probe in ``connect()`` wires it).
    plugin = __test_helper_plugin()
    plugin.position_port = plugin
    assert validate_plugin_ports(plugin) == []
