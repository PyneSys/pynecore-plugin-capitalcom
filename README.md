# PyneCore Capital.com Plugin

[Capital.com](https://capital.com) integration for
[PyneCore](https://pynesys.io): historical and live market data plus live
order execution over the public REST v1 + WebSocket API.

## Status

Both the **data provider** (`LiveProviderPlugin`) and **live order execution**
(`BrokerPlugin`) are implemented: session-based authentication, historical
plus live OHLCV, and position-based order routing with server-side stop-loss /
take-profit / trailing stop.

## Demo first

Capital.com has a full demo environment
(`demo-api-capital.backend-capital.com`) — run your strategy there before
risking funds. Set `demo = true` and the plugin talks to the demo host; the
same API key works on both hosts, and the host you authenticate against
decides which account list (demo or live) you see. Note two demo-side
differences: fills are effectively deterministic at the requested price
(slippage and partial fills are simulated away), and the hourly order-POST
rate limit is tighter than on live.

## Configuration

Settings live in `workdir/config/plugins/capitalcom.toml`, auto-generated
from [`CapitalComConfig`](src/pynecore_capitalcom/config.py) on first run.
One credential block serves both market data and order execution:

```toml
demo = false           # demo host vs. live real funds
user_email = ""        # your Capital.com account email
api_key = ""           # API key from Settings -> API integrations
api_password = ""      # the password you set when generating the key
```

Generating the key: enable two-factor authentication on your account first
(Capital.com requires 2FA before API key generation), then create the key
under **Settings -> API integrations** on the platform; the API password is
set there as part of key creation. The plugin encrypts the password with
the server-provided RSA key at login and keeps the session tokens
refreshed proactively.

## Symbols

Capital.com identifies instruments by **epic** (e.g. `EURUSD`, `GOLD`) —
use the epic directly in the provider string:

```
capitalcom:EURUSD@1
```

Pine scripts written with TradingView-style symbols keep working through
the optional `symbol_map` translation table in the config:

```toml
[symbol_map]
"FX:EURUSD" = "EURUSD"
"OANDA:XAUUSD" = "GOLD"
```

## Architecture

- **Transport**: REST v1 (`api-capital.backend-capital.com`, or the demo
  host with `demo = true`) for account, orders, and history; WebSocket for
  live quotes and OHLC updates. Execution events are polled from REST
  (`/positions`, `/workingorders`, `/history/activity`) — Capital.com has
  no order-event WebSocket channel.
- **Authentication**: API key + email + API password. The plugin encrypts
  the password with the server-provided RSA key and keeps the session
  tokens refreshed proactively.
- **Order model**: position-based (`dealId` rows) with server-side
  stop-loss / take-profit / trailing stop as position attributes. One-way
  (netting) accounts use the direct execution path; hedging-mode accounts
  run through PyneCore's one-way emulation layer, so Pine one-way
  semantics hold on both.

## Account-mode notes

- **Hedging mode** is detected at connect time (`GET /accounts/preferences`)
  and handled transparently: closes, reversals, and brackets are decomposed
  per position row by the core one-way emulator.
- **Partial closes on a hedging account are not supported**:
  `DELETE /positions/{dealId}` is full-row only (it ignores any `size`
  parameter), so a fractional `strategy.close(qty=...)` becomes a loud,
  non-halting skip. Use a one-way (netting) account for partial closes —
  there the plugin emulates them via opposite-direction orders.
- **Deferred trailing activation** (Pine `trail_price` + `trail_points`)
  is only available on netting accounts; on a hedging account the trailing
  stop arms immediately at the given offset (logged as a warning).

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
