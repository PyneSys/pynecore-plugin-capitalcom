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

Capital.com limits `POST /session` to one request per second per API key.
Concurrent broker/provider instances in one process therefore share a keyed
bootstrap gate. Session POST starts are spaced by at least one second, and a
bootstrap throttle receives one bounded retry. Existing tokens stay visible
while a replacement session is created, so an active WebSocket is never sent
an empty credential pair.

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

**Use a one-way (netting) account** — switch hedging mode off in the
Capital.com account settings before you start a bot. Pine strategies are
one-way, and on a netting account the venue itself nets every order against
the open position, so partial closes, resting opposite-side orders and
protection all behave as the script expects. A hedging account works for
strategies that only open and fully close positions, with the limits below.

- **Hedging mode** is detected at connect time (`GET /accounts/preferences`)
  and handled transparently: closes, reversals, and brackets are decomposed
  per position row by the core one-way emulator.
- **A partial close on a hedging account closes the whole position row.**
  `DELETE /positions/{dealId}` is full-row only (it ignores any `size`
  parameter), so a close that asks for part of a row — a fractional
  `strategy.close(qty=...)`, a partial `strategy.exit`, an opposite order
  smaller than the position — closes that row in full and logs a warning
  with the asked and the closed quantity. The position ends up smaller
  than the script asked for, never larger. To scale out in steps on a
  hedging account, enter in several smaller orders (each becomes its own
  row) and close whole rows. Use a one-way (netting) account for exact
  partial closes — there the plugin emulates them via opposite-direction
  orders.
- **Opposite positions on a hedging account stay open side by side.** A
  position you open by hand against the bot's, or a resting limit / stop
  order on the opposite side that fills, becomes its own row instead of
  reducing the existing one. While such a row partly offsets a position,
  that position carries no stop-loss / take-profit (a whole-row bracket
  would close more than the net exposure); the bot logs an error once and
  keeps running. Neither happens on a netting account.
- **Deferred trailing activation** (Pine `trail_price` + `trail_points`)
  is only available on netting accounts; on a hedging account the trailing
  stop arms immediately at the given offset (logged as a warning).

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
