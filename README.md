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
- **Closing over an entry whose fill never arrived** (PyneCore's
  `held_execution_close`, `supports_held_execution_close = True`) works on
  both account modes. When an entry's execution evidence never reaches the
  bot and the strategy then closes or reverses, PyneCore reads the venue
  position and asks the plugin to close it; the quarantine ends once the bot
  has recovered the entry's execution from the venue (see below). The deal
  the held entry's order opened counts as the bot's own,
  identified by its `dealId` or `workingOrderId` naming that order. On a
  netting account the plugin closes whole own deals with
  `DELETE /positions/{dealId}`, oldest first, never more than the position
  PyneCore read; a deal that does not fit whole stays open (with a warning)
  until PyneCore reads the venue again. It never sends an opposite-direction
  order for this close, so it cannot net someone else's deal or open the
  other side. On a hedging account the core one-way emulator closes the
  deals leg by leg. Deals other bots or you opened by hand are never closed.

## Recovering from its own trouble

The bot runs unattended. The entry blocks (`ENTRY_BLOCK` quarantines) below
end by themselves; a change someone else made to the bot's own orders or
positions (an external intervention) still waits for you.

- **Reading an order's execution again** (`fetch_order_execution`): when an
  entry's fill never reached PyneCore, PyneCore asks the plugin, which reads
  the last 24 hours of `/history/activity` and answers with the deal's
  opening (the fill the activity poll would have booked, under the same fill
  id, so it is never booked twice). A resting order the venue ended without
  a fill is reported as unexecuted. Capital.com reports no commission on
  these records, so such fills are booked with the commission marked
  unreported. An order the history does not show is never taken as absent:
  PyneCore asks again.
- **A taken-over entry whose deal shows another size than it sent** (after a
  restart, or a parked submission resolved late): the deal counts as the
  bot's own position at once. The bot proves the fill from the deal's opening
  in the activity history, books it, then books the deal's later closes in
  order; for a deal opened more than 24 hours ago it books the size the venue
  shows once that size stays unchanged for two minutes. Until then no close
  or netting order is sent to that deal.
- **A deal the bot's own resting order opened while its execution was taken
  as netting someone else's deal** is in no book: the bot closes it itself
  with `DELETE /positions/{dealId}` and lifts the entry block once the venue
  no longer lists it. It sends nothing while an external-intervention
  quarantine stands.
- **An entry sent just before the process ended, whose outcome it never
  learned.** Capital.com echoes no client order id, so after the restart the
  bot traces the entry in the activity history: a deal of the same epic,
  direction, size and kind, opened within 3 seconds of the send (the venue
  clock's offset, measured on the bot's own deals, applied). Until the trace
  settles, only that entry is held: it is never sent a second time, and the
  bot keeps running. Entries that share a deal are settled together: each
  deal goes to its own entry in a fixed order (earliest entry, earliest
  deal; a resting order's listed level must match), as nothing else on the
  venue tells them apart. An entry left without a deal was not sent, and
  PyneCore may send it again if the strategy still wants it. More matching
  deals than entries mean someone else trades the epic on this account, and
  which deal is the bot's cannot be proven: that is an external intervention,
  which waits for you. Close the deals that are not the bot's, then resume:
  the bot takes the earliest open matching deals as its own, one per entry,
  and leaves the rest as someone else's. An entry older than the 24-hour
  activity history is judged the same way from what the venue still lists
  open. An entry whose deal has closed since never reaches the bot's stream
  again: the bot hands it to PyneCore, which reads the deal's opening from
  the activity history, books it, and takes the venue's flat position as
  the close. New entries wait until the book matches the venue; if the
  history no longer shows the opening, they keep waiting while PyneCore
  reads it again. A resting entry order that executed while the bot was down,
  into a position closed since, is handed over the same way at startup, and
  so is one that executed and closed between two polls of a running bot
  without the bot seeing its position: the order is not taken for a cancel.
  The same holds for a market entry the venue accepted without opening it at
  once, for a submission whose confirm landed on a deal no snapshot lists
  any more, and for a resting entry whose cancel finds the order already
  gone: the bot reads the activity history before it calls any of them
  unfilled, and never sends such an entry a second time. A resting entry
  that is gone while one open position names it as its order is booked at
  once from that position's size and level, so its exits protect it right
  away; the execution's activity read later books nothing again.
- **Someone else's deal on the bot's epic** is not an intervention: an exit
  it keeps blocked for longer than the latch bound blocks new entries only
  until the exit is free again, and the netting of such a deal by the bot's
  own resting order blocks them until that order's execution is settled.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
