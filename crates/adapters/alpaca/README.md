# Alpaca US equities

The adapter exposes US equity instruments with IDs such as `SPY.ALPACA`.
Register the two native factories with a `LiveNode`. Data and execution
configs default to the Alpaca Paper environment. Set `AlpacaEnvironment::Live`
explicitly and provide the matching credentials for a live deployment.

| Mode | API key | API secret |
| --- | --- | --- |
| Paper | `ALPACA_PAPER_API_KEY` | `ALPACA_PAPER_API_SECRET` |
| Live | `ALPACA_LIVE_API_KEY` | `ALPACA_LIVE_API_SECRET` |

## Market data

Choose `AlpacaStockFeed::Sip` (default) or `Iex`. SIP covers pre-market,
regular and post-market trading. Enable `AlpacaOvernightFeed::Boats` for
overnight quotes, trades and minute bars. Historical quotes, trades and minute
bars include both configured sessions when BOATS is enabled. Historical
requests can set `params["feed"]` to `"sip"`, `"iex"` or `"boats"`.
External live bars use one-minute LAST aggregation. Historical bars retain the
Alpaca opening timestamp as `ts_event` and use the bar-window close for
`ts_init`. Historical quotes and trades use the event timestamp for both fields.
`load_symbols` restricts startup instrument discovery to the given stocks.

Market data credentials default to the trading environment's credentials.
Set `market_data_api_key` and `market_data_api_secret` together when the
data entitlement is held by a different Alpaca account. This leaves order
routing and instrument discovery in the configured trading environment.

## Orders

The adapter handles whole-share orders. Ordinary market and limit orders
use the regular trading session. Set the order parameter
`extended_hours: true` on a DAY or GTC limit order to make it eligible for
pre-market, after-hours and eligible overnight execution.

The adapter maps native StopMarket, StopLimit, TrailingStopMarket, and
supported OTO/OCO/bracket order lists. Trailing offsets can use absolute
price or basis points. Auction and immediate-fill instructions use the
Nautilus AtTheOpen, AtTheClose, IOC and FOK time-in-force values subject to
Alpaca account eligibility. Alpaca documents Algo Trader Plus as a data
subscription. The Elite Smart Router is a separate product.

See [order types and time-in-force](https://docs.alpaca.markets/us/docs/orders-at-alpaca)
and [24/5 Trading](https://docs.alpaca.markets/us/docs/245-trading-for-trading-api)
for the broker's order and session restrictions.

## Paper smoke test

Run with a Paper API key and secret. The example uses SIP and BOATS by default.
Set both `ALPACA_MARKET_DATA_API_KEY` and `ALPACA_MARKET_DATA_API_SECRET`
to use a separate data subscription.

```sh
cargo run -p nautilus-alpaca --features examples --example alpaca-paper-smoke
cargo run -p nautilus-alpaca --features examples --example alpaca-paper-smoke -- --history 2026-11-16
```

The first command checks account connection and streams SPY quotes, trades and
bars. The second requests one trading day across all four sessions. Use
`--regular-limit PRICE` or `--extended-limit PRICE` for a one-share
submit-and-cancel test, or `--market-round-trip` during regular hours for a
one-share paper round trip. `--extended-round-trip` uses marketable extended
limits, and `--advanced-checks` submits and cancels the stop, stop-limit,
trailing-stop and bracket smoke orders. Set `ALPACA_SMOKE_PROXY_URL` when
the connection requires an HTTP CONNECT proxy. The example always uses the
Paper trading endpoint for trading.
