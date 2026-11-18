# Alpaca US equities and options

The adapter exposes US equity instruments with IDs such as `SPY.ALPACA`.
Register the two native factories with a `LiveNode`. Data and execution
configs default to the Alpaca Paper environment. Set `AlpacaEnvironment::Live`
explicitly and provide the matching credentials for a live deployment.

The Python wheel exposes the same native factories, configs and environment
enum from `nautilus_trader.adapters.alpaca`. Python uses
`AlpacaEnvironment.PAPER` and `AlpacaEnvironment.LIVE`.

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

### Option market data

Set `option_feed` to `AlpacaOptionFeed::Opra` in Rust or
`AlpacaOptionFeed.OPRA` in Python to enable real-time option quotes and
trades. OPRA requires an Alpaca data entitlement and its signed agreement.
`Indicative` is available as an explicit alternative. Option subscriptions
use their own connection and the complete OCC symbol with venue `ALPACA`.
Set `load_option_symbols` to publish selected contracts at startup.

Call `request_instrument` for one contract or `request_instruments` with
`params["option_underlying"]` for a paginated chain. When a catalog is
configured, set `params["force_instrument_update"]` to `true` to route
the request to Alpaca. Set
`expiration_date_gte` and `expiration_date_lte` as `YYYY-MM-DD` strings to
bound discovery. Alpaca limits the default discovery window to the upcoming
weekend. Use `SPX` as the underlying to discover both SPX and SPXW contracts
or pass `SPXW` to select just that root. Discovery covers standard
100-share equity and ETF contracts and 100-multiplier SPX and SPXW index
contracts.

SPX and SPXW both use the SPX underlying and European cash settlement.
SPX has AM settlement and SPXW has PM settlement. Alpaca supplies an
expiration date, so Nautilus uses 09:30 or 16:00 New York time as a
nominal expiry timestamp. Consult the broker calendar and
contract rules for the actual last tradable time and settlement, including
early closes. SPX index spot data requires a separate provider.

The option adapter currently provides definitions and streaming quote and
trade ticks. Equity order workflows, auction instructions, overnight
execution and historical data requests remain equity features.

For a read-only check of Paper instrument definitions and OPRA ticks, pass
one or more active OCC contract symbols. Tick counts depend on the contracts
and time of day.

```sh
cargo run -p nautilus-alpaca --features examples --example alpaca-option-data -- OCC_SYMBOL
```

See [Alpaca Index Options](https://docs.alpaca.markets/us/docs/index-options)
and [real-time option data](https://docs.alpaca.markets/us/docs/real-time-option-data).

## Orders

The adapter handles whole-share orders. Ordinary market and limit orders
use the regular trading session. Set the order parameter
`extended_hours: true` on a DAY or GTC limit order to make it eligible for
pre-market, after-hours and eligible overnight execution.

The adapter maps native StopMarket, StopLimit, TrailingStopMarket, and
supported OTO/OCO/bracket order lists. Trailing offsets can use absolute
price or basis points. Opening auction orders (MOO and LOO) use a Market or
Limit order with `AtTheOpen` (`opg`). Closing auction orders (MOC and LOC)
use `AtTheClose` (`cls`). Immediate-fill orders use `Ioc` or `Fok`.
These time-in-force values depend on Alpaca account eligibility.

See [order types and time-in-force](https://docs.alpaca.markets/us/docs/orders-at-alpaca)
and [24/5 Trading](https://docs.alpaca.markets/us/docs/245-trading-for-trading-api)
for the broker's order and session restrictions.

## Elite Smart Router

Select Elite Smart Router for your Alpaca trading account before using
VWAP, TWAP, or direct market access (DMA). Alpaca routes DMA orders through
DASH Financial Technologies to the specified exchange. See
[Alpaca Elite](https://alpaca.markets/elite) and the
[advanced orders guide](https://docs.alpaca.markets/us/docs/alpaca-elite-smart-router).

Pass `params["advanced_instructions"]` on a single whole-share US equity
Market or Limit order with DAY time in force. For example, submit an
existing DAY Limit order from a Python strategy with a VWAP participation
cap of 12.3%.

```python
self.submit_order(
    order,
    params={"advanced_instructions": {"algorithm": "VWAP", "max_percentage": "0.123"}},
)
```

- For VWAP and TWAP, set `algorithm` to `"VWAP"` or `"TWAP"`. Optional
  `start_time` and `end_time` are RFC 3339 timestamps within the same
  regular session. Set `max_percentage` to a decimal fraction between
  0 and 1, exclusive, with at most three decimal places.
- For DMA, set `algorithm` to `"DMA"` and `destination` to `"NYSE"`,
  `"NASDAQ"`, or `"ARCA"`. Pass optional `display_qty` inside
  `advanced_instructions` in increments of 100 shares. NASDAQ and ARCA
  accept the separate `extended_hours: true` parameter on DAY Limit orders
  for their pre-market and post-market sessions.

For VWAP or TWAP modifications, pass the complete instruction object to
`modify_order` together with a quantity or limit-price change. Omit the
object to preserve the existing instructions during a replacement. To
change a DMA order, cancel it and submit a new order.

Paper accepts the instruction field for submission and rejection checks.
Verification of algorithmic fills and exchange routing requires live execution.
Alpaca lists PVOL and Midpoint Peg as upcoming on its
[Elite feature table](https://alpaca.markets/elite).

An explicit VWAP or TWAP replacement must match the recorded original
algorithm. For restored orders, that record can be recovered from the
instruction object in the broker response when available.

The adapter reports zero commission on fills. Use broker-recorded fees
when calculating net P&L for Elite live trades. See
[Alpaca account activities](https://docs.alpaca.markets/us/docs/account-activities).

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
submit-and-cancel test. During regular hours, use `--market-round-trip` or
`--limit-round-trip` for a one-share buy and sell using DAY orders. Run
`--extended-round-trip` in the pre-market, after-hours, or overnight session
to test marketable DAY limit orders with extended hours enabled. Begin each
round trip with a flat SPY position and verify the terminal fills and flat
position in Paper afterward.

To check carryover, use `--hold-limit DAY PRICE` during the overnight session
or `--hold-limit GTC PRICE` on the first trading date. Choose a resting buy
price below the current bid. The smoke strategy leaves one share open after
acceptance and prints the broker order UUID. Run `--verify-carryover DAY UUID`
in the following pre-market or `--verify-carryover GTC UUID` on a later trading
date. This checks the same unfilled order and confirms it cancels. Use
`--cancel-carryover UUID` to cancel the test order early.

`--advanced-checks` submits and cancels the stop, stop-limit, trailing-stop
and bracket smoke orders. `--suite PRICE` runs the regular limit, extended
round trip and advanced checks over one connection. Set `ALPACA_SMOKE_PROXY_URL`
when the connection requires an HTTP CONNECT proxy. The example uses the
Paper trading endpoint for all order operations.

### Paper auction checks

Build the same example, then run the check for the corresponding auction during
the 30 minutes before it, with time to spare before Alpaca's 09:28 ET opening
cutoff or 15:50 ET closing cutoff. The runner checks the Alpaca calendar and
requires a full trading day and a flat AAPL position with no AAPL orders.

```sh
cargo build -p nautilus-alpaca --features examples --example alpaca-paper-smoke
python3 crates/adapters/alpaca/examples/paper_auction_check.py open
python3 crates/adapters/alpaca/examples/paper_auction_check.py close
```

Each run submits one-share AAPL market, marketable limit and resting limit orders
through the native strategy. It checks the native terminal events and Paper order
records, then closes the test fills. A manual cancellation does not pass the
resting-order check. Both phases use Paper execution and verify simulated fills.
