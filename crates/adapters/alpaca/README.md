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

Diagnostic output omits API credentials and custom HTTP, WebSocket and proxy URLs.
Connection and authentication errors report the failing operation and the available
status or broker code. Order rejections retain the broker's business reason, including
invalid prices, insufficient quantity and buying power.

## Market data

Choose `AlpacaStockFeed::Sip` (default) or `Iex`. SIP covers pre-market,
regular and post-market trading. Enable `AlpacaOvernightFeed::Boats` for
overnight quotes, trades and minute bars. Historical quotes, trades and minute
bars include both configured sessions when BOATS is enabled. Historical
requests can set `params["feed"]` to `"sip"`, `"iex"` or `"boats"`.
External live bars use one-minute LAST aggregation. Historical bars retain the
Alpaca opening timestamp as `ts_event` and use the bar-window close for
`ts_init`. Historical quotes and trades use the event timestamp for both fields.

Market data credentials default to the trading environment's credentials.
Set `market_data_api_key` and `market_data_api_secret` together when the
data entitlement is held by a different Alpaca account. This leaves order
routing and instrument discovery in the configured trading environment.

### Instrument definition refresh

The data client reads definitions from the Trading API at startup, after a
market data connection reauthenticates, and every 60 minutes. Set
`update_instruments_interval_mins` to change the interval. Zero disables
the timer and leaves reconnect and explicit refreshes active.

An empty `load_symbols` discovers all active US equities on each refresh.
Otherwise the client fetches the configured stocks by ID, along with
`load_option_symbols` and instruments with active definition, quote, trade
or bar subscriptions. Removing the last subscription drops the ID from future
refreshes unless configuration still includes it. An execution-only node reads
definitions for its positions and open orders during startup reconciliation.

For each instrument, the latest REST request start time wins. An unchanged
definition advances that time without sending another update. A first definition
subscription receives the current definition. An unbounded instrument request
publishes changes before returning the current definitions to its caller.
A request with a Nautilus `start` or `end` returns a separate snapshot.
Option expiry filters select contracts for that query without adding the chain
to periodic refreshes. A failed background lookup preserves the previous
definition. Explicit lookup failures reach the caller through the request error channel.

### Option market data

Set `option_feed` to `AlpacaOptionFeed::Opra` in Rust or
`AlpacaOptionFeed.OPRA` in Python to enable real-time option quotes and
trades. OPRA requires an Alpaca data entitlement and its signed agreement.
`Indicative` is available as an explicit alternative. Option subscriptions
use their own connection and the complete OCC symbol with venue `ALPACA`.
Set `load_option_symbols` to publish selected contracts at startup.

Use the standard history requests for option trades and externally aggregated
LAST bars on individual OCC contracts. A trade request can include both stock
and option contract IDs and returns one chronological batch. Request each bar
type separately. Option historical trades and bars use Alpaca's account data
entitlement and accept no feed override. For mixed stock and option trade
requests, leave the feed parameter unset. Real-time `option_feed` selects the
WebSocket feed. Alpaca currently has no supported historical option quote
endpoint in this adapter.

Query the Trading API contract directory by underlying to discover every active
root Alpaca returns for a date range. Each row retains the broker's OCC symbol,
underlying and root. The following Python request includes newly listed roots
and requires only trading credentials.

```python
from nautilus_trader.adapters.alpaca import AlpacaDataClientConfig, request_option_catalog

rows = await request_option_catalog(
    AlpacaDataClientConfig(),
    "SPX",
    expiration_date_gte="2026-11-18",
    expiration_date_lte="2026-12-18",
)
for root, session, status in sorted({
    (row["root_symbol"], row["expiration_session"] or "—", row["instrument_status"])
    for row in rows
}):
    print(root, session, status)
```

Supply `root="SPXO"` to filter this directory to one root. Without an explicit
expiry window, Alpaca limits the default query to the upcoming weekend. To see
per-root counts from Rust with the same Paper credentials:

```sh
cargo run -p nautilus-alpaca --features examples --example alpaca-option-data -- --catalog SPX 2026-12-16 2026-12-16
```

For strategy instruments, use `request_instrument` with an exact OCC contract ID
or `request_instruments` with `params["option_underlying"]`. Both the directory
and the instrument chain accept explicit expiry bounds. The chain also accepts
`params["root_symbol"]`. Existing `option_underlying="SPXO"` and `"SPXW"` calls
continue to select those roots. Set `params["force_instrument_update"]` to
`true` when a Nautilus catalog is configured and fresh broker definitions are needed.

The instrument chain includes standard equity and ETF contracts and index
contracts with verified specifications. A newly observed index root remains
visible in the directory with its broker fields and a null expiration session
until its settlement rules are known. A standard index contract without a
verified series is marked `unknown_series`. Contracts outside the supported
specifications are marked `unsupported_contract`. A `ready` row has a supported
instrument definition; the broker's `tradable` field is reported separately.
Group chain rows by underlying and root, keeping the complete OCC symbol for
subscriptions and broker reconciliation. For SPX, the directory exposes
`asset_class="index"`, `underlying_symbol="SPX"` and the optional display name
`underlying_name="S&P 500 Index"`.

For example, SPX, SPXO and SPXW share the index underlying `SPX`. SPX and
SPXO use AM settlement and SPXW uses PM settlement. The adapter uses 09:30
or 16:00 New York time as the nominal expiry for these known series. The
broker calendar and series rules determine the actual last trading time and
settlement, including early closes. SPX index spot data requires a separate
provider. For direct OPRA chain snapshots of SPXO, use
`/v1beta1/options/snapshots/SPXO`.

Options support instrument definitions and streaming quote and trade ticks.
Single-leg execution supports loaded standard equity and ETF option contracts.

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

### Single-leg options

Submit a single Market or Limit order with DAY time in force for a loaded
equity, ETF, or supported index option instrument. Index option orders
execute during regular hours. Quantity counts whole contracts. To change an option
order, cancel and resubmit.

`AlpacaExecutionClientConfig` defaults `infer_option_position_intent` to
`true`. For regular-hours Limit DAY orders, Alpaca determines opening or
closing from the account's positions when the order has no explicit
`position_intent` and is not reduce-only. A sell while flat may open a short
subject to the account's options permissions and collateral.

Set `reduce_only=true` for a close-only order. The adapter sends
`buy_to_close` or `sell_to_close` for the corresponding side. Market orders,
extended-hours orders, and orders submitted with inference disabled use an
explicit intent. Their ordinary default is `buy_to_open` for buys and
`sell_to_close` for sells. An opening sell on these paths uses
`params={"position_intent": "sell_to_open"}`.

The adapter routes cancellation by Alpaca order ID and reconciles single-leg
option orders and FILL activities using the complete OCC symbol. Position reports
also include option positions.

Set `extended_hours: true` on DAY Limit orders for broker-enabled equity and
ETF option contracts. Check the Paper asset's `has_options_extended_hours`
attribute and the current [Alpaca options session rules](https://docs.alpaca.markets/us/docs/equity-options-eth)
before testing this route. For regular-hours Paper smoke tests, choose an
active non-0DTE contract with a fresh two-sided OPRA quote and no existing
position or orders.

```sh
cargo run -p nautilus-alpaca --features examples --example alpaca-paper-smoke -- --option OCC_SYMBOL --limit-round-trip
```

Use `--market-round-trip` for DAY Market orders and `--regular-limit PRICE`
for a passive DAY Limit cancellation. The eligible morning session can use
`--extended-round-trip` or `--extended-limit PRICE`. Confirm both order
receipts and the final position in Paper after each round trip. For a
cash-secured put, use `--short-limit-round-trip` or
`--extended-short-round-trip` with `--option OCC_SYMBOL`. The smoke test caps
strike collateral at $2,000 and sends the closing buy with reduce-only set.

### Multi-leg option orders

Use the same generic spread ID as other Nautilus spread adapters. Load both
the leg contracts and the spread definition before submitting an order. For a
vertical call spread, build the ID with the long leg at ratio `+1` and the
short leg at ratio `-1`:

```python
from nautilus_trader.model import new_generic_spread_id

spread_id = new_generic_spread_id([(long_leg.id, 1), (short_leg.id, -1)])
```

Request the spread through the strategy's instrument request or pass the leg
OCC symbols and `str(spread_id.symbol)` in the Alpaca data config's
`load_option_symbols`. Subscribe to the legs for quotes.

Submit one Limit DAY order on `spread_id`. The adapter supports one lot with
two to four option legs on the same underlying and requires Alpaca options
Level 3. Set `extended_hours: true` in the submit parameters to include
Alpaca's options morning or curb session on eligible equity or ETF
underlyings. Alpaca checks underlying and account eligibility against its
current list. Index spreads use regular hours, and their legs must share
the same expiration and settlement session.

A non-reduce-only order opens the legs. Set
`reduce_only=True` and reverse the side on the same spread ID to close them.
For example, a Buy at 0.47 limits the debit to 0.47 for a long call spread.
A reduce-only Sell at 0.35 limits the credit to at least 0.35. The adapter
translates the Sell limit to Alpaca's signed cash-flow price of -0.35.

The adapter submits one Alpaca `mleg` order and cancels it by parent UUID.
It uses the parent net fill to update the combo order and the broker's leg
fills to update the option positions. On reconciliation the adapter reports
known parent orders from the local cache and reconciles positions by OCC
symbol. Orders submitted outside Nautilus have no cached combo identity.
Combo replacement and spread quote subscriptions are outside this initial
support. See [Alpaca multi-leg orders](https://docs.alpaca.markets/us/docs/options-level-3-trading)
and [index options](https://docs.alpaca.markets/us/docs/index-options).

## Execution reconciliation

Enable LiveNode startup reconciliation to restore Alpaca orders, executions,
and positions. The historical lookback bounds activity collection. Startup
also queries all working orders, including orders submitted before the lookback,
and retrieves missing order records for fills inside the window. After a successful
account positions query, startup reports a flat position for each previously
open or traded equity or option contract absent from the snapshot. This lets
Nautilus recover offline closing fills and their realized PnL on restart.

Order history uses the last broker order ID on each full page to continue, even
when orders share a submission timestamp. Alpaca accepts time bounds on the first
request. On subsequent pages the adapter stops at the original exclusive end time.

Alpaca uses a weighted average for intraday equity positions and compressed FIFO
for overnight holdings. Equity reports supply the broker average to establish a
position from flat. Existing positions retain their fill-based entry average
when Alpaca updates the cost basis overnight.

The execution client reauthenticates the private stream and catches up on order
updates and fills after a reconnect. If a fill references an order not yet visible
in REST, or a recovery read encounters a transient HTTP or transport error,
the client retries with capped exponential backoff from the original lookback.
It processes private-stream updates between attempts and permits new submissions
and modifications once reconciliation completes on the current connection.
WebSocket and REST fills share a trade
identifier derived from the broker order ID and cumulative filled quantity.

After a restart, the adapter uses the cached Bracket, OTO, or OCO order list to
identify active children that still lack a broker order ID. It requests the
parent once per group with `nested=true` and binds its legs to the original
strategy order IDs, including when the parent has already filled. Closed children
retain their cached broker IDs for late fills. An individual status query can
confirm a bound child's zero-fill cancellation using the order record alone.
If a known group's child cannot be identified, the child query returns an error.
The account snapshot returns confirmed orders and fills and marks its coverage incomplete.

An order placed outside Nautilus is imported by the execution engine as an
external order. To claim external orders at startup, configure
`StrategyConfig.external_order_instrument_ids`. Native Rust strategies return
the list from the `Strategy::external_order_instrument_ids` hook. Set
`filter_unclaimed_external_orders` to omit orders without a strategy claim.
Ownership of previously cached Nautilus orders is preserved.

Reload the original Nautilus order cache to recover a native multi-leg parent
after a restart. The parent uses the broker's net execution price and quantity.
Startup recovers leg positions from the broker's position snapshots. Individual
combo leg executions are not imported as independent orders. Without the
original Nautilus combo identity, an external multi-leg parent is left
unmanaged, while its exposure remains visible in the leg position snapshots.

## Account snapshots

The execution client reads `GET /v2/account` after subscribing to the private
stream, on `QueryAccount`, and during reconnect reconciliation. Order events
that can change cash or buying power also trigger a read. The first such event
starts a ten-second window. Additional events share the same read, including
events for orders placed outside Nautilus. If another event arrives during
the read, the next window starts when the read finishes. Transient errors retry
with backoff. An idle stream makes no periodic account requests.
Across all these paths, an older request cannot overwrite a newer published
snapshot. Use `QueryAccount` for an immediate read, including for changes
unrelated to orders.

Read `AccountState.info["equity"]`
for the broker's net account equity, including marked positions. The same
snapshot exposes `cash`, `buying_power`, `effective_buying_power`,
`regt_buying_power`, `daytrading_buying_power`, `non_marginable_buying_power`,
`options_buying_power`, `initial_margin`, `maintenance_margin`, and `multiplier`
as decimal strings when returned by Alpaca.

`AccountBalance.total` holds cash. `free` is cash capped by reported
non-marginable and effective buying power, with `buying_power` as a fallback
for the effective field. On a margin account with a cash debit, any positive
effective (or fallback), non-marginable, or options buying power sets `free`
to zero. The debit stays in `total` and in the signed difference `locked = total - free`.
Orders with zero local initial margin can then reach Alpaca for a product-specific
buying power check. With no reported capacity, or on a cash account, the debit
also stays in `free`. Use the venue fields for product-specific buying power.
For margin accounts, the reported initial and maintenance requirements appear
as one USD account-wide `MarginBalance` when both values are available.

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

## Historical option settlement

Run the read-only report for one Paper or Live account with a fixed RFC 3339
posting cutoff, for example:

```sh
cargo run --locked -p nautilus-alpaca --example alpaca-option-lifecycle -- paper 2026-09-21T00:00:00Z
```

The report retrieves all available `OPEXP`, `OPEXC`, `OPASN`, `OPTRD` and
`OPCSH` history through the cutoff. When removals exist it also pages historical
`FILL` activities, retaining only fills for those contracts. This is an on-demand
audit. It does not run on the order path or change account balances or positions.

* `closing_position` is the signed end-of-day contract quantity reconstructed from
  available fills and executed removals through the activity's New York business date.
  A missing supporting fill gives `null`. Transfers, adjustments and missing history
  require separate evidence. Partial exercises can leave a nonzero position.
* The report links an exercise or assignment to a delivery only when both executed
  records share a unique broker group, date and currency. For standard stock and ETF
  options it verifies the historical contract's deliverable, signed share count,
  strike and posted cash. Missing terms, missing groups and ambiguous legs remain
  unresolved.
* For SPX-family index options it matches the cash leg by full contract symbol and
  direction. When Alpaca supplies an SPX settlement reference in the cash narrative,
  it also recomputes intrinsic value using the OCC strike, right, multiplier and removed
  contracts. With only a generic cash posting, the reference-based amount is `null`.
* Cash totals use each executed activity ID once, including unmatched deliverables.
  The JSON contains the evidence rows, their broker IDs and unmatched delivery IDs.
  Reports are replacement snapshots scoped to one account and environment. Rebuild
  after late postings and compare or replace overlapping snapshots without adding
  their totals together.

## Fee reconciliation

Use the Rust `execution::fees::request_fee_report` helper to read posted
Trading API `FEE` activities. The report returns one row per broker fee
activity ID and signed totals per currency. Fees with verified order and execution
IDs link to the corresponding broker fill activity, including option leg
fills. The report derives the Nautilus trade ID from each REST fill. A combo
leg can emit one aggregate event for several broker executions. Attribute
fees to the broker fills and roll them up once when reporting the combo.
Fees without either ID stay at the account level. A fee with a reference
that cannot be verified still contributes to the overall per-currency total.

Supply the fee creation-time window and a fill start time early enough to
include executions whose fees posted later. Use the activity ID to deduplicate
overlapping reports when persisting results. Apply these fees to gross P&L
reconstructed from fills. Broker account balances already include posted fees.
The execution client continues to report zero commission on live and recovered fills.
See [Alpaca account activities](https://docs.alpaca.markets/us/docs/account-activities).

To print a read-only summary with the standard Paper credentials, run:

```sh
cargo run --locked -p nautilus-alpaca --example alpaca-fee-report -- \
    paper 2026-11-20T00:00:00Z 2026-11-26T00:00:00Z 2026-11-17T00:00:00Z
```

Use `live` to read the production account with the standard Live credentials.
The example prints aggregate amounts and counts.

### Incremental fee synchronization

Use `--sync START [INTERVAL_SECONDS]` to receive posted fees as they arrive.
The example writes one JSON object to stdout per completed poll, including the
Paper or Live environment, broker account ID, query upper bound (`through`),
and new or revised fee rows (`fees`).

```sh
cargo run --locked -p nautilus-alpaca --example alpaca-fee-report -- \
    paper --sync 2026-11-20T00:00:00Z
```

The example runs once by default. Append an interval in seconds (for example,
`900`) to keep polling. To use the native Rust execution client, set
`AlpacaExecutionClientConfig.fee_sync` to `AlpacaFeeSyncSettings { start, interval }`.
Subscribe to `data.AlpacaFeeSyncUpdate` before connecting to receive the
initial batch. The client publishes each subsequent successful poll, including
empty batches, and stops the worker on disconnect.

The first poll reads fees from `start`. Subsequent polls overlap by three days
of broker creation time. The worker reuses verified order and execution IDs.
It groups missing matches by order and looks up their fills by broker UUID.
A larger group of new orders shares a paginated fill scan starting seven days
before the fee window. Each poll reads all fee pages for its interval and
requests up to eight fill pages. An unresolved fee can arrive first as
`Unmatched` and later with its verified fill.
Account-level fees carry the original signed amount and require no fill lookup.

The worker keeps its checkpoint and matching state in memory. On restart it
replays from the configured start. When upgrading, remove the directory from
the configuration or command line. Existing journal files are no longer used.
The node's cache database does not automatically store these custom fee rows.
A subscriber that retains them should upsert by environment, broker account
ID, and fee activity ID and commit its checkpoint with the rows. When resuming
from a saved checkpoint, include the overlap and any older unresolved fees
in the replay range.
Use the windowed report above for a complete summary of a specified interval.

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
`--extended-round-trip` (DAY) or `--extended-gtc-round-trip` (GTC) in the
pre-market, after-hours, or overnight session to test marketable limit orders
with extended hours enabled on both legs. These extended-hours modes use SPY
by default. Prefix the mode with `--symbol SYMBOL` to select another equity.
Begin each round trip with a flat position in that equity and verify both
terminal fills and the final position in Paper.

For a regular-session GTC round trip, run `--regular-gtc-round-trip`. The
example checks the Paper clock and calendar, including early closes, and
requires a flat SPY position with zero open SPY orders. It buys and sells one
share using GTC limits with `extended_hours=false`. It checks each native
client order ID against the Paper REST fill quantity, price, limit and session
timestamps, and checks the final position and open orders.

To verify regular GTC carryover, run `--hold-regular-gtc PRICE` during regular
hours with a positive limit below the SPY bid. Record the printed broker UUID,
client order ID, limit, creation time and zero-fill working status. On a later
New York trading date, run `--verify-regular-gtc UUID`. It checks the same
order is still working with zero fills, cancels it by UUID, and confirms
terminal cancellation and zero SPY position and open orders. Use
`--cancel-regular-gtc UUID` for early cleanup. Early cleanup leaves cross-date
verification pending. These checks use the Paper simulator.

For an Elite DMA submit-and-cancel check, use `--extended-dma-limit NASDAQ|ARCA PRICE`.
Choose a limit price below the current bid. The strategy submits a one-share DAY
buy with extended hours enabled, logs the destination and client order ID, and
requests cancellation on acceptance. For example, after building
`alpaca-paper-smoke`, the following commands select QQQ. Set `LIMIT_PRICE` to
your chosen resting price before running the DMA check.

```sh
target/debug/examples/alpaca-paper-smoke --symbol QQQ --extended-round-trip
target/debug/examples/alpaca-paper-smoke --symbol QQQ --extended-gtc-round-trip
target/debug/examples/alpaca-paper-smoke --symbol QQQ --extended-dma-limit NASDAQ "$LIMIT_PRICE"
```

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

### Advanced order execution checks

Build the native example and select one Paper case during a regular US equity
session with at least seven minutes remaining, or nine minutes for OCO.
The runner requires a flat SPY position and zero open SPY orders at the start,
checks the Paper clock and calendar, and uses fresh SIP market data. Each
order uses one SPY share. The OCO case establishes one long share before
submitting the two linked exits.

```sh
cargo build -p nautilus-alpaca --features examples --example alpaca-paper-smoke
target/debug/examples/alpaca-paper-smoke --advanced-order stop-limit
```

Cases are `stop-market`, `stop-limit`, `trailing-stop`, `bracket`, `oto`,
`oco`, `market-ioc`, `limit-ioc`, `market-fok`, and `limit-fok`. The runner
reports native order events and Paper REST status by client order ID and
broker UUID, including the broker child IDs for groups. A stop fill can
establish triggered execution with an unknown trigger time. The one-share
IOC cases cover full execution or immediate zero-fill cancellation.

The runner observes natural results before cleanup. An order that does not
reach its expected outcome before the deadline fails the check. Cleanup
cancels attributable Paper UUIDs, verifies their terminal status, and closes
an attributed remaining share with a native order. Each run prints its unique
order ID prefix for reconciliation if the broker result is inconclusive.

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
