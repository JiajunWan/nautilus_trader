#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""Verify native Alpaca Paper auction orders and close the test's AAPL position."""

# ruff: noqa: T201 (the CLI reports progress and verification on stdout)

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from datetime import time as clock_time
from datetime import timedelta
from decimal import ROUND_DOWN
from decimal import Decimal
from http import HTTPStatus
from pathlib import Path
from typing import Any
from typing import NoReturn
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler
from urllib.request import Request
from urllib.request import build_opener
from uuid import UUID
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[4]
BINARY = REPO_ROOT / "target" / "debug" / "examples" / "alpaca-paper-smoke"
PAPER_URL = "https://paper-api.alpaca.markets"
DATA_URL = "https://data.alpaca.markets"
SYMBOL = "AAPL"
NEW_YORK = ZoneInfo("America/New_York")
CASES = {"MKT": "market", "TAKE": "limit", "REST": "limit"}
TERMINAL = {"filled", "canceled", "expired", "rejected", "replaced"}
SUBMISSION_LEAD = timedelta(minutes=30)
TERMINAL_WINDOW = timedelta(minutes=4)

CREDENTIAL_VARS = (
    "ALPACA_PAPER_API_KEY",
    "ALPACA_PAPER_API_SECRET",
    "ALPACA_MARKET_DATA_API_KEY",
    "ALPACA_MARKET_DATA_API_SECRET",
)

type WireObject = dict[str, Any]


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args: object, **_kwargs: object) -> NoReturn:
        raise RuntimeError("Alpaca API redirected a credentialed request")


class _PaperApi:
    def __init__(self) -> None:
        key = os.environ.get("ALPACA_PAPER_API_KEY", "")
        secret = os.environ.get("ALPACA_PAPER_API_SECRET", "")
        if not key.startswith("PK") or not key[2:] or not secret:
            raise RuntimeError("Set ALPACA_PAPER_API_KEY and ALPACA_PAPER_API_SECRET for Paper")
        market_key = os.environ.get("ALPACA_MARKET_DATA_API_KEY", "")
        market_secret = os.environ.get("ALPACA_MARKET_DATA_API_SECRET", "")
        if bool(market_key) != bool(market_secret):
            raise RuntimeError("Set both Alpaca market data credentials or omit both")
        self.headers = self._auth(key, secret)
        self.data_headers = self._auth(market_key, market_secret) if market_key else self.headers
        self.opener = build_opener(_NoRedirect)

    @staticmethod
    def _auth(key: str, secret: str) -> dict[str, str]:
        return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}

    def request(
        self,
        path: str,
        *,
        method: str = "GET",
        payload: WireObject | None = None,
        missing_ok: bool = False,
        market_data: bool = False,
    ) -> Any:  # noqa: ANN401 (Alpaca has multiple response shapes)
        if market_data and method != "GET":
            raise RuntimeError("Alpaca market data requests must be read-only")
        url, auth = (DATA_URL, self.data_headers) if market_data else (PAPER_URL, self.headers)
        headers = {**auth, "Content-Type": "application/json"}
        body = json.dumps(payload).encode() if payload is not None else None

        request = Request(  # noqa: S310 (fixed HTTPS origins)
            url + path,
            data=body,
            headers=headers,
            method=method,
        )

        try:
            with self.opener.open(request, timeout=8) as response:
                content = response.read()
                return json.loads(content) if content else None
        except HTTPError as e:
            if missing_ok and e.code == HTTPStatus.NOT_FOUND:
                return None
            raise RuntimeError(f"Alpaca {method} {path}: HTTP {e.code}") from None

    def order(self, client_id: str) -> WireObject | None:
        query = urlencode({"client_order_id": client_id})
        return self.request(f"/v2/orders:by_client_order_id?{query}", missing_ok=True)

    def position(self) -> Decimal:
        position = self.request(f"/v2/positions/{SYMBOL}", missing_ok=True)
        return Decimal(position["qty"]) if position else Decimal(0)

    def open_orders(self) -> list[WireObject]:
        query = urlencode({"symbols": SYMBOL, "status": "open"})
        return [o for o in self.request(f"/v2/orders?{query}") if o["symbol"] == SYMBOL]


def _redact(message: str) -> str:
    for variable in CREDENTIAL_VARS:
        if value := os.environ.get(variable):
            message = message.replace(value, "[credential redacted]")
    return message


def _auction_window(api: _PaperApi, phase: str) -> tuple[datetime, datetime]:
    now = datetime.now(NEW_YORK)
    day = now.date()
    query = urlencode({"start": day.isoformat(), "end": day.isoformat()})
    calendar = api.request(f"/v2/calendar?{query}")

    if (
        len(calendar) != 1
        or calendar[0]["date"] != day.isoformat()
        or clock_time.fromisoformat(calendar[0]["open"]) != clock_time(9, 30)
        or clock_time.fromisoformat(calendar[0]["close"]) != clock_time(16)
    ):
        raise RuntimeError("Auction checks require a full Alpaca trading day")

    if phase == "open":
        target = now.replace(hour=9, minute=30, second=0, microsecond=0)
        cutoff = target - timedelta(minutes=2)
    else:
        target = now.replace(hour=16, minute=0, second=0, microsecond=0)
        cutoff = target - timedelta(minutes=10)
    return target, cutoff


@dataclass(frozen=True)
class _OrderSpec:
    client_id: str
    order_type: str
    tif: str
    side: str = "buy"
    quantity: Decimal = Decimal(1)
    extended_hours: bool = False


def _validate_order(order: WireObject, expected: _OrderSpec) -> None:
    actual = _OrderSpec(
        client_id=order["client_order_id"],
        order_type=order["type"],
        tif=order["time_in_force"],
        side=order["side"],
        quantity=Decimal(order["qty"]),
        extended_hours=order["extended_hours"],
    )

    if actual != expected or order["symbol"] != SYMBOL:
        raise RuntimeError(f"Alpaca Paper order does not match {expected.client_id}")
    filled = Decimal(order["filled_qty"])
    if not 0 <= filled <= expected.quantity:
        raise RuntimeError(f"Unexpected filled quantity for {expected.client_id}")


def _verify_auction_order(
    order: WireObject,
    spec: _OrderSpec,
    case: str,
    target: datetime,
    cutoff: datetime,
) -> None:
    _validate_order(order, spec)
    client_id = spec.client_id
    created = datetime.fromisoformat(order["created_at"]).astimezone(NEW_YORK)
    if not target - SUBMISSION_LEAD <= created < cutoff:
        raise RuntimeError(f"{client_id} was created outside its auction submission window")
    status = order["status"]
    filled = Decimal(order["filled_qty"])
    if case == "REST":
        if status not in {"canceled", "expired"} or filled != 0:
            raise RuntimeError(f"{client_id} did not expire or cancel unfilled")
        event_at = order.get("canceled_at" if status == "canceled" else "expired_at")
    else:
        if status != "filled" or filled != 1:
            raise RuntimeError(f"{client_id} did not fill exactly one share")
        price = Decimal(order["filled_avg_price"])
        if price <= 0 or (case == "TAKE" and price > Decimal(order["limit_price"])):
            raise RuntimeError(f"{client_id} filled outside its price bound")
        event_at = order.get("filled_at")

    if not event_at:
        raise RuntimeError(f"{client_id} has no broker terminal timestamp")
    terminal_at = datetime.fromisoformat(event_at).astimezone(NEW_YORK)
    if not target <= terminal_at <= target + TERMINAL_WINDOW:
        raise RuntimeError(f"{client_id} completed outside the auction observation window")
    print(
        f"Paper verified {client_id} {status} qty={filled} "
        f"price={order.get('filled_avg_price')} time={terminal_at.isoformat()}",
        flush=True,
    )


def _wait_terminal(api: _PaperApi, client_id: str) -> WireObject:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        order = api.order(client_id)
        if order and order["status"] in TERMINAL:
            return order
        time.sleep(1)
    raise RuntimeError(f"Paper order {client_id} did not reach a terminal state")


def _cancel_order(api: _PaperApi, order: WireObject) -> WireObject:
    client_id = order["client_order_id"]
    if order["status"] in TERMINAL:
        return order
    venue_id = UUID(order["id"])
    print(f"Manually canceling residual Paper order {client_id}", flush=True)
    try:
        api.request(f"/v2/orders/{venue_id}", method="DELETE")
    except (OSError, RuntimeError):
        print(f"Reconciling uncertain Paper cancellation for {client_id}", flush=True)
    return _wait_terminal(api, client_id)


def _cleanup_position(api: _PaperApi, client_id: str, filled: Decimal) -> None:
    position = api.position()
    if position != filled or api.open_orders():
        raise RuntimeError("AAPL orders or position do not match the attributable Paper fills")

    if not filled:
        return
    quote = api.request(f"/v2/stocks/{SYMBOL}/quotes/latest?feed=sip", market_data=True)["quote"]
    bid = Decimal(str(quote["bp"]))
    age = datetime.now(NEW_YORK) - datetime.fromisoformat(quote["t"])
    if bid <= 0 or not timedelta(0) <= age <= timedelta(seconds=30):
        raise RuntimeError("No fresh AAPL SIP bid for position cleanup")
    price = (bid * Decimal("0.98")).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

    payload = {
        "symbol": SYMBOL,
        "side": "sell",
        "qty": str(filled),
        "type": "limit",
        "time_in_force": "day",
        "extended_hours": True,
        "limit_price": str(price),
        "client_order_id": client_id,
    }

    try:
        api.request("/v2/orders", method="POST", payload=payload)
        order = _wait_terminal(api, client_id)
    except (OSError, RuntimeError):
        order = api.order(client_id)
        if order is None:
            raise
    _validate_order(
        order,
        _OrderSpec(client_id, "limit", "day", side="sell", quantity=filled, extended_hours=True),
    )
    order = _cancel_order(api, order)
    if Decimal(order["filled_qty"]) != filled:
        raise RuntimeError(f"Paper cleanup filled {order['filled_qty']} of {filled} AAPL shares")
    print(f"Closed {filled} attributable AAPL shares on Paper", flush=True)


def _cleanup(api: _PaperApi, prefix: str, tif: str) -> None:
    filled = Decimal(0)
    failures = []

    for case, order_type in CASES.items():
        client_id = f"{prefix}-{case}"
        try:
            order = api.order(client_id)
            if order is None:
                continue
            spec = _OrderSpec(client_id, order_type, tif)
            _validate_order(order, spec)
            order = _cancel_order(api, order)
            _validate_order(order, spec)
            filled += Decimal(order["filled_qty"])
        except (OSError, RuntimeError) as e:
            failures.append(e)

    if failures:
        raise ExceptionGroup("Could not reconcile Paper auction orders", failures)
    _cleanup_position(api, f"{prefix}-FLAT", filled)
    if api.position() or api.open_orders():
        raise RuntimeError("Paper auction did not leave AAPL flat with no open orders")
    print("Paper AAPL position=0 open_orders=0", flush=True)


def _run_native(binary: Path, phase: str, deadline: datetime) -> None:
    try:
        result = subprocess.run(  # noqa: S603 (explicitly selected native example)
            [str(binary), "--auction-check", phase],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=(deadline - datetime.now(NEW_YORK)).total_seconds(),
            check=False,
        )
    except subprocess.TimeoutExpired as e:
        print(_redact((e.stdout or b"").decode(errors="replace")), flush=True)
        raise RuntimeError("Native Paper auction timed out") from None
    output = result.stdout.decode(errors="replace")
    print(_redact(output), end="", flush=True)
    if result.returncode or "attempting manual timeout cleanup" in output:
        raise RuntimeError(f"Native Paper auction did not complete naturally ({result.returncode})")


def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("open", "close"))
    parser.add_argument("--binary", type=Path, default=BINARY)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    api = _PaperApi()
    target, cutoff = _auction_window(api, args.phase)
    tif = "opg" if args.phase == "open" else "cls"
    prefix = f"PAPER-AUC-{target:%Y%m%d}-{tif.upper()}"

    if api.position() or api.open_orders():
        raise RuntimeError("Paper auction requires no AAPL position or open orders")

    if any(api.order(f"{prefix}-{case}") for case in (*CASES, "FLAT")):
        raise RuntimeError(f"Paper auction already used the order prefix {prefix}")
    now = datetime.now(NEW_YORK)
    if not target - SUBMISSION_LEAD <= now < cutoff - timedelta(minutes=1):
        raise RuntimeError(f"Paper auction start is outside the window before {cutoff.isoformat()}")
    try:
        _run_native(binary, args.phase, target + timedelta(minutes=5))

        for case, order_type in CASES.items():
            client_id = f"{prefix}-{case}"
            order = api.order(client_id)
            if order is None:
                raise RuntimeError(f"Paper broker has no order for {client_id}")
            _verify_auction_order(
                order, _OrderSpec(client_id, order_type, tif), case, target, cutoff
            )
    finally:
        _cleanup(api, prefix, tif)
    print(
        f"PAPER_AUCTION_RESULT phase={args.phase} status=passed position=0 open_orders=0",
        flush=True,
    )


if __name__ == "__main__":
    try:
        _main()
    except Exception as e:  # noqa: BLE001 (redact error output at the CLI boundary)
        raise SystemExit(_redact("".join(traceback.format_exception(e)))) from None
