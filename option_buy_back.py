#!/usr/bin/env python3
"""
Option Buy-Back Script

Automatically places GTC LMT BUY orders to close short options positions.
Scans existing open orders for buy-backs already in place, downloads portfolio
positions, computes the delta, and places paired call+put orders with order splitting.
"""

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.common import TickerId
import threading
import time
import csv
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dataclasses import dataclass
from random import randrange


# ============================================================
# CONFIGURATION
# ============================================================

CSV_FILE = "buy_back_input.csv"
IB_HOST = "127.0.0.1"
IB_PORT = 7497  # TWS paper trading
CLIENT_ID = 6   # Client ID 6: option buy-back orders (Trading range 1-9)

ACCOUNTS = ["YOUR_ACCOUNT"]

IGNORE = {
    "ES.100.0.20211015",
    "ES.5800.0.20211130",
    "ES.5800.0.20211231",
    "ES.6000.0.20210930",
    "JPY.0.00932.20200814",
    "ES.100.0.20210521",
    "EFA.68.0.20200320",
    "SPY.338.0.20200318",
    "LE.149.0.20220107",
    "HE.85.0.20220214",
    "HE.87.0.20211214",
    "CL.78.5.20211116",
    "CL.84.5.20211116",
}

OPTION_TYPES = ("OPT", "FOP")
DISCONNECT_DELAY = 1  # seconds after last order before disconnecting


# ============================================================
# DATA STRUCTURES
# ============================================================

@dataclass
class OrderParams:
    """Parameters for a symbol's buy-back orders from CSV."""
    symbol: str
    exchange: str
    order_size: int
    price_increment: float
    order_type: str
    tif: str
    aux_price: float
    lmt_price: float
    transmit: bool


# ============================================================
# CSV READING
# ============================================================

def _safe_float(value, default=0.0):
    """Convert string to float, returning default for empty or invalid values."""
    value = value.strip() if value else ""
    if not value or value == "#N/A":
        return default
    return float(value)


def read_order_params(csv_path):
    """Read order parameters from CSV, keyed by symbol."""
    params = {}
    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sym = row['symbol'].strip()
            exchange = row['exchange'].strip()
            if not sym or exchange == "#N/A":
                continue
            params[sym] = OrderParams(
                symbol=sym,
                exchange=exchange,
                order_size=int(row['OrderSize'].strip()),
                price_increment=_safe_float(row['priceIncrement']),
                order_type=row['orderType'].strip(),
                tif=row['tif'].strip(),
                aux_price=_safe_float(row['auxPrice']),
                lmt_price=_safe_float(row['lmtPrice']),
                transmit=row['transmit'].strip() != '0',
            )
    return params


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def compute_good_after_time():
    """Compute goodAfterTime for today (if pre-market) or next day at 09:35 Eastern."""
    eastern = ZoneInfo("US/Eastern")
    now_et = datetime.now(eastern)
    market_open = now_et.replace(hour=9, minute=35, second=0, microsecond=0)
    if now_et < market_open:
        target = now_et
    else:
        target = now_et + timedelta(days=1)
        # Skip weekends (Saturday=5, Sunday=6)
        while target.weekday() >= 5:
            target += timedelta(days=1)
    return f"{target.strftime('%Y%m%d')} 09:35:00 US/Eastern"


def make_strike_id(strike, expiry):
    """Create a composite ID from strike and expiry."""
    return str(strike) + expiry


def make_ignore_id(symbol, strike, expiry):
    """Create ignore-list key: SYMBOL.STRIKE.EXPIRY."""
    return f"{symbol}.{strike}.{expiry}"


# ============================================================
# IB API APPLICATION CLASS
# ============================================================

class BuyBackApp(EWrapper, EClient):
    """IB API application for placing option buy-back orders."""

    def __init__(self, accounts, order_params, ignore, good_after_time):
        EClient.__init__(self, self)

        # Configuration
        self.accounts = accounts
        self.order_params = order_params
        self.ignore = ignore
        self.good_after_time = good_after_time

        # Threading events
        self.connected_event = threading.Event()
        self.orders_received_event = threading.Event()
        self.account_download_event = threading.Event()

        # Order tracking
        self.next_order_id = 0
        self.request_id = 0
        self.seen_order_ids = set()

        # Existing buy-backs: {symbol: {strike_id: qty}}
        self.buy_backs = {}
        self.total_buy_backs = 0
        self.buy_back_conids = set()
        self.expirations = set()

        # Portfolio data
        self.portfolio_positions = []
        self.all_options_contracts = {}

    # --------------------------------------------------------
    # CONNECTION CALLBACKS
    # --------------------------------------------------------

    def nextValidId(self, orderId):
        self.next_order_id = orderId
        print(f"Connected. Next valid order ID: {orderId}")
        self.connected_event.set()

    def error(self, reqId: TickerId, errorCode: int, errorString: str,
              advancedOrderRejectJson: str = ""):
        if errorCode in (2104, 2106, 2158, 2119, 10167):
            return
        print(f"Error (reqId={reqId}) {errorCode}: {errorString}")

    # --------------------------------------------------------
    # OPEN ORDER CALLBACKS
    # --------------------------------------------------------

    def openOrder(self, orderId, contract, order, orderState):
        if orderId in self.seen_order_ids:
            return
        self.seen_order_ids.add(orderId)

        if contract.secType not in OPTION_TYPES:
            return
        if order.account not in self.accounts:
            return

        strike_id = make_strike_id(contract.strike, contract.lastTradeDateOrContractMonth)

        if contract.symbol not in self.buy_backs:
            self.buy_backs[contract.symbol] = {}
        if strike_id in self.buy_backs[contract.symbol]:
            self.buy_backs[contract.symbol][strike_id] += order.totalQuantity
        else:
            self.buy_backs[contract.symbol][strike_id] = order.totalQuantity

        self.total_buy_backs += order.totalQuantity
        self.expirations.add((contract.symbol, contract.lastTradeDateOrContractMonth))
        self.buy_back_conids.add(contract.conId)

    def openOrderEnd(self):
        # Ensure next_order_id is higher than any existing order
        if self.seen_order_ids:
            self.next_order_id = max(max(self.seen_order_ids) + 1, self.next_order_id)
            print(f"Adjusted next order ID to {self.next_order_id}")
        print("Open orders received")
        self.orders_received_event.set()

    # --------------------------------------------------------
    # PORTFOLIO CALLBACKS
    # --------------------------------------------------------

    def updatePortfolio(self, contract, position, marketPrice, marketValue,
                        averageCost, unrealizedPNL, realizedPNL, accountName):
        if contract.secType == 'STK':
            position /= 100

        if abs(position) > 0:
            self.portfolio_positions.append({
                'contract': contract,
                'position': position,
                'account': accountName,
            })

    def accountDownloadEnd(self, accountName):
        print(f"Account download complete: {accountName}")
        self.account_download_event.set()

    # --------------------------------------------------------
    # PORTFOLIO DOWNLOAD
    # --------------------------------------------------------

    def download_portfolios(self):
        """Download portfolio for each account, waiting for each to complete."""
        for account in self.accounts:
            self.account_download_event.clear()
            self.reqAccountUpdates(True, account)
            self.account_download_event.wait(timeout=15)

    # --------------------------------------------------------
    # ORDER PROCESSING
    # --------------------------------------------------------

    def process_and_place_orders(self):
        """Compute delta between positions and buy-backs, place needed orders."""
        opt_positions = [p for p in self.portfolio_positions
                         if p['contract'].secType in OPTION_TYPES]

        # Aggregate positions: {symbol: {strike_id: total_position}}
        for p in opt_positions:
            sym = p['contract'].symbol
            strike_id = make_strike_id(p['contract'].strike,
                                       p['contract'].lastTradeDateOrContractMonth)
            if sym not in self.all_options_contracts:
                self.all_options_contracts[sym] = {}
            if strike_id in self.all_options_contracts[sym]:
                self.all_options_contracts[sym][strike_id] += p['position']
            else:
                self.all_options_contracts[sym][strike_id] = p['position']

        print(f"\nAll options positions: {self.all_options_contracts}")

        # Compute deltas: expected buy-backs = -2 * position (paired call+put)
        nop = self.all_options_contracts
        bbs = self.buy_backs

        print()
        print("symbol  strike_id  position  expected  current  delta")

        for sym in nop:
            for strike_id in nop[sym]:
                pos = nop[sym][strike_id]
                expected = -2 * pos
                current = bbs.get(sym, {}).get(strike_id, 0)
                delta = expected - current
                nop[sym][strike_id] = (pos, expected, current, delta)

        for sym in nop:
            for strike_id in nop[sym]:
                t = nop[sym][strike_id]
                print(f"  {sym}  {strike_id}  {int(t[0])}  {int(t[1])}  {int(t[2])}  {int(t[3])}")

        # Collect needed buy-backs
        print()
        print("Buy-backs needed:")

        symbol_strike_qty = {}
        for sym in nop:
            for strike_id in nop[sym]:
                delta = nop[sym][strike_id][3]
                if delta > 0:
                    t = nop[sym][strike_id]
                    print(f"  {sym}  {strike_id}  pos={int(t[0])}  expected={int(t[1])}  current={int(t[2])}  delta={int(t[3])}")
                    if sym not in symbol_strike_qty:
                        symbol_strike_qty[sym] = {}
                    symbol_strike_qty[sym][strike_id] = int(delta)

        print()
        print(f"symbol_strike_qty: {symbol_strike_qty}")
        print()

        # Place orders for each option contract that needs buy-backs
        for p in opt_positions:
            contract = p['contract']
            sym = contract.symbol
            strike_id = make_strike_id(contract.strike,
                                       contract.lastTradeDateOrContractMonth)
            ignore_id = make_ignore_id(sym, contract.strike,
                                       contract.lastTradeDateOrContractMonth)

            if (sym not in symbol_strike_qty or
                    strike_id not in symbol_strike_qty[sym] or
                    ignore_id in self.ignore or
                    sym == 'SPX' or
                    sym not in self.order_params):
                continue

            qty = symbol_strike_qty[sym][strike_id] / 2
            params = self.order_params[sym]
            ord_size = params.order_size

            if qty <= ord_size:
                self.place_order(p['account'], contract, qty, 0)
            else:
                orders = int(qty / ord_size)
                extra = qty % ord_size
                # First order gets the extra quantity
                self.place_order(p['account'], contract, ord_size + extra, 0)
                # Remaining orders at incremented prices
                for n in range(orders - 1):
                    increment = params.price_increment * (n + 1)
                    self.place_order(p['account'], contract, ord_size, increment)

            # Remove from queue to avoid duplicate placement
            symbol_strike_qty[sym].pop(strike_id, None)

    def place_order(self, account, portfolio_contract, quantity, increment):
        """Place a paired call+put buy-back order."""
        quantity = round(quantity - 0.49)
        if int(quantity) <= 0:
            return

        sym = portfolio_contract.symbol
        params = self.order_params[sym]

        # Build contract from portfolio data with exchange from CSV
        contract = Contract()
        contract.symbol = sym
        contract.lastTradeDateOrContractMonth = portfolio_contract.lastTradeDateOrContractMonth
        contract.secType = portfolio_contract.secType
        contract.exchange = params.exchange
        contract.currency = portfolio_contract.currency
        contract.strike = portfolio_contract.strike
        contract.right = portfolio_contract.right
        contract.tradingClass = portfolio_contract.tradingClass

        # Build order
        order = Order()
        order.account = account
        order.orderType = params.order_type
        order.tif = params.tif
        order.auxPrice = params.aux_price
        order.lmtPrice = round(params.lmt_price + increment, 8)
        order.transmit = params.transmit
        order.orderRef = "buy_back"

        # goodAfterTime only for SMART-routed equity options;
        # futures exchanges (CBOT, CME, etc.) reject it for GTC orders
        if contract.exchange == "SMART":
            order.goodAfterTime = self.good_after_time

        # OCA groups only for futures options (non-SMART exchange)
        if contract.exchange != "SMART":
            order.ocaGroup = f"{sym}_{randrange(100000)}"
            order.ocaType = 3

        # Determine action
        if quantity >= 0:
            order.action = "BUY"
        else:
            quantity *= -1
            order.action = "SELL"
        order.totalQuantity = int(quantity)

        # Place order for original right
        self.request_id += 1
        order_id = self.next_order_id + self.request_id
        print(f"  Order {order_id}: {order.action} {int(order.totalQuantity)} {sym} "
              f"{contract.right} {contract.strike} {contract.lastTradeDateOrContractMonth} "
              f"@ {order.lmtPrice}")
        self.placeOrder(order_id, contract, order)

        # Place paired order for opposite right
        self.request_id += 1
        order_id = self.next_order_id + self.request_id
        paired_contract = Contract()
        paired_contract.symbol = contract.symbol
        paired_contract.lastTradeDateOrContractMonth = contract.lastTradeDateOrContractMonth
        paired_contract.secType = contract.secType
        paired_contract.exchange = contract.exchange
        paired_contract.currency = contract.currency
        paired_contract.strike = contract.strike
        paired_contract.right = "P" if contract.right == "C" else "C"
        paired_contract.tradingClass = contract.tradingClass

        print(f"  Order {order_id}: {order.action} {int(order.totalQuantity)} {sym} "
              f"{paired_contract.right} {contract.strike} {contract.lastTradeDateOrContractMonth} "
              f"@ {order.lmtPrice}")
        self.placeOrder(order_id, paired_contract, order)

    # --------------------------------------------------------
    # SHUTDOWN
    # --------------------------------------------------------

    def stop(self):
        """Unsubscribe from account updates and disconnect."""
        for account in self.accounts:
            self.reqAccountUpdates(False, account)
        self.disconnect()
        print("Disconnected")
        os.system('say "finished"')


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    good_after_time = compute_good_after_time()
    print(f"goodAfterTime: {good_after_time}")

    # Read CSV order parameters
    order_params = read_order_params(CSV_FILE)
    print(f"Loaded {len(order_params)} symbol(s) from CSV")

    # Create app
    app = BuyBackApp(ACCOUNTS, order_params, IGNORE, good_after_time)

    # Connect
    print(f"Connecting to {IB_HOST}:{IB_PORT} with client ID {CLIENT_ID}...")
    app.connect(IB_HOST, IB_PORT, CLIENT_ID)

    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()

    if not app.connected_event.wait(timeout=10):
        print("Failed to connect")
        return

    # Scan existing open orders for buy-backs
    print("\nScanning existing buy-back orders...")
    app.reqAllOpenOrders()
    if not app.orders_received_event.wait(timeout=10):
        print("Timeout waiting for open orders")
        app.disconnect()
        return

    print(f"Found {app.total_buy_backs} existing buy-back order(s)")
    if app.expirations:
        print("Expirations:")
        for sym, exp in sorted(app.expirations, key=lambda t: t[1]):
            print(f"  {sym} {exp}")
    print(f"Buy-backs: {app.buy_backs}")

    # Download portfolio positions
    print("\nDownloading portfolio positions...")
    app.download_portfolios()

    # Process and place orders
    print("\nProcessing positions and placing orders...")
    app.process_and_place_orders()

    # Wait and disconnect
    time.sleep(DISCONNECT_DELAY)
    app.stop()


if __name__ == "__main__":
    main()
