#!/usr/bin/env python3
"""
Refresh Buy-Back Good-After-Time

Updates goodAfterTime on all existing buy-back orders to today 09:35:00 Eastern.
Connects with Client ID 6 (same as option_buy_back.py) to modify its orders.
"""

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.common import TickerId
from ibapi.const import UNSET_DOUBLE, UNSET_INTEGER
import threading
import time
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# CONFIGURATION
# ============================================================

IB_HOST = "127.0.0.1"
IB_PORT = 7497  # TWS paper trading
CLIENT_ID = 6   # Same client ID as option_buy_back.py to allow order modification


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


# ============================================================
# IB API APPLICATION CLASS
# ============================================================

class RefreshGATApp(EWrapper, EClient):
    """IB API application for refreshing goodAfterTime on buy-back orders."""

    def __init__(self, good_after_time):
        EClient.__init__(self, self)
        self.good_after_time = good_after_time

        # Threading events
        self.connected_event = threading.Event()
        self.orders_received_event = threading.Event()

        # Order tracking
        self.next_order_id = 0
        self.buy_back_orders = []  # list of (orderId, contract, order)
        self.updated_count = 0

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
        print(f"Error {errorCode}: {errorString}")

    # --------------------------------------------------------
    # OPEN ORDER CALLBACKS
    # --------------------------------------------------------

    def openOrder(self, orderId, contract, order, orderState):
        if order.orderRef == "buy_back":
            self.buy_back_orders.append((orderId, contract, order))

    def openOrderEnd(self):
        print(f"Found {len(self.buy_back_orders)} buy-back order(s)")
        self.orders_received_event.set()

    # --------------------------------------------------------
    # ORDER UPDATE
    # --------------------------------------------------------

    def update_good_after_times(self):
        """Update goodAfterTime on all buy-back orders."""
        # Snapshot the list — placeOrder triggers openOrder callbacks that
        # would append duplicates during iteration
        orders_to_update = list(self.buy_back_orders)
        for order_id, contract, order in orders_to_update:
            # Skip futures options — futures exchanges (CBOT, CME, etc.)
            # reject goodAfterTime on GTC orders
            if contract.exchange != "SMART":
                print(f"  Skipped order {order_id}: {contract.symbol} {contract.right} "
                      f"{contract.strike} — non-SMART exchange ({contract.exchange})")
                continue
            old_gat = order.goodAfterTime or "(none)"
            order.goodAfterTime = self.good_after_time
            # Clear volatility fields that IB rejects on non-VOL orders
            order.volatility = UNSET_DOUBLE
            order.volatilityType = UNSET_INTEGER
            order.referencePriceType = UNSET_INTEGER
            self.placeOrder(order_id, contract, order)
            self.updated_count += 1
            print(f"  Updated order {order_id}: {contract.symbol} {contract.right} "
                  f"{contract.strike} — GAT: {old_gat} -> {self.good_after_time}")


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    good_after_time = compute_good_after_time()
    print(f"New goodAfterTime: {good_after_time}")

    app = RefreshGATApp(good_after_time)

    print(f"Connecting to {IB_HOST}:{IB_PORT} with client ID {CLIENT_ID}...")
    app.connect(IB_HOST, IB_PORT, CLIENT_ID)

    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()

    if not app.connected_event.wait(timeout=10):
        print("Failed to connect")
        return

    # Clear state from auto-delivery (orders arrive on connect)
    app.buy_back_orders.clear()
    app.orders_received_event.clear()

    print("\nScanning for buy-back orders...")
    app.reqOpenOrders()
    if not app.orders_received_event.wait(timeout=10):
        print("Timeout waiting for open orders")
        app.disconnect()
        return

    if not app.buy_back_orders:
        print("No buy-back orders found")
    else:
        print(f"\nUpdating {len(app.buy_back_orders)} order(s)...")
        app.update_good_after_times()
        time.sleep(2)
        print(f"\nDone. Updated {app.updated_count} order(s)")

    app.disconnect()
    print("Disconnected")
    os.system('say "finished"')


if __name__ == "__main__":
    main()
