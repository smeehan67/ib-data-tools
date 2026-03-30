#!/usr/bin/env python3
"""
Balance Checker for Buy-Back Orders

Checks all open buy-back orders for call/put imbalances at each strike.
When one side of a paired buy-back fills, the orphaned counterpart should
be canceled. Lists orphaned orders and optionally cancels them.

Usage:
    python3 balance_checker.py          # List orphans, prompt before canceling
    python3 balance_checker.py -y       # Cancel orphans without prompting
"""

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.common import TickerId
from ibapi.order_cancel import OrderCancel
import threading
import time
import argparse
import os


# ============================================================
# CONFIGURATION
# ============================================================

IB_HOST = "127.0.0.1"
IB_PORT = 7497  # TWS paper trading
CLIENT_ID = 6   # Client ID 6: same as option_buy_back.py (can cancel its own orders)


# ============================================================
# IB API APPLICATION CLASS
# ============================================================

class BalanceCheckerApp(EWrapper, EClient):
    """IB API application for checking buy-back order balance."""

    def __init__(self):
        EClient.__init__(self, self)

        # Threading events
        self.connected_event = threading.Event()
        self.orders_received_event = threading.Event()

        # Order tracking
        self.next_order_id = 0
        self.buy_back_orders = []  # list of (orderId, contract, order)
        self.canceled_ids = set()

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
    # ORDER STATUS CALLBACK
    # --------------------------------------------------------

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice,
                    permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        if orderId in self.canceled_ids and status == "Cancelled":
            print(f"  Confirmed canceled: order {orderId}")


# ============================================================
# BALANCE CHECKING LOGIC
# ============================================================

def find_orphaned_orders(buy_back_orders):
    """Find buy-back orders that lost their call/put counterpart.

    Orders are paired by (symbol, strike, expiry, lmtPrice).
    If one side filled, the other is orphaned and should be canceled.

    Returns list of (orderId, contract, order) tuples to cancel.
    """
    # Group by (symbol, strike, expiry, lmtPrice)
    # value = {'C': [(orderId, contract, order), ...], 'P': [...]}
    groups = {}
    for order_id, contract, order in buy_back_orders:
        key = (contract.symbol, contract.strike,
               contract.lastTradeDateOrContractMonth, order.lmtPrice)
        if key not in groups:
            groups[key] = {'C': [], 'P': []}
        groups[key][contract.right].append((order_id, contract, order))

    orphans = []
    for key, sides in sorted(groups.items()):
        calls = sides['C']
        puts = sides['P']

        if len(calls) == len(puts):
            continue

        if len(calls) > len(puts):
            # Extra calls with no matching put — orphaned
            for entry in calls[len(puts):]:
                orphans.append(entry)
        else:
            # Extra puts with no matching call — orphaned
            for entry in puts[len(calls):]:
                orphans.append(entry)

    return orphans


def display_orders_summary(buy_back_orders):
    """Display summary of all buy-back orders grouped by symbol/strike/expiry."""
    groups = {}
    for order_id, contract, order in buy_back_orders:
        key = (contract.symbol, contract.strike,
               contract.lastTradeDateOrContractMonth)
        if key not in groups:
            groups[key] = {'C': 0, 'P': 0}
        groups[key][contract.right] += 1

    print(f"\n  {'Symbol':<8} {'Strike':>8} {'Expiry':<12} {'Calls':>6} {'Puts':>6} {'Status'}")
    print(f"  {'------':<8} {'------':>8} {'------':<12} {'-----':>6} {'----':>6} {'------'}")
    for key in sorted(groups.keys()):
        sym, strike, expiry = key
        c = groups[key]['C']
        p = groups[key]['P']
        status = "OK" if c == p else "IMBALANCED"
        print(f"  {sym:<8} {strike:>8} {expiry:<12} {c:>6} {p:>6}  {status}")


def display_orphans(orphans):
    """Display orders that need to be canceled."""
    print(f"\nOrphaned orders to cancel ({len(orphans)}):")
    print(f"  {'OrderID':>8}  {'Symbol':<8} {'Right':<5} {'Strike':>8} "
          f"{'Expiry':<12} {'Price':>8} {'Qty':>5}")
    print(f"  {'-------':>8}  {'------':<8} {'-----':<5} {'------':>8} "
          f"{'------':<12} {'-----':>8} {'---':>5}")
    for order_id, contract, order in orphans:
        print(f"  {order_id:>8}  {contract.symbol:<8} {contract.right:<5} "
              f"{contract.strike:>8} {contract.lastTradeDateOrContractMonth:<12} "
              f"{order.lmtPrice:>8.2f} {int(order.totalQuantity):>5}")


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Check buy-back order balance and cancel orphans")
    parser.add_argument('-y', '--yes', action='store_true',
                        help='Cancel orphaned orders without asking for confirmation')
    args = parser.parse_args()

    app = BalanceCheckerApp()

    print(f"Connecting to {IB_HOST}:{IB_PORT} with client ID {CLIENT_ID}...")
    app.connect(IB_HOST, IB_PORT, CLIENT_ID)

    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()

    if not app.connected_event.wait(timeout=10):
        print("Failed to connect")
        return

    # Clear state before requesting orders
    app.buy_back_orders.clear()
    app.orders_received_event.clear()

    print("\nScanning for buy-back orders...")
    app.reqAllOpenOrders()
    if not app.orders_received_event.wait(timeout=10):
        print("Timeout waiting for open orders")
        app.disconnect()
        return

    if not app.buy_back_orders:
        print("No buy-back orders found")
        app.disconnect()
        print("Disconnected")
        os.system('say "finished"')
        return

    # Display summary
    display_orders_summary(app.buy_back_orders)

    # Find orphans
    orphans = find_orphaned_orders(app.buy_back_orders)

    if not orphans:
        print("\nAll buy-back orders are balanced. No action needed.")
        app.disconnect()
        print("Disconnected")
        os.system('say "finished"')
        return

    display_orphans(orphans)

    # Cancel orphans
    if args.yes:
        proceed = True
    else:
        response = input(f"\nCancel {len(orphans)} orphaned order(s)? [y/N]: ").strip().lower()
        proceed = response in ('y', 'yes')

    if proceed:
        print()
        for order_id, contract, order in orphans:
            app.canceled_ids.add(order_id)
            app.cancelOrder(order_id, OrderCancel())
            print(f"  Canceling order {order_id}: {contract.symbol} {contract.right} "
                  f"{contract.strike} @ {order.lmtPrice}")
        time.sleep(2)
        print(f"\nCanceled {len(orphans)} order(s)")
    else:
        print("\nNo orders canceled.")

    app.disconnect()
    print("Disconnected")
    os.system('say "finished"')


if __name__ == "__main__":
    main()
