#!/usr/bin/env python3
"""
Generic Limit Order Script

Reads contract IDs from CSV and creates limit orders with specified action and price.
By default, orders are NOT transmitted - created for manual review in TWS.
Use --transmit to automatically transmit orders.
"""

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
import threading
import time
import csv
import os
import argparse
from dataclasses import dataclass


# ============================================================
# CONFIGURATION
# ============================================================

CSV_FILE = "generic_lmt_input.csv"
IB_HOST = "127.0.0.1"
IB_PORT = 7497  # TWS paper trading
CLIENT_ID = 0  # Client ID 5: Generic limit orders (Trading range 1-9)


# ============================================================
# DATA STRUCTURES
# ============================================================

@dataclass
class OrderSpec:
    """Specification for a single limit order from CSV"""
    conid: int
    qty: int
    action: str        # BUY or SELL from CSV
    lmt_price: float   # Limit price from CSV


# ============================================================
# IB API APPLICATION CLASS
# ============================================================

class GenericLimitOrderApp(EWrapper, EClient):
    """IB API application for creating generic limit orders"""

    def __init__(self, order_specs, transmit=False):
        EClient.__init__(self, self)
        self.order_specs = order_specs
        self.transmit = transmit
        self.next_order_id = None
        self.orders_created = 0
        self.orders_total = len(order_specs)

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        """Handle IB API errors"""
        # Filter out non-critical status messages
        if errorCode in [2104, 2106, 2158, 2119, 10167]:
            print(f"INFO: {errorString}")
        else:
            print(f"ERROR {errorCode}: {errorString}")

    def nextValidId(self, orderId: int):
        """Connection established - receives next valid order ID"""
        super().nextValidId(orderId)
        self.next_order_id = orderId
        print(f"Connected to IB. Next valid order ID: {orderId}")

        # Start creating orders
        self.create_orders()

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice,
                   permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        """Callback for order status updates"""
        print(f"\nOrder Status Update:")
        print(f"  Order ID: {orderId}")
        print(f"  Status: {status}")
        if whyHeld:
            print(f"  Why Held: {whyHeld}")

    def openOrder(self, orderId, contract, order, orderState):
        """Callback when order is created"""
        print(f"  ✓ Order {orderId} created (status: {orderState.status})")
        self.orders_created += 1

    def create_lmt_order(self, order_spec):
        """Create LMT (Limit) order

        Args:
            order_spec: OrderSpec with action and limit price

        Returns:
            Order object configured as LMT
        """
        order = Order()
        order.action = order_spec.action
        order.totalQuantity = order_spec.qty
        order.orderType = "LMT"
        order.lmtPrice = order_spec.lmt_price

        # Transmit based on command line flag
        order.transmit = self.transmit

        return order

    def create_orders(self):
        """Create all orders from specs"""
        print("\n" + "="*60)
        print(f"Creating {self.orders_total} Generic Limit Orders")
        if self.transmit:
            print("Orders WILL be transmitted automatically")
        else:
            print("Orders will NOT be transmitted - manual review mode")
        print("="*60)

        for i, spec in enumerate(self.order_specs):
            order_id = self.next_order_id + i

            print(f"\nCreating order {order_id}:")
            print(f"  ConID: {spec.conid}")
            print(f"  Action: {spec.action}")
            print(f"  Quantity: {spec.qty}")
            print(f"  Order Type: LMT")
            print(f"  Limit Price: {spec.lmt_price}")

            # Create contract from ConID
            contract = Contract()
            contract.conId = spec.conid
            contract.exchange = "SMART"

            # Create LMT order
            order = self.create_lmt_order(spec)

            # Place order
            self.placeOrder(order_id, contract, order)

            # Small delay between orders
            time.sleep(0.1)

        print("\n" + "="*60)
        if self.transmit:
            print("ALL ORDERS CREATED AND TRANSMITTED")
        else:
            print("ALL ORDERS CREATED BUT NOT TRANSMITTED")
            print("Review the orders in TWS before transmitting")
        print("="*60)


# ============================================================
# CSV READING FUNCTION
# ============================================================

def read_orders_from_csv(csv_path):
    """Read order specifications from CSV file"""
    order_specs = []

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)

        # Verify required columns
        required_columns = {'CONID', 'QTY', 'ACTION', 'LMT_PRICE'}
        if not required_columns.issubset(reader.fieldnames):
            raise ValueError(f"CSV must contain columns: {required_columns}")

        for row in reader:
            action = row['ACTION'].strip().upper()

            # Validate action is BUY or SELL
            if action not in ['BUY', 'SELL']:
                raise ValueError(f"ACTION must be BUY or SELL, got: {action}")

            order_spec = OrderSpec(
                conid=int(row['CONID'].strip()),
                qty=int(row['QTY'].strip()),
                action=action,
                lmt_price=float(row['LMT_PRICE'].strip())
            )
            order_specs.append(order_spec)

    return order_specs


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main entry point"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Create limit orders from CSV file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 generic_lmt_order.py              # Create orders without transmitting
  python3 generic_lmt_order.py --transmit   # Create and transmit orders
        """
    )
    parser.add_argument('--transmit', action='store_true',
                       help='Transmit orders immediately (default: create but do not transmit)')
    args = parser.parse_args()

    # Determine CSV file path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, CSV_FILE)

    print("="*60)
    print("Generic Limit Orders from CSV")
    print("="*60)
    print(f"CSV file: {csv_path}")
    print(f"Transmit mode: {'ENABLED' if args.transmit else 'DISABLED'}")

    # Read order specifications
    try:
        order_specs = read_orders_from_csv(csv_path)
        print(f"Loaded {len(order_specs)} order(s) from CSV\n")
    except Exception as e:
        print(f"ERROR reading CSV: {e}")
        return

    # Create application instance
    app = GenericLimitOrderApp(order_specs, transmit=args.transmit)

    # Connect to TWS/Gateway
    print(f"Connecting to IB at {IB_HOST}:{IB_PORT}...")
    app.connect(IB_HOST, IB_PORT, CLIENT_ID)

    # Run message processing loop in separate thread
    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()

    # Wait for all orders to be created
    timeout = 30
    elapsed = 0
    while app.orders_created < app.orders_total and elapsed < timeout:
        time.sleep(0.5)
        elapsed += 0.5

    if app.orders_created == app.orders_total:
        print(f"\n{'='*60}")
        print(f"SUCCESS: All {app.orders_total} orders created")
        print(f"{'='*60}")
        if args.transmit:
            print("Orders were transmitted to the market")
        else:
            print("Orders are NOT transmitted - review in TWS before submitting")
        print("Keeping connection open for 5 seconds...")
        time.sleep(5)
    else:
        print(f"\nWarning: Only {app.orders_created}/{app.orders_total} orders confirmed")

    # Disconnect
    print("\nDisconnecting from IB...")
    app.disconnect()
    print("Done.")


if __name__ == "__main__":
    main()
