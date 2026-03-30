#!/usr/bin/env python3
"""
SPY Short Options Orders Script

Reads SPY option contracts from CSV and creates REL orders with time conditions.
Orders are NOT transmitted - created for manual review in TWS.
"""

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.order_condition import TimeCondition
import threading
import time
import csv
import os
from datetime import datetime
from dataclasses import dataclass


# ============================================================
# CONFIGURATION
# ============================================================

CSV_FILE = "SPY_short_options.csv"
IB_HOST = "127.0.0.1"
IB_PORT = 7497  # TWS paper trading
CLIENT_ID = 3  # Client ID 3: SPY options orders (Trading range 1-9)


# ============================================================
# DATA STRUCTURES
# ============================================================

@dataclass
class OrderSpec:
    """Specification for a single SPY option order from CSV"""
    conid: int
    qty: int
    offset: float  # Maps to auxPrice for REL orders
    lmt: str       # Ignored for REL orders
    time: str      # Time condition string


# ============================================================
# IB API APPLICATION CLASS
# ============================================================

class SPYOptionsApp(EWrapper, EClient):
    """IB API application for creating SPY option orders"""

    def __init__(self, order_specs):
        EClient.__init__(self, self)
        self.order_specs = order_specs
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

    def create_time_condition(self, time_str):
        """Create time condition for specified time on current date

        Args:
            time_str: Format "HH:MM:SS Eastern Time" or similar

        Returns:
            TimeCondition object with isMore=True (>=)
        """
        time_condition = TimeCondition()

        # Get current date in YYYYMMDD format
        current_date = datetime.now().strftime("%Y%m%d")

        # Parse time and timezone from the time string
        parts = time_str.strip().split()
        time_only = parts[0]  # HH:MM:SS

        # Map timezone names to IB format
        timezone_map = {
            "Eastern Time": "US/Eastern",
            "Central Time": "US/Central",
            "Mountain Time": "US/Mountain",
            "Pacific Time": "US/Pacific",
            "US/Eastern": "US/Eastern",
            "US/Central": "US/Central",
            "US/Mountain": "US/Mountain",
            "US/Pacific": "US/Pacific",
        }

        # Get timezone (default to US/Eastern if not specified)
        if len(parts) > 1:
            tz_input = " ".join(parts[1:])
            timezone = timezone_map.get(tz_input, "US/Eastern")
        else:
            timezone = "US/Eastern"

        # Set time condition
        time_condition.time = f"{current_date} {time_only} {timezone}"
        time_condition.isMore = True  # Order active when time >= specified time

        return time_condition

    def create_rel_order(self, order_spec):
        """Create REL (Relative) order for SPY option

        REL orders are priced relative to the market midpoint.

        Args:
            order_spec: OrderSpec with offset value

        Returns:
            Order object configured as REL with time condition
        """
        order = Order()
        order.action = "SELL"  # Always SELL for short options
        order.totalQuantity = order_spec.qty
        order.orderType = "REL"
        order.auxPrice = order_spec.offset  # Relative price offset

        # Do NOT transmit - only create for review
        order.transmit = False

        # Add time condition if TIME field is provided
        if order_spec.time:
            time_condition = self.create_time_condition(order_spec.time)
            order.conditions = [time_condition]
            print(f"  Time Condition: Order active when time >= {order_spec.time}")

        return order

    def create_orders(self):
        """Create all orders from specs"""
        print("\n" + "="*60)
        print(f"Creating {self.orders_total} SPY Option Orders")
        print("="*60)

        for i, spec in enumerate(self.order_specs):
            order_id = self.next_order_id + i

            print(f"\nCreating order {order_id}:")
            print(f"  ConID: {spec.conid}")
            print(f"  Action: SELL")
            print(f"  Quantity: {spec.qty}")
            print(f"  Order Type: REL")
            print(f"  Offset (auxPrice): {spec.offset}")

            # Create contract from ConID
            contract = Contract()
            contract.conId = spec.conid
            contract.exchange = "SMART"

            # Create REL order with time condition
            order = self.create_rel_order(spec)

            # Place order
            self.placeOrder(order_id, contract, order)

            # Small delay between orders
            time.sleep(0.1)

        print("\n" + "="*60)
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
        required_columns = {'CONID', 'QTY', 'OFFSET', 'LMT', 'TIME'}
        if not required_columns.issubset(reader.fieldnames):
            raise ValueError(f"CSV must contain columns: {required_columns}")

        for row in reader:
            order_spec = OrderSpec(
                conid=int(row['CONID'].strip()),
                qty=int(row['QTY'].strip()),
                offset=float(row['OFFSET'].strip()),
                lmt=row['LMT'].strip(),  # Ignored for REL
                time=row['TIME'].strip()
            )
            order_specs.append(order_spec)

    return order_specs


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main entry point"""
    # Determine CSV file path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, CSV_FILE)

    print("="*60)
    print("SPY Short Options Orders - REL with Time Conditions")
    print("="*60)
    print(f"CSV file: {csv_path}")

    # Read order specifications
    try:
        order_specs = read_orders_from_csv(csv_path)
        print(f"Loaded {len(order_specs)} order(s) from CSV\n")
    except Exception as e:
        print(f"ERROR reading CSV: {e}")
        return

    # Create application instance
    app = SPYOptionsApp(order_specs)

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
