#!/usr/bin/env python3
"""
Futures Conditional Orders from CSV
Reads contract specifications from CSV and creates conditional market orders
with Adaptive algo and time conditions. Orders are NOT transmitted.
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


@dataclass
class OrderSpec:
    """Specification for a single order from CSV"""
    symbol: str
    conid: int
    exchange: str
    time: str


class FuturesOrderApp(EWrapper, EClient):
    """IB API application for creating conditional futures orders from CSV"""

    def __init__(self, order_specs):
        EClient.__init__(self, self)
        self.order_specs = order_specs
        self.next_order_id = None
        self.orders_created = 0
        self.orders_total = len(order_specs)
        self.current_order_id = None

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        """Handle IB API errors"""
        # Filter out non-critical status messages
        if errorCode in [2104, 2106, 2158, 2119, 10167]:
            print(f"INFO: {errorString}")
        else:
            print(f"ERROR {errorCode}: {errorString}")

    def nextValidId(self, orderId: int):
        """Callback when connection established - receives next valid order ID"""
        super().nextValidId(orderId)
        self.next_order_id = orderId
        print(f"Connected to IB. Next valid order ID: {orderId}")

        # Start creating orders for all contracts
        self.create_all_orders()

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice,
                   permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        """Callback for order status updates"""
        print(f"\nOrder Status Update:")
        print(f"  Order ID: {orderId}")
        print(f"  Status: {status}")
        if whyHeld:
            print(f"  Why Held: {whyHeld}")

    def openOrder(self, orderId, contract, order, orderState):
        """Callback when order is opened/modified"""
        print(f"\nOrder Created Successfully:")
        print(f"  Order ID: {orderId}")
        print(f"  Symbol: {contract.symbol}")
        print(f"  ConID: {contract.conId}")
        print(f"  Exchange: {contract.exchange}")
        print(f"  Action: {order.action}")
        print(f"  Quantity: {order.totalQuantity}")
        print(f"  Order Type: {order.orderType}")
        print(f"  Algo Strategy: {order.algoStrategy}")
        print(f"  Transmit: {order.transmit}")
        print(f"  Order State: {orderState.status}")
        if order.conditions:
            for condition in order.conditions:
                if hasattr(condition, 'time'):
                    print(f"  Time Condition: >= {condition.time}")

        self.orders_created += 1
        print(f"  Progress: {self.orders_created}/{self.orders_total} orders created")

    def create_contract_from_conid(self, order_spec):
        """Create contract using ConID and exchange"""
        contract = Contract()
        contract.conId = order_spec.conid
        contract.exchange = order_spec.exchange
        return contract

    def create_adaptive_market_order(self):
        """Create market order with Adaptive algo set to Normal priority"""
        order = Order()
        order.action = "BUY"
        order.totalQuantity = 1
        order.orderType = "MKT"

        # Adaptive algo configuration
        order.algoStrategy = "Adaptive"
        order.algoParams = []
        from ibapi.tag_value import TagValue
        # adaptivePriority: Normal = 0, Urgent = 1, Patient = 2
        order.algoParams.append(TagValue("adaptivePriority", "Normal"))

        # Do NOT transmit - only create for review
        order.transmit = False

        return order

    def create_time_condition(self, time_str):
        """Create time condition for specified time on current date"""
        time_condition = TimeCondition()

        # Get current date in YYYYMMDD format
        current_date = datetime.now().strftime("%Y%m%d")

        # Parse time and timezone from the time string
        # Format: "HH:MM:SS Eastern Time" or "HH:MM:SS Central Time"
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

        # Get timezone (default to US/Eastern if not specified or found)
        if len(parts) > 1:
            tz_input = " ".join(parts[1:])
            timezone = timezone_map.get(tz_input, "US/Eastern")
        else:
            timezone = "US/Eastern"

        # Set time with the provided time string
        time_condition.time = f"{current_date} {time_only} {timezone}"

        # isMore = True means "greater than or equal to"
        time_condition.isMore = True

        return time_condition

    def create_all_orders(self):
        """Create orders for all contracts from CSV"""
        if self.next_order_id is None:
            print("ERROR: No valid order ID available")
            return

        print("\n" + "="*60)
        print(f"Creating {self.orders_total} Conditional Orders from CSV")
        print("="*60)

        for order_spec in self.order_specs:
            print(f"\n--- Processing {order_spec.symbol} (ConID: {order_spec.conid}) ---")

            # Create contract using ConID
            contract = self.create_contract_from_conid(order_spec)

            # Create order
            order = self.create_adaptive_market_order()

            # Add time condition
            time_condition = self.create_time_condition(order_spec.time)
            order.conditions = [time_condition]
            print(f"Time Condition: Order active when time >= {order_spec.time} ET")

            # Place order
            print(f"Placing order {self.next_order_id}...")
            self.placeOrder(self.next_order_id, contract, order)

            # Increment order ID for next order
            self.next_order_id += 1

            # Small delay between orders
            time.sleep(0.1)

        print("\n" + "="*60)
        print("ALL ORDERS CREATED BUT NOT TRANSMITTED")
        print("Review the orders in TWS before transmitting")
        print("="*60)


def read_orders_from_csv(csv_path):
    """Read order specifications from CSV file"""
    order_specs = []

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)

        # Verify required columns exist
        required_columns = {'SYMBOL', 'CONID', 'EXCHANGE', 'TIME'}
        if not required_columns.issubset(reader.fieldnames):
            raise ValueError(f"CSV must contain columns: {required_columns}")

        for row in reader:
            order_spec = OrderSpec(
                symbol=row['SYMBOL'].strip(),
                conid=int(row['CONID'].strip()),
                exchange=row['EXCHANGE'].strip(),
                time=row['TIME'].strip()
            )
            order_specs.append(order_spec)

    return order_specs


def main():
    """Main entry point"""
    print("Futures Conditional Orders from CSV")
    print("="*60)

    # CSV file path (same directory as script)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, "EOD FUTURES ORDERS.csv")

    # Read order specifications from CSV
    try:
        order_specs = read_orders_from_csv(csv_path)
        print(f"\nLoaded {len(order_specs)} orders from CSV:")
        for spec in order_specs:
            print(f"  {spec.symbol} (ConID: {spec.conid}, Exchange: {spec.exchange}, Time: {spec.time})")
    except Exception as e:
        print(f"ERROR reading CSV: {e}")
        return

    # Create application instance
    app = FuturesOrderApp(order_specs)

    # Connect to TWS/Gateway on paper trading port
    host = "127.0.0.1"
    port = 7497  # TWS paper trading
    client_id = 2  # Client ID 2: Futures order placement

    print(f"\nConnecting to IB at {host}:{port}...")
    app.connect(host, port, client_id)

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
        print(f"\nAll {app.orders_total} orders created successfully.")
        print("Keeping connection open for 5 seconds...")
        time.sleep(5)
    else:
        print(f"\nWarning: Only {app.orders_created}/{app.orders_total} orders confirmed.")

    # Disconnect
    print("\nDisconnecting from IB...")
    app.disconnect()
    print("Done.")


if __name__ == "__main__":
    main()
