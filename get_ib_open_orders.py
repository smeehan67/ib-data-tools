#!/usr/bin/env python3
"""
Fetch all working orders from TWS/IB Gateway and export to Excel.

Usage:
    python3 get_ib_open_orders.py              # uses default port 7497
    python3 get_ib_open_orders.py -p 7496      # uses specified port (live trading)
    python3 get_ib_open_orders.py --client-id 12
    python3 get_ib_open_orders.py --show-all   # show all orders including API orders

Prerequisites:
  1. pip install ibapi openpyxl
  2. TWS or IB Gateway running on localhost
"""

import argparse
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from openpyxl import Workbook

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.common import TickerId


# =============================================================================
# CONFIGURATION
# =============================================================================

SCRIPT_DIR = Path(__file__).resolve().parent
EXCEL_FILE = SCRIPT_DIR / "ib_open_orders.xlsx"
ADJUST_INPUTS_FILE = SCRIPT_DIR / "adjust_ib_inputs.xlsx"

# IB Connection settings
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 7497  # 7497=TWS paper, 7496=TWS live, 4002=Gateway
DEFAULT_CLIENT_ID = 0  # Client ID 0 can see orders from any source (TWS, other clients)

# Order filtering
# Default: show only TWS manual orders (negative IDs), hide API orders (positive IDs)
# Can be overridden with --show-all command-line flag
FILTER_API_ORDERS = True

# Polling settings for waiting for orders
POLL_INTERVAL = 2  # seconds between retries
POLL_TIMEOUT = 300  # 5 minutes max wait time


# =============================================================================
# EXCEL HELPERS
# =============================================================================

def close_excel_file(filepath):
    """Close file if open in Excel on macOS."""
    try:
        filename = os.path.basename(filepath)
        script = f'''
        tell application "Microsoft Excel"
            if it is running then
                set wbCount to count of workbooks
                repeat with i from 1 to wbCount
                    set w to workbook i
                    if name of w is "{filename}" then
                        close w saving no
                        exit repeat
                    end if
                end repeat
            end if
        end tell
        '''
        subprocess.run(['osascript', '-e', script], capture_output=True, timeout=5)
    except Exception as e:
        print(f"Note: Could not close Excel file: {e}")


def open_excel_file(filepath):
    """Open file in Excel and set zoom to 210%."""
    try:
        subprocess.run(['open', str(filepath)], check=True)
        time.sleep(2)  # Wait for Excel to open the file
        script = f'''
        tell application "Microsoft Excel"
            activate
            set n to count of sheets of active workbook
            repeat with i from 1 to n
                activate object sheet i of active workbook
                set zoom of active window to 210
            end repeat
            activate object sheet 1 of active workbook
        end tell
        '''
        subprocess.run(['osascript', '-e', script], capture_output=True, timeout=5)
        print(f"Opened {filepath.name} in Excel at 210% zoom")
    except Exception as e:
        print(f"ERROR: Could not open file: {e}")


# =============================================================================
# IB API WRAPPER AND CLIENT
# =============================================================================

class IBOrderViewer(EWrapper, EClient):
    """IB API client for viewing open orders."""

    def __init__(self):
        EClient.__init__(self, self)

        # Threading events
        self.connected_event = threading.Event()
        self.orders_received_event = threading.Event()
        self.account_ready_event = threading.Event()

        # Account info
        self.accounts: list[str] = []

        # Order storage
        self.open_orders: list[dict] = []

    # -------------------------------------------------------------------------
    # Connection Callbacks
    # -------------------------------------------------------------------------

    def connectAck(self):
        """Called when connection is acknowledged."""
        pass

    def nextValidId(self, orderId: int):
        """Receives next valid order ID - signals connection is ready."""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Connected. Next valid order ID: {orderId}")
        self.connected_event.set()

    def managedAccounts(self, accountsList: str):
        """Receives comma-separated list of managed accounts."""
        self.accounts = [a.strip() for a in accountsList.split(",") if a.strip()]

    def error(self, reqId: TickerId, errorCode: int, errorString: str,
              advancedOrderRejectJson: str = ""):
        """Handle errors from IB."""
        # Filter out non-critical messages
        if errorCode in [2100, 2104, 2106, 2158, 2119]:  # 2100=unsubscribed, others=market data farm
            pass  # Silently ignore
        elif errorCode == 10167:  # Delayed market data
            pass  # Silently ignore
        elif errorCode == 202:  # Order cancelled
            pass  # Expected when orders fill or get cancelled
        else:
            print(f"[ERROR] ReqId: {reqId}, Code: {errorCode}, Msg: {errorString}")

    def connectionClosed(self):
        """Called when connection is closed."""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Connection closed")

    # -------------------------------------------------------------------------
    # Account Callbacks
    # -------------------------------------------------------------------------

    def accountDownloadEnd(self, accountName: str):
        """Called when account data download is complete."""
        self.account_ready_event.set()

    # -------------------------------------------------------------------------
    # Order Callbacks
    # -------------------------------------------------------------------------

    def openOrder(self, orderId: int, contract: Contract, order: Order,
                  orderState):
        """Receive open order information."""
        # Build description based on security type
        # Use getattr for safe attribute access (attribute names vary by IB version)
        expiry = getattr(contract, 'lastTradeDateOrExpiry', None) or getattr(contract, 'expiry', '') or ''
        strike = getattr(contract, 'strike', '')
        right = getattr(contract, 'right', '')

        if contract.secType == "BAG":
            description = f"{contract.symbol} combo"
        elif contract.secType == "OPT":
            description = f"{contract.symbol} {expiry} {strike} {right}"
        elif contract.secType == "FUT":
            description = f"{contract.symbol} {expiry}"
        else:
            description = contract.symbol

        # Determine price based on order type
        if order.orderType == "LMT":
            price = order.lmtPrice
        elif order.orderType in ("STP", "STP LMT"):
            price = order.auxPrice
        elif order.orderType == "STP LMT":
            price = f"{order.auxPrice}/{order.lmtPrice}"
        else:
            price = ""

        self.open_orders.append({
            "ORDER_ID": orderId,
            "ACCOUNT": order.account,
            "STATUS": orderState.status,
            "ORDER_TYPE": order.orderType,
            "ACTION": order.action,
            "QTY": int(order.totalQuantity),
            "PRICE": price,
            "SYMBOL": contract.symbol,
            "SEC_TYPE": contract.secType,
            "DESCRIPTION": description,
            "CONID": contract.conId,
            "PERM_ID": order.permId,
        })

    def openOrderEnd(self):
        """Called when all open orders have been received."""
        self.orders_received_event.set()


# =============================================================================
# DISPLAY
# =============================================================================

COLUMNS = ["ORDER_ID", "ACCOUNT", "STATUS", "ORDER_TYPE", "ACTION",
           "QTY", "PRICE", "SYMBOL", "SEC_TYPE", "DESCRIPTION", "CONID"]


def print_orders_table(rows):
    """Print orders as a formatted terminal table."""
    if not rows:
        print("No open orders found.")
        return

    # Calculate column widths
    widths = {}
    for col in COLUMNS:
        widths[col] = max(len(col), max(len(str(r[col])) for r in rows))

    # Limit DESCRIPTION width for terminal
    widths["DESCRIPTION"] = min(widths["DESCRIPTION"], 60)

    # Header
    header = "  ".join(f"{col:<{widths[col]}}" for col in COLUMNS)
    print()
    print(header)
    print("  ".join("-" * widths[col] for col in COLUMNS))

    # Rows
    for r in rows:
        parts = []
        for col in COLUMNS:
            val = str(r[col])
            w = widths[col]
            if col == "DESCRIPTION" and len(val) > w:
                val = val[:w - 3] + "..."
            parts.append(f"{val:<{w}}")
        print("  ".join(parts))

    print()
    print(f"  {len(rows)} open order(s)")
    print()


# =============================================================================
# EXCEL OUTPUT
# =============================================================================

def write_excel(rows):
    """Write orders to Excel and open at 210% zoom."""
    close_excel_file(EXCEL_FILE)

    wb = Workbook()
    ws = wb.active
    ws.title = "Open Orders"

    # Header row
    ws.append(COLUMNS)

    # Data rows
    for r in rows:
        ws.append([r[col] for col in COLUMNS])

    wb.save(str(EXCEL_FILE))
    print(f"Written to {EXCEL_FILE.name}")

    open_excel_file(EXCEL_FILE)


# =============================================================================
# MAIN
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch all working orders from TWS/IB Gateway and export to Excel.")
    parser.add_argument("--port", "-p", type=int, default=DEFAULT_PORT,
                        help=f"TWS/Gateway port (default: {DEFAULT_PORT})")
    parser.add_argument("--client-id", "-c", type=int, default=DEFAULT_CLIENT_ID,
                        help=f"Client ID (default: {DEFAULT_CLIENT_ID})")
    parser.add_argument("--show-all", "-a", action="store_true",
                        help="Show all orders including API orders (positive IDs)")
    return parser.parse_args()


def main():
    args = parse_args()

    start_time = time.time()

    while True:
        # Create fresh viewer instance each iteration
        viewer = IBOrderViewer()

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Connecting to IB on {DEFAULT_HOST}:{args.port}...")
        viewer.connect(DEFAULT_HOST, args.port, args.client_id)

        # Start message processing thread
        api_thread = threading.Thread(target=viewer.run)
        api_thread.daemon = True
        api_thread.start()

        # Wait for connection
        if not viewer.connected_event.wait(timeout=10):
            print("Failed to connect to TWS/Gateway")
            sys.exit(1)

        # Prime the connection by requesting account updates (TWS needs this before order requests work)
        # Use first account from managedAccounts callback
        time.sleep(0.1)  # Brief pause for managedAccounts callback
        if not viewer.accounts:
            print("No accounts found")
            viewer.disconnect()
            sys.exit(1)
        account = viewer.accounts[0]
        viewer.reqAccountUpdates(True, account)
        if not viewer.account_ready_event.wait(timeout=10):
            print("Timeout waiting for account data")
            viewer.disconnect()
            sys.exit(1)
        viewer.reqAccountUpdates(False, account)  # Stop account updates

        # Request open orders - client ID 0 sees all orders including manually placed TWS orders
        # First call wakes up TWS's API binding for freshly transmitted BAG/combo orders;
        # second call (2s later) gets the complete list including any newly bound orders.
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Requesting open orders...")
        viewer.reqOpenOrders()

        # Wait for openOrderEnd callback
        if not viewer.orders_received_event.wait(timeout=10):
            print("Timeout waiting for open orders")
            viewer.disconnect()
            sys.exit(1)

        # Second pass: freshly transmitted BAG/combo orders often miss the first response.
        # Don't clear open_orders — accumulate both passes, then deduplicate by ORDER_ID.
        time.sleep(2)
        viewer.orders_received_event.clear()
        viewer.reqOpenOrders()
        if not viewer.orders_received_event.wait(timeout=10):
            print("Timeout waiting for open orders (second pass)")
            viewer.disconnect()
            sys.exit(1)

        # Deduplicate: if an order appeared in both passes, keep the latest version
        seen = {}
        for o in viewer.open_orders:
            seen[o["ORDER_ID"]] = o
        viewer.open_orders = list(seen.values())

        # Filter orders
        orders = [o for o in viewer.open_orders if o["ORDER_ID"] != 0]  # Always exclude zero IDs
        if not args.show_all:  # Filter unless --show-all flag is used
            orders = [o for o in orders if o["ORDER_ID"] < 0]

        # Sort chronologically by permId (IB assigns these sequentially)
        orders.sort(key=lambda o: o["PERM_ID"])

        if orders:
            break  # Exit loop, proceed to display

        # Check timeout
        elapsed = time.time() - start_time
        if elapsed >= POLL_TIMEOUT:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Timeout: No open orders found after 5 minutes.")
            viewer.disconnect()
            sys.exit(0)

        # Wait and retry
        remaining = int(POLL_TIMEOUT - elapsed)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] No orders found. Retrying in {POLL_INTERVAL}s... ({remaining}s remaining)")
        viewer.disconnect()
        time.sleep(POLL_INTERVAL)

    # Display table
    print_orders_table(orders)

    # Write Excel
    write_excel(orders)
    # Also open adjust_ib_inputs.xlsx for easy editing
    if ADJUST_INPUTS_FILE.exists():
        open_excel_file(ADJUST_INPUTS_FILE)

    # Disconnect
    viewer.disconnect()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Disconnected")


if __name__ == "__main__":
    main()
