#!/usr/bin/env python3
"""
Downloads last prices for contracts specified in last_inputs.csv via IB API.
Reads SYMBOL,CONID pairs and writes SYMBOL,LAST to last_outputs.csv.
"""

import csv
import fcntl
import os
import subprocess
import sys
import threading
import time
from decimal import Decimal
from pathlib import Path
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from openpyxl import Workbook

SCRIPT_DIR = Path(__file__).resolve().parent


# ================================================================
# CONFIGURATION
# ================================================================

OPEN_EXCEL_ON_COMPLETION = True   # Set to False to skip opening Excel after download
EXCEL_ZOOM_PERCENT = 100          # Zoom level when opening Excel (only used if OPEN_EXCEL_ON_COMPLETION is True)
OPEN_EXCEL_IN_BACKGROUND = True   # Set to True to open last_outputs.xlsx without stealing focus
RUN_IN_BACKGROUND = True          # Set to True to detach from Terminal (window closes automatically)


# ================================================================
# BACKGROUND MODE
# ================================================================

def daemonize_if_needed():
    """Re-launch as a detached daemon process if RUN_IN_BACKGROUND is set."""
    if not RUN_IN_BACKGROUND:
        return  # Normal foreground behavior

    if os.environ.get('LAST_PRICES_DAEMON') == '1':
        return  # Already the daemon process; proceed normally

    # Re-launch self as a detached child process
    log_path = SCRIPT_DIR / 'last_prices.log'
    with open(log_path, 'w') as log_file:
        new_env = os.environ.copy()
        new_env['LAST_PRICES_DAEMON'] = '1'
        new_env['PYTHONUNBUFFERED'] = '1'
        subprocess.Popen(
            [sys.executable] + sys.argv,
            stdout=log_file,
            stderr=log_file,
            start_new_session=True,
            cwd=str(SCRIPT_DIR),
            env=new_env,
        )
    sys.exit(0)  # Parent exits immediately; Terminal window can close


# ================================================================
# DATA STRUCTURES
# ================================================================

class PriceData:
    """Stores last price for a contract."""
    def __init__(self, symbol: str, conid: int):
        self.symbol = symbol
        self.conid = conid
        self.last_price = None
        self.received = False


# ================================================================
# UTILITY FUNCTIONS
# ================================================================

def close_excel_file(filepath):
    """Close file if open in Excel on macOS."""
    try:
        filename = os.path.basename(filepath)
        # Use AppleScript to close the workbook in Excel
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
        print(f"Closed {filename} if it was open in Excel")
    except Exception as e:
        print(f"Note: Could not close Excel file: {e}")


def open_excel_file(filepath, zoom=210, background=False):
    """Open file in Excel and set zoom."""
    try:
        if background:
            script = f'''
tell application "System Events"
    set frontApp to name of first application process whose frontmost is true
end tell
set activeWB to ""
tell application "Microsoft Excel"
    if it is running then
        try
            set activeWB to name of active workbook
        end try
    end if
    open POSIX file "{filepath}"
end tell
delay 1
if frontApp is "Microsoft Excel" and activeWB is not "" then
    set wbMenuName to activeWB
    if wbMenuName ends with ".xlsx" or wbMenuName ends with ".xlsm" then
        set wbMenuName to text 1 thru -6 of wbMenuName
    else if wbMenuName ends with ".xls" then
        set wbMenuName to text 1 thru -5 of wbMenuName
    end if
    tell application "System Events"
        tell process "Microsoft Excel"
            click menu bar item "Window" of menu bar 1
            delay 0.3
            click menu item wbMenuName of menu 1 of menu bar item "Window" of menu bar 1
        end tell
    end tell
else
    tell application frontApp to activate
end if
'''
            # Use a lock file to prevent concurrent AppleScript UI clicks from
            # multiple scripts running simultaneously (would leave menus stuck open)
            with open('/tmp/excel_open_lock', 'w') as lf:
                fcntl.flock(lf, fcntl.LOCK_EX)
                subprocess.run(['osascript', '-e', script], capture_output=True, timeout=15)
            print(f"Opened {filepath} in Excel (background, focus restored)")
        else:
            subprocess.run(['open', filepath], check=True)
            if zoom != 100:
                time.sleep(2)  # Wait for Excel to open the file
                script = f'''
                tell application "Microsoft Excel"
                    activate
                    set zoom of active window to {zoom}
                end tell
                '''
                subprocess.run(['osascript', '-e', script], capture_output=True, timeout=5)
                print(f"Opened {filepath} in Excel at {zoom}% zoom")
            else:
                print(f"Opened {filepath} in Excel")
    except Exception as e:
        print(f"ERROR: Could not open file: {e}")


# ================================================================
# MAIN APPLICATION CLASS
# ================================================================

class LastPriceApp(EWrapper, EClient):
    """IB API application for fetching last prices."""

    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, self)

        self.lock = threading.Lock()
        self.next_req_id = 1
        self.price_data = {}  # req_id -> PriceData
        self.req_id_map = {}  # conid -> req_id
        self.all_received = False

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        """Handle IB API errors."""
        # Filter out informational messages
        if errorCode in [2104, 2106, 2158, 2119, 10167]:
            return
        print(f"Error {errorCode}: {errorString} (ReqId: {reqId})")

    def nextValidId(self, orderId: int):
        """Callback when connection is established."""
        print("Connected to IB. Starting data requests...")
        self.next_req_id = orderId
        self.request_all_prices()

    def tickPrice(self, reqId, tickType, price, attrib):
        """Receive price updates."""
        # TickType 4 = LAST price
        if tickType == 4 and reqId in self.price_data:
            with self.lock:
                data = self.price_data[reqId]
                data.last_price = Decimal(str(price))
                data.received = True
                print(f"Received last price for {data.symbol}: {price}")

    def tickSnapshotEnd(self, reqId: int):
        """Called when snapshot is complete."""
        if reqId in self.price_data:
            with self.lock:
                data = self.price_data[reqId]
                if not data.received:
                    print(f"No last price data received for {data.symbol}")
                    data.last_price = None
                    data.received = True

    def request_all_prices(self):
        """Read CSV and request market data for all contracts."""
        try:
            with open(SCRIPT_DIR / 'last_inputs.csv', 'r') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header

                for row in reader:
                    if len(row) < 2:
                        continue

                    symbol = row[0].strip()
                    conid = int(row[1].strip())

                    req_id = self.next_req_id
                    self.next_req_id += 1

                    # Create contract using CONID
                    contract = Contract()
                    contract.conId = conid
                    contract.exchange = "SMART"

                    # Store price data tracker
                    data = PriceData(symbol, conid)
                    self.price_data[req_id] = data
                    self.req_id_map[conid] = req_id

                    # Request snapshot (not streaming)
                    print(f"Requesting price for {symbol} (CONID: {conid})...")
                    self.reqMktData(req_id, contract, "", True, False, [])

                    # Small delay between requests
                    time.sleep(0.1)

        except FileNotFoundError:
            print("ERROR: last_inputs.csv not found!")
            self.disconnect()
            return
        except Exception as e:
            print(f"ERROR reading CSV: {e}")
            self.disconnect()
            return

        # Schedule check for completion
        threading.Timer(5.0, self.check_completion).start()

    def check_completion(self):
        """Check if all data received and write results."""
        with self.lock:
            all_done = all(data.received for data in self.price_data.values())

            if not all_done:
                # Check again in 2 seconds
                threading.Timer(2.0, self.check_completion).start()
                return

            # All data received, write CSV and Excel
            self.write_results()
            self.all_received = True

            # Open the Excel output file
            if OPEN_EXCEL_ON_COMPLETION:
                open_excel_file(str(SCRIPT_DIR / 'last_outputs.xlsx'),
                                zoom=EXCEL_ZOOM_PERCENT,
                                background=OPEN_EXCEL_IN_BACKGROUND)

            # Disconnect after short delay
            threading.Timer(1.0, self.disconnect).start()

    def write_results(self):
        """Write results to last_outputs.xlsx."""
        try:
            sorted_data = sorted(self.price_data.values(),
                               key=lambda x: x.symbol)

            wb = Workbook()
            ws = wb.active
            ws.append(['Symbol', 'Last'])
            for data in sorted_data:
                last_val = float(data.last_price) if data.last_price else "N/A"
                ws.append([data.symbol, last_val])
            wb.save(str(SCRIPT_DIR / 'last_outputs.xlsx'))
            print(f"\nResults written to last_outputs.xlsx ({len(self.price_data)} symbols)")

        except Exception as e:
            print(f"ERROR writing results: {e}")


# ================================================================
# MAIN ENTRY POINT
# ================================================================

def main():
    """Main execution."""
    daemonize_if_needed()

    # Close last_outputs.xlsx if open in Excel before starting
    close_excel_file(str(SCRIPT_DIR / 'last_outputs.xlsx'))

    app = LastPriceApp()

    # Connect to TWS paper trading
    print("Connecting to IB TWS (port 7497)...")
    app.connect("127.0.0.1", 7497, clientId=10)  # Client ID 10: Last price queries

    # Run message processing loop in daemon thread
    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()

    # Wait for completion
    try:
        while not app.all_received:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user")

    print("Done.")


if __name__ == "__main__":
    main()
