from ibapi.client import *
from ibapi.wrapper import *
from ibapi.contract import Contract
import csv
import os
import subprocess
import time
from threading import Timer
from openpyxl import Workbook
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
EXCEL_FILE = str(SCRIPT_DIR / "conid_outputs.xlsx")
CSV_FILE = ""  # Set to "conid_outputs.csv" to enable CSV output
EXCEL_ZOOM_PERCENT = 100          # Zoom level when opening Excel
OPEN_EXCEL_IN_BACKGROUND = True   # Set to True to open Excel without stealing focus


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
        print(f"Closed {filename} if it was open in Excel")
    except Exception as e:
        print(f"Note: Could not close Excel file: {e}")


def open_excel_file(filepath, zoom=210, background=False):
    """Open file in Excel and set zoom to 210%."""
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


class ConIdApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.conids = []
        self.done = False
        self.contracts_requested = 0
        self.contracts_received = 0

    def nextValidId(self, orderId):
        self.nextOrderId = orderId
        self.start()

    def contractDetails(self, reqId, contractDetails):
        symbol = contractDetails.contract.symbol
        conid = contractDetails.contract.conId
        multiplier = contractDetails.contract.multiplier
        last_trade_date = contractDetails.contract.lastTradeDateOrContractMonth

        # Special handling for Silver (SI) - differentiate by multiplier
        if symbol == 'SI':
            if multiplier == '5000':
                # Keep as 'SI' for multiplier 5000
                print(f'SI with multiplier {multiplier} -> SI')
            elif multiplier == '1000':
                # Rename to 'SI2' for multiplier 1000
                symbol = 'SI2'
                print(f'SI with multiplier {multiplier} -> SI2')

        self.conids.append((symbol, conid, last_trade_date))
        print(f'{symbol}: {conid} (Last Trade Date: {last_trade_date})')
        self.contracts_received += 1

        # Check if all contracts have been received
        if self.contracts_received >= self.contracts_requested:
            print('All contract details received. Writing to CSV...')
            Timer(1, self.write_and_stop).start()

    def contractDetailsEnd(self, reqId):
        pass

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        # Filter out non-critical IB status messages
        if errorCode not in [2104, 2106, 2158, 2119, 10167]:
            print(f"Error {errorCode}: {errorString}")

    def start(self):
        # Read input CSV
        markets = []
        with open(SCRIPT_DIR / 'conid_inputs.csv', 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbol = row['SYMBOL'].strip()
                exchange = row['EXCHANGE'].strip()
                markets.append((symbol, exchange))

        # Count total requests (SI gets requested twice)
        total_requests = len(markets)
        for symbol, exchange in markets:
            if symbol == 'SI':
                total_requests += 1  # SI gets 2 requests

        print(f'Requesting contract details for {len(markets)} symbols ({total_requests} total requests)...\n')
        self.contracts_requested = total_requests

        # Request contract details for each symbol
        request = 0
        for symbol, exchange in markets:
            contract = Contract()
            contract.secType = "CONTFUT"
            contract.exchange = exchange
            contract.symbol = symbol

            # Special handling for Silver - request with multiplier 5000 first
            if symbol == 'SI':
                contract.multiplier = '5000'
                self.reqContractDetails(request, contract)
                request += 1

                # Request SI again with multiplier 1000
                contract2 = Contract()
                contract2.secType = "CONTFUT"
                contract2.exchange = exchange
                contract2.symbol = symbol
                contract2.multiplier = '1000'
                self.reqContractDetails(request, contract2)
                request += 1
            else:
                self.reqContractDetails(request, contract)
                request += 1

    def write_and_stop(self):
        # Sort by symbol alphabetically
        self.conids.sort(key=lambda x: x[0])

        # Write results to CSV (optional)
        if CSV_FILE:
            with open(CSV_FILE, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['SYMBOL', 'CONID', 'LAST_TRADE_DATE'])
                for symbol, conid, last_trade_date in self.conids:
                    writer.writerow([symbol, conid, last_trade_date])
            print(f'\nSuccessfully wrote {len(self.conids)} contract IDs to {CSV_FILE}')

        # Write results to Excel
        close_excel_file(EXCEL_FILE)
        wb = Workbook()
        ws = wb.active
        ws.append(['SYMBOL', 'CONID', 'LAST_TRADE_DATE'])
        for symbol, conid, last_trade_date in self.conids:
            # Convert last_trade_date to integer if it's a valid number string
            try:
                last_trade_date_num = int(last_trade_date) if last_trade_date else last_trade_date
            except (ValueError, TypeError):
                last_trade_date_num = last_trade_date
            ws.append([symbol, conid, last_trade_date_num])
        wb.save(EXCEL_FILE)
        print(f'Successfully wrote {len(self.conids)} contract IDs to {EXCEL_FILE}')
        open_excel_file(EXCEL_FILE, zoom=EXCEL_ZOOM_PERCENT, background=OPEN_EXCEL_IN_BACKGROUND)

        self.done = True
        self.disconnect()


def main():
    app = ConIdApp()
    app.connect("127.0.0.1", 7497, 11)  # Client ID 11: Contract details lookup
    app.run()


if __name__ == "__main__":
    main()
