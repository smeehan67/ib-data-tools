#!/usr/bin/env python3
"""
aggregate_greeks.py - Aggregate Delta Calculator

Reads a CSV of positions (ETFs, futures, options), connects to IB API,
fetches live position quantities + market data, computes contract delta
per position, and prints a summary table.

CSV Format:
    SYMBOL,EXCHANGE,CONID
    ES,CME,
    SPY,ARCA,756733

Fields:
    SYMBOL:   Instrument symbol
    EXCHANGE: Exchange (e.g. CME, ARCA, SMART)
    CONID:    Contract ID — empty for FUT/FOP (matched by symbol+exchange),
              non-empty for ETF/STK (matched by conid)
"""

import argparse
import csv
import fcntl
import json
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from openpyxl import Workbook

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract


# =============================================================================
# CONFIGURATION
# =============================================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(SCRIPT_DIR, "greeks_input.csv")
CACHE_FILE = os.path.join(SCRIPT_DIR, "positions_cache.json")
IB_HOST = "127.0.0.1"
IB_PORT = 7497   # TWS paper trading
CLIENT_ID = 11   # Client ID 11: Greeks/delta aggregator
MARKET_DATA_TIMEOUT = 10  # seconds to wait for market data
OUTPUT_XLSX = os.path.join(SCRIPT_DIR, "greeks_output.xlsx")
OPEN_EXCEL_ON_COMPLETION = True
EXCEL_ZOOM_PERCENT = 100
OPEN_EXCEL_IN_BACKGROUND = True
# When True, contract_delta = quantity * delta * multiplier for all types,
# matching Risk Navigator's Delta(Δ) column (e.g. ZN FOP × 1000).
# When False, FOP/FUT contract_delta = quantity * delta (futures-equivalent
# contracts), which is easier to interpret as "how many futures am I long/short".
APPLY_CONTRACT_MULTIPLIER = True
RUN_IN_BACKGROUND = True          # Set to True to detach from Terminal (window closes automatically)


# =============================================================================
# BACKGROUND MODE
# =============================================================================

def daemonize_if_needed():
    """Re-launch as a detached daemon process if RUN_IN_BACKGROUND is set."""
    if not RUN_IN_BACKGROUND:
        return  # Normal foreground behavior

    if os.environ.get('AGGREGATE_GREEKS_DAEMON') == '1':
        return  # Already the daemon process; proceed normally

    # Re-launch self as a detached child process
    log_path = os.path.join(SCRIPT_DIR, 'aggregate_greeks.log')
    with open(log_path, 'w') as log_file:
        new_env = os.environ.copy()
        new_env['AGGREGATE_GREEKS_DAEMON'] = '1'
        new_env['PYTHONUNBUFFERED'] = '1'
        subprocess.Popen(
            [sys.executable] + sys.argv,
            stdout=log_file,
            stderr=log_file,
            start_new_session=True,
            cwd=SCRIPT_DIR,
            env=new_env,
        )
    sys.exit(0)  # Parent exits immediately; Terminal window can close


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class PositionSpec:
    symbol: str
    exchange: str
    conid: int  # 0 means match by symbol+exchange (FUT/FOP)


@dataclass
class PositionResult:
    symbol: str
    underlying: str       # root symbol for grouping (e.g. "SPY", "GLD", "ES")
    sec_type: str         # STK, FUT, OPT, FOP
    quantity: float
    delta: float          # 1.0 for STK/FUT; option delta for OPT/FOP
    price: float          # mid or last price
    und_price: float      # underlying price (same as price for non-options)
    multiplier: float     # 1 for STK, contract multiplier for FUT/FOP/OPT
    # Computed fields
    contract_delta: float = 0.0   # quantity * delta

    def compute(self):
        if APPLY_CONTRACT_MULTIPLIER or self.sec_type == "OPT":
            self.contract_delta = self.quantity * self.delta * self.multiplier
        else:
            self.contract_delta = self.quantity * self.delta


# =============================================================================
# CSV READING
# =============================================================================

def read_specs_from_csv(csv_path: str) -> list[PositionSpec]:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    specs = []

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        if reader.fieldnames is None:
            raise ValueError("CSV file is empty or has no header")

        actual_columns = {col.strip() for col in reader.fieldnames}
        required = {'SYMBOL', 'EXCHANGE', 'CONID'}
        missing = required - actual_columns
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")

        for row in reader:
            symbol = row.get('SYMBOL', '').strip().upper()
            exchange = row.get('EXCHANGE', '').strip().upper()
            conid_str = row.get('CONID', '').strip()

            if not symbol or not exchange:
                print(f"  WARNING: Skipping row missing SYMBOL or EXCHANGE")
                continue

            try:
                conid = int(conid_str) if conid_str else 0
            except ValueError:
                print(f"  WARNING: Invalid CONID '{conid_str}' for {symbol}, skipping")
                continue

            specs.append(PositionSpec(symbol=symbol, exchange=exchange, conid=conid))

    return specs


# =============================================================================
# CACHE HELPERS
# =============================================================================

def load_cache(cache_path: str) -> dict | None:
    """Return parsed cache dict, or None if missing/invalid."""
    try:
        with open(cache_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def is_cache_valid(cache: dict) -> bool:
    """True if cache is from today's calendar date."""
    return cache.get('date') == datetime.now().date().isoformat()


def save_cache(cache_path: str,
               conid_details: dict):
    """Serialize contract details (multipliers) to JSON. Positions are always fetched live."""
    data = {
        'date': datetime.now().date().isoformat(),
        'contract_details': {
            str(conid): {'multiplier': mult, 'sec_type': st}
            for conid, (mult, st) in conid_details.items()
        },
    }
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)


# =============================================================================
# EXCEL HELPERS
# =============================================================================

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


# =============================================================================
# IB API APPLICATION CLASS
# =============================================================================

class GreeksCalculator(EWrapper, EClient):
    def __init__(self, specs: list[PositionSpec]):
        EClient.__init__(self, self)
        self.specs = specs
        self.lock = threading.Lock()
        self.connected_event = threading.Event()
        self.positions_event = threading.Event()
        self.details_events: dict[int, threading.Event] = {}
        self.next_req_id: int = 100

        # Phase 3: positions from IB keyed by conid
        # conid -> (Contract, quantity)
        self.ib_positions: dict[int, tuple[Contract, float]] = {}

        # Phase 4: matched positions — spec -> (contract, quantity)
        self.matched: list[tuple[PositionSpec, Contract, float]] = []

        # Phase 5: contract details keyed by req_id -> (multiplier, sec_type)
        self.contract_details: dict[int, tuple[float, str]] = {}
        # conid -> (multiplier, sec_type) for caching
        self.conid_details: dict[int, tuple[float, str]] = {}

        # Phase 6: market data collection
        # req_id -> partial price/delta data
        self.mkt_data: dict[int, dict] = {}
        self.mkt_data_ready: dict[int, threading.Event] = {}

        # Final results
        self.results: list[PositionResult] = []

    # -------------------------------------------------------------------------
    # Connection & Error Callbacks
    # -------------------------------------------------------------------------

    def nextValidId(self, orderId: int):
        print(f"[{self._ts()}] Connected. Next valid order ID: {orderId}")
        self.connected_event.set()

    def error(self, reqId, errorCode: int, errorString: str,
              advancedOrderRejectJson: str = ""):
        if errorCode in [2104, 2106, 2158, 2119, 10167]:
            pass  # Non-critical market data farm messages
        else:
            print(f"[{self._ts()}] ERROR {errorCode}: {errorString} (reqId={reqId})")

    def connectionClosed(self):
        print(f"[{self._ts()}] Connection closed")

    # -------------------------------------------------------------------------
    # Position Callbacks
    # -------------------------------------------------------------------------

    def position(self, account: str, contract: Contract, position: float,
                 avgCost: float):
        with self.lock:
            if position != 0:
                self.ib_positions[contract.conId] = (contract, float(position))

    def positionEnd(self):
        self.positions_event.set()

    # -------------------------------------------------------------------------
    # Contract Details Callbacks
    # -------------------------------------------------------------------------

    def contractDetails(self, reqId: int, contractDetails):
        multiplier = 1.0
        try:
            m = contractDetails.contract.multiplier
            if m and str(m).strip():
                multiplier = float(m)
        except (ValueError, AttributeError):
            pass

        sec_type = contractDetails.contract.secType or ""

        with self.lock:
            self.contract_details[reqId] = (multiplier, sec_type)

    def contractDetailsEnd(self, reqId: int):
        with self.lock:
            ev = self.details_events.get(reqId)
        if ev:
            ev.set()

    # -------------------------------------------------------------------------
    # Market Data Callbacks
    # -------------------------------------------------------------------------

    def tickPrice(self, reqId: int, tickType: int, price: float, attrib):
        if price <= 0:
            return
        with self.lock:
            if reqId not in self.mkt_data:
                return
            md = self.mkt_data[reqId]
            # tickType 1=BID, 2=ASK, 4=LAST, 9=CLOSE
            # Delayed: 67=BID, 68=ASK, 71=LAST, 75=CLOSE
            if tickType in (1, 67):
                md['bid'] = price
            elif tickType in (2, 68):
                md['ask'] = price
            elif tickType in (4, 71):
                md['last'] = price
            elif tickType in (9, 75):
                md['close'] = price
            self._check_mkt_data_ready(reqId, md)

    def tickOptionComputation(self, reqId: int, tickType: int, tickAttrib,
                              impliedVol: float, delta: float, optPrice: float,
                              pvDividend: float, gamma: float, vega: float,
                              theta: float, undPrice: float):
        # Live: 10=BID_OPTION, 11=ASK_OPTION, 13=MODEL_OPTION
        # Delayed: 53=DELAYED_BID, 54=DELAYED_ASK, 55=DELAYED_LAST
        if delta is None or delta == -2.0:
            return

        with self.lock:
            if reqId not in self.mkt_data:
                return
            md = self.mkt_data[reqId]
            if tickType in (10, 11, 13, 53, 54, 55):
                # Model delta (13) is preferred; otherwise take first available
                if 'delta' not in md or tickType == 13:
                    md['delta'] = delta
                if undPrice and undPrice > 0:
                    md['und_price'] = undPrice
            self._check_mkt_data_ready(reqId, md)

    def _check_mkt_data_ready(self, reqId: int, md: dict):
        """Must be called with self.lock held."""
        is_option = md.get('is_option', False)
        has_price = ('bid' in md and 'ask' in md) or 'last' in md or 'close' in md

        # Options only need delta (und_price comes with tickOptionComputation)
        # Non-options need a price (delta is always 1.0)
        ready = (is_option and 'delta' in md) or (not is_option and has_price)

        if ready:
            ev = self.mkt_data_ready.get(reqId)
            if ev and not ev.is_set():
                ev.set()

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _ts(self) -> str:
        return datetime.now().strftime("%H:%M:%S")

    def _next_req_id(self) -> int:
        rid = self.next_req_id
        self.next_req_id += 1
        return rid

    # -------------------------------------------------------------------------
    # Workflow Phases
    # -------------------------------------------------------------------------

    def run_workflow(self, force_refresh: bool = False):
        # Phase 1: Fetch all positions live from IB (always fresh — quantities change intraday)
        print(f"\n{'='*60}")
        print("Phase 1: Fetching positions from IB")
        print(f"{'='*60}\n")

        # Load cached contract details only (multipliers never change intraday)
        cache = None if force_refresh else load_cache(CACHE_FILE)
        if cache and is_cache_valid(cache):
            for conid_str, d in cache['contract_details'].items():
                self.conid_details[int(conid_str)] = (d['multiplier'], d['sec_type'])

        self.reqPositions()
        if not self.positions_event.wait(timeout=15):
            print("WARNING: Timed out waiting for positions")
        self.cancelPositions()
        print(f"[{self._ts()}] Received {len(self.ib_positions)} position(s) from IB")

        # Phase 4: Match CSV specs to IB positions
        print(f"\n{'='*60}")
        print("Phase 2: Matching CSV entries to positions")
        print(f"{'='*60}\n")

        for spec in self.specs:
            if spec.conid != 0:
                # ETF/STK: match by conid
                if spec.conid in self.ib_positions:
                    contract, qty = self.ib_positions[spec.conid]
                    if not contract.exchange:
                        contract.exchange = spec.exchange or "SMART"
                    self.matched.append((spec, contract, qty))
                    print(f"  Matched {spec.symbol} (conid={spec.conid}): qty={qty}")
                else:
                    print(f"  WARNING: {spec.symbol} conid={spec.conid} not found in positions")
            else:
                # FUT/FOP: match by symbol + exchange; may match multiple expirations
                found = False
                for conid, (contract, qty) in self.ib_positions.items():
                    if (contract.symbol == spec.symbol and
                            contract.secType in ("FUT", "CONTFUT") and
                            (contract.exchange == spec.exchange or not contract.exchange)):
                        if not contract.exchange:
                            contract.exchange = spec.exchange
                        self.matched.append((spec, contract, qty))
                        print(f"  Matched {spec.symbol} @ {spec.exchange} "
                              f"(conid={conid}): qty={qty}")
                        found = True
                if not found:
                    print(f"  WARNING: {spec.symbol} @ {spec.exchange} not found in positions")

        # Auto-discover options for matched underlyings
        # Build symbol -> exchange map from CSV for fallback exchange lookup
        csv_symbol_exchange = {spec.symbol: spec.exchange for spec in self.specs}
        csv_symbols = set(csv_symbol_exchange.keys())
        already_matched_conids = {contract.conId for _, contract, _ in self.matched}

        for conid, (contract, qty) in self.ib_positions.items():
            if conid in already_matched_conids:
                continue
            if contract.secType not in ("OPT", "FOP"):
                continue
            if contract.symbol not in csv_symbols:
                continue
            display_name = contract.localSymbol or contract.symbol
            # Ensure exchange is set — position callbacks often leave it blank.
            # For FOPs use the CSV's exchange (e.g. CME); for OPTs default SMART.
            if not contract.exchange:
                if contract.secType == "FOP":
                    contract.exchange = csv_symbol_exchange.get(contract.symbol, "SMART")
                else:
                    contract.exchange = "SMART"
            synthetic_spec = PositionSpec(
                symbol=display_name,
                exchange=contract.exchange,
                conid=conid,
            )
            self.matched.append((synthetic_spec, contract, qty))
            print(f"  Auto-matched option {display_name} "
                  f"(conid={conid}): qty={qty}")

        if not self.matched:
            print("\nNo positions matched. Nothing to compute.")
            return

        # Phase 3: Get contract details for multiplier and sec_type
        print(f"\n{'='*60}")
        print("Phase 3: Fetching contract details")
        print(f"{'='*60}\n")

        detail_req_map: dict[int, tuple[PositionSpec, Contract, float]] = {}
        # Items whose details came from cache (keyed by conid for later lookup)
        cached_details: dict[int, tuple[PositionSpec, Contract, float]] = {}

        for spec, contract, qty in self.matched:
            if contract.conId in self.conid_details:
                cached_details[contract.conId] = (spec, contract, qty)
                continue
            req_id = self._next_req_id()
            ev = threading.Event()
            with self.lock:
                self.details_events[req_id] = ev
            detail_req_map[req_id] = (spec, contract, qty)

            # Build a minimal contract with conid to look up details
            lookup = Contract()
            lookup.conId = contract.conId
            lookup.exchange = contract.exchange or "SMART"
            self.reqContractDetails(req_id, lookup)

        # Wait for all detail responses
        for req_id, ev in [(r, self.details_events[r]) for r in detail_req_map]:
            ev.wait(timeout=10)

        # Populate conid_details from fresh API responses
        for req_id, (spec, contract, qty) in detail_req_map.items():
            if req_id in self.contract_details:
                self.conid_details[contract.conId] = self.contract_details[req_id]

        # Save cache whenever we fetched anything from IB
        if detail_req_map or not (cache and is_cache_valid(cache)):
            save_cache(CACHE_FILE, self.conid_details)
            print(f"[{self._ts()}] Cache saved")

        print(f"[{self._ts()}] Contract details received")

        # Phase 4: Request market data
        print(f"\n{'='*60}")
        print("Phase 4: Requesting market data")
        print(f"{'='*60}\n")

        self.reqMarketDataType(4)  # Delayed frozen — returns last cached data when market closed

        mkt_req_map: dict[int, tuple[PositionSpec, Contract, float, float, str]] = {}

        # Combine freshly-fetched and cached-detail items into one iterable
        all_items: list[tuple[PositionSpec, Contract, float]] = (
            list(detail_req_map.values()) +
            list(cached_details.values())
        )

        for spec, contract, qty in all_items:
            multiplier, sec_type = self.conid_details.get(
                contract.conId, (1.0, contract.secType or "STK"))
            if not sec_type:
                sec_type = "STK"

            is_option = sec_type in ("OPT", "FOP")

            mkt_req_id = self._next_req_id()
            mkt_ev = threading.Event()
            with self.lock:
                self.mkt_data[mkt_req_id] = {'is_option': is_option}
                self.mkt_data_ready[mkt_req_id] = mkt_ev

            mkt_req_map[mkt_req_id] = (spec, contract, qty, multiplier, sec_type)

            # Request market data; for options also request greek ticks
            tick_list = "106" if is_option else ""
            self.reqMktData(mkt_req_id, contract, tick_list, False, False, [])
            print(f"  Subscribed: {spec.symbol} ({sec_type}) reqId={mkt_req_id}")

        # Wait for market data (with timeout)
        deadline = time.time() + MARKET_DATA_TIMEOUT
        for mkt_req_id, ev in [(r, self.mkt_data_ready[r]) for r in mkt_req_map]:
            remaining = deadline - time.time()
            if remaining > 0:
                ev.wait(timeout=remaining)

        # Cancel all subscriptions
        for mkt_req_id in mkt_req_map:
            try:
                self.cancelMktData(mkt_req_id)
            except Exception:
                pass

        # Phase 7: Compute results
        print(f"\n{'='*60}")
        print("Phase 5: Computing results")
        print(f"{'='*60}\n")

        for mkt_req_id, (spec, contract, qty, multiplier, sec_type) in mkt_req_map.items():
            with self.lock:
                md = self.mkt_data.get(mkt_req_id, {})

            # Determine price (mid preferred, fallback to last, then close)
            price = 0.0
            if 'bid' in md and 'ask' in md:
                price = (md['bid'] + md['ask']) / 2.0
            elif 'last' in md:
                price = md['last']
            elif 'close' in md:
                price = md['close']

            # Determine delta
            if sec_type in ("OPT", "FOP"):
                delta = md.get('delta', 0.0)
                und_price = md.get('und_price', price)
                if delta == 0.0:
                    print(f"  WARNING: No delta for {spec.symbol} ({sec_type}), skipping")
                    continue
            else:
                delta = 1.0
                if price == 0.0:
                    print(f"  WARNING: No price data for {spec.symbol}, skipping")
                    continue
                und_price = price

            result = PositionResult(
                symbol=spec.symbol,
                underlying=contract.symbol,
                sec_type=sec_type,
                quantity=qty,
                delta=delta,
                price=price,
                und_price=und_price,
                multiplier=multiplier,
            )
            result.compute()
            self.results.append(result)

        self._print_summary()

    def _print_summary(self):
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}\n")

        # Seed all input symbols at 0 so missing positions/deltas still appear
        groups: dict[str, float] = {spec.symbol: 0.0 for spec in self.specs}

        # Accumulate actual results
        for r in self.results:
            groups[r.underlying] = groups.get(r.underlying, 0.0) + r.contract_delta

        hdr = f"{'SYMBOL':<12} {'CONTRACT Δ':>12}"
        sep = f"{'-'*12} {'-'*12}"

        print(hdr)
        print(sep)

        total_contract_delta = 0.0

        for symbol in sorted(groups):
            cd = groups[symbol]
            cd_str = f"{cd:+.2f}"
            print(f"{symbol:<12} {cd_str:>12}")
            total_contract_delta += cd

        print(sep)

        cd_total_str = f"{total_contract_delta:+.2f}"
        print(f"{'TOTAL':<12} {cd_total_str:>12}")
        print()

        self._write_excel(groups)

    def _write_excel(self, groups: dict[str, float]):
        wb = Workbook()
        ws = wb.active
        ws.append(['SYMBOL', 'DELTA'])
        for symbol in sorted(groups):
            ws.append([symbol, groups[symbol]])
        wb.save(OUTPUT_XLSX)
        print(f"Results written to {os.path.basename(OUTPUT_XLSX)}")
        if OPEN_EXCEL_ON_COMPLETION:
            open_excel_file(OUTPUT_XLSX, zoom=EXCEL_ZOOM_PERCENT,
                            background=OPEN_EXCEL_IN_BACKGROUND)


# =============================================================================
# MAIN
# =============================================================================

def main():
    daemonize_if_needed()

    parser = argparse.ArgumentParser(description="Aggregate Greeks Calculator")
    parser.add_argument('--refresh', action='store_true',
                        help='Force refresh of positions cache')
    args = parser.parse_args()

    print("Aggregate Greeks Calculator")
    print("=" * 60)

    close_excel_file(OUTPUT_XLSX)

    try:
        specs = read_specs_from_csv(CSV_FILE)
        print(f"\nLoaded {len(specs)} spec(s) from {os.path.basename(CSV_FILE)}:\n")
        for s in specs:
            conid_label = f"conid={s.conid}" if s.conid else "match by symbol+exchange"
            print(f"  {s.symbol} @ {s.exchange}  ({conid_label})")
    except Exception as e:
        print(f"ERROR reading CSV: {e}")
        sys.exit(1)

    if not specs:
        print("No valid entries found in CSV.")
        sys.exit(0)

    app = GreeksCalculator(specs)

    print(f"\nConnecting to IB {IB_HOST}:{IB_PORT} (client {CLIENT_ID})...")
    app.connect(IB_HOST, IB_PORT, CLIENT_ID)

    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()

    if not app.connected_event.wait(timeout=10):
        print("Failed to connect to TWS/Gateway")
        sys.exit(1)

    try:
        app.run_workflow(force_refresh=args.refresh)
    except Exception as e:
        print(f"\n[ERROR] {e}")
        raise
    finally:
        time.sleep(0.5)
        app.disconnect()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Disconnected")


if __name__ == "__main__":
    main()
