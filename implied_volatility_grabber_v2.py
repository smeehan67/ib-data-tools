"""
implied_volatility_grabber.py — Fetches implied volatility for the two nearest
monthly option expirations from Interactive Brokers TWS API.

Input:  CSV file with SYMBOL,EXCHANGE[,CONID] columns (same format as
        futures_data_grabber_v2.py).
Output: CSV file with Symbol,IV_Front,IV_Second columns.

Four-phase approach:
  Phase 1 — Resolve underlying contracts and fetch last prices (single connection).
  Phase 2 — Fetch option-chain parameters: expirations & strikes (single connection).
  Phase 3 — Resolve ATM option contracts to get conIds (single connection).
  Phase 4 — Subscribe to market data for resolved options and collect IV (single connection).
"""

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import csv
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
import json
from pathlib import Path
from openpyxl import Workbook


# ================================================================
# EXCEL HELPERS
# ================================================================

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
        print(f"  Note: Could not close Excel file: {e}")


def open_excel_file(filepath, zoom=210, background=False):
    """Open file in Excel and set zoom level."""
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
                # Give Excel time to open the file
                time.sleep(2)
                script = f'''
            tell application "Microsoft Excel"
                activate
                repeat 10 times
                    if (count of workbooks) > 0 then
                        set zoomLevel to {zoom}
                        set zoom of active window to zoomLevel
                        exit repeat
                    end if
                    delay 0.5
                end repeat
            end tell
            '''
                subprocess.run(['osascript', '-e', script], capture_output=True, timeout=10)
                print(f"Opened {filepath} in Excel at {zoom}% zoom")
            else:
                print(f"Opened {filepath} in Excel")
    except Exception as e:
        print(f"  ERROR: Could not open file: {e}")


# ================================================================
# CONFIGURATION
# ================================================================

SCRIPT_DIR = Path(__file__).resolve().parent
SYMBOLS_FILE = str(SCRIPT_DIR / "implied_vol_input.csv")
OUTPUT_FILE = str(SCRIPT_DIR / "implied_volatility.xlsx")
PORT = 7497
IV_WAIT_SECONDS = 15   # seconds to wait for IV data after subscribing
IV_RETRY_SECONDS = 10  # seconds for second pass retry (0 to disable)
MIN_DTE = 15           # minimum days to expiration for front-month option
CACHE_FILE = str(SCRIPT_DIR / "iv_cache.json")
EXCEL_ZOOM_PERCENT = 210          # Zoom level when opening Excel (only used if Excel opens on completion)
OPEN_EXCEL_IN_BACKGROUND = True   # Set to True to open Excel without stealing focus


# ================================================================
# UTILITY FUNCTIONS
# ================================================================

def read_symbols_csv(file_name):
    """Read symbols CSV and return list of (symbol, exchange, conid) tuples.
    conid is '' if not provided."""
    symbols = []
    with open(file_name, "r") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            symbol = row[0].strip()
            exchange = row[1].strip()
            conid = row[2].strip() if len(row) > 2 else ""
            if symbol:
                symbols.append((symbol, exchange, conid))
    return symbols


def make_contract(symbol, exchange, conid=""):
    """Create a Contract for the underlying (CONTFUT or STK)."""
    contract = Contract()
    contract.symbol = symbol
    contract.exchange = exchange
    if conid:
        contract.conId = int(conid)
        contract.secType = "STK"
    else:
        contract.secType = "CONTFUT"
    return contract


def find_atm_strike(price, strikes):
    """Return the strike nearest to price."""
    return min(strikes, key=lambda s: abs(s - price))


def filter_monthly_expirations(expirations):
    """Keep one expiration per calendar month, picking the one closest to the
    3rd Friday (standard monthly option expiration for US markets).
    This filters out weeklies while retaining standard monthly expirations."""
    from calendar import monthcalendar

    by_month = {}  # "YYYYMM" -> list of expiry strings
    for exp in expirations:
        ym = exp[:6]
        if ym not in by_month:
            by_month[ym] = []
        by_month[ym].append(exp)

    result = []
    for ym, exps in sorted(by_month.items()):
        if len(exps) == 1:
            result.append(exps[0])
            continue

        year = int(ym[:4])
        month = int(ym[4:6])
        weeks = monthcalendar(year, month)
        fridays = [w[4] for w in weeks if w[4] != 0]
        third_fri = fridays[2] if len(fridays) >= 3 else fridays[-1]

        best = min(exps, key=lambda e: abs(int(e[6:8]) - third_fri))
        result.append(best)

    return sorted(result)


# ================================================================
# CACHE HELPERS (Phases 1b and 2)
# ================================================================

def load_cache(cache_file):
    """Return (extra_months, chains) from cache if still valid, else (None, None)."""
    if not os.path.exists(cache_file):
        return None, None
    try:
        with open(cache_file) as f:
            data = json.load(f)
        expiry = datetime.strptime(data["cache_expiry"], "%Y%m%d").date()
        if datetime.now().date() < expiry:
            print(f"  Cache valid until {expiry} — skipping Phases 1b and 2")
            return data["extra_months"], data["chains"]
        print(f"  Cache expired ({expiry}) — running full fetch")
    except Exception as e:
        print(f"  Cache load failed ({e}) — running full fetch")
    return None, None


def save_cache(cache_file, extra_months, chains, symbol_expirations, today_date):
    """Save Phase 1b and Phase 2 results with a computed expiry date."""
    if not symbol_expirations:
        return
    min_dte_front = min(
        (datetime.strptime(exp1, "%Y%m%d").date() - today_date).days
        for exp1, _ in symbol_expirations.values()
    )
    valid_days = max(1, min_dte_front - MIN_DTE)
    expiry = today_date + timedelta(days=valid_days)
    data = {
        "cache_expiry": expiry.strftime("%Y%m%d"),
        "extra_months": extra_months,
        "chains": chains,
    }
    try:
        with open(cache_file, "w") as f:
            json.dump(data, f, indent=2)
        print(f"  Cache saved — valid for {valid_days} day(s) until {expiry}")
    except Exception as e:
        print(f"  Cache save failed: {e}")


# ================================================================
# PHASE 1: RESOLVE UNDERLYING CONTRACTS AND GET LAST PRICES
# ================================================================

class ContractPriceResolver(EWrapper, EClient):
    """Single connection: reqContractDetails + reqHistoricalData for each symbol."""

    def __init__(self):
        EClient.__init__(self, self)
        self.detail_reqs = {}   # req_id -> (symbol, exchange, conid)
        self.hist_reqs = {}     # req_id -> (symbol, exchange, conid)
        self.resolved = {}      # symbol -> dict(conId, exchange, tradingClass, multiplier, currency)
        self.prices = {}        # symbol -> float
        self._pending = set()
        self.lock = threading.Lock()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode not in (2104, 2106, 2158):
            print(f"  Error: reqId={reqId} code={errorCode} msg={errorString}")
        if reqId >= 0 and errorCode in (200, 321, 354, 162):
            self._done(reqId)

    def nextValidId(self, orderId):
        for rid, (sym, exch, cid) in self.detail_reqs.items():
            self._pending.add(rid)
            self.reqContractDetails(rid, make_contract(sym, exch, cid))
        for rid, (sym, exch, cid) in self.hist_reqs.items():
            self._pending.add(rid)
            what = "ADJUSTED_LAST" if cid else "TRADES"
            self.reqHistoricalData(
                reqId=rid,
                contract=make_contract(sym, exch, cid),
                endDateTime="",
                durationStr="5 D",
                barSizeSetting="1 day",
                whatToShow=what,
                useRTH=0,
                formatDate=1,
                keepUpToDate=False,
                chartOptions=[],
            )
        threading.Timer(30, self._timeout).start()

    def contractDetails(self, reqId, contractDetails):
        if reqId not in self.detail_reqs:
            return
        symbol = self.detail_reqs[reqId][0]
        if symbol in self.resolved:
            return  # keep first result only
        cd = contractDetails.contract
        self.resolved[symbol] = {
            "conId": cd.conId,
            "exchange": cd.exchange,
            "tradingClass": cd.tradingClass,
            "multiplier": cd.multiplier,
            "currency": cd.currency or "USD",
        }
        print(f"  [{symbol}] conId={cd.conId} class={cd.tradingClass}")

    def contractDetailsEnd(self, reqId):
        self._done(reqId)

    def historicalData(self, reqId, bar):
        if reqId in self.hist_reqs:
            self.prices[self.hist_reqs[reqId][0]] = bar.close

    def historicalDataEnd(self, reqId, start, end):
        if reqId in self.hist_reqs:
            sym = self.hist_reqs[reqId][0]
            print(f"  [{sym}] price={self.prices.get(sym, 'N/A')}")
        self._done(reqId)

    def _done(self, reqId):
        with self.lock:
            self._pending.discard(reqId)
            if not self._pending:
                threading.Timer(1, self.disconnect).start()

    def _timeout(self):
        with self.lock:
            if self._pending:
                print(f"  Phase 1 timeout: {len(self._pending)} requests still pending")
                self._pending.clear()
        self.disconnect()


def resolve_contracts_and_prices(symbols, client_id=0):
    app = ContractPriceResolver()
    rid = 0
    for sym, exch, cid in symbols:
        app.detail_reqs[rid] = (sym, exch, cid)
        rid += 1
        app.hist_reqs[rid] = (sym, exch, cid)
        rid += 1
    app.connect("127.0.0.1", PORT, client_id)
    app.run()
    return app.resolved, app.prices


# ================================================================
# PHASE 1b: RESOLVE NEXT-MONTH FUTURES CONTRACTS
# ================================================================

class FuturesMonthResolver(EWrapper, EClient):
    """For each futures symbol, enumerate all available contract months via
    reqContractDetails(FUT) to find the next-month contract."""

    def __init__(self):
        EClient.__init__(self, self)
        self.reqs = {}          # req_id -> (symbol, exchange, front_conId)
        self.all_months = {}    # symbol -> list of {expiry, conId, exchange, ...}
        self._pending = set()
        self.lock = threading.Lock()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode not in (2104, 2106, 2158):
            print(f"  Error: reqId={reqId} code={errorCode} msg={errorString}")
        if reqId >= 0 and errorCode in (200, 321, 354):
            self._done(reqId)

    def nextValidId(self, orderId):
        for rid, (sym, exch, front_conId) in self.reqs.items():
            self._pending.add(rid)
            c = Contract()
            c.symbol = sym
            c.secType = "FUT"
            c.exchange = exch
            self.reqContractDetails(rid, c)
        if not self._pending:
            threading.Timer(1, self.disconnect).start()
        else:
            threading.Timer(45, self._timeout).start()

    def contractDetails(self, reqId, contractDetails):
        if reqId not in self.reqs:
            return
        sym = self.reqs[reqId][0]
        cd = contractDetails.contract
        exp = getattr(cd, 'lastTradeDateOrExpiry', None) or getattr(cd, 'lastTradeDate', '')
        if sym not in self.all_months:
            self.all_months[sym] = []
        self.all_months[sym].append({
            "expiry": exp,
            "conId": cd.conId,
            "exchange": cd.exchange,
            "tradingClass": cd.tradingClass,
            "multiplier": cd.multiplier,
            "currency": cd.currency or "USD",
        })

    def contractDetailsEnd(self, reqId):
        sym = self.reqs[reqId][0]
        if sym in self.all_months:
            self.all_months[sym].sort(key=lambda x: x["expiry"])
            n = len(self.all_months[sym])
            print(f"  [{sym}] {n} futures month(s)")
        self._done(reqId)

    def _done(self, reqId):
        with self.lock:
            self._pending.discard(reqId)
            if not self._pending:
                threading.Timer(1, self.disconnect).start()

    def _timeout(self):
        with self.lock:
            if self._pending:
                print(f"  Phase 1b timeout: {len(self._pending)} requests still pending")
                self._pending.clear()
        self.disconnect()


def resolve_extra_months(symbols, resolved, num_extra=2, client_id=10):
    """For each futures symbol, find up to num_extra contract months after front-month.

    Returns:
        extra_months: dict of symbol -> list of {conId, exchange, tradingClass, ...} dicts
    """
    app = FuturesMonthResolver()
    rid = 0
    for sym, exch, cid in symbols:
        if cid:  # skip stocks/ETFs
            continue
        if sym not in resolved:
            continue
        app.reqs[rid] = (sym, exch, resolved[sym]["conId"])
        rid += 1

    if not app.reqs:
        return {}

    app.connect("127.0.0.1", PORT, client_id)
    app.run()

    extra_months = {}
    for sym, exch, cid in symbols:
        if cid or sym not in resolved:
            continue
        front_conId = resolved[sym]["conId"]
        front_class = resolved[sym].get("tradingClass", "")
        months = sorted(app.all_months.get(sym, []), key=lambda x: x["expiry"])
        # Filter to same trading class as front-month
        if front_class:
            months = [m for m in months if m["tradingClass"] == front_class]

        # Find front-month in list, take next num_extra
        front_idx = None
        for i, m in enumerate(months):
            if m["conId"] == front_conId:
                front_idx = i
                break

        if front_idx is not None:
            extras = months[front_idx + 1 : front_idx + 1 + num_extra]
        elif len(months) > 1:
            # Fallback: assume first is front, take rest
            extras = months[1 : 1 + num_extra]
        else:
            extras = []

        if extras:
            extra_months[sym] = extras
            for nm in extras:
                print(f"  [{sym}] extra month: conId={nm['conId']} expiry={nm['expiry']}")
        else:
            print(f"  [{sym}] no extra months found")

    return extra_months


# ================================================================
# PHASE 2: GET OPTION CHAIN PARAMETERS
# ================================================================

class OptionChainFetcher(EWrapper, EClient):
    """Single connection: reqSecDefOptParams for each symbol."""

    def __init__(self):
        EClient.__init__(self, self)
        self.reqs = {}      # req_id -> (symbol, sec_type, conId, futFopExchange)
        self.chains = {}    # symbol -> [dict(exchange, tradingClass, multiplier, expirations, strikes)]
        self._pending = set()
        self.lock = threading.Lock()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode not in (2104, 2106, 2158):
            print(f"  Error: reqId={reqId} code={errorCode} msg={errorString}")
        if reqId >= 0 and errorCode in (200, 321, 354):
            self._done(reqId)

    def nextValidId(self, orderId):
        for rid, (sym, sec_type, con_id, fop_exchange) in self.reqs.items():
            self._pending.add(rid)
            self.reqSecDefOptParams(rid, sym, fop_exchange, sec_type, con_id)
        if not self._pending:
            threading.Timer(1, self.disconnect).start()
        else:
            threading.Timer(30, self._timeout).start()

    def securityDefinitionOptionParameter(self, reqId, exchange, underlyingConId,
                                           tradingClass, multiplier, expirations, strikes):
        sym = self.reqs[reqId][0]
        if sym not in self.chains:
            self.chains[sym] = []
        self.chains[sym].append({
            "exchange": exchange,
            "tradingClass": tradingClass,
            "multiplier": multiplier,
            "expirations": sorted(expirations),
            "strikes": sorted(strikes),
            "underlyingConId": underlyingConId,
        })

    def securityDefinitionOptionParameterEnd(self, reqId):
        sym, _, _, _ = self.reqs[reqId]
        n = len(self.chains.get(sym, []))
        print(f"  [{sym}] {n} chain(s)")
        self._done(reqId)

    def _done(self, reqId):
        with self.lock:
            self._pending.discard(reqId)
            if not self._pending:
                threading.Timer(1, self.disconnect).start()

    def _timeout(self):
        with self.lock:
            if self._pending:
                print(f"  Phase 2 timeout: {len(self._pending)} requests still pending")
                self._pending.clear()
        self.disconnect()


def fetch_option_chains(symbols, resolved, extra_months=None, client_id=1):
    app = OptionChainFetcher()
    rid = 0
    for sym, exch, cid in symbols:
        if sym not in resolved:
            continue
        sec_type = "STK" if cid else "FUT"
        fop_exchange = resolved[sym]["exchange"] if not cid else ""
        app.reqs[rid] = (sym, sec_type, resolved[sym]["conId"], fop_exchange)
        rid += 1
        # Also query option chains for extra futures months
        if not cid and extra_months and sym in extra_months:
            for nm in extra_months[sym]:
                app.reqs[rid] = (sym, sec_type, nm["conId"], nm.get("exchange", fop_exchange))
                rid += 1
    if not app.reqs:
        return {}
    app.connect("127.0.0.1", PORT, client_id)
    app.run()
    return app.chains


# ================================================================
# BUILD ATM OPTION CONTRACTS (unresolved — need Phase 3 to get conId)
# ================================================================

def build_option_contracts(symbols, resolved, prices, chains):
    """For each symbol, create ATM call option contracts for the 2 nearest
    monthly expirations.

    Monthly filtering:
      1. Prefer chains where tradingClass matches the symbol (standard monthlies).
      2. Keep only one expiration per calendar month (latest in each month).

    Returns:
        contracts: list of (symbol, expiry, Contract, underlying_conId)
        symbol_expirations: dict of symbol -> (expiry_1, expiry_2)
    """
    today = datetime.now().strftime("%Y%m%d")
    today_date = datetime.now().date()
    contracts = []
    symbol_expirations = {}

    for sym, _exch, cid in symbols:
        if sym not in resolved:
            print(f"  [{sym}] Skipping (contract not resolved)")
            continue
        if sym not in prices:
            print(f"  [{sym}] Skipping (no price data)")
            continue
        if sym not in chains:
            print(f"  [{sym}] Skipping (no option chains)")
            continue

        info = resolved[sym]
        price = prices[sym]
        chain_list = chains[sym]

        # Prefer chains whose tradingClass matches the symbol (standard monthlies).
        # If those yield < 2 monthly expirations, expand to all chains (which may
        # include other trading classes covering additional underlying months).
        all_chains = [c for c in chain_list if c["strikes"]]
        std_chains = [c for c in chain_list if c["tradingClass"] == sym and c["strikes"]]

        use_chains = std_chains if std_chains else all_chains
        # Check if preferred chains have enough monthly expirations
        if use_chains and use_chains is not all_chains:
            test_exps = [e for c in use_chains for e in c["expirations"] if e >= today]
            if len(filter_monthly_expirations(test_exps)) < 2:
                use_chains = all_chains

        if not use_chains:
            print(f"  [{sym}] Skipping (no chains with strikes)")
            continue

        # Collect future expirations across the selected chains.
        # Track which chain and underlying each expiration belongs to.
        exp_chain_map = {}  # exp -> (chain, underlyingConId)
        for chain in use_chains:
            underlying = chain.get("underlyingConId", 0)
            for exp in chain["expirations"]:
                if exp >= today:
                    if exp not in exp_chain_map:
                        exp_chain_map[exp] = (chain, underlying)
                    elif cid:  # Stock/ETF: prefer SMART (accurate strike list)
                        if chain["exchange"] == "SMART" and exp_chain_map[exp][0]["exchange"] != "SMART":
                            exp_chain_map[exp] = (chain, underlying)
                    else:  # Futures: prefer specific (non-SMART) exchange
                        if exp_chain_map[exp][0]["exchange"] == "SMART":
                            exp_chain_map[exp] = (chain, underlying)

        # Filter to monthly: keep only one expiration per calendar month
        monthly_exps = filter_monthly_expirations(list(exp_chain_map.keys()))
        # Filter out expirations with fewer than MIN_DTE days remaining
        monthly_exps = [e for e in monthly_exps
                        if (datetime.strptime(e, "%Y%m%d").date() - today_date).days >= MIN_DTE]

        if len(monthly_exps) < 2:
            print(f"  [{sym}] Only {len(monthly_exps)} monthly expiration(s) with >= {MIN_DTE} DTE, need 2")
            continue

        nearest_two = monthly_exps[:2]
        symbol_expirations[sym] = tuple(nearest_two)

        for exp in nearest_two:
            chain, exp_under_conid = exp_chain_map[exp]
            # When using all chains (fallback), skip the underConId filter in Phase 3
            under_conid = exp_under_conid if use_chains is not all_chains else 0
            strikes = chain["strikes"]
            # For stock/ETF options, filter to whole-dollar strikes only.
            # IB's securityDefinitionOptionParameter can report phantom sub-dollar
            # strikes (e.g. $0.50 increments from BOX) that don't have tradeable
            # contracts on SMART.
            if cid:
                whole = [s for s in strikes if s % 1 == 0]
                if whole:
                    strikes = whole
            atm = find_atm_strike(price, strikes)

            c = Contract()
            c.symbol = sym
            if cid:
                c.secType = "OPT"
                c.exchange = "SMART"
            else:
                c.secType = "FOP"
                c.exchange = chain["exchange"]
            c.lastTradeDateOrExpiry = exp
            c.strike = atm
            c.right = "C"
            c.multiplier = chain["multiplier"]
            c.currency = info.get("currency", "USD")
            if chain["tradingClass"]:
                c.tradingClass = chain["tradingClass"]

            contracts.append((sym, exp, c, under_conid))
            print(f"  [{sym}] {exp} strike={atm} class={chain['tradingClass']} "
                  f"exchange={chain['exchange']}")

    return contracts, symbol_expirations


# ================================================================
# PHASE 3: RESOLVE OPTION CONTRACTS TO GET conId
# ================================================================

class OptionContractResolver(EWrapper, EClient):
    """Single connection: reqContractDetails for each ATM option to get its conId.
    Filters results by underConId to match the correct underlying contract."""

    def __init__(self):
        EClient.__init__(self, self)
        self.reqs = {}              # req_id -> (symbol, expiry, Contract, underlying_conId)
        self.resolved = {}          # req_id -> resolved Contract
        self._pending = set()
        self.lock = threading.Lock()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode not in (2104, 2106, 2158):
            print(f"  Error: reqId={reqId} code={errorCode} msg={errorString}")
        if reqId >= 0 and errorCode in (200, 321, 354):
            self._done(reqId)

    def nextValidId(self, orderId):
        for rid, (sym, exp, contract, _) in self.reqs.items():
            self._pending.add(rid)
            self.reqContractDetails(rid, contract)
        if not self._pending:
            threading.Timer(1, self.disconnect).start()
        else:
            threading.Timer(30, self._timeout).start()

    def contractDetails(self, reqId, contractDetails):
        if reqId not in self.reqs:
            return
        sym, exp, query_contract, underlying_conid = self.reqs[reqId]
        cd = contractDetails.contract

        if reqId in self.resolved:
            return

        cd_exp = getattr(cd, 'lastTradeDateOrExpiry', None) or getattr(cd, 'lastTradeDate', '')
        query_exp = getattr(query_contract, 'lastTradeDateOrExpiry', '') or \
                    getattr(query_contract, 'lastTradeDate', '')

        # Filter by expiry date — must match exactly
        if query_exp and cd_exp != query_exp:
            return
        # Filter by tradingClass if specified
        if query_contract.tradingClass and cd.tradingClass != query_contract.tradingClass:
            return
        # Filter by underlying conId to pick the right underlying month
        if underlying_conid and contractDetails.underConId != underlying_conid:
            return

        self.resolved[reqId] = cd
        print(f"  [{sym}] {exp} -> conId={cd.conId} local={cd.localSymbol}")

    def contractDetailsEnd(self, reqId):
        if reqId in self.reqs and reqId not in self.resolved:
            sym = self.reqs[reqId][0]
            exp = self.reqs[reqId][1]
            print(f"  [{sym}] {exp} -> no matching contract found")
        self._done(reqId)

    def _done(self, reqId):
        with self.lock:
            self._pending.discard(reqId)
            if not self._pending:
                threading.Timer(1, self.disconnect).start()

    def _timeout(self):
        with self.lock:
            if self._pending:
                print(f"  Phase 3 timeout: {len(self._pending)} requests still pending")
                self._pending.clear()
        self.disconnect()


def resolve_option_contracts(option_contracts, client_id=2):
    """Resolve each option contract via reqContractDetails to get its conId.

    Args:
        option_contracts: list of (symbol, expiry, Contract, underlying_conId)

    Returns:
        list of (symbol, expiry, resolved_Contract) for successfully resolved options
    """
    if not option_contracts:
        return []

    app = OptionContractResolver()
    for i, (sym, exp, contract, underlying_conid) in enumerate(option_contracts):
        app.reqs[i] = (sym, exp, contract, underlying_conid)
    app.connect("127.0.0.1", PORT, client_id)
    app.run()

    resolved = []
    for rid in sorted(app.resolved.keys()):
        sym = app.reqs[rid][0]
        exp = app.reqs[rid][1]
        resolved.append((sym, exp, app.resolved[rid]))
    return resolved


# ================================================================
# PHASE 4: GET IMPLIED VOLATILITY
# ================================================================

class IVFetcher(EWrapper, EClient):
    """Single connection: reqMktData for resolved ATM options, collect IV via tickOptionComputation."""

    def __init__(self, option_contracts, wait_seconds=IV_WAIT_SECONDS):
        EClient.__init__(self, self)
        self.option_contracts = option_contracts  # [(symbol, expiry, Contract), ...]
        self.wait_seconds = wait_seconds
        self.req_map = {}   # req_id -> (symbol, expiry)
        self.iv = {}        # req_id -> float (implied vol)
        self._finished = False
        self.lock = threading.Lock()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode not in (2104, 2106, 2158, 10167):
            print(f"  Error: reqId={reqId} code={errorCode} msg={errorString}")

    def nextValidId(self, orderId):
        # Request delayed data so IV is available for delayed-only subscriptions
        # (e.g. NYBOT). Also falls back to delayed-frozen when markets are closed.
        self.reqMarketDataType(3)
        for i, (sym, exp, contract) in enumerate(self.option_contracts):
            self.req_map[i] = (sym, exp)
            self.reqMktData(i, contract, "106", False, False, [])
        threading.Timer(self.wait_seconds, self._finish).start()

    def tickOptionComputation(self, reqId, tickType, tickAttrib, impliedVol,
                               delta, optPrice, pvDividend, gamma, vega, theta, undPrice):
        if reqId not in self.req_map:
            return
        if impliedVol is None or impliedVol <= 0:
            return
        sym, exp = self.req_map[reqId]
        # Prefer model computation: tickType 13 (live) or 56 (delayed)
        if tickType in (13, 56) or reqId not in self.iv:
            self.iv[reqId] = impliedVol
            print(f"  [{sym}] {exp} IV={impliedVol:.4f} (tick={tickType})")

    def tickPrice(self, reqId, tickType, price, attrib):
        pass

    def tickSize(self, reqId, tickType, size):
        pass

    def tickString(self, reqId, tickType, value):
        pass

    def tickGeneric(self, reqId, tickType, value):
        pass

    def marketDataType(self, reqId, marketDataType):
        pass

    def _finish(self):
        with self.lock:
            if self._finished:
                return
            self._finished = True
        for i in self.req_map:
            try:
                self.cancelMktData(i)
            except Exception:
                pass
        threading.Timer(1, self.disconnect).start()


def fetch_ivs(option_contracts, client_id=3, wait_seconds=IV_WAIT_SECONDS):
    """Fetch IV for a list of (symbol, expiry, Contract) tuples.
    Contracts must have conId set (resolved via Phase 3)."""
    if not option_contracts:
        return {}
    app = IVFetcher(option_contracts, wait_seconds=wait_seconds)
    app.connect("127.0.0.1", PORT, client_id)
    app.run()
    result = {}
    for rid, iv_val in app.iv.items():
        sym, exp = app.req_map[rid]
        result[(sym, exp)] = iv_val
    return result


# ================================================================
# MAIN
# ================================================================

def main():
    start_time = time.time()
    symbols_file = sys.argv[1] if len(sys.argv) > 1 else SYMBOLS_FILE
    output_file = sys.argv[2] if len(sys.argv) > 2 else OUTPUT_FILE

    symbols = read_symbols_csv(symbols_file)
    print(f"Loaded {len(symbols)} symbols from {symbols_file}\n")

    # ----------------------------------------------------------
    # PHASE 1: Resolve underlying contracts and fetch last prices
    # ----------------------------------------------------------
    print(f"{'=' * 60}")
    print(f"  PHASE 1: Resolving contracts and fetching prices")
    print(f"{'=' * 60}\n")
    resolved, prices = resolve_contracts_and_prices(symbols, client_id=30)  # Client ID 30: Resolve contracts
    print(f"\n  Resolved {len(resolved)}/{len(symbols)} contracts, "
          f"got prices for {len(prices)}/{len(symbols)}")

    # ----------------------------------------------------------
    # PHASE 1b + PHASE 2: Load from cache or fetch fresh
    # ----------------------------------------------------------
    cached_extra_months, cached_chains = load_cache(CACHE_FILE)

    if cached_extra_months is not None:
        extra_months = cached_extra_months
        chains = cached_chains
    else:
        # Phase 1b
        print(f"\n{'=' * 60}")
        print(f"  PHASE 1b: Resolving extra futures months")
        print(f"{'=' * 60}\n")
        extra_months = resolve_extra_months(symbols, resolved, num_extra=6, client_id=31)  # Client ID 31: Extra months
        print(f"\n  Found extra months for {len(extra_months)} futures symbols")

        # Phase 2
        print(f"\n{'=' * 60}")
        print(f"  PHASE 2: Fetching option chain parameters")
        print(f"{'=' * 60}\n")
        chains = fetch_option_chains(symbols, resolved, extra_months=extra_months, client_id=32)  # Client ID 32: Option chains
        print(f"\n  Got chains for {len(chains)} symbols")

    # Build ATM contracts (needs current prices — never cached)
    print(f"\n{'=' * 60}")
    print(f"  Building ATM option contracts (monthly only)")
    print(f"{'=' * 60}\n")
    option_contracts, symbol_expirations = build_option_contracts(
        symbols, resolved, prices, chains
    )
    print(f"\n  Created {len(option_contracts)} option contracts "
          f"for {len(symbol_expirations)} symbols")

    # Save cache after build_option_contracts (needs symbol_expirations for expiry)
    if cached_extra_months is None:
        save_cache(CACHE_FILE, extra_months, chains, symbol_expirations, datetime.now().date())

    # ----------------------------------------------------------
    # PHASE 3: Resolve option contracts to get conIds
    # ----------------------------------------------------------
    print(f"\n{'=' * 60}")
    print(f"  PHASE 3: Resolving option contracts (reqContractDetails)")
    print(f"{'=' * 60}\n")
    resolved_options = resolve_option_contracts(option_contracts, client_id=33)  # Client ID 33: Resolve options
    print(f"\n  Resolved {len(resolved_options)}/{len(option_contracts)} option contracts")

    # ----------------------------------------------------------
    # PHASE 4: Fetch implied volatility
    # ----------------------------------------------------------
    print(f"\n{'=' * 60}")
    print(f"  PHASE 4: Fetching implied volatility ({IV_WAIT_SECONDS}s wait)")
    print(f"{'=' * 60}\n")
    iv_data = fetch_ivs(resolved_options, client_id=34)  # Client ID 34: Fetch IV
    print(f"\n  Received IV for {len(iv_data)} option contracts")

    # ----------------------------------------------------------
    # PHASE 4b: Retry missing IV
    # ----------------------------------------------------------
    if IV_RETRY_SECONDS > 0:
        missing = [(sym, exp, c) for sym, exp, c in resolved_options
                   if (sym, exp) not in iv_data]
        if missing:
            print(f"\n{'=' * 60}")
            print(f"  PHASE 4b: Retrying {len(missing)} contracts ({IV_RETRY_SECONDS}s wait)")
            print(f"{'=' * 60}\n")
            retry_data = fetch_ivs(missing, client_id=35, wait_seconds=IV_RETRY_SECONDS)  # Client ID 35: IV retry
            iv_data.update(retry_data)
            print(f"\n  Retry received IV for {len(retry_data)} additional contracts")
            print(f"  Total IV: {len(iv_data)} option contracts")

    # ----------------------------------------------------------
    # Write output Excel
    # ----------------------------------------------------------
    print(f"\n{'=' * 60}")
    print(f"  Writing output to {output_file}")
    print(f"{'=' * 60}\n")

    close_excel_file(output_file)

    today = datetime.now().date()
    wb = Workbook()
    ws = wb.active
    ws.append(["Symbol", "IV_Front", "IV_Second",
               "Expiry_Front", "Expiry_Second",
               "DTE_Front", "DTE_Second"])
    for sym, _exch, _cid in symbols:
        if sym not in symbol_expirations:
            ws.append([sym, "", "", "", "", "", ""])
            continue
        exp1, exp2 = symbol_expirations[sym]
        iv1 = iv_data.get((sym, exp1), "")
        iv2 = iv_data.get((sym, exp2), "")
        if isinstance(iv1, float):
            iv1 = round(iv1, 4)
        if isinstance(iv2, float):
            iv2 = round(iv2, 4)
        dte1 = (datetime.strptime(exp1, "%Y%m%d").date() - today).days
        dte2 = (datetime.strptime(exp2, "%Y%m%d").date() - today).days
        exp1_int = int(exp1)
        exp2_int = int(exp2)
        ws.append([sym, iv1, iv2, exp1_int, exp2_int, dte1, dte2])
        iv1_str = f"{iv1:.4f}" if isinstance(iv1, float) else "N/A"
        iv2_str = f"{iv2:.4f}" if isinstance(iv2, float) else "N/A"
        print(f"  {sym:8s}  front={iv1_str:>8s}  second={iv2_str:>8s}"
              f"  exp={exp1}({dte1}d),{exp2}({dte2}d)")

    wb.save(output_file)
    print(f"\nSaved to {output_file}")
    open_excel_file(output_file, zoom=EXCEL_ZOOM_PERCENT, background=OPEN_EXCEL_IN_BACKGROUND)

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"  COMPLETE — {elapsed:.1f} seconds")
    print(f"{'=' * 60}")

    os.system('say "Implied volatility download complete"')


if __name__ == "__main__":
    main()
