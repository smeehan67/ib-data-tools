# IB Data Tools

Utilities for fetching market data from Interactive Brokers. Three standalone scripts that share the same EWrapper/EClient pattern.

## Scripts

### `futures_data_grabber_v2.py`

Downloads historical daily data for a list of continuous futures contracts and writes to an Excel workbook. Uses a two-phase batched connection approach:

- **Phase 1**: Downloads 1 month of data for symbols already in the workbook (validation)
- **Phase 2**: Downloads 3 years of data for any symbols that failed validation or are new

A single IB connection is reused for all symbols, making it roughly 10x faster than reconnecting per symbol.

**Input**: `futures_historical_data.csv` (SYMBOL, EXCHANGE, CONID columns)

**Output**: `futures_combined.xlsx`

### `get_conids.py`

Resolves contract symbols to Interactive Brokers ConIDs (contract identifiers) and writes results to Excel. Useful for setting up input files for other scripts.

**Input**: `conid_inputs.csv` (SYMBOL, EXCHANGE columns)

**Output**: `conid_outputs.xlsx`

### `get_last_prices.py`

Fetches current last prices for a list of contracts (by ConID) and writes to Excel. Supports a background/daemon mode where the Terminal window closes automatically after launch.

**Input**: `last_inputs.csv` (SYMBOL, CONID columns)

**Output**: `last_outputs.xlsx`

## Requirements

```bash
pip install ibapi openpyxl
```

Requires TWS (Trader Workstation) or IB Gateway running locally.

## Connection Ports

| Port | Description |
|------|-------------|
| `7497` | TWS paper trading (default) |
| `7496` | TWS live trading |
| `4002` | IB Gateway |

## Usage

```bash
python3 futures_data_grabber_v2.py
python3 get_conids.py
python3 get_last_prices.py
```

Edit the configuration block near the top of each script to change ports, file paths, or output options.

## Input File Format

**`futures_historical_data.csv`** (futures downloader):
```
SYMBOL,EXCHANGE,CONID
CL,NYMEX,
GC,COMEX,
ES,CME,
```

**`conid_inputs.csv`** (ConID resolver):
```
SYMBOL,EXCHANGE
CL,NYMEX
GC,COMEX
```

**`last_inputs.csv`** (last price fetcher):
```
SYMBOL,CONID
SPY,756733
GLD,51529211
```

## Architecture

All three scripts inherit from both `EWrapper` (receives async callbacks) and `EClient` (sends API requests). IB's API is callback-driven: you call `reqXxx()` methods and receive results in overridden callback methods. Each script runs `app.run()` on a daemon thread to process incoming messages.

## License

MIT
