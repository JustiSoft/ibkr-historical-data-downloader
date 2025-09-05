# IBKR Historical Data Downloader

A comprehensive Python script to download historical OHLCV (Open, High, Low, Close, Volume) data from Interactive Brokers using their TWS API.

## Goals

This script provides an easy-to-use command-line interface for:
- Downloading historical market data from Interactive Brokers
- Supporting multiple asset classes (stocks, forex, futures)
- Flexible timeframe and duration options
- Automatic data formatting and timezone handling
- Professional CSV output with descriptive filenames

## Features

- **Multiple Timeframes**: Support for all IBKR timeframes from 1 second to 1 month
- **Asset Classes**: Stocks, forex pairs, and futures contracts
- **Extended Hours**: Option to include pre-market and after-hours trading data
- **Timezone Support**: Market time, UTC, or local timezone output
- **Date Ranges**: Specify custom date ranges or use duration-based requests
- **Smart Filenames**: Auto-generated descriptive filenames based on parameters
- **Error Handling**: Comprehensive validation and user-friendly error messages
- **File Management**: Conflict detection with rename/overwrite options

## Requirements

- Python 3.7+
- Interactive Brokers TWS or IB Gateway running
- Valid IB account with market data permissions

## Installation

1. Clone this repository:
```bash
git clone https://github.com/JustiSoft/ibkr-historical-data-downloader.git
cd ibkr-historical-data-downloader
```

2. Create a virtual environment:
```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
# or
source .venv/bin/activate  # Linux/Mac
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

## Setup

1. **Start TWS or IB Gateway**
   - Launch Interactive Brokers TWS (Trader Workstation) or IB Gateway
   - Enable API connections: Configure → Global Configuration → API → Settings
   - Check "Enable ActiveX and Socket Clients"
   - Note the socket port number from your TWS/Gateway settings

2. **Configure Port Settings**
   - Open `get_hist.py` and locate line 61: `IB_PORT = 4001`
   - Change this port number to match your TWS/Gateway port
   - **Common port numbers:**
     - TWS Paper Trading: 7497
     - TWS Live Trading: 7496  
     - IB Gateway Paper: 4002
     - IB Gateway Live: 4001
   - **Important:** The port in the code MUST match your TWS/Gateway port setting

3. **Additional Configuration** (optional)
   - Edit other settings in the configuration section of `get_hist.py` if needed
   - Default settings work for most use cases

## Usage

### Basic Examples

Download 30 days of 1-minute SPY data:
```bash
python get_hist.py -s SPY -t "1 min" -d "30 D"
```

Download 1 year of 5-minute AAPL data:
```bash
python get_hist.py -s AAPL -t "5 mins" -d "1 Y"
```

Download 6 months of 1-hour EURUSD forex data:
```bash
python get_hist.py -s EURUSD -t "1 hour" -d "6 M"
```

### Date Range Examples

Download specific date range:
```bash
python get_hist.py --from 2024-01-01 --to 2024-01-31 -t "5 mins" -s SPY
```

Download single day with extended hours:
```bash
python get_hist.py --from 2024-01-15 -t "1 min" -s SPY --eth
```

### Advanced Options

Include extended trading hours:
```bash
python get_hist.py -s SPY -t "1 min" -d "1 D" --eth
```

Specify output filename:
```bash
python get_hist.py -s SPY -t "1 min" -d "30 D" -o my_spy_data.csv
```

Use different timezone for timestamps:
```bash
python get_hist.py -s SPY -t "1 min" -d "1 D" --timezone UTC
```

### Command Line Options

| Option | Description | Example |
|--------|-------------|---------|
| `-s, --symbol` | Trading symbol | `-s AAPL` |
| `-t, --timeframe` | Bar size/timeframe | `-t "5 mins"` |
| `-d, --duration` | History duration | `-d "30 D"` |
| `-o, --output` | Output filename | `-o data.csv` |
| `--from` | Start date | `--from 2024-01-01` |
| `--to` | End date | `--to 2024-01-31` |
| `--eth` | Include extended hours | `--eth` |
| `--timezone` | Output timezone | `--timezone UTC` |
| `--overwrite` | Overwrite existing files | `--overwrite` |

## Valid Timeframes

- **Seconds**: 1 secs, 5 secs, 10 secs, 15 secs, 30 secs
- **Minutes**: 1 min, 2 mins, 3 mins, 5 mins, 10 mins, 15 mins, 20 mins, 30 mins
- **Hours**: 1 hour, 2 hours, 3 hours, 4 hours, 8 hours
- **Days and longer**: 1 day, 1 week, 1 month

## Output Format

The script generates CSV files with the following columns:
- **DateTime_EST** (for intraday) or **Date** (for daily+): Timestamp in specified timezone
- **Open**: Opening price
- **High**: Highest price
- **Low**: Lowest price  
- **Close**: Closing price
- **Volume**: Trading volume

## Configuration

The script includes configuration variables at the top for:
- Default symbol and security type
- Connection parameters (host, port, client ID)
- Exchange and currency settings
- Historical data parameters

## Troubleshooting

### Connection Issues
- Ensure TWS or IB Gateway is running
- Check that API connections are enabled in TWS settings
- Verify the port number matches between your code (line 61) and TWS/Gateway settings
- Make sure no other applications are using the same client ID

### Data Issues
- Verify you have market data subscriptions for the requested symbol
- Check symbol format (e.g., "EURUSD" for forex, not "EUR/USD")
- Remember: Sub-30-second bars older than 6 months are not available from IBKR

### File Issues  
- Close any CSV files that might be open in Excel or other applications
- Ensure you have write permissions in the directory
- Use `--overwrite` flag to automatically overwrite existing files

## License

This project is provided as-is for educational and personal use. Please ensure compliance with Interactive Brokers' API terms of service.

## Contributing

Feel free to submit issues, feature requests, or pull requests to improve this tool.

**Note**: This repository uses branch protection rules. External contributions must be submitted via pull requests for security review.