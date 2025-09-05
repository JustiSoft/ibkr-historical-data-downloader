"""
IBKR Historical Data Downloader

Enhanced script to download historical OHLCV data from Interactive Brokers
with support for multiple timeframes and configurable durations.

Features:
- Support for all IBKR timeframes from 1 second to 1 month
- Command line interface with flexible parameters
- Auto-generated descriptive filenames
- Input validation and warnings for API limitations
- Support for stocks, forex, and futures

Usage Examples:
    python get_hist.py -s SPY -t "1 min" -d "30 D"
    python get_hist.py -s AAPL -t "5 mins" -d "1 Y" 
    python get_hist.py -s EURUSD -t "1 hour" -d "6 M"
    python get_hist.py --help

Author: Enhanced version of original IBKR script
"""

import logging
import argparse
import sys
import os
from datetime import datetime
import pytz
from ib_async import IB, Stock, Forex, Future, util
import pandas as pd

# --- Configuration: Modify these variables ---

# Contract details
TARGET_SYMBOL = "SPY"      # e.g., "AAPL", "MSFT", "GOOG" for stocks; "EURUSD", "GBPUSD" for forex; "ES", "NQ" for futures
SECURITY_TYPE = "STK"        # "STK" for Stock, "CASH" for Forex, "FUT" for Future
                               # Add more types like "IND" for Index, "CMDTY" for Commodity if needed.

# For STK (Stock):
STOCK_EXCHANGE = "SMART"     # General exchange for stocks, IB will route. Can specify (e.g., "NASDAQ", "NYSE")
STOCK_CURRENCY = "USD"

# For FUT (Future):
# If SECURITY_TYPE="FUT", these must be set.
FUTURE_LAST_TRADE_DATE_OR_CONTRACT_MONTH = ""  # YYYYMM or YYYYMMDD, e.g., "202409" or "20240920"
FUTURE_EXCHANGE = ""         # e.g., "CME", "NYMEX", "CBOT"
FUTURE_CURRENCY = "USD"      # Currency of the future contract

# For CASH (Forex):
# TARGET_SYMBOL should be the currency pair itself, e.g., "EURUSD". Currency is implicit in the pair.

# Historical data parameters
HISTORY_DURATION = "1 Y"     # Examples: "6 M" (6 months), "1 Y" (1 year), "2 Y" (2 years), "30 D" (30 days)
                               # Valid duration units: S (seconds), D (day), W (week), M (month), Y (year)

WHAT_TO_SHOW = "TRADES"      # Type of data: "TRADES", "MIDPOINT", "BID", "ASK", "ADJUSTED_LAST"
USE_REGULAR_TRADING_HOURS = True  # True for Regular Trading Hours (RTH) only, False for data outside RTH

# IBKR Connection details
IB_HOST = '127.0.0.1'        # Typically localhost
IB_PORT = 4001               # Default for TWS Paper: 7497, TWS Live: 7496
                               # Gateway Paper: 4002, Gateway Live: 4001
CLIENT_ID = 77                # Choose a unique client ID for this script connection

# --- End of Configuration ---

# Supported IBKR bar sizes (timeframes)
VALID_BAR_SIZES = [
    '1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
    '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins', '20 mins', '30 mins',
    '1 hour', '2 hours', '3 hours', '4 hours', '8 hours',
    '1 day', '1 week', '1 month'
]

# Optional: Enable more detailed logging from ib_async
# logging.basicConfig(level=logging.INFO)
# logging.getLogger('ib_async').setLevel(logging.INFO)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Download historical OHLCV data from Interactive Brokers',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s -s SPY -t "1 min" -d "30 D"           # 30 days of 1-minute SPY data
  %(prog)s -s AAPL -t "5 mins" -d "1 Y"          # 1 year of 5-minute AAPL data  
  %(prog)s -s EURUSD -t "1 hour" -d "6 M"        # 6 months of 1-hour EURUSD data
  %(prog)s -s ES -t "1 day" -d "2 Y"             # 2 years of daily ES futures data
  
  # Date range examples:
  %(prog)s --from 2024-01-15 -t "1 min"          # Single day of 1-minute data
  %(prog)s --from 2024-01-01 --to 2024-01-31 -t "5 mins"  # January 2024, 5-minute bars
  %(prog)s --to 2024-12-31 -d "30 D" -t "1 hour" # 30 days ending Dec 31, 2024
  
  # Extended hours examples:
  %(prog)s -s SPY -t "1 min" -d "1 D" --eth       # 1 day with extended hours
  %(prog)s --from 2024-01-15 -t "5 mins" --eth    # Single day with extended hours

Valid timeframes: 1 secs, 5 secs, 10 secs, 15 secs, 30 secs, 1 min, 2 mins, 
3 mins, 5 mins, 10 mins, 15 mins, 20 mins, 30 mins, 1 hour, 2 hours, 3 hours, 
4 hours, 8 hours, 1 day, 1 week, 1 month

Note: Bars 30 seconds or smaller older than 6 months are not available from IBKR.
        '''
    )
    
    parser.add_argument('-s', '--symbol', 
                        default=TARGET_SYMBOL,
                        help=f'Symbol to download (default: {TARGET_SYMBOL})')
    
    parser.add_argument('-t', '--timeframe', 
                        default='1 day',
                        choices=VALID_BAR_SIZES,
                        help='Bar size/timeframe (default: 1 day)')
    
    parser.add_argument('-d', '--duration', 
                        default=HISTORY_DURATION,
                        help=f'History duration (default: {HISTORY_DURATION}). Examples: "30 D", "6 M", "1 Y"')
    
    parser.add_argument('-o', '--output',
                        help='Output filename (auto-generated if not specified)')
    
    parser.add_argument('--overwrite', 
                        action='store_true',
                        help='Overwrite existing files without prompting')
    
    parser.add_argument('--timezone', 
                        default='market',
                        choices=['UTC', 'market', 'local'],
                        help='Output timezone (default: market). market=US/Eastern for US stocks, UTC=keep as UTC, local=system timezone')
    
    parser.add_argument('--start-date', '--from',
                        dest='start_date',
                        help='Start date for data collection (format: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)')
    
    parser.add_argument('--end-date', '--to',
                        dest='end_date', 
                        help='End date for data collection (format: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)')
    
    parser.add_argument('--eth',
                        action='store_true',
                        help='Include extended trading hours (pre-market and after-hours data)')
    
    return parser.parse_args()


def validate_timeframe_duration(timeframe, duration):
    """
    Validate timeframe and duration combination and provide warnings for potential issues.
    """
    warnings = []
    
    # Check for small bar sizes with long durations
    small_bars = ['1 secs', '5 secs', '10 secs', '15 secs', '30 secs']
    if timeframe in small_bars:
        warnings.append(f"WARNING: Timeframe '{timeframe}' with duration '{duration}' may hit IBKR pacing limits")
        warnings.append("WARNING: Bars 30 seconds or smaller older than 6 months are not available from IBKR")
    
    # Check for very long durations with small timeframes
    if timeframe in small_bars and any(x in duration.upper() for x in ['Y', 'YEAR']):
        warnings.append("WARNING: Small timeframes with yearly durations may result in very large datasets")
    
    return warnings


def process_date_arguments(start_date_str, end_date_str, default_duration, include_extended_hours=False):
    """
    Process start and end date arguments to determine endDateTime and duration for IBKR API.
    
    Args:
        start_date_str: Start date string from command line (or None)
        end_date_str: End date string from command line (or None) 
        default_duration: Default duration string to use if no dates provided
        include_extended_hours: If True, use extended hours times
        
    Returns:
        tuple: (end_datetime_str, duration_str, processed_info)
            end_datetime_str: String for IBKR endDateTime parameter
            duration_str: String for IBKR durationStr parameter
            processed_info: Dict with processing information for display
    """
    from datetime import datetime, timedelta, time
    import pytz
    
    def parse_date_string(date_str):
        """Parse date string in various formats"""
        if not date_str:
            return None
            
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%Y-%m-%d',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
    
    def format_ibkr_datetime(dt, has_time_component):
        """Format datetime for IBKR API in correct format"""
        if has_time_component:
            return dt.strftime('%Y%m%d %H:%M:%S')
        else:
            if include_extended_hours:
                # For ETH, request data until the early morning of the next day
                # to ensure the full post-market session is included.
                next_day = dt + timedelta(days=1)
                end_time = next_day.replace(hour=2, minute=0, second=0)
            else:
                # *** THIS IS THE FIX FOR RTH ***
                # Regular hours must end at 16:00 (4:00 PM).
                end_time = dt.replace(hour=16, minute=0, second=0)
            return end_time.strftime('%Y%m%d %H:%M:%S')
    
    start_date = parse_date_string(start_date_str) if start_date_str else None
    end_date = parse_date_string(end_date_str) if end_date_str else None
    
    if start_date and end_date and start_date > end_date:
        raise ValueError("Start date cannot be after end date")
    
    processed_info = {}
    
    if start_date and end_date:
        # Case 1: Both dates provided
        duration_days = (end_date - start_date).days + 1
        if duration_days == 1:
            duration_str = "1 D"
        elif duration_days <= 365:
            duration_str = f"{duration_days} D"
        else:
            duration_years = duration_days // 365
            duration_str = f"{duration_years} Y"
        
        has_time = end_date.time() != datetime.min.time()
        end_datetime_str = format_ibkr_datetime(end_date, has_time)
        
        processed_info = { 'mode': 'date_range', 'start_date': start_date_str, 'end_date': end_date_str, 'calculated_duration': duration_str }
    
    elif start_date and not end_date:
        # Case 2: Only start date provided
        duration_str = "1 D"
        # For single day requests, set end_datetime to the same day
        # but let format_ibkr_datetime handle the proper RTH end time (4:00 PM)
        end_datetime = start_date
        has_time = start_date.time() != datetime.min.time()
        
        # For RTH requests without a specific time, we need the end time to be 4:00 PM
        # to capture the full trading session, not midnight of the same day
        if not has_time and not include_extended_hours:
            # Set end time to 4:00 PM of the same day for RTH
            end_datetime = end_datetime.replace(hour=16, minute=0, second=0)
            has_time = True  # Now it has a time component
            
        end_datetime_str = format_ibkr_datetime(end_datetime, has_time)
        
        processed_info = { 'mode': 'single_day', 'target_date': start_date_str, 'duration': duration_str }
    
    elif not start_date and end_date:
        # Case 3: Only end date provided
        duration_str = default_duration
        has_time = end_date.time() != datetime.min.time()
        end_datetime_str = format_ibkr_datetime(end_date, has_time)
        
        processed_info = { 'mode': 'duration_with_end', 'end_date': end_date_str, 'duration': duration_str }
    
    else:
        # Case 4: Neither date provided
        duration_str = default_duration
        end_datetime_str = ''
        
        # *** FIX FOR BOTH RTH AND ETH ***
        # For both RTH and ETH requests without a date, we must provide an explicit end time
        # to prevent the API from defaulting to the current time (which cuts off data)
        if include_extended_hours:
            # For ETH, end at 2:00 AM next day to capture full post-market session
            today = datetime.now()
            end_datetime_str = format_ibkr_datetime(today, False)
        else:
            # For RTH, end at 4:00 PM today to capture full regular trading hours
            today = datetime.now()
            end_datetime_str = format_ibkr_datetime(today, False)
        
        processed_info = { 'mode': 'duration_only', 'duration': duration_str }
    
    return end_datetime_str, duration_str, processed_info


def get_target_timezone(timezone_choice, symbol):
    """
    Get the target timezone for timestamp conversion.
    
    Args:
        timezone_choice: 'UTC', 'market', or 'local'
        symbol: The trading symbol (used to determine market timezone)
        
    Returns:
        pytz timezone object
    """
    if timezone_choice == 'UTC':
        return pytz.UTC
    elif timezone_choice == 'local':
        return pytz.timezone('UTC').localize(datetime.now()).astimezone().tzinfo
    elif timezone_choice == 'market':
        # For US symbols, use Eastern Time (NYSE/NASDAQ)
        # Could be enhanced to support international markets
        symbol_upper = symbol.upper()
        if symbol_upper in ['SPY', 'QQQ', 'IWM'] or len(symbol_upper) <= 5:  # Assume US symbols
            return pytz.timezone('US/Eastern')
        else:
            # Default to US/Eastern for now, could be expanded for forex/international
            return pytz.timezone('US/Eastern')
    else:
        return pytz.UTC


def convert_datetime_column(datetime_series, target_timezone):
    """
    Convert a pandas datetime series to the target timezone.
    
    Args:
        datetime_series: Pandas datetime series (should be UTC timezone-aware)
        target_timezone: Target pytz timezone object
        
    Returns:
        Converted datetime series
    """
    # Ensure the series is timezone-aware (should be UTC from formatDate=2)
    if datetime_series.dt.tz is None:
        # If somehow not timezone-aware, assume UTC
        datetime_series = datetime_series.dt.tz_localize('UTC')
    elif datetime_series.dt.tz != pytz.UTC:
        # Convert to UTC first if it's in a different timezone
        datetime_series = datetime_series.dt.tz_convert('UTC')
    
    # Convert to target timezone
    return datetime_series.dt.tz_convert(target_timezone)


def format_timezone_aware_datetime(datetime_series, target_timezone, is_intraday):
    """
    Format datetime series with appropriate timezone information.
    
    Args:
        datetime_series: Pandas datetime series
        target_timezone: Target pytz timezone object
        is_intraday: Boolean indicating if this is intraday data
        
    Returns:
        Formatted datetime strings
    """
    # Convert to target timezone
    converted_series = convert_datetime_column(datetime_series, target_timezone)
    
    if is_intraday:
        # For intraday, include time but not timezone in each row (timezone is in column header)
        return converted_series.dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        # For daily+, just show date (timezone less relevant)
        return converted_series.dt.strftime('%Y-%m-%d')


def is_intraday_timeframe(timeframe):
    """
    Determine if a timeframe is intraday (requires timestamp) or daily+ (date only).
    
    Args:
        timeframe: Bar size string (e.g., '1 min', '1 day')
        
    Returns:
        bool: True if intraday timeframe, False if daily or longer
    """
    intraday_timeframes = [
        '1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
        '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins', '20 mins', '30 mins',
        '1 hour', '2 hours', '3 hours', '4 hours', '8 hours'
    ]
    return timeframe in intraday_timeframes


def generate_filename(symbol, security_type, duration, timeframe, future_contract_month=None, include_extended_hours=False):
    """
    Generate a descriptive filename based on the parameters.
    """
    # Clean up timeframe for filename (replace spaces with nothing, handle special chars)
    timeframe_clean = timeframe.replace(' ', '').replace('secs', 's').replace('mins', 'm').replace('min', 'm')
    timeframe_clean = timeframe_clean.replace('hour', 'h').replace('day', 'd').replace('week', 'w').replace('month', 'M')
    
    # Clean up duration for filename
    duration_clean = duration.replace(' ', '')
    
    # Build filename parts
    filename_parts = [
        symbol.upper(),
        security_type
    ]
    
    # Add future contract month if applicable
    if security_type == "FUT" and future_contract_month:
        filename_parts.append(future_contract_month)
    
    filename_parts.extend([duration_clean, timeframe_clean])
    
    # Add extended hours indicator if applicable
    if include_extended_hours:
        filename_parts.append("ETH")  # Extended Trading Hours
    
    filename_parts.append("OHLCV")
    
    return "_".join(filename_parts) + ".csv"


def generate_unique_filename(base_filename):
    """
    Generate a unique filename by appending timestamp if the base filename already exists.
    
    Args:
        base_filename: Original filename (e.g., 'SPY_STK_1D_1m_OHLCV.csv')
        
    Returns:
        str: Unique filename with timestamp if needed (e.g., 'SPY_STK_1D_1m_OHLCV_20250904_163045.csv')
    """
    if not os.path.exists(base_filename):
        return base_filename
    
    # Split filename and extension
    name_part, ext = os.path.splitext(base_filename)
    
    # Generate timestamp suffix
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create new filename with timestamp
    unique_filename = f"{name_part}_{timestamp}{ext}"
    
    # If somehow this filename also exists, keep adding seconds until unique
    counter = 1
    while os.path.exists(unique_filename):
        unique_filename = f"{name_part}_{timestamp}_{counter:02d}{ext}"
        counter += 1
    
    return unique_filename


def handle_file_conflict(filename, overwrite_flag=False):
    """
    Handle file conflicts by prompting user or using overwrite flag.
    
    Args:
        filename: The target filename
        overwrite_flag: If True, automatically overwrite
        
    Returns:
        tuple: (final_filename, should_proceed)
            final_filename: The filename to use (original or renamed)
            should_proceed: Boolean indicating if operation should continue
    """
    if not os.path.exists(filename):
        return filename, True
    
    # If overwrite flag is set, proceed with original filename
    if overwrite_flag:
        print(f"Overwriting existing file: {filename}")
        return filename, True
    
    # Get file creation time for display
    try:
        creation_time = os.path.getctime(filename)
        formatted_time = datetime.fromtimestamp(creation_time).strftime('%Y-%m-%d %H:%M:%S')
    except (OSError, ValueError):
        formatted_time = "unknown"
    
    # Show conflict information
    print(f"\nFile conflict detected!")
    print(f"Output file '{filename}' already exists (created: {formatted_time})")
    print(f"Full path: {os.path.abspath(filename)}")
    print()
    print("Choose action:")
    print("  [O]verwrite - Replace the existing file")
    print("  [R]ename    - Create new file with timestamp suffix")
    print("  [C]ancel    - Abort the operation")
    print()
    
    # Get user choice
    while True:
        choice = input("Enter choice [O/R/C]: ").strip().upper()
        
        if choice in ['O', 'OVERWRITE']:
            print(f"Overwriting: {filename}")
            return filename, True
        elif choice in ['R', 'RENAME']:
            new_filename = generate_unique_filename(filename)
            print(f"Creating new file: {new_filename}")
            return new_filename, True
        elif choice in ['C', 'CANCEL']:
            print("Operation cancelled by user.")
            return filename, False
        else:
            print("Invalid choice. Please enter O, R, or C.")


def create_contract(symbol=None, security_type=None):
    """Creates an IBKR contract object based on provided or global configuration."""
    # Use provided parameters or fall back to global configuration
    target_symbol = symbol if symbol is not None else TARGET_SYMBOL
    sec_type = security_type if security_type is not None else SECURITY_TYPE
    
    print(f"Creating contract for {target_symbol} ({sec_type})...")
    
    if sec_type == "STK":
        return Stock(target_symbol, STOCK_EXCHANGE, STOCK_CURRENCY)
    elif sec_type == "CASH":
        # For Forex, target_symbol is expected to be the pair, e.g., "EURUSD"
        return Forex(target_symbol)
    elif sec_type == "FUT":
        if not FUTURE_LAST_TRADE_DATE_OR_CONTRACT_MONTH or not FUTURE_EXCHANGE:
            raise ValueError(
                "For Futures (FUT), FUTURE_LAST_TRADE_DATE_OR_CONTRACT_MONTH and FUTURE_EXCHANGE must be specified."
            )
        return Future(target_symbol, FUTURE_LAST_TRADE_DATE_OR_CONTRACT_MONTH, FUTURE_EXCHANGE, currency=FUTURE_CURRENCY)
    # Add other security types (Option, Index, etc.) here if needed, for example:
    # elif sec_type == "IND":
    #     return Index(target_symbol, STOCK_EXCHANGE, STOCK_CURRENCY) # Adjust params as needed for Index
    else:
        raise ValueError(f"Unsupported SECURITY_TYPE: {sec_type}. Please use 'STK', 'CASH', or 'FUT'.")


def fetch_and_save_historical_data(symbol=None, timeframe=None, duration=None, output_filename=None, overwrite=False, timezone_choice='market', start_date=None, end_date=None, include_extended_hours=False):
    """
    Connects to IBKR, fetches historical OHLC data for the specified symbol,
    timeframe, and duration, and stores it in a CSV file.
    
    Args:
        symbol: Target symbol (uses global TARGET_SYMBOL if None)
        timeframe: Bar size (e.g., '1 min', '1 day') (uses '1 day' if None) 
        duration: History duration (e.g., '1 Y', '30 D') (uses global HISTORY_DURATION if None)
        output_filename: Output CSV filename (auto-generated if None)
        overwrite: If True, overwrite existing files without prompting
        timezone_choice: 'UTC', 'market', or 'local' for timestamp timezone
        start_date: Start date string for date range requests
        end_date: End date string for date range requests
        include_extended_hours: If True, include extended trading hours data
    """
    # Use provided parameters or fall back to defaults
    target_symbol = symbol if symbol is not None else TARGET_SYMBOL
    bar_size = timeframe if timeframe is not None else '1 day'
    default_duration = duration if duration is not None else HISTORY_DURATION
    
    # Process date arguments to determine actual duration and end date
    try:
        end_datetime_str, hist_duration, date_info = process_date_arguments(start_date, end_date, default_duration, include_extended_hours)
    except ValueError as e:
        print(f"ERROR: {e}")
        return
    
    # Display date processing information
    if date_info['mode'] == 'date_range':
        print(f"Date range mode: {date_info['start_date']} to {date_info['end_date']}")
        print(f"Calculated duration: {date_info['calculated_duration']}")
    elif date_info['mode'] == 'single_day':
        print(f"Single day mode: {date_info['target_date']}")
    elif date_info['mode'] == 'duration_with_end':
        print(f"Duration with end date: {date_info['duration']} ending at {date_info['end_date']}")
    
    # Determine RTH setting (Regular Trading Hours)
    use_rth = not include_extended_hours
    
    # Validate parameters and show warnings
    warnings = validate_timeframe_duration(bar_size, hist_duration)
    for warning in warnings:
        print(warning)
    
    # Generate output filename if not provided
    if output_filename is None:
        future_month = FUTURE_LAST_TRADE_DATE_OR_CONTRACT_MONTH if SECURITY_TYPE == "FUT" else None
        output_filename = generate_filename(target_symbol, SECURITY_TYPE, hist_duration, bar_size, future_month, include_extended_hours)
    
    ib = IB()


    try:
        print(f"Attempting to connect to IBKR at {IB_HOST}:{IB_PORT} with Client ID {CLIENT_ID}...")
        ib.connect(IB_HOST, IB_PORT, clientId=CLIENT_ID, timeout=15) # Connection timeout
        print("Successfully connected to IBKR.")

        contract = create_contract(target_symbol, SECURITY_TYPE)

        # Qualify the contract (important to resolve ambiguities and get full contract details)
        print("Qualifying contract...")
        qualified_contracts = ib.qualifyContracts(contract)
        if not qualified_contracts:
            raise LookupError(
                f"Contract for {TARGET_SYMBOL} ({SECURITY_TYPE}) could not be qualified. "
                "Please check symbol, security type, exchange, and other parameters."
            )
        qualified_contract = qualified_contracts[0] # Use the first qualified contract
        print(f"Contract qualified: {qualified_contract.localSymbol} on {qualified_contract.exchange} (conId: {qualified_contract.conId})")

        print(f"\nRequesting historical data for {qualified_contract.symbol}:")
        print(f"  Duration: {hist_duration}")
        print(f"  Bar size: {bar_size}")
        print(f"  Data type: {WHAT_TO_SHOW}")
        print(f"  Regular Trading Hours: {use_rth}")
        if include_extended_hours:
            print("  Extended Hours: Included (pre-market and after-hours data)")
        if end_datetime_str:
            print(f"  End DateTime: {end_datetime_str}")
        
        # Show timezone info for intraday data
        if is_intraday_timeframe(bar_size):
            target_timezone = get_target_timezone(timezone_choice, target_symbol)
            tz_name = target_timezone.tzname(datetime.now()) if timezone_choice != 'UTC' else 'UTC'
            print(f"  Output timezone: {timezone_choice} ({tz_name})")

        # Request historical data
        # formatDate=1 for 'yyyyMMdd HH:mm:ss', 2 for seconds since epoch.
        # For smaller timeframes, the time part becomes more important.
        # util.df handles date conversion well.
        bars = ib.reqHistoricalData(
            qualified_contract,
            endDateTime=end_datetime_str,  # Use calculated end date or empty for most recent
            durationStr=hist_duration,
            barSizeSetting=bar_size,
            whatToShow=WHAT_TO_SHOW,
            useRTH=use_rth,  # Use the calculated RTH setting
            formatDate=2,  # Use formatDate=2 for UTC timezone-aware datetime objects (better for intraday)
            # timeout parameter for reqHistoricalData is also available (default 60s)
        )

        if not bars:
            print("\nNo historical data received. This could be due to several reasons:")
            print("  - No data available for the requested contract or period.")
            print("  - Market data subscriptions might be required for this specific data.")
            print("  - Incorrect contract details or parameters.")
            print(f"  - {bar_size} data may not be available if the duration is too short or data restrictions apply.")
            if bar_size in ['1 secs', '5 secs', '10 secs', '15 secs', '30 secs']:
                print("  - Remember: Bars 30 seconds or smaller older than 6 months are not available from IBKR.")
        else:
            print(f"\nSuccessfully received {len(bars)} bars of data.")

            # Convert list of bar data to a pandas DataFrame
            df = util.df(bars)

            if df is not None and not df.empty:
                # Determine if this is intraday data that needs timestamps
                is_intraday = is_intraday_timeframe(bar_size)
                
                # Get target timezone for timestamp conversion
                target_timezone = get_target_timezone(timezone_choice, target_symbol)
                
                # Standardize column names (util.df usually gives: date, open, high, low, close, volume, average, barCount)
                df_output = df[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
                
                # Use appropriate column naming based on timeframe and timezone
                if is_intraday:
                    # For intraday data, include timezone info in column name
                    tz_abbrev = target_timezone.tzname(datetime.now()) if timezone_choice != 'UTC' else 'UTC'
                    date_column_name = f'DateTime_{tz_abbrev}'
                else:
                    # For daily+ data, timezone is less relevant
                    date_column_name = 'Date'
                
                df_output.rename(columns={
                    'date': date_column_name,
                    'open': 'Open',
                    'high': 'High',
                    'low': 'Low',
                    'close': 'Close',
                    'volume': 'Volume'
                }, inplace=True)

                # Format the date/datetime column with timezone awareness
                # The 'date' column from util.df with formatDate=2 should be timezone-aware datetime objects
                if pd.api.types.is_datetime64_any_dtype(df_output[date_column_name]):
                    df_output[date_column_name] = format_timezone_aware_datetime(
                        df_output[date_column_name], target_timezone, is_intraday
                    )
                elif df_output[date_column_name].dtype == 'object': # If it's strings
                    try:
                        # Try to parse as datetime first, then format with timezone
                        datetime_series = pd.to_datetime(df_output[date_column_name])
                        df_output[date_column_name] = format_timezone_aware_datetime(
                            datetime_series, target_timezone, is_intraday
                        )
                    except (ValueError, TypeError):
                        print(f"Warning: {date_column_name} column could not be parsed. Leaving as is.")
                # else if it's datetime.date objects, strftime will work if applied row-wise or converted first

                # Handle file conflicts before saving
                final_filename, should_proceed = handle_file_conflict(output_filename, overwrite)
                
                if not should_proceed:
                    print("Data download was successful but file was not saved due to user cancellation.")
                    return
                
                # Save to CSV file
                try:
                    df_output.to_csv(final_filename, index=False)
                    print(f"SUCCESS: Historical OHLCV data saved to: {final_filename}")
                except PermissionError as e:
                    print(f"\nERROR: Permission denied while saving file: {final_filename}")
                    print("Possible causes:")
                    print("  - File is currently open in Excel or another application")
                    print("  - Insufficient write permissions in the directory")
                    print("  - File is marked as read-only")
                    print("\nSolutions:")
                    print("  - Close the file in any applications and try again")
                    print("  - Run the script as administrator")
                    print("  - Choose a different output directory")
                    raise e
            else:
                print("Data was received, but the resulting DataFrame is empty or None after processing.")

    except ConnectionRefusedError:
        print(f"\nERROR: Connection refused. Ensure IB Gateway or TWS is running on {IB_HOST}:{IB_PORT} and API access is enabled.")
    except TimeoutError: # Catches generic timeout, ib_async might raise specific IBError for timeouts
        print(f"\nERROR: Connection to IBKR timed out. Check network or if TWS/Gateway is responsive.")
    except (LookupError, ValueError) as e: # For contract qualification or configuration issues
        print(f"\nERROR: {e}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if ib.isConnected():
            print("\nDisconnecting from IBKR...")
            ib.disconnect()
            print("Disconnected.")
        else:
            print("\nNo active connection to disconnect or connection was not established.")

def main():
    """
    Main function that handles command line arguments and calls the data fetching function.
    """
    args = parse_arguments()
    
    print(f"IBKR Historical Data Downloader")
    print(f"================================")
    print(f"Symbol: {args.symbol}")
    print(f"Timeframe: {args.timeframe}")
    print(f"Duration: {args.duration}")
    print(f"Timezone: {args.timezone}")
    if args.start_date:
        print(f"Start date: {args.start_date}")
    if args.end_date:
        print(f"End date: {args.end_date}")
    if args.eth:
        print(f"Extended hours: Enabled")
    if args.output:
        print(f"Output file: {args.output}")
    print()
    
    try:
        fetch_and_save_historical_data(
            symbol=args.symbol,
            timeframe=args.timeframe, 
            duration=args.duration,
            output_filename=args.output,
            overwrite=args.overwrite,
            timezone_choice=args.timezone,
            start_date=args.start_date,
            end_date=args.end_date,
            include_extended_hours=args.eth
        )
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()