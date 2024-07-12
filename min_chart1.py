import socket
import logging
import time
import pandas as pd
import sqlite3
import os
from datetime import datetime, timedelta
from threading import Lock, Thread

logging.basicConfig(level=logging.DEBUG)

DAS_API_BASE_URL = '127.0.0.1'
DAS_API_PORT = 9800
DAS_API_USERNAME = ''
DAS_API_PASSWORD = ''
DAS_API_ACCOUNT = ''

# Store the last processed timestamp globally
last_timestamp = None
one_minute_data = []  # Store 1-minute data temporarily for 5-minute aggregation

db_lock = Lock()  # Lock to manage database access

def create_socket():
    logging.debug('Creating socket...')
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    logging.debug(f'Socket created. Attempting to connect to {DAS_API_BASE_URL}:{DAS_API_PORT}...')
    s.connect((DAS_API_BASE_URL, DAS_API_PORT))
    logging.debug('Connection established')
    return s

def send_command(sock, command):
    try:
        full_command = f'{command}\r\n'
        logging.debug(f'Sending command to DAS: {full_command}')
        sock.sendall(full_command.encode())
    except (OSError, BrokenPipeError) as e:
        logging.error(f'Error sending command: {e}')
        sock.close()
        return None

def receive_response(sock, buffer_size=4096):
    try:
        response = sock.recv(buffer_size).decode()
        logging.debug(f'Received response: {response}')
        return response
    except (OSError, BrokenPipeError) as e:
        logging.error(f'Error receiving response: {e}')
        sock.close()
        return None

def login(sock):
    login_command = f'LOGIN {DAS_API_USERNAME} {DAS_API_PASSWORD} {DAS_API_ACCOUNT} 0'
    send_command(sock, login_command)
    while True:
        login_response = receive_response(sock)
        if login_response:
            if 'LOGIN SUCCESSED' in login_response:
                logging.info('Login successful')
                return True
            elif '#Please login to continue.' in login_response:
                logging.warning('Received prompt to login again, retrying...')
                send_command(sock, login_command)
            else:
                logging.error(f'Unexpected login response: {login_response}')
                return False

def request_minute_chart(sock, symbol, start_time, end_time='LATEST', min_type=1):
    minchart_command = f'SB {symbol} MINCHART {start_time} {end_time} {min_type}'
    send_command(sock, minchart_command)

def insert_into_db(table, data):
    with db_lock:  # Use the lock to ensure thread-safe database access
        conn = sqlite3.connect('tms_data.db')
        c = conn.cursor()
        if table == 'ohlc_1min':
            c.executemany('INSERT OR IGNORE INTO ohlc_1min (ticker, open, high, low, close, volume, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)', data)
        elif table == 'ohlc_5min':
            c.executemany('INSERT OR IGNORE INTO ohlc_5min (ticker, open, high, low, close, volume, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)', data)
        conn.commit()
        conn.close()

def generate_5min_data(one_minute_data):
    df = pd.DataFrame(one_minute_data, columns=['ticker', 'timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['datetime'] = pd.to_datetime(df['timestamp'], format='%Y/%m/%d-%H:%M')
    df.set_index('datetime', inplace=True)

    # Ensure numeric columns are properly converted
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].apply(pd.to_numeric, errors='coerce')

    # Sort data to ensure chronological order
    df.sort_index(inplace=True)

    # Log any missing 5-minute intervals
    all_5min_intervals = pd.date_range(start=df.index.min(), end=df.index.max(), freq='5min')
    missing_intervals = set(all_5min_intervals) - set(df.resample('5min').first().index)
    if missing_intervals:
        logging.warning(f'Missing 5-minute intervals: {missing_intervals}')

    # Resample to 5-minute intervals
    df_5min = df.resample('5min', closed='left', label='left').agg({
        'ticker': 'first',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna().reset_index()

    df_5min['timestamp'] = df_5min['datetime'].dt.strftime('%Y/%m/%d-%H:%M')
    df_5min.drop(columns=['datetime'], inplace=True)

    return df_5min[['ticker', 'open', 'high', 'low', 'close', 'volume', 'timestamp']].values.tolist()

def parse_and_store_data(response, symbol):
    global last_timestamp, one_minute_data
    logging.debug(f'Parsing response: {response}')
    lines = response.strip().split('\n')
    data = []
    for line in lines:
        logging.debug(f'Processing line: {line}')
        if line.startswith('$Bar'):
            parts = line.split()
            logging.debug(f'Line parts: {parts}')
            logging.debug(f'Raw line content: {line}')  # Log the raw line content
            if len(parts) < 8:  # Ensure the line has enough parts
                logging.error(f'Malformed line: {line}')
                continue
            try:
                date_time = parts[2]
                open_price = parts[5]
                high_price = parts[3]
                low_price = parts[4]
                close_price = parts[6]
                volume = parts[7] if len(parts) > 7 else '0'
                # Check if this timestamp is the same as the last processed one
                if date_time == last_timestamp:
                    logging.info(f'Skipping duplicate timestamp: {date_time}')
                    continue
                # Update last_timestamp
                last_timestamp = date_time
                data.append([symbol, open_price, high_price, low_price, close_price, volume, date_time])
                one_minute_data.append([symbol, date_time, open_price, high_price, low_price, close_price, volume])
                print(f'{symbol} | Timestamp: {date_time} | Open: {open_price} | High: {high_price} | Low: {low_price} | Close: {close_price} | Volume: {volume}')
            except IndexError as e:
                logging.error(f'Error parsing line: {e}')
            except Exception as e:
                logging.error(f'Unexpected error: {e}')

    if data:
        insert_into_db('ohlc_1min', data)
        five_min_data = generate_5min_data(one_minute_data)
        if five_min_data:
            insert_into_db('ohlc_5min', five_min_data)
            one_minute_data = []  # Reset the one-minute data for the next aggregation

def check_write_permission(directory):
    try:
        testfile = os.path.join(directory, 'testfile.tmp')
        with open(testfile, 'w') as f:
            f.write('test')
        os.remove(testfile)
        logging.info(f'Write permission to directory {directory} is OK.')
        return True
    except IOError as e:
        logging.error(f'Write permission to directory {directory} failed: {e}')
        return False

def process_ticker(symbol, start_time):
    try:
        sock = create_socket()
    
        # Receive the welcome message
        welcome_message = receive_response(sock)
        logging.debug(f'Received welcome message: {welcome_message}')
        
        time.sleep(1)  # Ensure server readiness
    
        # Login
        if not login(sock):
            sock.close()
            return
    
        # Request Minute Chart data
        request_minute_chart(sock, symbol, start_time)
    
        # Monitor and append data to database
        while True:
            response = receive_response(sock)
            if response:
                logging.debug(f'Received minute chart response: {response}')
                parse_and_store_data(response, symbol)
            else:
                logging.debug('No data received, retrying...')
            time.sleep(30)  # Polling interval (1 minute)
    
        sock.close()
        logging.info(f'Finished collecting data for {symbol}.')
    
    except Exception as e:
        logging.error(f'Error during DAS API test for {symbol}: {e}')

def get_tickers_from_db():
    conn = sqlite3.connect('tms_data.db')
    c = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')
    c.execute("SELECT DISTINCT TICKER FROM TradeParameters WHERE DATE=?", (today,))
    tickers = [row[0] for row in c.fetchall()]
    conn.close()
    return tickers

def main():
    symbols = get_tickers_from_db()
    if not symbols:
        logging.error('No tickers found for today.')
        return

    # Calculate start time as 04:00 EST on the current day
    est_offset = timedelta(hours=-5)  # EST is UTC-5
    current_date = datetime.now()
    start_time = (current_date + est_offset).replace(hour=4, minute=0, second=0, microsecond=0).strftime('%Y/%m/%d-%H:%M')

    # Check write permission
    if not check_write_permission('.'):
        logging.error('Permission issue. Please check your directory permissions.')
        return

    # Create and start a separate thread for each ticker
    threads = []
    for symbol in symbols:
        ticker_thread = Thread(target=process_ticker, args=(symbol, start_time))
        ticker_thread.start()
        threads.append(ticker_thread)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
