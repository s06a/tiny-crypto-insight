import asyncio
import json
import websockets
import aiohttp
import sqlite3
import time
from aiohttp_socks import ProxyConnector
from dotenv import load_dotenv
import os
import pandas as pd

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "mexc_tickers.db")

# Ensure "data" directory exists
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# MEXC Futures WebSocket URL
MEXC_FUTURES_WS_URL = "wss://contract.mexc.com/edge"

# Use a single shared connection (prevents locking)
conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
cursor = conn.cursor()

# Enable WAL mode for concurrent reads/writes
cursor.execute("PRAGMA journal_mode=WAL;")
cursor.execute("PRAGMA synchronous=NORMAL;")  # Optimize write speed
conn.commit()

async def cleanup_old_data():
    """
    Deletes rows older than 10 minutes from SQLite to keep the database lightweight.
    """
    while True:
        await asyncio.sleep(300)  # Runs cleanup every 5 minutes
        try:
            cutoff_time = int(time.time() * 1000) - (10 * 60 * 1000)
            cursor.execute("DELETE FROM mexc_tickers WHERE event_time < ?", (cutoff_time,))
            conn.commit()
        except Exception as e:
            print(f"❌ Cleanup Error: {e}")

async def compute_and_store_volatility():
    """
    Compute mean price and volatility for all symbols using pandas, updating existing records.
    """
    while True:
        await asyncio.sleep(60)  # Compute volatility every 60 seconds

        cutoff_time = int(time.time() * 1000) - (10 * 60 * 1000)

        try:
            # Fetch data from SQLite in chunks to avoid memory overload
            query = """
            SELECT symbol, event_time, last_price 
            FROM mexc_tickers 
            WHERE event_time >= ?
            """
            df = pd.read_sql_query(query, conn, params=(cutoff_time,))

            if df.empty:
                continue  # Skip if no data is available

            # Group by symbol
            grouped = df.groupby('symbol')

            volatility_data = []

            for symbol, group in grouped:
                # Calculate mean price
                mean_price = group['last_price'].mean()

                # Only calculate volatility if there are enough data points
                if len(group) > 1:
                    # Calculate returns and their standard deviation (volatility)
                    group['return'] = group['last_price'].pct_change()
                    volatility = group['return'].std() if len(group) > 1 else 0.0  # Avoid division by zero
                else:
                    volatility = 0.0

                # Store result for this symbol, keeping track of the event time
                volatility_data.append({
                    'symbol': symbol,
                    'event_time': group['event_time'].max(),  # Most recent timestamp
                    'mean_price': mean_price,
                    'volatility': volatility
                })

            # Convert results to DataFrame
            volatility_df = pd.DataFrame(volatility_data)

            # Skip if no data to insert
            if volatility_df.empty:
                continue

            # Insert new volatility data into the table (no replace)
            with conn:
                cursor.executemany("""
                INSERT INTO volatility (symbol, event_time, mean_price, volatility)
                VALUES (?, ?, ?, ?)
                """, [(row['symbol'], row['event_time'], row['mean_price'], row['volatility']) for _, row in volatility_df.iterrows()])

            # Fetch top 10 most volatile symbols ranked based on the latest data
            query = """
            SELECT symbol, mean_price, volatility
            FROM (
                SELECT symbol, mean_price, volatility,
                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY event_time DESC) AS rn
                FROM volatility
            ) AS subquery
            WHERE rn = 1
            ORDER BY volatility DESC
            LIMIT 10
            """
            results = conn.execute(query).fetchall()

            print("\n📊 Updated Volatility for All Symbols:")
            for row in results:
                symbol = row[0]
                mean_price = row[1] if row[1] is not None else 0.0
                volatility = row[2] if row[2] is not None else 0.0
                print(f"🔹 {symbol}: Mean = {mean_price:.2f}, Volatility = {volatility:.4f}")

        except Exception as e:
            print(f"❌ SQLite Volatility Error: {e}")


def create_tables():
    """
    Create necessary SQLite tables if they don't exist.
    """
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS mexc_tickers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        event_time INTEGER,  -- Store as Unix Timestamp
        last_price REAL,
        rise_fall_rate REAL,
        volume_24 REAL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS volatility (
        symbol TEXT PRIMARY KEY,
        event_time INTEGER,
        mean_price REAL,
        volatility REAL
    );
    """)

    conn.commit()
    print("✅ SQLite tables are ready.")

async def send_ping(ws):
    """
    Send a heartbeat 'ping' to keep the WebSocket connection alive.
    """
    while True:
        await asyncio.sleep(30)  # Send ping every 30 seconds
        try:
            await ws.ping()  # Ping the server to keep connection alive
        except Exception as e:
            print(f"❌ Ping Error: {e}")
            break

async def save_to_sqlite(ticker_batch):
    """
    Insert MEXC ticker data into SQLite in batches.
    """
    try:
        with conn:
            for ticker in ticker_batch:
                if ticker['symbol'].endswith('USDT'):
                    cursor.execute("""
                    INSERT INTO mexc_tickers (symbol, event_time, last_price, rise_fall_rate, volume_24)
                    VALUES (?, ?, ?, ?, ?)
                    """, (
                        ticker['symbol'],
                        ticker['timestamp'],
                        float(ticker['lastPrice']),
                        float(ticker['riseFallRate']),
                        float(ticker['volume24'])
                    ))
    except Exception as e:
        print(f"❌ SQLite Insert Error: {e}")

async def fetch_mexc_tickers():
    """
    Connect to MEXC WebSocket and insert data into SQLite.
    """
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(MEXC_FUTURES_WS_URL) as ws:
                    print("✅ Connected to MEXC WebSocket.")
                    subscription_message = {
                        "method": "sub.tickers",
                        "param": {}
                    }
                    await ws.send_json(subscription_message)
                    asyncio.create_task(send_ping(ws))

                    ticker_batch = []
                    while True:
                        try:
                            response = await ws.receive(timeout=60)
                            if isinstance(response.data, str):
                                message = json.loads(response.data)
                                if 'data' in message:
                                    if isinstance(message['data'], str) and message['data'] == "success":
                                        continue
                                    tickers = message['data']
                                    for ticker in tickers:
                                        ticker_batch.append(ticker)
                                    if len(ticker_batch) >= 100:
                                        await save_to_sqlite(ticker_batch)
                                        ticker_batch.clear()
                        except asyncio.TimeoutError:
                            print("❌ WebSocket timeout.")
                            break
                        except Exception as e:
                            print(f"Error during WebSocket message handling: {e}")
                            break
        except Exception as e:
            print(f"Error: {e}. Retrying connection...")
            await asyncio.sleep(5)


async def main():
    """
    Start WebSocket listener, periodic volatility computation, and cleanup.
    """
    create_tables()

    await asyncio.gather(
        fetch_mexc_tickers(),
        compute_and_store_volatility(),
        cleanup_old_data()
    )

if __name__ == "__main__":
    asyncio.run(main())
