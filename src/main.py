import asyncio
import json
import websockets
import aiohttp
import sqlite3
import time
from aiohttp_socks import ProxyConnector
from dotenv import load_dotenv
import os

# Define pathes
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "binance_tickers.db")
ENV_PATH = os.path.join(BASE_DIR, ".env")

# Ensure "data" directory exists
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# Load .env
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)
else:
    print(f"⚠️ Warning: .env file not found at {ENV_PATH}")

# Load variables from .env
if os.getenv("NETWORK_CONFIG"):
    NETWORK_CONFIG = os.getenv("NETWORK_CONFIG")

# Binance WebSocket URL
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/!ticker@arr"

# Use a single shared connection (prevents locking)
conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
cursor = conn.cursor()

# Enable WAL mode for concurrent reads/writes
cursor.execute("PRAGMA journal_mode=WAL;")
cursor.execute("PRAGMA synchronous=NORMAL;")  # Optimize write speed
conn.commit()

def create_table():
    """
    Create the SQLite table if it doesn't exist.
    """
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS binance_tickers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        event_time INTEGER,  -- Store as Unix Timestamp
        last_price REAL,
        high_price REAL,
        low_price REAL,
        base_volume REAL
    );
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_event_time ON binance_tickers(event_time);")
    conn.commit()
    print("✅ SQLite table is ready.")

async def save_to_sqlite(ticker):
    """
    Insert Binance ticker data into SQLite (without proxy).
    """
    try:
        symbol = ticker['s']

        # Filter: Only process symbols ending with 'USDT'
        if not symbol.endswith("USDT"):
            return  # Skip if it's not a USDT pair

        event_time = int(ticker['E'])  # Convert to integer (milliseconds)

        # Insert data using shared connection (prevents locking)
        cursor.execute("""
        INSERT INTO binance_tickers (symbol, event_time, last_price, high_price, low_price, base_volume)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (
            symbol,
            event_time,
            float(ticker['c']),  # last_price
            float(ticker['h']),  # high_price
            float(ticker['l']),  # low_price
            float(ticker['v'])   # base_volume
        ))
        conn.commit()

    except Exception as e:
        print(f"❌ SQLite Insert Error: {e}")

async def cleanup_old_data():
    """
    Deletes rows older than 10 minutes from SQLite to keep the database lightweight.
    Runs every 5 minutes.
    """
    while True:
        # Wait a few minutes before first cleanup
        await asyncio.sleep(300)  # Runs cleanup every 5 minutes

        try:
            # Establish a NEW connection to prevent shared connection issues
            conn_cleanup = sqlite3.connect(DB_PATH, isolation_level=None)
            cursor_cleanup = conn_cleanup.cursor()

            # Compute time cutoff (10 minutes ago in milliseconds)
            cutoff_time = int(time.time() * 1000) - (10 * 60 * 1000)

            # Delete old data
            cursor_cleanup.execute("DELETE FROM binance_tickers WHERE event_time < ?", (cutoff_time,))
            conn_cleanup.commit()
            print(f"🗑️ Deleted rows older than 10 minutes.")

            conn_cleanup.close()

        except Exception as e:
            print(f"❌ Cleanup error: {e}")

async def ping_websocket(ws):
    """
    Periodically send a ping to the WebSocket to keep the connection alive.
    """
    while True:
        try:
            await ws.ping()
            await asyncio.sleep(30)  # Ping every 30 seconds to keep connection alive
        except Exception as e:
            print(f"Ping error: {e}")
            break

async def fetch_binance_tickers():
    """
    Connect to Binance WebSocket and insert data into SQLite.
    """
    while True:
        try:
            # Use ProxyConnector for WebSocket if necessary
            connector = ProxyConnector.from_url(NETWORK_CONFIG) if NETWORK_CONFIG else None
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.ws_connect(BINANCE_WS_URL) as ws:
                    print("✅ Connected to Binance WebSocket.")

                    while True:
                        try:
                            response = await ws.receive()
                            tickers = json.loads(response.data)

                            # Process tickers in smaller batches with a delay
                            batch_size = 100
                            for i in range(0, len(tickers), batch_size):
                                batch = tickers[i:i + batch_size]
                                tasks = [save_to_sqlite(ticker) for ticker in batch]
                                await asyncio.gather(*tasks)

                        except websockets.exceptions.ConnectionClosedError:
                            print("WebSocket connection closed. Reconnecting...")
                            break  # Break to reconnect

                        except Exception as e:
                            print(f"Error during WebSocket message handling: {e}")
                            break  # Break to reconnect
        except Exception as e:
            print(f"Error: {e}. Retrying connection...")
            await asyncio.sleep(5)  # Wait before reconnecting

async def main():
    # Create SQLite table before starting
    create_table()

    # Start WebSocket, data fetching, and auto cleaning
    await asyncio.gather(fetch_binance_tickers(), cleanup_old_data())

if __name__ == "__main__":
    asyncio.run(main())