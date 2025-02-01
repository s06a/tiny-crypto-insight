import asyncio
import json
import websockets
import aiohttp
import sqlite3
import time
from aiohttp_socks import ProxyConnector
from dotenv import load_dotenv
import os

# Define paths
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

# Binance WebSocket URL for Futures Market
BINANCE_FUTURES_WS_URL = "wss://fstream.binance.com/ws/!ticker@arr"

# Use a single shared connection (prevents locking)
conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
cursor = conn.cursor()

# Enable WAL mode for concurrent reads/writes
cursor.execute("PRAGMA journal_mode=WAL;")
cursor.execute("PRAGMA synchronous=NORMAL;")  # Optimize write speed
conn.commit()

async def ping_websocket(ws):
    """
    Periodically send a ping to the WebSocket to keep the connection alive.
    """
    while True:
        try:
            await ws.ping()
            await asyncio.sleep(30)  # Ping every 30 seconds
        except Exception as e:
            print(f"❌ WebSocket Ping Error: {e}")
            break

def create_tables():
    """
    Create necessary SQLite tables if they don't exist.
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

async def save_to_sqlite(ticker):
    """
    Insert Binance ticker data into SQLite.
    """
    try:
        symbol = ticker['s']
        if not symbol.endswith("USDT"):  # Only process USDT pairs
            return

        event_time = int(ticker['E'])  # Convert to integer (milliseconds)

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
    """
    while True:
        await asyncio.sleep(300)  # Runs cleanup every 5 minutes

        try:
            cutoff_time = int(time.time() * 1000) - (10 * 60 * 1000)
            conn.execute("DELETE FROM binance_tickers WHERE event_time < ?", (cutoff_time,))
            conn.commit()

        except Exception as e:
            print(f"❌ Cleanup Error: {e}")

async def compute_and_store_volatility():
    """
    Compute mean price and volatility for all symbols, updating existing records.
    """
    while True:
        await asyncio.sleep(60)  # Compute volatility every 60 seconds

        cutoff_time = int(time.time() * 1000) - (10 * 60 * 1000)

        try:
            conn.execute("""
            REPLACE INTO volatility (symbol, event_time, mean_price, volatility)
            SELECT 
                symbol,
                MAX(event_time) AS event_time,
                AVG(last_price) AS mean_price,
                CASE 
                    WHEN COUNT(last_price) > 1 
                    THEN COALESCE(SQRT(
                        CASE 
                            WHEN (AVG(last_price * last_price) - AVG(last_price) * AVG(last_price)) > 0 
                            THEN (AVG(last_price * last_price) - AVG(last_price) * AVG(last_price)) 
                            ELSE 0 
                        END
                    ), 0)
                    ELSE 0 
                END AS volatility
            FROM binance_tickers
            WHERE event_time >= ?
            GROUP BY symbol
            """, (cutoff_time,))

            conn.commit()

            # Fetch latest volatility data
            results = conn.execute("SELECT symbol, mean_price, volatility FROM volatility ORDER BY volatility DESC LIMIT 10").fetchall()

            print("\n📊 Updated Volatility for All Symbols:")
            for row in results:
                symbol = row[0]
                mean_price = row[1] if row[1] is not None else 0.0
                volatility = row[2] if row[2] is not None else 0.0
                print(f"🔹 {symbol}: Mean = {mean_price:.2f}, Volatility = {volatility:.4f}")

        except Exception as e:
            print(f"❌ SQLite Volatility Error: {e}")

async def fetch_binance_tickers():
    """
    Connect to Binance WebSocket and insert data into SQLite.
    """
    while True:
        try:
            connector = ProxyConnector.from_url(NETWORK_CONFIG) if NETWORK_CONFIG else None
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.ws_connect(BINANCE_FUTURES_WS_URL) as ws:
                    print("✅ Connected to Binance WebSocket.")

                    while True:

                        # Start WebSocket ping task
                        asyncio.create_task(ping_websocket(ws))

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
    """
    Start WebSocket listener, periodic volatility computation, and cleanup.
    """
    create_tables()

    await asyncio.gather(
        fetch_binance_tickers(),
        compute_and_store_volatility(),
        cleanup_old_data()
    )

if __name__ == "__main__":
    asyncio.run(main())