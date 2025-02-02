⚡ Streamy — Real-time crypto streaming & insightful analytics
---

## **1. Features**
- **Tiny & Efficient** – Minimal resource usage, runs on any device
- **Real-Time Data** – Streams live MEXC ticker data  
- **Historical Storage** – Saves tickers data in SQLite  
- **Auto-Cleanup** – Deletes old data to keep the database light  
- **Future-Ready** – Designed for ML and time-series analysis

---

## **2. Installation**
1️⃣ **Clone the repository**  
```sh
git clone https://github.com/yourusername/streamy.git
cd streamy
```
2️⃣ **Install dependencies**  
```sh
pip install -r requirements.txt
```

---

## **3. Running the App**
Start the crypto data streamer:  
```sh
python src/main.py # or bash run.sh
```
💡 **This will:**  
- Connect to MEXC Futures WebSocket
- Stream real-time crypto futures ticker data
- Store crypto futures price, volume, and volatility in SQLite
- Automatically remove old data every 10 minutes to keep the database light
- Rank tickers based on volatility for better trading insights

---

## **4. License**
MIT License – Free to use, modify & share