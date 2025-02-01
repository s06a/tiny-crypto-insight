# **Streamy** 🌊 

**Streamy** is a lightweight, real-time crypto data streamer with time series analysis capabilities.

---

## **✨ Features**
✅ **Tiny & Efficient** – Minimal resource usage, runs on any device  
✅ **Real-Time Data** – Streams live Binance ticker data  
✅ **Historical Storage** – Saves price & volume in SQLite  
✅ **Auto-Cleanup** – Deletes old data to keep the database light  
✅ **Future-Ready** – Designed for ML, time-series analysis, and volatility tracking

---

## **🚀 Installation**
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

## **▶️ Running the App**
Start the crypto data streamer:  
```sh
python src/main.py # or bash run.sh
```
💡 **This will:**  
- Connect to Binance WebSocket  
- Store crypto ticker data in SQLite  
- Automatically remove old data every 10 minutes  

---

## **📜 License**
MIT License – Free to use, modify & share
