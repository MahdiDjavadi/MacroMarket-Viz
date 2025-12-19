# src/market_data_loader.py
# All-in-one: Fetch, save JSON, detect environment, insert to DB

import os
import json
import requests
from pathlib import Path
from datetime import datetime
from config.settings import API_KEYS
from src.symbol_mapper import get_symbol_id
import yfinance as yf
from src.db_loader import get_connection  # Hybrid: Local or CI

DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "market_indexes.json"

# ---------------- JSON ----------------
def save_json(data, filepath):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print(f"✅ Data saved to {filepath}")

# ---------------- Alpha Vantage ----------------
def fetch_alpha_vantage_index(symbol):
    base_url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": API_KEYS.get("ALPHA_VANTAGE_API_KEY")
    }
    try:
        r = requests.get(base_url, params=params, timeout=30)
        data = r.json()
    except Exception as e:
        print(f"⚠️ AlphaVantage request failed for {symbol}: {e}")
        return None

    if "Time Series (Daily)" not in data:
        print(f"⚠️ AlphaVantage returned no data for {symbol}")
        return None

    ts = data["Time Series (Daily)"]
    symbol_id = get_symbol_id(symbol)
    if not symbol_id:
        print(f"⚠️ symbol_id not found for {symbol}")
        return None

    formatted = []
    for date, values in list(ts.items())[:90]:
        formatted.append({
            "symbol_id": symbol_id,
            "date": date,
            "open": float(values.get("1. open", 0)),
            "high": float(values.get("2. high", 0)),
            "low": float(values.get("3. low", 0)),
            "close": float(values.get("4. close", 0)),
            "volume": int(values.get("5. volume", 0))
        })
    return formatted

# ---------------- yfinance ----------------
def fetch_yfinance_index(symbol):
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="3mo", interval="1d", auto_adjust=False)
    except Exception as e:
        print(f"⚠️ yfinance request failed for {symbol}: {e}")
        return None

    if df is None or df.empty:
        print(f"⚠️ yfinance returned no data for {symbol}")
        return None

    # اگر طلا باشه، symbol_id همان GOLD باشد
    if symbol == "GC=F":
        symbol_id = get_symbol_id("GOLD")
    else:
        symbol_id = get_symbol_id(symbol)

    if not symbol_id:
        print(f"⚠️ symbol_id not found for {symbol}")
        return None

    formatted = []
    for _, row in df.reset_index().iterrows():
        date = row["Date"].strftime("%Y-%m-%d")
        formatted.append({
            "symbol_id": symbol_id,
            "date": date,
            "open": float(row["Open"]) if not row.isna().get("Open") else None,
            "high": float(row["High"]) if not row.isna().get("High") else None,
            "low": float(row["Low"]) if not row.isna().get("Low") else None,
            "close": float(row["Close"]) if not row.isna().get("Close") else None,
            "volume": int(row["Volume"]) if not row.isna().get("Volume") else None,
        })
    return formatted

# ---------------- Insert to DB ----------------
def insert_market_data(data):
    conn = get_connection()
    if not conn:
        print("❌ DB connection failed.")
        return

    cursor = conn.cursor()
    query = """
        INSERT INTO market_data (symbol_id, date, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open = VALUES(open),
            high = VALUES(high),
            low = VALUES(low),
            close = VALUES(close),
            volume = VALUES(volume)
    """
    try:
        for row in data:
            cursor.execute(query, (
                row["symbol_id"],
                row["date"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row.get("volume", None)
            ))
        conn.commit()
        print(f"✅ Inserted {len(data)} rows into 'market_data'.")
    except Exception as err:
        print(f"❌ Error inserting market data: {err}")
    finally:
        cursor.close()
        conn.close()

# ---------------- MAIN ----------------
def main():
    print("🚀 Fetching market index data...")

    all_data = []

    # Alpha Vantage US ETFs
    us_symbols = ["SPY", "DIA", "QQQ"]
    for sym in us_symbols:
        print(f"📈 Fetching Alpha Vantage data for {sym}...")
        res = fetch_alpha_vantage_index(sym)
        if res:
            all_data.extend(res)

    # Global indexes via yfinance
    yahoo_symbols = ["^STOXX50E", "^FTSE", "^GDAXI", "^N225", "^HSI", "000001.SS"]
    for sym in yahoo_symbols:
        print(f"🌍 Fetching yfinance data for {sym}...")
        res = fetch_yfinance_index(sym)
        if res:
            all_data.extend(res)

    # GOLD Futures (GC=F) via yfinance
    gold_symbols = ["GC=F"]
    for sym in gold_symbols:
        print(f"🌟 Fetching yfinance data for {sym} (GOLD)...")
        res = fetch_yfinance_index(sym)
        if res:
            all_data.extend(res)

    # Save JSON
    save_json(all_data, DATA_PATH)

    # Insert into DB
    print("💾 Inserting data into database...")
    insert_market_data(all_data)

    print("✅ Market data pipeline complete.")


if __name__ == "__main__":
    main()
