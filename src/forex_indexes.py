# src/fetch_forex.py
# Fetch Forex data (last ~90 days) and insert into DB
# JSON file: data/forex_indexes.json

import os
import json
import requests
from pathlib import Path
from config.settings import API_KEYS
from src.symbol_mapper import get_symbol_id
from src.db_loader import get_connection

DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "forex_indexes.json"
FOREX_SYMBOLS = ["USD/EUR", "USD/JPY", "EUR/GBP"]

# ---------------- Save JSON ----------------
def save_json(data, filepath):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print(f"✅ Forex data saved to {filepath}")

# ---------------- Load JSON ----------------
def load_json(filepath):
    if not filepath.exists():
        print(f"❌ JSON file not found: {filepath}")
        return None
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"📄 Loaded {len(data)} records from JSON.")
        return data
    except Exception as e:
        print(f"❌ Error loading JSON: {e}")
        return None

# ---------------- Fetch Forex ----------------
def fetch_forex(symbol):
    from_symbol, to_symbol = symbol.split("/")
    base_url = "https://www.alphavantage.co/query"
    params = {
        "function": "FX_DAILY",
        "from_symbol": from_symbol,
        "to_symbol": to_symbol,
        "outputsize": "compact",  # last ~100 days
        "apikey": API_KEYS.get("ALPHA_VANTAGE_API_KEY")
    }
    try:
        r = requests.get(base_url, params=params, timeout=30)
        data = r.json()
    except Exception as e:
        print(f"⚠️ AlphaVantage request failed for {symbol}: {e}")
        return None

    ts = data.get("Time Series FX (Daily)")
    if not ts:
        print(f"⚠️ No data returned for {symbol}")
        return None

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
            "close": float(values.get("4. close", 0))
        })
    return formatted

# ---------------- Insert to DB ----------------
def insert_forex_data(data):
    conn = get_connection()
    if not conn:
        print("❌ DB connection failed.")
        return

    cursor = conn.cursor()
    query = """
        INSERT INTO market_data (symbol_id, date, open, high, low, close)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open = VALUES(open),
            high = VALUES(high),
            low = VALUES(low),
            close = VALUES(close)
    """
    try:
        for row in data:
            cursor.execute(query, (
                row["symbol_id"],
                row["date"],
                row["open"],
                row["high"],
                row["low"],
                row["close"]
            ))
        conn.commit()
        print(f"✅ Inserted {len(data)} rows into 'market_data'.")
    except Exception as err:
        print(f"❌ Error inserting Forex data: {err}")
    finally:
        cursor.close()
        conn.close()

# ---------------- MAIN ----------------
def main():
    all_data = []

    # Fetch and collect data
    for sym in FOREX_SYMBOLS:
        print(f"🌍 Fetching Forex data for {sym}...")
        res = fetch_forex(sym)
        if res:
            all_data.extend(res)

    # Save JSON
    save_json(all_data, DATA_PATH)

    # Load from JSON and insert to DB
    data_to_insert = load_json(DATA_PATH)
    if data_to_insert:
        insert_forex_data(data_to_insert)

    print("✅ Forex pipeline complete.")

if __name__ == "__main__":
    main()
