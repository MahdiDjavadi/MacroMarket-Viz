# src/commodities_alpha.py
# Fetch Brent Oil & Gold (daily ~90 days) from Alpha Vantage
# Save to JSON + insert into MySQL (auto env detection)

import json
import requests
from pathlib import Path
from config.settings import API_KEYS
from src.symbol_mapper import get_symbol_id
from src.db_loader import get_connection

DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "commodities_indexes.json"

COMMODITIES = {
    "BRENT": {
        "function": "BRENT",
        "symbol": "BRENT"
    },
    "GOLD": {
        "function": "XAUUSD",
        "symbol": "GOLD"
    }
}


def fetch_commodity(cfg):
    url = "https://www.alphavantage.co/query"
    params = {
        "function": cfg["function"],
        "apikey": API_KEYS.get("ALPHA_VANTAGE_API_KEY")
    }

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    key = next((k for k in data.keys() if "data" in k.lower()), None)
    if not key:
        return []

    symbol_id = get_symbol_id(cfg["symbol"])
    if not symbol_id:
        return []

    out = []
    for row in data[key][:90]:
        out.append({
            "symbol_id": symbol_id,
            "date": row["date"],
            "open": float(row["value"]),
            "high": None,
            "low": None,
            "close": float(row["value"]),
            "volume": None
        })
    return out


def save_json(data):
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    print(f"✅ Saved {len(data)} rows to commodities_indexes.json")


def insert_db(data):
    conn = get_connection()
    cur = conn.cursor()
    q = """
    INSERT INTO market_data (symbol_id, date, open, high, low, close, volume)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      open=VALUES(open),
      close=VALUES(close)
    """
    for r in data:
        cur.execute(q, (
            r["symbol_id"], r["date"], r["open"],
            r["high"], r["low"], r["close"], r["volume"]
        ))
    conn.commit()
    cur.close()
    conn.close()
    print(f"💾 Inserted {len(data)} rows into DB")


def main():
    print("📡 Fetching commodities from Alpha Vantage...")
    all_data = []
    for name, cfg in COMMODITIES.items():
        print(f"⛽ {name}")
        all_data.extend(fetch_commodity(cfg))
    save_json(all_data)
    insert_db(all_data)
    print("🏁 Done.")


if __name__ == "__main__":
    main()
