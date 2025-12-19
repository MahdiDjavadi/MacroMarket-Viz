# src/commodities_alpha.py
# BRENT (commodity) + GOLD (via XAUUSD FX)

import json
import requests
from pathlib import Path
from config.settings import API_KEYS
from src.symbol_mapper import get_symbol_id
from src.db_loader import get_connection

DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "commodities_indexes.json"

def fetch_brent():
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "BRENT",
        "apikey": API_KEYS["ALPHA_VANTAGE_API_KEY"]
    }

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    sid = get_symbol_id("BRENT")
    if not sid:
        return []

    return [
        {
            "symbol_id": sid,
            "date": row["date"],
            "open": float(row["value"]),
            "high": None,
            "low": None,
            "close": float(row["value"]),
            "volume": None
        }
        for row in data.get("data", [])[:90]
    ]


def fetch_gold_fx():
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "FX_DAILY",
        "from_symbol": "XAU",
        "to_symbol": "USD",
        "outputsize": "compact",
        "apikey": API_KEYS["ALPHA_VANTAGE_API_KEY"]
    }

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    ts = data.get("Time Series FX (Daily)", {})
    sid = get_symbol_id("GOLD")
    if not sid:
        return []

    out = []
    for date, v in list(ts.items())[:90]:
        out.append({
            "symbol_id": sid,
            "date": date,
            "open": float(v["1. open"]),
            "high": float(v["2. high"]),
            "low": float(v["3. low"]),
            "close": float(v["4. close"]),
            "volume": None
        })
    return out


def main():
    print("📡 Fetching commodities...")

    data = []
    data.extend(fetch_brent())
    data.extend(fetch_gold_fx())

    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    print(f"✅ JSON saved ({len(data)} rows)")

    conn = get_connection()
    cur = conn.cursor()
    q = """
    INSERT INTO market_data (symbol_id, date, open, high, low, close, volume)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      open=VALUES(open),
      high=VALUES(high),
      low=VALUES(low),
      close=VALUES(close)
    """
    for r in data:
        cur.execute(q, (
            r["symbol_id"], r["date"],
            r["open"], r["high"], r["low"],
            r["close"], r["volume"]
        ))
    conn.commit()
    cur.close()
    conn.close()

    print("🏁 Done")


if __name__ == "__main__":
    main()
