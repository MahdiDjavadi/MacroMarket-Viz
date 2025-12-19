# src/crypto_coingecko.py
# Fetch Crypto daily prices (~90 days) from CoinGecko
# Save to JSON + insert into MySQL (auto env detection)

import json
import requests
from pathlib import Path
from src.symbol_mapper import get_symbol_id
from src.db_loader import get_connection

DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "crypto_indexes.json"

CRYPTOS = {
    "bitcoin": "bitcoin",
    "ethereum": "ethereum",
    "solana": "solana",
    "ripple": "ripple"
}


def fetch_crypto(coin_id):
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": "usd",
        "days": 90,
        "interval": "daily"
    }

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    symbol_id = get_symbol_id(coin_id)
    if not symbol_id:
        return []

    out = []
    prices = data.get("prices", [])
    vols = data.get("total_volumes", [])

    for i in range(len(prices)):
        date = prices[i][0]
        price = prices[i][1]
        volume = vols[i][1] if i < len(vols) else None

        out.append({
            "symbol_id": symbol_id,
            "date": str(__import__("datetime").datetime.utcfromtimestamp(date / 1000).date()),
            "open": price,
            "high": None,
            "low": None,
            "close": price,
            "volume": volume
        })
    return out


def save_json(data):
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    print(f"✅ Saved {len(data)} rows to crypto_indexes.json")


def insert_db(data):
    conn = get_connection()
    cur = conn.cursor()
    q = """
    INSERT INTO market_data (symbol_id, date, open, high, low, close, volume)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      open=VALUES(open),
      close=VALUES(close),
      volume=VALUES(volume)
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
    print("🪙 Fetching crypto market data...")
    all_data = []
    for name, cid in CRYPTOS.items():
        print(f"🚀 {name}")
        all_data.extend(fetch_crypto(cid))
    save_json(all_data)
    insert_db(all_data)
    print("🏁 Done.")


if __name__ == "__main__":
    main()
