# src/fetch_macro_data.py

import requests
from datetime import datetime, timedelta, UTC

from config.settings import API_KEYS
from src.symbol_mapper import get_symbol_id

# ❗ استاندارد پروژه – اتصال دیتابیس
# from src.db_connection import get_connection
from src.db_loader import get_connection  # یا هر چیزی که db.py و CI_db.py ارائه می‌دهند


FRED_API_KEY = API_KEYS.get("FRED_API_KEY")


def fetch_fred_series(series_id, lookback_days=1800):
    start_date = (datetime.now(UTC) - timedelta(days=lookback_days)).date().isoformat()

    url = (
        f"https://api.stlouisfed.org/fred/series/observations?"
        f"series_id={series_id}&api_key={FRED_API_KEY}&file_type=json"
        f"&observation_start={start_date}"
    )

    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"❌ Request failed for {series_id}: {e}")
        return None

    data = r.json()
    return data.get("observations", [])


def upsert_macro_data(symbol_id, series_data, source="FRED"):
    conn = get_connection()
    cur = conn.cursor()

    sql = """
        INSERT INTO macro_indicators (symbol_id, date, value, unit, source)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE value=VALUES(value)
    """

    count = 0
    for obs in series_data:
        if obs.get("value") in ("", ".", None):
            continue

        cur.execute(sql, (
            symbol_id,
            obs["date"],
            float(obs["value"]),
            obs.get("units", None),
            source
        ))
        count += 1

    conn.commit()
    cur.close()
    conn.close()
    return count


def main():
    print("🚀 Fetching macro indicators from FRED...")

    series_map = {
        "FEDFUNDS": "FEDFUNDS",
        "CPI_US": "CPIAUCSL",
        "EU_CPI": "CP0000EZ19M086NEST",
        "ECB_RATE": "ECBDFR",
        "JAPAN_RATE": "IR3TIB01JPM156N",
    }

    for symbol, fred_id in series_map.items():
        print(f"📡 Fetching {symbol} ({fred_id}) ...")

        symbol_id = get_symbol_id(symbol)
        if not symbol_id:
            print(f"⚠️ No symbol_id for {symbol}")
            continue

        data = fetch_fred_series(fred_id)
        if not data:
            print(f"⚠️ No data for {symbol}")
            continue

        rows = upsert_macro_data(symbol_id, data)
        print(f"✅ Upserted {rows} rows for {symbol}")


if __name__ == "__main__":
    main()
