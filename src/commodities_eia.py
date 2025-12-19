# src/commodities_eia.py
# EIA fetch with explicit headers (anti-403 hardening)

import os
import json
import requests
from pathlib import Path
from datetime import datetime

EIA_API_KEY = os.getenv("EIA_API_KEY")

DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "commodities_indexes.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

SERIES_MAP = {
    "BRENT": {
        "series": "RBRTE",
        "symbol_id": 11,  # adjust if needed
    },
}

def fetch_series(name, cfg):
    url = "https://api.eia.gov/v2/petroleum/pri/spt/data"
    params = {
        "api_key": EIA_API_KEY,
        "frequency": "daily",
        "data[0]": "value",
        "facets[series]": cfg["series"],
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": 90,
    }

    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    payload = r.json()

    rows = payload.get("response", {}).get("data", [])
    out = []

    for row in rows:
        out.append({
            "symbol_id": cfg["symbol_id"],
            "date": row["period"],
            "open": None,
            "high": None,
            "low": None,
            "close": float(row["value"]) if row.get("value") is not None else None,
            "volume": None,
        })

    return out


def save_json(data):
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    print(f"✅ Saved {len(data)} rows → {DATA_PATH}")


def main():
    print("📡 Fetching commodities from EIA...")
    all_data = []

    for name, cfg in SERIES_MAP.items():
        print(f"⛽ Fetching {name} ...")
        res = fetch_series(name, cfg)
        all_data.extend(res)

    save_json(all_data)


if __name__ == "__main__":
    main()
