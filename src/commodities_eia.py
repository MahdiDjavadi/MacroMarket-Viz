# src/fetch_commodities_eia.py

import json
import requests
from pathlib import Path
import os
from datetime import datetime

EIA_API_KEY = os.getenv("EIA_API_KEY")

BASE_URL = "https://api.eia.gov/v2/"

DATA_PATH = Path(__file__).resolve().parents[1] / "data"
DATA_PATH.mkdir(exist_ok=True)

OUTPUT_FILE = DATA_PATH / "commodities.json"

SERIES = {
    "BRENT": {
        "endpoint": "petroleum/pri/spt/data",
        "params": {
            "frequency": "daily",
            "data[0]": "value",
            "facets[series]": "RBRTE",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 90
        }
    },
    "GOLD": {
        "endpoint": "steo/price/data",
        "params": {
            "frequency": "daily",
            "data[0]": "value",
            "facets[series]": "GOLDAMGBD228NLBM",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 90
        }
    }
}


def fetch_series(symbol, config):
    url = BASE_URL + config["endpoint"]
    params = config["params"].copy()
    params["api_key"] = EIA_API_KEY

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()

    rows = r.json()["response"]["data"]

    result = []
    for row in rows:
        result.append({
            "symbol": symbol,
            "date": row["period"],
            "price": row["value"]
        })

    return result


def main():
    all_data = []

    for symbol, config in SERIES.items():
        print(f"📡 Fetching {symbol} from EIA...")
        data = fetch_series(symbol, config)
        all_data.extend(data)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=2)

    print(f"✅ Saved {len(all_data)} rows to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
