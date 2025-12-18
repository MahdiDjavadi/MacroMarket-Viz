# src/load_and_insert.py

import json
from pathlib import Path
from src.save_to_db import insert_market_data

DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "market_indexes.json"


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


def main():
    print("🚀 Loading market index JSON data...")

    data = load_json(DATA_PATH)
    if not data:
        print("⚠️ No data loaded. Aborting.")
        return

    print("💾 Inserting data into MySQL database...")
    insert_market_data(data)

    print("🏁 Done.")


if __name__ == "__main__":
    main()
