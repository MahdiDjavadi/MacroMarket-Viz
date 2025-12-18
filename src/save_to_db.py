# save_to_mysql.py
# Works both locally (dotenv) and GitHub CI (Secrets)

import os

from dotenv import load_dotenv

# Try to detect environment
is_local = os.path.exists(".env")

if is_local:
    from src.db_connection import get_connection as get_db_connection
    print("🌍 Environment detected: Local (.env)")
else:
    from src.db_connection_ci import get_connection as get_db_connection
    print("☁️ Environment detected: GitHub CI/Secrets")

def insert_market_data(data):
    """Insert market data into the market_data table."""
    conn = get_db_connection()
    if not conn:
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

if __name__ == "__main__":
    print("🔍 Testing database connection...")
    conn = get_db_connection()
    if conn:
        conn.close()
        print("✅ Connection closed cleanly.")
