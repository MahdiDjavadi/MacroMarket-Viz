# save_to_db.py (Hybrid with db_loader)

from src.db_loader import get_connection  # خودش لوکال یا CI رو تشخیص میده

def insert_market_data(data):
    """Insert market data into the market_data table."""
    conn = get_connection()
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
    conn = get_connection()
    if conn:
        conn.close()
        print("✅ Connection closed cleanly.")
