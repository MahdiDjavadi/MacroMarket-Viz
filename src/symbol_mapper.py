# src/symbol_mapper.py

from src.db import get_connection
import os

def load_symbol_map():
    """Load all symbols from DB and return {symbol: symbol_id}"""
    conn = get_connection()
    if not conn:
        return {}

    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT symbol_id, symbol FROM symbols")
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return {row["symbol"]: row["symbol_id"] for row in rows}

# Load once and cache it
SYMBOL_MAP = load_symbol_map()

def get_symbol_id(symbol):
    """Return symbol_id or None"""
    return SYMBOL_MAP.get(symbol)
