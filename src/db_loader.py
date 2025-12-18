import os
import sys
from pathlib import Path

# --- تشخیص محیط ---
# اگر env محلی (وجود فایل .env) => لوکال
project_root = Path(__file__).resolve().parents[1]
dotenv_file = project_root / ".env"

if dotenv_file.exists():
    # لوکال
    from src.db import *
    print("🌍 Running in LOCAL mode, db.py loaded")
else:
    # CI / GitHub
    from src.CI_db import *
    print("☁️ Running in CI mode, CI_db.py loaded")
