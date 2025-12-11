import json
import os
from pathlib import Path
from dotenv import load_dotenv

# --- Hybrid Environment Loader (local + CI) ---

# If .env exists (local mode), load it
dotenv_path = Path(__file__).resolve().parent.parent / ".env"
if dotenv_path.exists():
    load_dotenv(dotenv_path)
    print("🌍 Loaded local .env configuration")
else:
    print("☁️ Using GitHub Secrets / Environment variables")

# --- Load configuration files ---
CONFIG_DIR = Path(__file__).resolve().parent
CONFIG_PATH = CONFIG_DIR / "api_sources.json"

if CONFIG_PATH.exists():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        CONFIG_SOURCES = json.load(f)
else:
    CONFIG_SOURCES = {}
    print("⚠️ Missing api_sources.json file in config directory.")

# --- API Keys (both local .env and GitHub Secrets) ---
API_KEYS = {
    "ALPHA_VANTAGE_API_KEY": os.getenv("ALPHA_VANTAGE_API_KEY"),
    "FRED_API_KEY": os.getenv("FRED_API_KEY"),
    "EIA_API_KEY": os.getenv("EIA_API_KEY"),
    "RAPIDAPI_KEY": os.getenv("RAPIDAPI_KEY"),
    "NEWS_API_KEY": os.getenv("NEWS_API_KEY"),
}

# --- Validation ---
missing = [k for k, v in API_KEYS.items() if not v]
if missing:
    print(f"⚠️ Missing API keys: {', '.join(missing)}")
else:
    print("✅ All API keys loaded successfully.")
