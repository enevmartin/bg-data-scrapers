import os
from pathlib import Path

# Base directory
BASE_DIR = os.environ.get("SCRAPER_BASE_DIR", str(Path.home() / "bg_data_scrapers"))

# Storage directory for scraped files
STORAGE_DIR = os.environ.get("SCRAPER_STORAGE_DIR", os.path.join(BASE_DIR, "storage"))

# Ensure directories exist
os.makedirs(STORAGE_DIR, exist_ok=True)

# Default scraper settings
SCRAPER_SETTINGS = {
    "USER_AGENT": "Mozilla/5.0 (compatible; BG-Data-Scraper/1.0; +https://github.com/yourusername/bg-data-scrapers)",
    "REQUEST_TIMEOUT": 30,  # seconds
    "DOWNLOAD_TIMEOUT": 300,  # seconds
    "REQUEST_DELAY": 1.0,  # seconds between requests
    "MAX_RETRIES": 3,

    # Institution-specific settings
    "INSTITUTIONS": {
        "NSI": {
            "base_url": "https://www.nsi.bg/",
            "data_path": "bg/content/766/статистически-данни",
            "enabled": True,
        },
        "BNB": {
            "base_url": "https://www.bnb.bg/",
            "data_path": "Statistics/index.htm",
            "enabled": True,
        },
        "MF": {  # Ministry of Finance
            "base_url": "https://www.minfin.bg/",
            "data_path": "bg/statistics",
            "enabled": True,
        },
        "NAP": {  # National Revenue Agency
            "base_url": "https://www.nap.bg/",
            "data_path": "page?id=524",
            "enabled": True,
        },
        # Add more institutions as needed
    }
}

# Try to load local settings if available
try:
    from local_settings import *
except ImportError:
    pass