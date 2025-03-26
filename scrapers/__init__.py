from .base import *

# Import app-specific settings
from .scraper import SCRAPER_SETTINGS

# Try to import local settings
try:
    from .local import *
except ImportError:
    pass