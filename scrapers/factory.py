from scrapers.scrapers.nsi_scraper import NSIScraper
from scrapers.scrapers.bnb_scraper import BNBScraper


# Import other scrapers as needed

class ScraperFactory:
    """Factory for creating scrapers for different institutions"""

    @staticmethod
    def create_scraper(institution_code):
        """Create a scraper instance for the specified institution"""
        scrapers = {
            "NSI": NSIScraper,
            "BNB": BNBScraper,
            # Add more scrapers here
        }

        scraper_class = scrapers.get(institution_code)
        if not scraper_class:
            raise ValueError(f"No scraper available for institution: {institution_code}")

        return scraper_class()

    @staticmethod
    def get_available_scrapers():
        """Get list of available scraper institution codes"""
        return ["NSI", "BNB"]  # Add more as you implement them