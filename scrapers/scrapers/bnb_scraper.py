import os
import re
import pandas as pd
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from .base import BaseScraper

logger = logging.getLogger(__name__)


class BNBScraper(BaseScraper):
    """Scraper for Bulgarian National Bank (BNB) statistical data"""

    def __init__(self):
        super().__init__("BNB")
        self.statistics_url = self.get_url()
        self.categories = {
            'exchange_rates': 'Exchange Rates',
            'monetary_statistics': 'Monetary Statistics',
            'interest_rates': 'Interest Rates',
            'banking_system': 'Banking System',
            'fiscal_services': 'Fiscal Services',
            'balance_of_payments': 'Balance of Payments',
            'external_debt': 'External Debt',
            'external_sector': 'External Sector',
            'government_securities': 'Government Securities',
            'financial_accounts': 'Financial Accounts',
        }

    def get_statistics_categories(self):
        """Get all available statistics categories from the main page"""
        soup = self.get_soup(self.statistics_url)

        if not soup:
            logger.error("Failed to retrieve statistics page")
            return {}

        categories = {}

        # Finding the statistics categories in the page (this may need adjustment based on BNB's site structure)
        category_links = soup.select('a[href*="Statistics"]')

        for link in category_links:
            href = link.get('href')
            text = link.get_text(strip=True)

            if href and text:
                categories[text] = self.get_url(href)

        return categories

    def get_exchange_rates(self):
        """Get current exchange rates"""
        url = self.get_url("Statistics/ExchangeRates/Statistics.aspx")
        soup = self.get_soup(url)

        if not soup:
            logger.error("Failed to retrieve exchange rates page")
            return None

        # Find the exchange rates table
        # This selector may need adjustment based on the actual HTML structure
        table = soup.select_one('table.table-rates')

        if not table:
            logger.error("Exchange rates table not found")
            return None

        # Extract data from the table
        data = []
        rows = table.select('tr')

        for row in rows[1:]:  # Skip header row
            cols = row.select('td')
            if len(cols) >= 4:
                currency = cols[0].get_text(strip=True)
                code = cols[1].get_text(strip=True)
                rate = cols[2].get_text(strip=True).replace(',', '.')

                try:
                    rate_value = float(rate)
                    data.append({
                        'currency': currency,
                        'code': code,
                        'rate': rate_value,
                        'date': datetime.now().strftime('%Y-%m-%d')
                    })
                except ValueError:
                    logger.warning(f"Could not parse rate value: {rate}")

        if not data:
            logger.warning("No exchange rate data found")
            return None

        # Create DataFrame and save to CSV
        df = pd.DataFrame(data)
        filename = f"exchange_rates_{datetime.now().strftime('%Y%m%d')}"

        file_obj = self.storage.store_csv(
            df,
            filename,
            self.institution,
            'exchange_rates',
            metadata={
                'source_url': url,
                'scraped_at': datetime.now().isoformat()
            }
        )

        logger.info(f"Saved exchange rates to {file_obj.file_path}")
        return file_obj

    def get_monetary_statistics(self):
        """Get monetary statistics data"""
        url = self.get_url("Statistics/MonetaryStatistics/Statistics.aspx")
        soup = self.get_soup(url)

        if not soup:
            logger.error("Failed to retrieve monetary statistics page")
            return None

        # Find data tables or download links
        download_links = soup.select('a[href$=".xlsx"], a[href$=".xls"], a[href$=".csv"]')

        results = []
        for link in download_links:
            href = link.get('href')
            text = link.get_text(strip=True)

            if href:
                full_url = self.get_url(href)
                filename = os.path.basename(href)

                file_obj = self.download_file(
                    full_url,
                    filename=filename,
                    category='monetary_statistics'
                )

                if file_obj:
                    results.append(file_obj)
                    logger.info(f"Downloaded {filename}")

        return results

    def get_interest_rates(self):
        """Get interest rates data"""
        url = self.get_url("Statistics/InterestRates/Statistics.aspx")
        soup = self.get_soup(url)

        if not soup:
            logger.error("Failed to retrieve interest rates page")
            return None

        # Find download links for interest rates data
        download_links = soup.select('a[href$=".xlsx"], a[href$=".xls"], a[href$=".csv"]')

        results = []
        for link in download_links:
            href = link.get('href')
            text = link.get_text(strip=True)

            if href:
                full_url = self.get_url(href)
                filename = os.path.basename(href)

                file_obj = self.download_file(
                    full_url,
                    filename=filename,
                    category='interest_rates'
                )

                if file_obj:
                    results.append(file_obj)
                    logger.info(f"Downloaded {filename}")

        return results

    def run(self):
        """Run the BNB scraper to collect all data"""
        logger.info("Starting BNB scraper")

        results = {
            'exchange_rates': self.get_exchange_rates(),
            'monetary_statistics': self.get_monetary_statistics(),
            'interest_rates': self.get_interest_rates(),
        }

        # Count total files downloaded
        total_files = sum(1 for category, data in results.items()
                          if data is not None and
                          (isinstance(data, list) and len(data) > 0 or not isinstance(data, list)))

        logger.info(f"BNB scraper completed. Total files downloaded: {total_files}")
        return results