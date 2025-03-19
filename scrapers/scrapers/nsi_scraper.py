import os
import re
import logging
from urllib.parse import urljoin
from scrapers.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class NSIScraper(BaseScraper):
    """Scraper for National Statistical Institute (NSI)"""

    def __init__(self):
        super().__init__("NSI")
        self.start_url = self.get_full_url(self.data_path)

    def extract_links(self, soup, file_extensions=None):
        """Extract links from soup object with optional file extension filtering"""
        if file_extensions is None:
            file_extensions = ['.xls', '.xlsx', '.csv', '.pdf', '.doc', '.docx', '.zip']

        links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            url = urljoin(self.base_url, href)

            # Check if it's a file we're interested in
            if any(url.lower().endswith(ext) for ext in file_extensions):
                title = a_tag.get_text(strip=True) or os.path.basename(url)
                links.append({
                    'url': url,
                    'title': title,
                    'extension': os.path.splitext(url)[1].lower()
                })

        return links

    def extract_categories(self, soup):
        """Extract category links from the main statistics page"""
        categories = []

        # Find all category containers (adjust selector based on NSI HTML structure)
        for category_elem in soup.select('div.category-container'):
            category_title = category_elem.select_one('h3, h2').get_text(strip=True)
            category_link = category_elem.select_one('a')

            if category_link and category_link.has_attr('href'):
                href = category_link['href']
                url = urljoin(self.base_url, href)

                categories.append({
                    'title': category_title,
                    'url': url
                })

        return categories

    def process_category(self, category):
        """Process a single category page"""
        logger.info(f"Processing category: {category['title']}")

        # Create category directory
        category_dir = os.path.join(
            self.storage_dir,
            re.sub(r'[^\w\-_\. ]', '_', category['title'])
        )
        os.makedirs(category_dir, exist_ok=True)

        # Fetch category page
        soup = self.fetch_page(category['url'])
        if not soup:
            return []

        # Extract file links
        file_links = self.extract_links(soup)

        # Download files
        downloaded_files = []
        for link in file_links:
            # Create filename from title if available
            filename = f"{re.sub(r'[^\w\-_\. ]', '_', link['title'])}{link['extension']}"

            # Download the file
            file_path = self.download_file(
                link['url'],
                os.path.join(category_dir, filename)
            )

            if file_path:
                metadata = self.get_file_metadata(file_path)
                metadata['category'] = category['title']
                metadata['original_url'] = link['url']
                metadata['title'] = link['title']

                downloaded_files.append(metadata)

        return downloaded_files

    def run(self):
        """Run the NSI scraper"""
        logger.info(f"Starting NSI scraper at {self.start_url}")

        # Fetch main statistics page
        soup = self.fetch_page(self.start_url)
        if not soup:
            logger.error("Failed to fetch main page")
            return self.finalize()

        # Extract categories
        categories = self.extract_categories(soup)
        logger.info(f"Found {len(categories)} categories")

        # Process each category
        all_files = []
        for category in categories:
            category_files = self.process_category(category)
            all_files.extend(category_files)

        # Save all metadata
        self.save_metadata({
            'files': all_files,
            'total_files': len(all_files)
        })

        return self.finalize()