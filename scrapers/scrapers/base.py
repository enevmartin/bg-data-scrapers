# apps/scrapers/scrapers/base.py
import requests
import os
import hashlib
import logging
import time
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from django.conf import settings
from apps.storage.factory import StorageFactory
from apps.scrapers.models import ScrapedFile

logger = logging.getLogger(__name__)


class BaseScraper:
    """Base class for all scrapers with common functionality for Django integration"""

    def __init__(self, institution):
        """Initialize the scraper with settings for a specific institution"""
        self.institution = institution
        self.config = settings.SCRAPER_SETTINGS['INSTITUTIONS'].get(institution, {})
        self.base_url = self.config.get('base_url', '')
        self.data_path = self.config.get('data_path', '')

        # Get settings with defaults
        self.user_agent = settings.SCRAPER_SETTINGS.get('USER_AGENT')
        self.request_timeout = settings.SCRAPER_SETTINGS.get('REQUEST_TIMEOUT', 30)
        self.download_timeout = settings.SCRAPER_SETTINGS.get('DOWNLOAD_TIMEOUT', 300)
        self.request_delay = settings.SCRAPER_SETTINGS.get('REQUEST_DELAY', 1.0)
        self.max_retries = settings.SCRAPER_SETTINGS.get('MAX_RETRIES', 3)

        # Initialize storage
        self.storage = StorageFactory.get_storage('file')

        # Set up HTTP session with proper headers
        self.session = self._create_session()

        # Initialize stats
        self.stats = {
            'start_time': datetime.now(),
            'end_time': None,
            'files_scraped': 0,
            'files_failed': 0,
            'total_size': 0,
            'errors': []
        }

    def _create_session(self):
        """Create and configure requests session"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        return session

    def get_full_url(self, relative_path):
        """Convert relative URL to absolute URL"""
        return urljoin(self.base_url, relative_path)

    def fetch_page(self, url, params=None):
        """Fetch a web page and return BeautifulSoup object"""
        try:
            logger.info(f"Fetching: {url}")
            response = self._make_request(url)

            # Return parsed HTML
            return BeautifulSoup(response.text, "lxml")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch {url}: {str(e)}")
            self.stats["errors"].append({
                "url": url,
                "error": str(e),
                "time": datetime.now()
            })
            return None

    def _make_request(self, url, stream=False):
        """Make HTTP request with retries and exponential backoff"""
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(
                    url,
                    timeout=self.download_timeout if stream else self.request_timeout,
                    stream=stream
                )
                response.raise_for_status()

                # Be polite to the server
                time.sleep(self.request_delay)

                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {url}, error: {str(e)}")
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(self.request_delay * (2 ** attempt))  # Exponential backoff

    def _get_mime_type(self, file_name):
        """Get MIME type based on file extension"""
        ext = os.path.splitext(file_name)[1].lower()
        mime_types = {
            '.pdf': 'application/pdf',
            '.xls': 'application/vnd.ms-excel',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.doc': 'application/msword',
            '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            '.csv': 'text/csv',
            '.txt': 'text/plain',
            '.html': 'text/html',
            '.xml': 'application/xml',
            '.json': 'application/json',
            '.zip': 'application/zip',
            '.rar': 'application/x-rar-compressed',
        }
        return mime_types.get(ext, 'application/octet-stream')

    def _calculate_hash(self, content):
        """Calculate SHA-256 hash of file content"""
        return hashlib.sha256(content).hexdigest()

    def calculate_file_hash(self, file_path):
        """Calculate SHA-256 hash of a file on disk"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def download_file(self, url, file_name=None, overwrite=False):
        """Download file and save to storage with streaming support for large files"""
        if file_name is None:
            # Extract filename from URL
            file_name = os.path.basename(urlparse(url).path)
            if not file_name:
                file_name = f"file_{int(time.time())}"

        # Check if file already exists in database
        existing_file = ScrapedFile.objects.filter(
            institution=self.institution,
            original_url=url
        ).first()

        if existing_file and not overwrite:
            # Check if file has changed using ETag
            try:
                head_response = self.session.head(url, timeout=self.request_timeout)
                etag = head_response.headers.get('ETag')

                if etag and etag == existing_file.hash_value:
                    logger.info(f"File unchanged (ETag match): {url}")
                    return existing_file.file_path
            except requests.exceptions.RequestException:
                # If head request fails, proceed with full download
                pass

        try:
            logger.info(f"Downloading: {url}")

            # Get the file with streaming to handle large files
            response = self._make_request(url, stream=True)

            # Storage path for the file
            storage_path = os.path.join(
                settings.SCRAPED_FILES_DIR,
                self.institution,
                file_name
            )

            # Process the file in chunks for memory efficiency
            content = bytearray()
            hash_obj = hashlib.sha256()
            total_size = 0

            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    content.extend(chunk)
                    hash_obj.update(chunk)
                    total_size += len(chunk)

            # Calculate hash from the streamed content
            hash_value = hash_obj.hexdigest()

            # Check if we already have this exact file
            if existing_file and existing_file.hash_value == hash_value and not overwrite:
                logger.info(f"File unchanged (content hash match): {url}")
                return existing_file.file_path

            # Save file to storage
            file_path = self.storage.save(storage_path, bytes(content))

            # Get MIME type
            mime_type = self._get_mime_type(file_name)

            # Create or update the database record
            if existing_file and (overwrite or existing_file.hash_value != hash_value):
                existing_file.file_path = file_path
                existing_file.file_size = total_size
                existing_file.mime_type = mime_type
                existing_file.hash_value = hash_value
                existing_file.last_updated = datetime.now()
                existing_file.save()
                scraped_file = existing_file
            elif not existing_file:
                scraped_file = ScrapedFile.objects.create(
                    institution=self.institution,
                    original_url=url,
                    file_path=file_path,
                    file_name=file_name,
                    file_size=total_size,
                    mime_type=mime_type,
                    hash_value=hash_value
                )

            # Update stats
            self.stats['files_scraped'] += 1
            self.stats['total_size'] += total_size

            return file_path

        except Exception as e:
            logger.error(f"Failed to download {url}: {str(e)}")
            self.stats['files_failed'] += 1
            self.stats['errors'].append({
                'url': url,
                'error': str(e),
                'time': datetime.now()
            })
            return None

    def run(self):
        """Run the scraper (to be implemented by subclasses)"""
        raise NotImplementedError("Subclasses must implement run() method")

    def finalize(self):
        """Finalize the scraping process and save stats"""
        self.stats['end_time'] = datetime.now()
        self.stats['duration'] = (self.stats['end_time'] - self.stats['start_time']).total_seconds()

        logger.info(f"Scraping completed for {self.institution}")
        logger.info(f"Stats: {self.stats}")

        return self.stats