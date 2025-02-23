import requests
from bs4 import BeautifulSoup
from datetime import datetime
from db import NewsDatabase
from config import DB_CONFIG
import logging
from typing import List, Tuple
import time
import random

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the database
db = NewsDatabase(**DB_CONFIG)

# Headers to mimic a real browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def fetch_sources() -> List[Tuple[str, str]]:
    """Fetch the list of sources from the database."""
    return db.get_sources()

def scrape_website(url: str, source_name: str):
    """Scrape news articles from a given website and insert them into the database."""
    try:
        # Send a GET request to the website
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Parse the HTML content
        soup = BeautifulSoup(response.content, "html.parser")

        # Extract articles (adjust based on website structure)
        articles = soup.find_all("article")
        if not articles:
            logger.warning(f"No articles found on {url}")
            return

        for article in articles:
            try:
                # Extract title, link, summary, and published date
                title = article.find("h2").text.strip()
                link = article.find("a")["href"].strip()
                summary = article.find("p").text.strip()
                published = datetime.now()  # Replace with actual published time if available

                # Insert the news article into the database
                db.insert_news(title, link, summary, published, source_name)
                logger.info(f"Inserted news: {title}")
            except Exception as e:
                logger.error(f"Error processing article: {e}")
    except requests.RequestException as e:
        logger.error(f"Error scraping {url}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error scraping {url}: {e}")

def scrape_all_sources():
    """Scrape news from all sources in the database."""
    sources = fetch_sources()
    if not sources:
        logger.warning("No sources found in the database.")
        return

    for source_name, url in sources:
        logger.info(f"Scraping {source_name} from {url}...")
        scrape_website(url, source_name)
        time.sleep(random.uniform(1, 3))  # Add a delay to avoid being blocked

if __name__ == "__main__":
    scrape_all_sources()
    db.close()