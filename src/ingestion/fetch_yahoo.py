import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def fetch_yahoo_finance_headlines():
    url = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^DJI,^IXIC,^GSPC&region=US&lang=en-US"
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Error fetching Yahoo Finance RSS feed: {e}")
        return []

    root = ET.fromstring(response.content)

    unified_docs = []
    for item in root.findall(".//item"):
        title = item.findtext("title")
        pubDate = item.findtext("pubDate")

        if title:
            doc = {
                "text": title,
                "source": "yahoo_finance_rss",
                "date": str(datetime.utcnow().date()) if not pubDate else pubDate,
                "ticker": None
            }
            unified_docs.append(doc)

    logger.info(f"Fetched {len(unified_docs)} Yahoo Finance RSS headlines.")
    return unified_docs

if __name__ == "__main__":
    headlines = fetch_yahoo_finance_headlines()
    for h in headlines[:5]:
        print(h)
