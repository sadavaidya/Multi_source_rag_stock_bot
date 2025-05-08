# src/data/fetch_news.py

import requests
from typing import List
import logging
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

API_KEY = os.getenv("MARKETAUX_API_KEY")


def fetch_news_from_marketaux(ticker: str):
    url = f"https://api.marketaux.com/v1/news/all?symbols={ticker}&filter_entities=true&language=en&api_token={API_KEY}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        articles = response.json().get("data", [])
    except Exception as e:
        logger.error(f"Error fetching news: {e}")
        return []

    unified_docs = []
    for article in articles:
        # Some headlines or dates might be missing; add fallbacks
        text = article.get("title", "No title available.")
        published_at = article.get("published_at", datetime.utcnow().isoformat())
        
        doc = {
            "text": text,
            "source": "marketaux",
            "date": published_at[:10],  # YYYY-MM-DD
            "ticker": ticker.upper()
        }
        unified_docs.append(doc)

    logger.info(f"Fetched {len(unified_docs)} headlines for {ticker}")
    return unified_docs


if __name__ == "__main__":
    news = fetch_news_from_marketaux('AAPL')
    for n in news[:5]:
        print(n)