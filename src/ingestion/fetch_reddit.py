import praw
from prawcore.exceptions import RequestException
from datetime import datetime, timedelta, timezone
import os
import time
from dotenv import load_dotenv
import logging

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

company_ticker_map = {
    "apple": "AAPL", "tesla": "TSLA", "microsoft": "MSFT",
    "amazon": "AMZN", "google": "GOOGL", "alphabet": "GOOGL",
    "meta": "META", "facebook": "META", "nvidia": "NVDA"
}

def match_ticker(text: str):
    text = text.lower()
    for name, ticker in company_ticker_map.items():
        if name in text:
            return ticker
    return None

def fetch_reddit_posts(subreddits, days_back=7, keywords=None, limit=100):
    reddit = praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        username=os.getenv("REDDIT_USERNAME"),
        password=os.getenv("REDDIT_PASSWORD"),
        user_agent=os.getenv("REDDIT_USER_AGENT")
    )

    all_docs = []
    since_date = datetime.now(timezone.utc) - timedelta(days=days_back)

    for sub in subreddits:
        logger.info(f"Fetching from r/{sub} since {since_date.date()}")
        attempt = 0
        while attempt < 3:
            try:
                for post in reddit.subreddit(sub).new(limit=limit):
                    created_time = datetime.fromtimestamp(post.created_utc, tz=timezone.utc)
                    if created_time < since_date:
                        continue

                    text = post.title
                    if post.selftext and post.selftext.lower() != "[removed]":
                        text += " " + post.selftext

                    if keywords and not any(kw.lower() in text.lower() for kw in keywords):
                        continue

                    doc = {
                        "text": text.strip(),
                        "source": "reddit",
                        "date": created_time.strftime("%Y-%m-%d"),
                        "ticker": match_ticker(text)
                    }
                    all_docs.append(doc)
                break  # If successful, break retry loop
            except RequestException as e:
                attempt += 1
                logger.warning(f"Attempt {attempt} failed with error: {e}")
                time.sleep(2)  # wait before retry
        else:
            logger.error(f"All 3 attempts failed for r/{sub}. Skipping...")

    logger.info(f"Fetched {len(all_docs)} Reddit posts.")
    return all_docs


if __name__ == "__main__":
    subreddits = ["stocks", "investing"]
    keywords = ["nvidia", "tesla", "market", "stock"]
    posts = fetch_reddit_posts(subreddits=subreddits, days_back=7, keywords=keywords, limit=100)

    if not posts:
        print("0 Reddit posts fetched.")
    else:
        for post in posts[:5]:
            print(post)
