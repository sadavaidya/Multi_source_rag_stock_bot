# store_in_db.py
import logging
from sqlalchemy import create_engine, Column, String, Date, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert

# Import your fetching functions (adjust paths as necessary)
from src.ingestion.fetch_marketaux import fetch_news_from_marketaux
from src.ingestion.fetch_yahoo import fetch_yahoo_finance_headlines
from src.ingestion.fetch_reddit import fetch_reddit_posts
from src.utils.extract_ticker import get_tickers_from_query

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure your database URL
DATABASE_URL = "postgresql://postgres:discovery20@localhost:5432/stock_db"  # <-- update this

# SQLAlchemy setup
Base = declarative_base()

class NewsPost(Base):
    __tablename__ = "news_posts"

    id = Column(String, primary_key=True)  # use hash or uuid if needed
    text = Column(Text, nullable=False)
    source = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=True)

def get_session():
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()

def generate_id(post):
    import hashlib
    return hashlib.md5((post['text'] + post['source'] + str(post['date'])).encode()).hexdigest()

def store_posts(posts, session):
    for post in posts:
        post_id = generate_id(post)
        news_post = NewsPost(
            id=post_id,
            text=post['text'],
            source=post['source'],
            date=post['date'],
            ticker=post.get('ticker')
        )
        
        stmt = insert(NewsPost).values(
            id=post_id,
            text=post['text'],
            source=post['source'],
            date=post['date'],
            ticker=post.get('ticker')
        )
        
        # Use ON CONFLICT DO NOTHING to skip duplicates
        stmt = stmt.on_conflict_do_nothing(index_elements=['id'])
        
        try:
            session.execute(stmt)
        except Exception as e:
            logger.warning(f"Skipping duplicate or error: {e}")
    
    session.commit()
    logger.info(f"Stored {len(posts)} posts.")

def main():
    session = get_session()

    #logger.info("Getting tickers from query")
    
    logger.info("Fetching Marketaux...")
    
    marketaux_posts = fetch_news_from_marketaux('AAPL')

    logger.info("Fetching Yahoo Finance...")
    yahoo_posts = fetch_yahoo_finance_headlines()

    logger.info("Fetching Reddit...")
    subreddits = ["stocks", "investing"]
    keywords = ["nvidia", "tesla", "market", "stock"]
    reddit_posts = fetch_reddit_posts(subreddits=subreddits, days_back=7, keywords=keywords, limit=100)

    all_posts = marketaux_posts + yahoo_posts + reddit_posts

    logger.info("Storing all posts in the database...")
    store_posts(all_posts, session)

if __name__ == "__main__":
    main()
