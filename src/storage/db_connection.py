from sqlalchemy import create_engine, Table, Column, Integer, String, Text, MetaData, DateTime
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime

# Replace with your credentials
DATABASE_URL = "postgresql://postgres:discovery20@localhost:5432/stock_db"

engine = create_engine(DATABASE_URL)
metadata = MetaData()

# Define table
stock_posts = Table(
    'stock_posts', metadata,
    Column('id', Integer, primary_key=True),
    Column('text', Text),
    Column('source', String(50)),
    Column('date', DateTime),
    Column('ticker', String(10)),
)

# Create table if not exists
metadata.create_all(engine)

def store_posts_to_db(posts):
    with engine.connect() as conn:
        for post in posts:
            stmt = insert(stock_posts).values(
                text=post["text"],
                source=post["source"],
                date=datetime.strptime(post["date"], "%Y-%m-%d") if isinstance(post["date"], str) else post["date"],
                ticker=post["ticker"]
            ).on_conflict_do_nothing()
            conn.execute(stmt)
