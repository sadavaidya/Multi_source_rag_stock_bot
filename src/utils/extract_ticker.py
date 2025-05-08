# src/utils/extract_ticker.py

company_ticker_map = {
    "apple": "AAPL",
    "tesla": "TSLA",
    "microsoft": "MSFT",
    "amazon": "AMZN",
    "google": "GOOGL",
    "alphabet": "GOOGL",
    "meta": "META",
    "facebook": "META",
    "nvidia": "NVDA"
}

def get_tickers_from_query(query: str) -> list:
    """
    Extract all tickers matching company names in the query.

    Args:
        query: User input query string

    Returns:
        List of ticker symbols (empty if none found)
    """
    query = query.lower()
    matched_tickers = []

    for name, ticker in company_ticker_map.items():
        if name in query:
            matched_tickers.append(ticker)

    return list(set(matched_tickers))  # remove duplicates if any
