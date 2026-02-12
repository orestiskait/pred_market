"""
Configuration for Kalshi NYC temperature market exploration.
"""

# Kalshi API (no auth required for public market data)
# api.elections.kalshi.com serves ALL markets (weather, elections, etc.) per Kalshi docs
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# NYC temp series ticker (uppercase for API)
KXHIGHNY_SERIES = "KXHIGHNY"

# NYC highest temperature market (API uses uppercase tickers)
NYC_TEMP_MARKET_TICKER = "KXHIGHNY-26FEB11"
NYC_TEMP_MARKET_URL = (
    "https://kalshi.com/markets/kxhighny/highest-temperature-in-nyc/kxhighny-26feb11"
)

# NWS station for NYC (Central Park) - per project_rules
NWS_STATION_NYC = "KNYC"

# Data paths (relative to exploration/)
DATA_RAW_DIR = "data/raw"
DATA_RAW_KALSHI = "data/raw/kalshi"
DATA_RAW_NWS = "data/raw/nws"
DATA_PROCESSED_DIR = "data/processed"

# NWS observation history (past 3 days hourly)
NWS_OBHISTORY_URL = "https://forecast.weather.gov/data/obhistory/KNYC.html"
