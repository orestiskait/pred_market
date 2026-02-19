FROM python:3.11-slim

WORKDIR /app

# Install only what pip needs to build cryptography (no venv cruft)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy collector source
COPY collector.py kalshi_client.py storage.py backfill.py fetch_nws_temps.py __init__.py ./
COPY config.yaml .

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Data output directory — mount a host volume here to persist parquet files
VOLUME ["/app/data"]

ENTRYPOINT ["docker-entrypoint.sh"]
