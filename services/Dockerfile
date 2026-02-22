FROM python:3.11-slim

WORKDIR /app

# Install only what pip needs to build cryptography (no venv cruft)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY services/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/ /app/services/
ENV PYTHONPATH=/app

COPY services/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Data output directory — mount a host volume here to persist parquet files
VOLUME ["/app/data"]

ENTRYPOINT ["docker-entrypoint.sh"]
