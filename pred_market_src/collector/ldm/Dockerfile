# =============================================================================
# LDM + Python ingest — Dockerfile
# =============================================================================
# Extends the official Unidata LDM Docker image with our Python dependencies
# and ingest scripts.  The LDM receives surface observations from the IDD
# and pipes them to ldm_ingest.py via pqact.
# =============================================================================

FROM unidata/ldm-docker:6.15.1

USER root

# Install Python and pip (LDM image is Debian-based)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Create a venv at /app/venv so pqact can call /app/venv/bin/python
RUN python3 -m venv /app/venv

# Install Python dependencies needed by ldm_ingest.py
RUN /app/venv/bin/pip install --no-cache-dir \
    pandas>=2.0.0 \
    pyarrow>=14.0.0

# Copy our Python weather code into the image
# We only need the weather subpackage + the __init__.py for imports
COPY weather/ /app/weather/

# Ensure the ingest script is executable
RUN chmod +x /app/weather/ldm_ingest.py

# The base LDM image expects config files mounted at /home/ldm/etc/
# and data at /home/ldm/var/data/.  Our docker-compose.yml handles that.
#
# The pqact.conf PIPEs data to:
#   /app/venv/bin/python /app/weather/ldm_ingest.py
# which writes parquet to the path in LDM_WEATHER_DATA_DIR env var.

# Set Python path so ldm_ingest.py can import from pred_market_src
ENV PYTHONPATH="/app"

USER ldm
