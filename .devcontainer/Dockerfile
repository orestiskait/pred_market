FROM python:3.11-slim

# System deps: cryptography build, ecCodes for cfgrib/GRIB2 (HRRR, RRFS, NBM, RTMA-RU)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    libeccodes-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps (matches services/requirements.txt)
COPY services/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Default working dir; workspace is mounted by devcontainer
WORKDIR /workspace
