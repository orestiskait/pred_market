FROM python:3.11-slim

# Create a non-root user for development and production
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

# Build deps (cryptography) + ecCodes for cfgrib/GRIB2 (HRRR, RRFS, NBM, RTMA-RU)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    libeccodes-dev \
    libcurl4 \
    libexpat1 \
    sudo \
    && groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Ensure the app directory is owned by the non-root user
RUN chown $USERNAME:$USER_GID /app

# Install dependencies
COPY --chown=$USERNAME:$USER_GID services/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code and project structure
# We copy services, research, and scripts to ensure the container has full context
COPY --chown=$USERNAME:$USER_GID services/ /app/services/
COPY --chown=$USERNAME:$USER_GID research/ /app/research/
COPY --chown=$USERNAME:$USER_GID scripts/ /app/scripts/

# Ensure PYTHONPATH includes the /app directory
ENV PYTHONPATH=/app

# Copy and setup the entrypoint script
COPY services/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Data directory setup
# This folder is used for persistent storage. Mount a host volume here.
RUN mkdir -p /app/data && chown $USERNAME:$USER_GID /app/data
VOLUME ["/app/data"]

# Switch to the non-root user
USER $USERNAME

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
