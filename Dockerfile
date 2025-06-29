# Use Python 3.11 slim image as base
FROM mcr.microsoft.com/playwright:v1.52.0-noble

# Set working directory
WORKDIR /app

# Install system dependencies (including wget for Cloud SQL Proxy)
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install Cloud SQL Proxy
RUN wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 -O /usr/local/bin/cloud_sql_proxy
RUN chmod +x /usr/local/bin/cloud_sql_proxy

# Install UV
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Fix the PATH - UV installs to /root/.local/bin, not /root/.cargo/bin
ENV PATH="/root/.local/bin:$PATH"

# Copy project files
COPY . .

# Install dependencies using UV
RUN uv sync

# Install the package in development mode
RUN uv pip install -e .

# Install camoufox browser
RUN uv run -m camoufox fetch

# Accept build argument for which scraper to run
ARG SCRAPER_PATH
ENV SCRAPER_PATH=${SCRAPER_PATH}

# Define environment variable for display
ENV DISPLAY=:99

# Create entrypoint script
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# Start Cloud SQL Proxy in background if connection string is provided\n\
if [ ! -z "$INSTANCE_CONNECTION_NAME" ]; then\n\
    echo "Starting Cloud SQL Proxy..."\n\
    cloud_sql_proxy -instances=$INSTANCE_CONNECTION_NAME=tcp:5432 &\n\
    sleep 5\n\
    echo "Cloud SQL Proxy started"\n\
fi\n\
\n\
# Start Xvfb and run the scraper\n\
Xvfb :99 -screen 0 1280x720x24 > /dev/null 2>&1 &\n\
sleep 2\n\
export DISPLAY=:99\n\
uv run ${SCRAPER_PATH}\n\
' > /entrypoint.sh && chmod +x /entrypoint.sh

# Use the entrypoint script
CMD ["/entrypoint.sh"]