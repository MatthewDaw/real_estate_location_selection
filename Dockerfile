# Use Python 3.11 slim image as base
FROM mcr.microsoft.com/playwright:v1.52.0-noble

# Set working directory
WORKDIR /app

# Install system dependencies (including wget for Cloud SQL Proxy and netcat for testing)
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    netcat-openbsd \
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

# Create enhanced entrypoint script with better debugging
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
echo "=== Container startup ===" \n\
echo "INSTANCE_CONNECTION_NAME: $INSTANCE_CONNECTION_NAME" \n\
echo "DB_NAME: $DB_NAME" \n\
echo "DB_USER: $DB_USER" \n\
echo "SCRAPER_PATH: $SCRAPER_PATH" \n\
\n\
# Start Cloud SQL Proxy in background if connection string is provided\n\
if [ ! -z "$INSTANCE_CONNECTION_NAME" ]; then\n\
    echo "Starting Cloud SQL Proxy..."\n\
    cloud_sql_proxy -instances=$INSTANCE_CONNECTION_NAME=tcp:5432 &\n\
    PROXY_PID=$!\n\
    echo "Cloud SQL Proxy started with PID: $PROXY_PID"\n\
    \n\
    # Wait longer and check if proxy is actually running\n\
    echo "Waiting for Cloud SQL Proxy to be ready..."\n\
    sleep 10\n\
    \n\
    # Check if proxy process is still running\n\
    if kill -0 $PROXY_PID 2>/dev/null; then\n\
        echo "✅ Cloud SQL Proxy process is running"\n\
    else\n\
        echo "❌ Cloud SQL Proxy process failed to start"\n\
        exit 1\n\
    fi\n\
    \n\
    # Test if port 5432 is listening\n\
    echo "Testing if port 5432 is listening..."\n\
    for i in {1..30}; do\n\
        if nc -z 127.0.0.1 5432; then\n\
            echo "✅ Port 5432 is ready"\n\
            break\n\
        fi\n\
        echo "Waiting for port 5432... attempt $i/30"\n\
        sleep 2\n\
    done\n\
    \n\
    if ! nc -z 127.0.0.1 5432; then\n\
        echo "❌ Port 5432 is not responding after 60 seconds"\n\
        echo "Cloud SQL Proxy status:"\n\
        ps aux | grep cloud_sql_proxy || echo "No proxy process found"\n\
        echo "Network status:"\n\
        netstat -tlnp 2>/dev/null | grep 5432 || echo "Port 5432 not listening"\n\
        exit 1\n\
    fi\n\
    \n\
    echo "✅ Cloud SQL Proxy is ready and accepting connections"\n\
else\n\
    echo "No INSTANCE_CONNECTION_NAME provided, skipping Cloud SQL Proxy"\n\
fi\n\
\n\
echo "=== Starting application ===" \n\
# Start Xvfb and run the scraper\n\
Xvfb :99 -screen 0 1280x720x24 > /dev/null 2>&1 &\n\
sleep 2\n\
export DISPLAY=:99\n\
uv run ${SCRAPER_PATH}\n\
' > /entrypoint.sh && chmod +x /entrypoint.sh

# Use the entrypoint script
CMD ["/entrypoint.sh"]