# Use Python 3.11 slim image as base
FROM mcr.microsoft.com/playwright:v1.52.0-noble

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install UV
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Fix the PATH - UV installs to /root/.local/bin, not /root/.cargo/bin
ENV PATH="/root/.local/bin:$PATH"

# Python environment variables for proper logging
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=utf-8

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

# Run the specified scraper with output redirection
CMD ["sh", "-c", "Xvfb :99 -screen 0 1280x720x24 > /dev/null 2>&1 & sleep 2 && export DISPLAY=:99 && exec uv run ${SCRAPER_PATH}"]
