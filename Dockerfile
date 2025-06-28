# Use Python 3.11 slim image as base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install UV
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:$PATH"

# Copy project files
COPY . .

# Install dependencies using UV
RUN uv sync

# Install the package in development mode
RUN uv pip install -e .

# Accept build argument for which scraper to run
ARG SCRAPER_PATH
ENV SCRAPER_PATH=${SCRAPER_PATH}

# Run the specified scraper
CMD ["sh", "-c", "uv run ${SCRAPER_PATH}"]