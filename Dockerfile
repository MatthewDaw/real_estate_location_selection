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

# Copy only dependency files first (for better caching)
COPY pyproject.toml uv.lock* ./

# Create a minimal src directory structure for dependency installation
RUN mkdir -p src/real_estate_location_selection src/pipelines && \
    touch src/real_estate_location_selection/__init__.py src/pipelines/__init__.py

# Copy README for setuptools
COPY README.md ./

# Install dependencies using UV (this layer will be cached)
RUN uv sync --frozen

# Install camoufox browser (this can also be cached)
RUN uv run -m camoufox fetch

# Now copy the rest of the project files (this will overwrite the minimal structure)
COPY . .

# Install the package in development mode
RUN uv pip install -e .

# Accept build argument for which scraper to run
ARG SCRAPER_PATH
ENV SCRAPER_PATH=${SCRAPER_PATH}

# Define environment variable for display
ENV DISPLAY=:99

# Set working directory to src before running the command
WORKDIR /app/src

# Run the specified scraper with output redirection
CMD ["sh", "-c", "Xvfb :99 -screen 0 1280x720x24 > /dev/null 2>&1 & sleep 2 && export DISPLAY=:99 && exec uv run ${SCRAPER_PATH}"]