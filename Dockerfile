# TorBoxed Docker Image
# Uses uv for Python package management

FROM python:3.12-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install --no-cache-dir uv

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash torboxed

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml ./
COPY README.md ./
COPY torboxed.py ./

# Create virtual environment and install dependencies as root, then fix permissions
# Install with zilean extras for PostgreSQL database support
RUN uv venv /app/.venv && \
    uv pip install --python /app/.venv/bin/python -e ".[zilean]" && \
    chown -R torboxed:torboxed /app

# Make the virtual environment's Python available
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Create directories for persistent data
RUN mkdir -p /data && chown torboxed:torboxed /data

# Switch to non-root user
USER torboxed

# Set working directory to /data so .env file is found
WORKDIR /data

# Set environment for database and logs location
ENV DB_PATH=/data/torboxed.db
ENV LOG_PATH=/data/torboxed.log
ENV ENV_PATH=/data/.env

# Expose volume for persistent data
VOLUME ["/data"]

# Default command uses uv run (from /app)
ENTRYPOINT ["uv", "run", "--python", "/app/.venv/bin/python", "/app/torboxed.py"]
CMD ["--help"]
