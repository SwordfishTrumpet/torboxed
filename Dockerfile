# TorBoxed Docker Image
# Uses uv for Python package management

# Base image pinned by index digest so a rebuild produces the same image and a
# retagged upstream image cannot run in this build. Version: python:3.12-slim
# (index digest resolved 2026-08-26).
#
# To move it on purpose:
#   docker buildx imagetools inspect python:3.12-slim
# and replace the digest below with the new index digest.
FROM python:3.12-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
# uv pinned to a fixed release: an unpinned install would silently change
# resolution and venv behavior under the same image tag.
RUN pip install --no-cache-dir uv==0.13.0

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
