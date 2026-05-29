# Scaled-down Dockerfile for the refactor stream processing system
FROM python:3.10-slim

WORKDIR /app

# Install runtime deps + curl for health checks
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir pytest

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy flat source layout into /app/refactor/ package directory
# (source was moved out of refactor/ folder into project root)
COPY . /app/refactor/

# Entrypoint: cleans stale checkpoint data from previous runs
COPY deploy/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]

# Health check endpoint (overridden per service in docker-compose)
HEALTHCHECK --interval=3s --timeout=2s --retries=5 \
  CMD curl -sf http://localhost:8000/health || exit 1

# Default command overridden by docker-compose
CMD ["python3", "-m", "refactor.run"]
