# Scaled-down Dockerfile for the refactor stream processing system
FROM python:3.10-slim

WORKDIR /app

# Install runtime deps + curl for health checks
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir pytest

# Install Python dependencies
COPY refactor/requirements.txt /app/refactor/requirements.txt
RUN pip install --no-cache-dir -r /app/refactor/requirements.txt

# Copy the refactor package and its dependencies
COPY refactor/ /app/refactor/

# Health check endpoint (overridden per service in docker-compose)
HEALTHCHECK --interval=3s --timeout=2s --retries=5 \
  CMD curl -sf http://localhost:8000/health || exit 1

# Default command overridden by docker-compose
CMD ["python3", "-m", "pytest", "refactor/tests/", "-v"]
