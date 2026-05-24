FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY deploy/ingestor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY deploy/ingestor/ ./deploy/ingestor/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

WORKDIR /app/deploy/ingestor

CMD ["python", "ingest.py", "--help"]
