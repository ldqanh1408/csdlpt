FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends iproute2 curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY deploy/node/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY wm/ ./wm/
COPY deploy/node/ ./deploy/node/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

HEALTHCHECK --interval=5s --timeout=2s --start-period=3s --retries=3 \
    CMD curl -sf http://localhost:8000/health || exit 1

CMD ["uvicorn", "deploy.node.main:app", "--host", "0.0.0.0", "--port", "8000"]
