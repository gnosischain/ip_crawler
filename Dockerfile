FROM python:3.12-slim

WORKDIR /app

# Every dependency ships a wheel for amd64 and arm64, so no compiler is needed.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/logs /app/migrations

COPY migrations/ /app/migrations/
COPY src/ /app/src/
COPY entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh

# Non-root; /app/logs holds health.log, crawler.log and the run summaries.
RUN useradd -m crawler && chown -R crawler:crawler /app/logs
USER crawler

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/entrypoint.sh"]
