# ─────────────────────────────────────────────────────────────────────────────
# Worker Dockerfile
# Multi-stage build with spaCy model
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim AS base
WORKDIR /app

# Install dependencies (spacy in a separate layer for caching)
COPY requirements/base.txt requirements/base.txt
COPY requirements/worker.txt requirements/worker.txt
RUN pip install --no-cache-dir -r requirements/worker.txt

# Download spaCy model in build layer (cached)
RUN python -m spacy download en_core_web_sm || echo "spaCy download skipped"

# Copy source
COPY core/ core/
COPY services/worker/ services/worker/

# Health check (metrics endpoint)
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -sf http://localhost:9100/metrics || exit 1

# Run as non-root
RUN useradd --create-home --shell /bin/bash sentinel
USER sentinel

# Prometheus metrics port
EXPOSE 9100

CMD ["python", "-u", "services/worker/worker.py"]