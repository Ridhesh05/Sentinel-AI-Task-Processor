# ─────────────────────────────────────────────────────────────────────────────
# API Dockerfile
# Multi-stage build for production optimization
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim AS base
WORKDIR /app

# Install dependencies
COPY requirements/base.txt requirements/base.txt
COPY requirements/api.txt requirements/api.txt
RUN pip install --no-cache-dir -r requirements/api.txt

# Copy source
COPY core/ core/
COPY services/api/ services/api/
COPY pyproject.toml ./

# Health check
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:8000/health/live || exit 1

# Run as non-root
RUN useradd --create-home --shell /bin/bash sentinel
USER sentinel

EXPOSE 8000

CMD ["uvicorn", "services.api.main:app", "--host", "0.0.0.0", "--port", "8000"]