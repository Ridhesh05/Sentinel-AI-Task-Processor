# ─────────────────────────────────────────────────────────────────────────────
# Monitor Dockerfile
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim
WORKDIR /app

COPY requirements/base.txt requirements/base.txt
COPY requirements/monitor.txt requirements/monitor.txt
RUN pip install --no-cache-dir -r requirements/monitor.txt

COPY core/ core/
COPY services/monitor/ services/monitor/

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:9000/health/live || exit 1

RUN useradd --create-home --shell /bin/bash sentinel
USER sentinel

EXPOSE 9000

CMD ["uvicorn", "services.monitor.main:app", "--host", "0.0.0.0", "--port", "9000"]