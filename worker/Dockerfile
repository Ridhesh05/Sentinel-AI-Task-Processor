FROM python:3.11-slim

WORKDIR /app

# Copy consolidated requirements
COPY requirements/base.txt requirements/base.txt
COPY requirements/worker.txt requirements/worker.txt
RUN pip install --no-cache-dir -r requirements/worker.txt

COPY worker/ .

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:9100/metrics')" || exit 1

CMD ["python", "-u", "worker.py"]
