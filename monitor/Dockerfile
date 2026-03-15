FROM python:3.11-slim

WORKDIR /app

# Copy consolidated requirements
COPY requirements/base.txt requirements/base.txt
COPY requirements/monitor.txt requirements/monitor.txt
RUN pip install --no-cache-dir -r requirements/monitor.txt

COPY monitor/ .

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:9000/health')" || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9000"]
