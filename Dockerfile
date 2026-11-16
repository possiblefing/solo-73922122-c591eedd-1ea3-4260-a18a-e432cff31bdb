FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    ARCHIVE_DATA_DIR=/data

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# The shared data volume is mounted at /data (see docker-compose.yml).
RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000

# A simple SIGTERM-aware start; uvicorn shuts down gracefully so an
# in-flight download can finish.
CMD ["python", "-m", "app.main"]
