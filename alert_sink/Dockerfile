FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir kafka-python duckdb

# Copy alert_sink module
COPY alert_sink/ ./alert_sink/

CMD ["python", "-m", "alert_sink.consumer"]
