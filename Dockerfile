FROM python:3.12-slim

LABEL maintainer="Mainlayer <hello@mainlayer.xyz>"
LABEL description="Data Broker Platform — sell datasets to AI agents"

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY src/ ./src/
COPY scripts/ ./scripts/

# Create data directory for JSON persistence
RUN mkdir -p /data

EXPOSE 8000

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
