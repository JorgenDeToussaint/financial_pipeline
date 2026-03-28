# STAGE 1: Builder
FROM python:3.11-slim AS builder
WORKDIR /install
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.11-slim
WORKDIR /app

# STAGE 2: Actual Image
COPY --from=builder /usr/local /usr/local

COPY src/ ./src
COPY main.py .
COPY dashboard/ ./dashboard
COPY run.sh /app/run.sh

RUN chmod +x /app/run.sh
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["/bin/bash", "/app/run.sh"]