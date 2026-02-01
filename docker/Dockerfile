# Obraz bazowy [cite: 2026-01-24]
FROM python:3.11-slim

# Ustawienia środowiska Python (brak plików .pyc, logowanie na bieżąco)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

# Instalacja zależności systemowych (jeśli będą potrzebne np. pod Postgres/Pandas)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Cache'owanie warstwy bibliotek
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Kopiowanie kodu źródłowego
COPY src/ ./src
COPY main.py .

# Domyślna komenda (można nadpisać w docker-compose)
CMD ["python", "main.py"]
