#!/bin/bash
set -e  # Przerwij, jeśli któryś krok wywali błąd

echo "🌊 Financial Lakehouse Pipeline: START"
echo "======================================"

# 1. Uruchomienie głównej rury (Bronze -> Silver)
echo "📥 Ingesting data (Bronze/Silver)..."
python main.py --pipe all

# 2. Uruchomienie nowej warstwy Gold (AaaS)
echo "💎 Refining data (Gold)..."
python main.py --gold valuation

echo "✅ Pipeline finished successfully!"

# 3. Utrzymanie kontenera przy życiu
# To ważne, żeby kontener nie zgasł po zakończeniu skryptu
echo "📡 Keeping container alive for logs and debug..."
tail -f /dev/null