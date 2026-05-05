#!/bin/bash
set -e

echo "🌊 Financial Lakehouse Pipeline: START"
echo "======================================"

python main.py

echo "✅ Pipeline finished successfully!"

if [ "$TESTING" != "true" ]; then
    echo "📡 Development mode: Keeping container alive..."
    tail -f /dev/null
fi