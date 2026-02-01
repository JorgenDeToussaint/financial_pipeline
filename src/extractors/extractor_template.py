import requests
import os
import json
from datetime import datetime

def extract(timestamp: datetime):
    # 1. Konfiguracja
    url = "https://api.coingecko.com/api/v3/coins/markets"
    query_params = {
        "vs_currency": "usd",
        "category": "stablecoins",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1
    }
    
    # Formatujemy timestamp do nazwy pliku
    ts_str = timestamp.strftime("%Y%m%d_%H%M%S")

    try:
        r = requests.get(url, params=query_params) 
        r.raise_for_status() # Automatycznie wyrzuci błąd dla statusów 4xx i 5xx
        
        data = r.json()
    
        # 2. Lokalny backup (żebyś widział progres na dysku)
        os.makedirs("data/raw", exist_ok=True)
        # Naprawiony NameError: używamy ts_str
        local_filename = f"data/raw/stable_coins_{ts_str}.json"
        
        with open(local_filename, "w") as f:
            json.dump(data, f, indent=4)
        
        print(f"DEBUG: Pobrany typ danych to: {type(data)} o długości: {len(data)}")
        
        # 3. Zwracamy dane, żeby main.py mógł je podać do Loadera
        return data

    except Exception as e:
        print(f"Krytyczny błąd podczas ekstrakcji: {e}!")
        return []

# Pozwala na testowanie samego ekstraktora bez main.py
if __name__ == "__main__":
    test_now = datetime.now()
    extract(timestamp=test_now)