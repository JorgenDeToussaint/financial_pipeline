import requests
import os
import json
from datetime import datetime

def extract(timestamp: datetime):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    query_params = {
        "vs_currency": "usd",
        "category": "stablecoins",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1
    }
    
    ts_str = timestamp.strftime("%Y%m%d_%H%M%S")

    try:
        r = requests.get(url, params=query_params) 
        r.raise_for_status()
        data = r.json()
    
        # Backup lokalny
        os.makedirs("data/raw", exist_ok=True)
        with open(f"data/raw/stable_coins_{ts_str}.json", "w") as f:
            json.dump(data, f, indent=4)
        
        return data
    except Exception as e:
        print(f"Krytyczny błąd ekstrakcji: {e}!")
        return []