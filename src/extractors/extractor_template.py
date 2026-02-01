import requests
import os
import json
from datetime import datetime
from src.utils.logger import get_logger


logger = get_logger(__name__)

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
        logger.info(f"Rozpoczynam pobieranie z: {url}")
        r = requests.get(url, params=query_params) 
        r.raise_for_status()
        logger.debug(f"Status odpowiedzi: {r.status_code}")
        data = r.json()
    
        
        os.makedirs("data/raw", exist_ok=True)
        with open(f"data/raw/stable_coins_{ts_str}.json", "w") as f:
            json.dump(data, f, indent=4)
        
        return data
    except Exception as e:
        logger.error(f"Nie udało się pobrać danych: {e}")
        return []