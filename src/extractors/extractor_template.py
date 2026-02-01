import requests
import os
import json





def extract():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    query_params = {
        "vs_currency": "usd",
        "category": "stablecoins",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1
    }
    try:
        r = requests.get(url, query_params) 

        r.raise_for_status()
        
        if r is None:
            print("[!] Błąd sieci: Obiekt odpowiedzi nie istnieje.")
            return [] # Empty list to not cause future errors in import process


        if r.status_code != 200:
            print(f"[!] Błąd API: Status {r.status_code}")
            return []
    
        data = r.json()
        if len(data) < 100:
            print(f"[!] Ostrzeżenie: Otrzymano tylko {len(data)} monet zamiast 100.")
    
        os.makedirs("data", exist_ok=True)
        with open("data/raw_stable_coins.json", "w") as f:
            json.dump(data, f, indent=4)
        
        print(f"DEBUG: Pobrany typ danych to: {type(data)} o długości: {len(data)}")
        return data

    except Exception as e:
        print(f"Krytyczny błąd: {e}!")
        return []

    
if __name__ == "__main__":
    extract()