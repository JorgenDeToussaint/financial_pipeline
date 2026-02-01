from src.extractors.extractor_template import extract
from src.loaders.s3_gecko_loader import s3_loader  # Importujemy klasę, nie funkcję
from datetime import datetime

if __name__ == "__main__":
    # 1. Pobieramy dane
    dane = extract()
    print(f"Main odebrał {len(dane)} rekordów.")

    # 2. Jeśli mamy dane, wysyłamy
    if dane:
        # Generujemy timestamp raz dla całego procesu
        now = datetime.now()
        
        # Inicjalizujemy loader (Upewnij się, że te dane pasują do Twojego MinIO)
        loader = s3_loader(
            endpoint_url="http://localhost:9000", 
            access_key="minioadmin", 
            secret_key="minioadmin"
        )
        
        # Wywołujemy metodę wysyłki
        loader.upload_raw_json(
            data=dane, 
            bucket="raw-data", 
            instrument="stable-coins", 
            timestamp=now
        )