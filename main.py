from src.extractors.extractor_template import extract 
from src.loaders.s3_gecko_loader import s3_loader 
from datetime import datetime
import os
from src.utils.logger import get_logger


logger = get_logger(__name__)

if __name__ == "__main__":
    logger.info("--- START PIPELINE ---")
    now = datetime.now()
    
    # 1. Pobierz
    dane = extract(timestamp=now)
    
    if dane:
        print(f"Main odebrał {len(dane)} rekordów.")
        
        # 2. Połącz (używamy nazw z docker-compose)
        loader = s3_loader(
            endpoint_url="http://minio:9000", 
            access_key=os.getenv("MINIO_USER", "minioadmin"), 
            secret_key=os.getenv("MINIO_PASSWORD", "minioadmin")
        )
        
        # 3. Wyślij
        loader.upload_raw_json(
            data=dane, 
            bucket="bronze", 
            instrument="stable-coins", 
            timestamp=now
        )

    logger.info("--- END PIPELINE ---")