from src.extractors.extractor_template import extract 
from src.loaders.s3_gecko_loader import s3_loader 
from datetime import datetime
import os

if __name__ == "__main__":
    now = datetime.now()
    dane = extract(timestamp=now) # Przekazujemy czas
    
    if dane:
        #
        loader = s3_loader(
            endpoint_url="http://minio:9000", 
            access_key=os.getenv("MINIO_USER"), 
            secret_key=os.getenv("MINIO_PASSWORD")
        )
        
        loader.upload_raw_json(dane, bucket="bronze", instrument="stable-coins", timestamp=now)