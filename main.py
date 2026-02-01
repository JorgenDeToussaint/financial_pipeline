import os
import json
from datetime import datetime
from src.utils.logger import get_logger
from src.extractors.extractor_template import extract
from src.loaders.s3_gecko_loader import s3_loader
from src.transformers.transformer_template import transform_to_silver


logger = get_logger(__name__)

if __name__ == "__main__":
    logger.info("--- START PIPELINE ---")
    now = datetime.now()
    
    # 1. Extract
    dane = extract(timestamp=now)
    if not dane:
        logger.warning("get empty list - stopping pipeline")
    else:
        print(f"Main odebrał {len(dane)} rekordów.")
        
        # 2. loader init
        loader = s3_loader(
            endpoint_url="http://minio:9000", 
            access_key=os.getenv("MINIO_USER", "minioadmin"), 
            secret_key=os.getenv("MINIO_PASSWORD", "minioadmin")
        )
        
        # 3. bronze layer
        if loader.upload_raw_json(
            data=dane, 
            bucket="bronze", 
            instrument="stable-coins", 
            timestamp=now
        ):
            #4. transform
            logger.info("Started transforming to silver")

            json_bytes = json.dumps(dane).encode('utf-8')
            parquet_data = transform_to_silver(json_bytes)

            if parquet_data:
                #5 silver data to minio3
                partition = f"instrument=stable-coins/year={now.year}/month={now.month:02d}/day={now.day:02d}"
                silver_key = f"processed/{partition}/data_{now.strftime('%H%M%S')}.parquet"

                loader.upload_bytes(parquet_data, "silver", silver_key)
            else:
                logger.error("Transform to silver did not end correctly.")


    logger.info("--- END PIPELINE ---")