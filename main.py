from src.extractors.extractor_template import extract
from src.loaders.s3_gecko_loader import upload_to_minio
from datetime import datetime



if __name__ == "__main__":
    dane = extract()
    print(f"Main odebrał {len(dane)} rekordów.")
    if dane:
        upload_to_minio(dane, bucket="raw-data", instrument="stable-coins")
