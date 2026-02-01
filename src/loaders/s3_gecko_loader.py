import boto3
import json
from datetime import datetime
from botocore.exceptions import EndpointConnectionError
from src.utils.logger import get_logger

logger = get_logger(__name__)

class s3_loader:
    def __init__(self, endpoint_url, access_key, secret_key):
        self.s3 = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

    def upload_raw_json(self, data: list, bucket: str, instrument: str, timestamp: datetime):
        partition_path = (
            f"instrument={instrument}/"
            f"year={timestamp.year}/"
            f"month={timestamp.month:02d}/"
            f"day={timestamp.day:02d}"
        )

        file_name = f"{instrument}_{timestamp.strftime('%H%M%S')}.json"
        full_key = f"raw/{partition_path}/{file_name}"

        try:
            logger.info(f"📤 Rozpoczynam upload do S3: {bucket}/{full_key}")
            json_string = json.dumps(data, indent=4)
            json_bytes = json_string.encode('utf-8')

            self.s3.put_object(
                Bucket=bucket,
                Key=full_key,
                Body=json_bytes 
            )
            logger.info(f"✅ Sukces: s3://{bucket}/{full_key}")
            return True
        
        except EndpointConnectionError as e:
            logger.error(f"❌ Błąd połączenia z S3 (MinIO): {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Krytyczny błąd ładowania: {type(e).__name__}: {e}")
            return False
        
    def upload_bytes(self, data: bytes, bucket: str, key: str):
        """Nowa metoda dla Silvera (Parquet) [cite: 2026-01-24]"""
        try:
            self.s3.put_object(Bucket=bucket, Key=key, Body=data)
            logger.info(f"✅ Silver: s3://{bucket}/{key}")
            return True
        except Exception as e:
            logger.error(f"❌ Silver Error: {e}")
            return False
        
    def download_object(self, bucket: str, key: str):
        try:
            logger.info(f' Pobieram obiekt: s3://{bucket}/{key}')
            response = self.s3.get_object(Bucket=bucket, Key=key)
            return response['Body'].read()
        except Exception as e:
            logger.error(f" NIe udało się pobrać obiektu {key}: {e}")
            return None