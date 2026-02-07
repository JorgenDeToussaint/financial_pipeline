import boto3
import json
from botocore.exceptions import EndpointConnectionError
from src.loaders.base import BaseLoader
from src.utils.logger import get_logger

logger = get_logger(__name__)

class S3Loader(BaseLoader): # Dziedziczymy po kontrakcie
    def __init__(self, endpoint_url, access_key, secret_key):
        self.s3 = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

    def save(self, data, bucket: str, path: str) -> bool:
        try:
            if isinstance(data, (list, dict)):
                body = json.dumps(data, indent=4).encode('utf-8')
            else:
                body = data  # Zakładamy, że to już są bytes (np. Parquet)

            logger.info(f"📤 Uploading to S3: {bucket}/{path}")
            self.s3.put_object(Bucket=bucket, Key=path, Body=body)
            logger.info(f"✅ Success: s3://{bucket}/{path}")
            return True
        
        except EndpointConnectionError as e:
            logger.error(f"❌ S3 Connection Error: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Critical Upload Error: {e}")
            return False

    def load(self, bucket: str, path: str):
        try:
            logger.info(f'📥 Downloading from S3: s3://{bucket}/{path}')
            response = self.s3.get_object(Bucket=bucket, Key=path)
            return response['Body'].read()
        except Exception as e:
            logger.error(f"❌ Download Error for {path}: {e}")
            return None