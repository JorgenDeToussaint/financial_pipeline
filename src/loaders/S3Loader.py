# src/loaders/S3Loader.py
import boto3
import json
from src.loaders.base import BaseLoader
from src.utils.logger import get_logger

class S3Loader(BaseLoader):
    def __init__(self, endpoint_url, access_key, secret_key):
        self.logger = get_logger("S3Loader")
        self.s3 = boto3.resource(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

    def save(self, data, bucket, path):
        try:
            obj = self.s3.Object(bucket, path)
            
            # Bronze: 1 Fetch -> 1 JSON
            if isinstance(data, (dict, list)):
                body = json.dumps(data)
            # Silver: 1 JSON -> 1 Parquet
            else:
                body = data
                
            obj.put(Body=body)
            self.logger.info(f"💾 File saved: {bucket}/{path}")
            return True
        except Exception as e:
            self.logger.error(f"❌ Save failed: {e}")
            return False

    def load(self, bucket, path):
        try:
            return self.s3.Object(bucket, path).get()['Body'].read()
        except Exception as e:
            self.logger.error(f"❌ Load failed: {e}")
            return None

    def exists(self, bucket, path):
        try:
            self.s3.meta.client.head_object(Bucket=bucket, Key=path)
            return True
        except:
            return False