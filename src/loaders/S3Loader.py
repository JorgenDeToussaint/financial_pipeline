import boto3
import json
import os
from src.loaders.base import BaseLoader
from src.utils.logger import get_logger


class S3Loader(BaseLoader):
    def __init__(
        self,
        # Pobieramy z env, ale mapujemy na nazwy, których użyłeś w Compose
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        access_key=os.getenv("S3_ACCESS_KEY"),
        secret_key=os.getenv("S3_SECRET_KEY"),
    ):
        self.logger = get_logger("S3Loader")

        # Inicjalizacja Resource
        self.s3 = boto3.resource(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        # Wyciągamy klienta z zasobu, żeby mieć dostęp do list_buckets itp.
        self.client = self.s3.meta.client
        self.logger.info(f"✅ S3Loader initialized for {endpoint_url}")

    def save(self, data, bucket, path):
        if data is None:
            self.logger.warning(f"⚠️ Skipping save: Data is None for {bucket}/{path}")
            return False

        try:
            obj = self.s3.Object(bucket, path)

            if isinstance(data, (dict, list)):
                # Dodajemy ensure_ascii dla poprawnych znaków PL i indent dla czytelności Bronze
                body = json.dumps(data, ensure_ascii=False, indent=2)
            else:
                body = data
                # Mały sanity check dla Silver (bytes)
                if len(body) == 0:
                    self.logger.error(f"❌ Attempted to save 0 bytes to {path}")
                    return False

            obj.put(Body=body)
            self.logger.info(f"💾 File saved: {bucket}/{path} ({len(body)} bytes)")
            return True

        except Exception as e:
            self.logger.error(f"❌ Save failed: {e}")
            return False

    def load(self, bucket, path):
        try:
            return self.s3.Object(bucket, path).get()["Body"].read()
        except Exception as e:
            self.logger.error(f"❌ Load failed: {e}")
            return None

    def exists(self, bucket: str, path: str) -> bool:
        try:
            self.s3.Object(bucket, path).load()
            return True
        except self.client.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def is_ready(self) -> bool:
        try:
            # Używamy klienta wyciągniętego w __init__
            self.client.list_buckets()
            return True
        except Exception as e:
            self.logger.error(f"📡 S3 Connection check failed: {e}")
            return False
