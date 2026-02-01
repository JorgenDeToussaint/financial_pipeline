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
            logger.info(f"Rozpoczynam ładowanie danych do bucketu: {bucket} (Key: {full_key})")
            json_string = json.dumps(data, indent=4)
            json_bytes = json_string.encode('utf-8')

            self.s3.put_object(
                Bucket=bucket,
                Key=full_key,
                Body=json_bytes 
            )
            print(f"Pomyślnie wysłano: s3://{bucket}/{full_key}")
            return True
        
        except EndpointConnectionError as e:  # <--- Dodaj 'as e'
            logger.error(f"❌ Błąd połączenia z S3: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"❌ Nieoczekiwany błąd: {str(e)}") # <--- Zaloguj błąd, nie tylko payload!
            logger.debug(f"Payload błędu: {data}")
            return False