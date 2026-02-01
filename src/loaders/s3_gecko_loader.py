import boto3
import json
from datetime import datetime
from botocore.exceptions import ClientConnectionError

class s3_loader:
    def __init__(self, endpoint_url, access_key, secret_key):
        self.s3 = boto3.client(
            's3',
            endpoint_url=endpoint_url
            aws_access_key=access_key,
            aws_secret_key=secret_key
        )

    def upload_raw_json(self, data: list, bucket: str, instrument: str, timestamp: datetime):
        #Hive-style path
        partition_path = (
            f"instrument={instrument}/"
            f"year={timestamp.year}/"
            f"month={timestamp.month}/"
            f"day={timestamp.day}/"
        )
        #filename convention
        file_name = f"{instrument}_{timestamp.strftime('%H%M%S')}.json"
        full_key = f"raw/{partition_path}/{file_name}"

        try:
            json_data = json.dumps(data, indent=4)
            self.s3.put_object(
                Bucket=bucket,
                Key=full_key,
                Body=json_data
            )
            print(f"Pomyślnie wysłano: s3://{bucket}/{full_key}")
            return True
        
        except ClientConnectionError:
            print("[!] Błąd: Nie można połączyć się z MinIO. Czy kontener działa?")
            return False
        
        except Exception as e:
            print(f"[!] Nieoczekiwany błąd Loadera: {e}")
            return False