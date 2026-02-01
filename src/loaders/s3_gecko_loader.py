import boto3
import json
from datetime import datetime
from botocore.exceptions import EndpointConnectionError

class s3_loader:
    def __init__(self, endpoint_url, access_key, secret_key):
        self.s3 = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

    def upload_raw_json(self, data: list, bucket: str, instrument: str, timestamp: datetime):
        # 1. Budujemy ścieżkę (z :02d dla porządku w MinIO)
        partition_path = (
            f"instrument={instrument}/"
            f"year={timestamp.year}/"
            f"month={timestamp.month:02d}/"
            f"day={timestamp.day:02d}"
        )
        file_name = f"{instrument}_{timestamp.strftime('%H%M%S')}.json"
        full_key = f"raw/{partition_path}/{file_name}"

        try:
            # KLUCZOWY MOMENT: Zamiana listy na bajty
            json_string = json.dumps(data, indent=4)
            json_bytes = json_string.encode('utf-8')

            self.s3.put_object(
                Bucket=bucket,
                Key=full_key,
                Body=json_bytes # Wysyłamy BAJTY, nie listę!
            )
            print(f"Pomyślnie wysłano: s3://{bucket}/{full_key}")
            return True
        except EndpointConnectionError:
            print("[!] Błąd połączenia z MinIO!")
            return False
        except Exception as e:
            print(f"[!] Błąd Loadera: {e}")
            return False