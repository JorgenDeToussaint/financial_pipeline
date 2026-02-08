from abc import ABC, abstractmethod
from src.utils.logger import get_logger
import requests

class BaseExtractor(ABC):
    def __init__(self, name:str, base_url: str):
        self.name = name
        self.base_url = base_url
        self.logger = get_logger(f"extractor.{name}")

    @abstractmethod
    def get_params(self) -> dict:
        pass

    def fetch(self) -> list:
        self.logger.info(f"🚀 Start: {self.base_url}")
        try:
            response = requests.get(
                self.base_url, 
                params=self.get_params(), 
                headers=self.get_headers(), # <--- TU JEST ZMIANA
                timeout=30
            )
            if response.status_code == 200:
                return response.json()
            
            self.logger.error(f"⚠️ API Error {response.status_code}: {response.text}")
            return []
        except Exception as e:
            self.logger.error(f"❌ Connection failed: {e}")
            return []