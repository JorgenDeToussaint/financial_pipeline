from abc import ABC, abstractmethod
import logging
import requests

class BaseExtractor(ABC):
    def __init__(self, name:str, base_url: str):
        self.name = name
        self.base_url = base_url
        self.logger = logging.getLogger(f'src.extractors.{name}')

    @abstractmethod
    def get_params(self) -> dict:
        pass

    def fetch(self) -> list:
        self.logger.info(f"Downloading: {self.name}")
        try:
            response = requests.get(self.base_url, params=self.get_params(), timeout=30)
            return self.validate(response)
        
        except Exception as e:
            self.logger.error(f"Error in pipe {self.name}: {e}")
            return []
        
    def validate(self, response: requests.Response) -> list:
        if response.status_code == 200:
            return response.json()
        return []