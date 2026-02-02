from abc import ABC, abstractmethod
import logging
import requests

class BaseExtractor(ABC):
    def __init__(self, name: str, base_url: str):
        self.name = name
        self.base_url = base_url
        self.logger = logging.getLogger(f'src.extractors.{name}')

        @abstractmethod
        def get_params(self) -> dict:
            #Here goes params for particular extractors
            pass

        def fetch(self) -> list:
            self.logger.info(f"Stared downloading from: {self.base_url}")

            try:
                params = self.get_params()
                response = requests.get(self.base_url)

                return self.validate(response)
            
            except Exception as e:
                self.logger.error(f"critical error during downloading from {self.name}: {e}")
                raise

        def validate(self, response: requests.Response) -> listL
            if response.status_code == 200:
                return response.json()
            
            self.logger.error(f"API {self.name} responded with {response.status_code}: {response.text}")
            response.raise_for_status()
