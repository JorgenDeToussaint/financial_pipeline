from abc import ABC, abstractmethod
from src.utils.logger import get_logger
import aiohttp
import asyncio
from src.exceptions.extractors import RateLimitError, AuthError, ServerError, ExtractorError
from src.utils.timer import execution_timer

class BaseExtractor(ABC):
    def __init__(self, name:str, base_url: str, timeout: int = 30):
        self.name = name
        self.base_url = base_url
        self.timeout = timeout
        self.logger = get_logger(f"extractor.{name}")

    @abstractmethod
    def get_params(self) -> dict:
        pass

    def get_headers(self) -> dict:
        return {}
    
    async def _check_status(self, response: aiohttp.ClientResponse):
        status = response.status
        if status == 200:
            return
        
        if status == 429:
            self.logger.warning(f"🛑 Rate Limit hit (429). Retry-After: {response.headers.get('Retry-After')}")
            raise RateLimitError(retry_after=response.headers.get("Retry-After"))
        
        if status in [401, 403]:
            self.logger.critical(f"🚫 Access Denied (403/401). Sprawdź uprawnienia!")
            raise AuthError(f"Forbidden: {status}")
        
        if status >= 500:
            self.logger.error(f"🏠 API Server Error: {status}")
            raise ServerError(f"Server error: {status}")
        
        raise ExtractorError(f"Unexpected status: {status}")
    
    async def fetch(self, session: aiohttp.ClientSession) -> list:
        with execution_timer(self.name, self.logger):
            self.logger.info(f"🚀 Inicjacja pobierania: {self.base_url}")

            try:
                async with session.get(
                    self.base_url, 
                    params=self.get_params(), 
                    headers=self.get_headers(),
                    timeout=aiohttp.ClientTimeout(total=self.timeout)
                ) as response:
                
                    await self._check_status(response)

                    data = await response.json()

                    count = len(data) if isinstance(data, list) else 1
                    self.logger.info(f"✅ Sukces: Pobrano {count} rekordów.")

                    return data
            
            except asyncio.TimeoutError:
                self.logger.error(f"⌛ Timeout po {self.timeout}s na {self.name}")
                raise ServerError("Timeout")
            except Exception as e:
                self.logger.error(f"❌ Krytyczny błąd w rurze {self.name}: {str(e)}")
                raise ExtractorError(f"Connection failed: {e}")
    