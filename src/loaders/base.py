from abc import ABC, abstractmethod

class BaseLoader(ABC):
    @abstractmethod
    def save(self, data, bucket: str, path: str) -> bool:
        pass

    @abstractmethod
    def load(self, bucket: str, path: str) -> bytes:
        pass

    @abstractmethod
    def exists(self, bucket: str, path: str) -> bool:
        pass