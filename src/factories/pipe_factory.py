from src.extractors.gecko import GeckoExtractor
from src.extractors.nbp import NBPExtractor
from src.extractors.yah_etf import YahooETF
from src.transformers.gecko import GeckoTransformer
from src.transformers.NBPTrans import NBPTransformer
from src.transformers.yahoo_transformer import YahooTransformer


class PipeFactory:
    @staticmethod
    def get_extractor(e_type: str, params: dict):
        mapping = {
            "gecko": lambda: GeckoExtractor(**params),
            "nbp": lambda: NBPExtractor(**params),
            "yahoo": lambda: YahooETF(**params)
        }
        return mapping[e_type]()
    
    @staticmethod
    def get_transformer(t_type: str, params: dict = None):
        mapping = {
            "gecko": lambda: GeckoTransformer(),
            "nbp": lambda: NBPTransformer(),
            "yahoo": lambda: YahooTransformer(),
        }
        return mapping[t_type]()