from src.registry import EXTRACTOR_REGISTRY, TRANSFORMER_REGISTRY

class PipeFactory:
    @staticmethod
    def get_extractor(e_type: str, params: dict = None):
        print(f"DEBUG: Szukam ekstraktora: '{e_type}'")
        print(f"DEBUG: Dostępne w rejestrze: {list(EXTRACTOR_REGISTRY.keys())}")
        
        cls = EXTRACTOR_REGISTRY.get(e_type)
        if not cls:
            raise ValueError(f"Unknown extractor: {e_type}. Check if decorated with @register_extractor")
        return cls(**params)
    
    @staticmethod
    def get_transformer(t_type: str, params: dict = None):
        cls = TRANSFORMER_REGISTRY.get(t_type)
        if not cls:
            raise ValueError(f"Unknown transformer type: {t_type}")
        return cls()
    

