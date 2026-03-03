EXTRACTOR_REGISTRY = {}

def register_extractor(name: str):
    def decorator(cls):
        EXTRACTOR_REGISTRY[name] = cls
        return cls
    return decorator