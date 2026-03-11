TRANSFORMER_REGISTRY = {}

def register_transformer(name: str):
    def decorator(cls):
        TRANSFORMER_REGISTRY[name] = cls
        return cls
    return decorator