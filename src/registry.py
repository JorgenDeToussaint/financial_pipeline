EXTRACTOR_REGISTRY = {}
TRANSFORMER_REGISTRY = {}


def register_extractor(name: str):
    def decorator(cls):
        EXTRACTOR_REGISTRY[name] = cls
        return cls

    return decorator


def register_transformer(name: str):
    def decorator(cls):
        TRANSFORMER_REGISTRY[name] = cls
        return cls

    return decorator
