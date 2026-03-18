import pytest
import polars as pl
from src.transformers.base import BaseTransformer

class ConcreteTransformer(BaseTransformer):
    """Minimal concrete implementation for testing BaseTransformer."""
    def __init__(self):
        super().__init__("test")

    def run_logic(self, data) -> pl.DataFrame:
        return pl.DataFrame({"value": [1, 2, 3]})
        
class TestValidate:
    def test_emptt_dataframe_returns_false(self):
            t = ConcreteTransformer()
            assert t.validate(pl.DataFrame()) is False

    def test_none_returns_false(self):
         t = ConcreteTransformer()
         assert t.validate(None) is False

    def test_clean_dataframe_returns_true(self):
        t = ConcreteTransformer()
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        assert t.validate(df) is True

    def test_too_many_nulls_returns_false(self):
        t = ConcreteTransformer()
        df = pl.DataFrame({"a": [None, None, None], "b": [None, None, None]})
        assert t.validate(df) is False


class TestTransform:
    def test_none_payload_returns_none(self):
        t = ConcreteTransformer()
        assert t.transform(None) is None
    
    def test_valid_payload_returns_none(self):
        t = ConcreteTransformer()
        result = t.transform([{"value": 1}])
        assert isinstance(result, bytes)
        assert len(result) > 0
