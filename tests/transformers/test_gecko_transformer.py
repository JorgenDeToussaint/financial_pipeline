import pytest
import polars as pl
from src.transformers.gecko_transformer import GeckoTransformer


VALID_PAYLOAD = [
    {
        "symbol": "btc",
        "current_price": 50000.0,
        "market_cap": 1000000000.0,
        "total_volume": 50000000.0,
        "last_updated": "2026-03-15T10:00:00.000Z",
        "circulating_supply": 19000000.0,
        "total_supply": 21000000.0,
        "max_supply": 21000000.0,
        "fully_diluted_valuation": 1050000000.0,
    }
]


class TestGeckoTransformer:
    def test_valid_payload_returns_bytes(self):
        t = GeckoTransformer()
        result = t.transform(VALID_PAYLOAD)
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_valid_payload_schema(self):
        t = GeckoTransformer()
        df = t.run_logic(VALID_PAYLOAD)
        assert "ticker" in df.columns
        assert "price_usd" in df.columns
        assert "market_cap" in df.columns
        assert "datetime" in df.columns

    def test_ticker_is_uppercase(self):
        t = GeckoTransformer()
        df = t.run_logic(VALID_PAYLOAD)
        assert df["ticker"][0] == "BTC"

    def test_empty_list_returns_empty_df(self):
        t = GeckoTransformer()
        df = t.run_logic([])
        assert df.height == 0

    def test_invalid_payload_returns_none(self):
        t = GeckoTransformer()
        result = t.transform("not a list")
        assert result is None