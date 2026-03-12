# Contributing

## Adding a New Data Source

Adding a source requires exactly two files and one config entry. No changes to existing code.

### 1. Extractor — `src/extractors/your_source.py`

```python
from src.extractors.base import BaseExtractor
from src.extractors.registry import register_extractor

@register_extractor("your_key")
class YourExtractor(BaseExtractor):
    def __init__(self, your_param: str, **kwargs):
        super().__init__(
            name=f"YourSource-{your_param}",
            base_url="https://api.yoursource.com/endpoint"
        )
        self.your_param = your_param

    def get_params(self) -> dict:
        return {"param": self.your_param}

    def get_headers(self) -> dict:
        return {"Authorization": f"Bearer {os.getenv('YOUR_API_KEY')}"}
```

The `@register_extractor` decorator registers the class automatically — `__init__.py` scans the package on import.

### 2. Transformer — `src/transformers/your_transformer.py`

```python
import polars as pl
from src.transformers.base import BaseTransformer

class YourTransformer(BaseTransformer):
    def __init__(self):
        super().__init__("YourSource")

    def run_logic(self, data: list | dict) -> pl.DataFrame:
        # Parse raw API response into a typed Polars DataFrame.
        # BaseTransformer.transform() will call validate() and serialize to Parquet.
        df = pl.DataFrame(data)
        return df.select([...]).with_columns([...])
```

`run_logic()` receives the parsed JSON (list or dict). Return a `pl.DataFrame` — `BaseTransformer` handles validation and Parquet serialization.

### 3. Config — `config/pipes.yaml`

```yaml
- id: your_source_id
  extractor_type: your_key
  transformer_type: your_key
  params:
    your_param: value
  granularity: daily  # or hourly
```

---

## Commit Convention

This project uses [Conventional Commits](https://www.conventionalcommits.org/):

```
feat:     new extractor, transformer, or pipeline feature
fix:      bug fix
refactor: code change with no functional difference
test:     adding or updating tests
docs:     README, CHANGELOG, docstrings
chore:    deps, config, tooling
```

Examples:
```
feat: add Kraken OHLCV extractor
fix: resolve i128 overflow in GeckoTransformer market_cap cast
refactor: replace PipeFactory hardcoded mapping with TRANSFORMER_REGISTRY
docs: add docstrings to BaseTransformer
```

---

## Environment Setup

```bash
cp .env.example .env
# fill in API keys and MinIO credentials

docker-compose up
```

Python 3.13 required for local development. Dependencies in `requirements.txt`.

---

## Data Quality Contract

Every transformer must guarantee that data reaching Silver layer passes `BaseTransformer.validate()`:
- Non-empty DataFrame
- Less than 50% null values across all cells

If your source has known sparse fields, override `validate()` in your transformer with appropriate thresholds and document why.
