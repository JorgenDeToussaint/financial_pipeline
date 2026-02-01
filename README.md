# Financial multi-instrument pipeline

So, I am making pipeline on various financial instruments out of curiosity.
Finall wersion will include full cloud synchronisation, analitical feauters etc.

Right now, I have scrapped 100 stablecoins from coingecko API.
Next thing is to set Mini03 for storage(data lake instead of table of facts).
For now it's running on single T480 thinkpad, future development includes cluster of thinkpads.

## Already Done:
 - [x] Folder structure for future updates
 - [x] Docker image for stable enviorment
 - [x] Extractor for stablecoins(top 100 on public api v3)

## To do next:
 - [ ] Mini03 on container
 - [ ] Tranform via polars to parquet(transformer)
 - [ ] Load to duckdb(loader)

### Why polars and duckdb instead of pandas and duckdb?
Storage is cheap, RAM is expensive right now.
Prefer to make these thinkpads as effective as they can.