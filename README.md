# Financial multi-instrument pipeline

So, I am making pipeline on various financial instruments out of curiosity.
Finall wersion will include full cloud synchronisation, analitical feauters etc.

Right now, I have scrapped 100 stablecoins from coingecko API.
Next thing is to set MinIO for storage(data lake instead of table of facts).
For now it's running on single T480 thinkpad, future development includes cluster of thinkpads.

## Already Done:
 - [x] Folder structure for future updates
 - [x] Docker image for stable enviorment
 - [x] Extractor for stablecoins(top 100 on public api v3)
 - [x] MinIO container loading
 - [x] Tranform via polars to parquet(transformer)
 - [x] logging on all scripts

## To do next:
 - [ ] Load to duckdb(loader)
 - [ ] full class templates for next pipes
 - [ ] forex pipe
 - [ ] nasdaq 100 pipe
 - [ ] structurized logging
 - [ ] basic orchiestration in main.py


 ## Issue that i do not recognised
 Well then, as these stablecoin works, im not sure on vertiacl/horizontal development as far, I'll propably go along with next scrapers, API's to not only relay on the public ones.

 As also i upgraded my main machine(lenovo loq 15iax9), there is no need to be that optimized for ram, but i'll stay with it.

 the last one, there i have oportunity for full claster of three thinkpads, that will come after the horizontal development.

### Why polars and duckdb instead of pandas and postgres?
Storage is cheap, RAM is expensive right now.
Prefer to make these thinkpads as effective as they can.