# LLM → Structured Data (Mini-POC)

A tiny, deterministic pipeline that asks an LLM for a tabular answer, validates the format, normalizes it, and emits to a pluggable sink (CSV file by default; SQLite and others support planned).

## What you get

* Prompt pack to force CSV (or JSON) with deterministic ordering.

* Validator (validator_csv.py) that enforces:
  * header = country,capital
  * exactly 20 rows
  * 2 columns per row
  * rows sorted A→Z by country
  * RFC4180-style quoting

* Driver / Main program (main.py) that:
  * calls OpenAI, retries on validation failure with a correction prompt
  * hands validated rows to a sink

* Sinks (sinks.py, emit.py) with a Sink protocol:
  * CsvSink → out/countries.csv
  * (Anticipated) SQLiteSink → out/countries.db (table countries)
  * Should be easy to add Postgres, HTTP webhook, S3, etc.

## How it works (flow)

```
LLM (prompted for CSV)
   ↓
validator_csv.py  ── checks shape/header/count/sort/quoting
   ↓
normalized matrix (header + 20 rows)
   ↓
csv_rows_to_dicts() → list[{"country":..., "capital":...}]
   ↓
emit(rows, Sink)  → CsvSink / SQLiteSink / ...
```

> On failure, `main.py` sends a precise correction prompt with the original output, then retries (up to N times).

## Next steps

* Add unit tests for validator and sinks.
* Add reference checks for factual accuracy (compare to a known country/capital list).
* Introduce observability: log runs, validation errors, and final sink stats.
* Support upserts in DB sinks and idempotency keys for re-runs.
