# Hyperliquid Multi-Program Market-Maker

A compact project README describing the market-making algorithm, the expected-value (EV) method used, and how data is stored using SQL and SQLite.

**Project Overview**

- This repository contains a Python market-making system built on a Hyperliquid connector.
- The strategy continuously quotes both sides of an L2 order book and chooses quotes using an expected-value (EV) model that estimates fill probability and profit.
- The system records market snapshots and trades to a dedicated WAL SQLite file and mirrors key rows into a primary SQL store via SQLAlchemy.

**Market-Making Algorithm (high-level)**

- Collect live L2 book snapshots (best bid/ask, deeper levels) via websocket and fall back to REST snapshots when necessary.
- Compute a fair mid price (VWAP or mid of best bid/ask) and select a half-spread target $h$ (price units) for maker quotes.
- For each candidate quote (side, size, price) estimate the probability of a fill $q$ from recent trade activity and trade-sweep statistics.
- Estimate expected profit per quote and place/update orders constrained by risk limits.

**Expected Value (EV) Method**

We estimate the expected profit of a maker quote by combining the estimated fill probability with the expected per-fill profit minus fees.

- Notation:
  - $s$: order size (base units)
  - $p_{mid}$: mid price (reference)
  - $h$: half-spread in price units (ask - mid or mid - bid)
  - $q$: probability the quote fills in the evaluation horizon
  - $f_m$: maker fee rate (fraction of notional), e.g. 0.0003 for 0.03%

- Expected profit per placed quote (per evaluation period):

$$\mathrm{EV_{quote}} = q \cdot (h \cdot s - f_m \cdot p_{mid} \cdot s)$$

- If we estimate expected fills per hour $F$ for this quote, EV per hour is:

$$\mathrm{EV_{hour}} = F \cdot (h - f_m \cdot p_{mid}) \cdot s$$

- Trade expectancy $q$ and $F$ are derived from public trade data: grouping trades into taker-sweeps and measuring how often our quoted price would have been hit within the lookback window. The system computes these statistics from cached websocket trades and backfilled REST recent trades.

**Practical considerations**

- Round price and size to exchange step sizes before placing orders.
- Respect per-market max leverage and margin rules when computing notional exposures.
- Use conservative fill-probability estimates for low-liquidity markets to avoid over-aggressive quoting.

**Data storage: SQL and SQLite usage**

- Dedicated recorder SQLite (`MARKET_DATA_DB_PATH`, default `market_data.sqlite`):
  - Implemented in `market_recorder.py` as `MarketDataRecorder`.
  - Stores `market_snapshots` (book snapshots, diagnostics, my quotes) and `market_trades` (deduplicated trades) in WAL mode so writes don't block the quote-edit hot path.
  - Recorder uses the standard library `sqlite3` and performs schema migration checks on startup.

- Mirrored relational store (SQL/SQLAlchemy):
  - Implemented in `db_store.py` as `DualDB` which builds SQL schema via SQLAlchemy metadata and `create_all()`.
  - Configuration lives in `settings.py`:
    - `PRIMARY_SQL_URL` — primary SQLAlchemy URL (default `sqlite:///./primary_sql_mirror.sqlite`). Can be a MySQL/Postgres URL (e.g. `mysql+pymysql://user:pass@host/db`) to write to a full RDBMS.
    - `SQLITE_URL` — local redundant sqlite mirror (default `sqlite:///./local_redundant.sqlite`).
    - `REPLICATION_SPOOL_FILE` — path for spool file when writes to an engine fail.
  - `DualDB.upsert()` writes to all configured engines; failures are recorded to the spool file so replication can be retried.

- Files present in this workspace that show DB activity:
  - `market_data.sqlite` — market snapshots and trades (recorder)
  - `local_redundant.sqlite` — local mirrored SQL store (via SQLAlchemy)
  - `primary_sql_mirror.sqlite` — primary mirrored SQL store (default; can be replaced by a remote SQL server)

**How the databases are used by the algorithm**

- EV estimation and trade-expectancy rely on recent market trades and VWAP statistics computed from `market_trades` and `market_snapshots` in `market_data.sqlite`.
- The runtime state (orders, fills, positions, market leverage and model quotes) is upserted into the primary SQL store so external tools or dashboards can consume it reliably.
- The spool/replication mechanism ensures transient primary DB outages do not lose rows — failed upserts are written to `REPLICATION_SPOOL_FILE` for later retry.

**Quick commands**

Open the recorder DB with sqlite3 (example):

```bash
sqlite3 market_data.sqlite
-- then e.g.:
SELECT coin, COUNT(*) FROM market_trades GROUP BY coin ORDER BY COUNT(*) DESC;
```

Run unit tests that exercise DB code:

```bash
.venv\\Scripts\\python.exe -m unittest test_market_recorder.py
.venv\\Scripts\\python.exe -m unittest test_hyperliquid_connector_offline.py
```

If you want the primary store to be MySQL, set `PRIMARY_SQL_URL` in `settings.py` to a MySQL SQLAlchemy URL before starting.

**Skills demonstrated / used**

- Python system design: modular connector + strategy separation.
- Real-time websockets and REST fallbacks for L2 book ingestion.
- Statistical EV modeling: trade expectancy, VWAP, sweep grouping.
- SQL and data engineering: WAL SQLite recorder, SQLAlchemy schema & mirrored upserts, replication spool.
- Concurrency: background recorder thread and safe caching.
- Testing: unit tests for connector and recorder code.

**Where to look in the repo**

- Connector and normalization: `connectors/hyperliquid_connector.py`
- Recorder: `market_recorder.py`
- Mirrored DB and schema: `db_store.py`
- Settings: `settings.py`
- Tests: `test_market_recorder.py`, `test_hyperliquid_connector_offline.py`, `test_market_data_web.py`

If you want, I can also add a short usage section to start the market-maker locally or create a small script to export EV reports from `market_data.sqlite`.
