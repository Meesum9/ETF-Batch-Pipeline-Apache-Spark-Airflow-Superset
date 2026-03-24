# ETF Batch Pipeline — Apache Spark + Airflow + Superset

A daily batch processing pipeline that ingests S&P 500 sector ETF data
via `yfinance`, stores it on AWS S3 using the **Medallion Architecture**
(Bronze → Silver → Gold), computes financial KPIs using **Apache Spark**,
and visualises them on an **Apache Superset** dashboard.

---

## Architecture

```
yfinance API
     │  (daily, after market close)
     ▼
[Airflow DAG]  ── schedule: Mon–Fri 18:00 UTC
     │
     ├─ Task 1: ingest_yfinance_to_bronze
     │         Fetch OHLCV for 11 sector ETFs → S3 Bronze (raw CSV)
     │
     ├─ Task 2: spark_bronze_to_silver
     │         PySpark — cast types, DQ checks, dedup → S3 Silver (Parquet)
     │
     └─ Task 3: spark_silver_to_gold
               PySpark — compute rolling KPIs → S3 Gold (Parquet) + Postgres
                                                                       │
                                                               Apache Superset
                                                               (reads etf_kpis table)
```

---

## Tickers Tracked

| Ticker | Sector                    |
|--------|---------------------------|
| XLK    | Technology                |
| XLF    | Financials                |
| XLE    | Energy                    |
| XLV    | Health Care               |
| XLI    | Industrials               |
| XLP    | Consumer Staples          |
| XLY    | Consumer Discretionary    |
| XLB    | Materials                 |
| XLRE   | Real Estate               |
| XLU    | Utilities                 |
| XLC    | Communication Services    |

---

## KPIs in Gold Layer

| KPI                  | Description                                    |
|----------------------|------------------------------------------------|
| `daily_return_pct`   | (close − prev_close) / prev_close × 100        |
| `monthly_return_pct` | (close − close_30d_ago) / close_30d_ago × 100  |
| `ma_50`              | 50-day simple moving average of close           |
| `ma_200`             | 200-day simple moving average of close          |
| `ma_signal`          | "bullish" / "bearish" based on MA50 vs MA200   |
| `avg_volume_30d`     | 30-day rolling average volume                  |
| `volume_spike`       | True when volume > 2× avg_volume_30d           |
| `high_low_range_pct` | (high − low) / low × 100 (intraday volatility) |

---

## S3 Layer Structure

```
s3://<bucket>/
  bronze/etf/ticker=XLK/year=2024/month=11/day=15/data.csv   ← raw CSV
  silver/etf/year=2024/month=11/day=15/*.parquet              ← cleaned
  gold/etf/ticker=XLK/*.parquet                               ← KPIs (full history)
```

---

## Project Structure

```
spark-etl-project/
├── config/
│   └── .env                        # AWS + Postgres credentials
├── ingestion/
│   └── ingest.py                   # yfinance → S3 Bronze
├── spark_jobs/
│   ├── bronze_to_silver.py         # Spark: clean + validate
│   └── silver_to_gold.py           # Spark: compute KPIs
├── dags/
│   └── daily_etf_etl_dag.py        # Airflow DAG (Mon–Fri)
├── superset/
│   └── dashboard_queries.sql       # Ready-to-paste Superset SQL charts
├── docker/
│   ├── docker-compose.yml          # Local dev stack
│   └── init_finance_db.sql         # Postgres schema
└── requirements.txt
```

---

## Quick Start (Local Dev)

### 1. Clone & configure
```bash
git clone <your-repo>
cd spark-etl-project
cp config/.env.example config/.env
# Edit config/.env with your AWS credentials and bucket name
```

### 2. Start the stack
```bash
cd docker
docker-compose up -d
```

| Service         | URL                    | Credentials     |
|-----------------|------------------------|-----------------|
| Airflow UI      | http://localhost:8080  | admin / admin   |
| Superset        | http://localhost:8088  | admin / admin   |
| Postgres        | localhost:5432         | airflow / airflow|

### 3. Run a manual backfill (optional)
```bash
# Fetch last 30 days of data
python ingestion/ingest.py
```

### 4. Connect Superset to Postgres
In Superset → Settings → Database Connections:
```
postgresql://admin:admin@postgres:5432/finance_gold
```
Then add the `etf_kpis` table as a dataset and use the queries in
`superset/dashboard_queries.sql` to build your charts.

---

## Dashboard Recommended Charts

1. **Latest prices** — Table: close, daily_return_pct, ma_signal per ETF
2. **Price + MA lines** — Line chart: close / MA50 / MA200 with ticker filter
3. **Return heatmap** — 30-day heatmap: tickers × dates × daily_return_pct
4. **Volume spike alerts** — Table: spikes only, sorted by volume ratio
5. **Monthly return bar** — Sorted bar: all ETFs by monthly_return_pct
6. **Intraday range area** — Area chart: high_low_range_pct over 90 days
7. **MA signal pie** — Bullish / Bearish / Neutral split across all ETFs

---

## Notes

- `yfinance` returns no data on weekends and US market holidays.
  The ingestion script handles empty responses gracefully (logs a warning, skips).
- The Spark Silver job uses `mode("overwrite")` per date partition so re-runs
  are idempotent — safe to backfill without duplicates.
- The Gold job rewrites the full `etf_kpis` Postgres table daily because
  rolling KPIs (MA50, MA200) for older rows can shift as new data arrives.
  For large datasets, switch to an upsert strategy using the (date, ticker) PK.
>>>>>>> a07ac0f (repo is ready)
