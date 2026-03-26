"""
ingestion/ingest.py
-------------------
Fetches daily OHLCV data for S&P 500 sector ETFs from yfinance
and lands raw CSV files in the S3 Bronze layer.

Partition structure:
  s3://<bucket>/bronze/etf/ticker=XLK/year=2024/month=01/day=15/data.csv
"""

import os
import io
import logging
from datetime import datetime, timedelta

import boto3
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

load_dotenv("../config/.env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SECTOR_ETFS = [
    "XLK",   # Technology
    "XLF",   # Financials
    "XLE",   # Energy
    "XLV",   # Health Care
    "XLI",   # Industrials
    "XLP",   # Consumer Staples
    "XLY",   # Consumer Discretionary
    "XLB",   # Materials
    "XLRE",  # Real Estate
    "XLU",   # Utilities
    "XLC",   # Communication Services
]

BUCKET      = os.getenv("S3_BUCKET")
BRONZE_PFX  = os.getenv("S3_BRONZE_PREFIX", "bronze/etf")
AWS_REGION  = os.getenv("AWS_REGION", "us-east-1")

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=AWS_REGION,
)


def fetch_daily(ticker: str, run_date: datetime) -> pd.DataFrame:
    """Download one day of OHLCV data for a single ticker."""
    start = run_date.strftime("%Y-%m-%d")
    end   = (run_date + timedelta(days=1)).strftime("%Y-%m-%d")

    df = yf.download(ticker, start=start, end=end, auto_adjust=True, progress=False)
    if df.empty:
        logger.warning(f"No data returned for {ticker} on {start} (market may be closed)")
        return df

    df = df.reset_index()
    df.columns = [c.lower() if isinstance(c, str) else c[0].lower() for c in df.columns]
    df["ticker"] = ticker
    df["ingest_ts"] = datetime.utcnow().isoformat()
    return df


def upload_to_bronze(df: pd.DataFrame, ticker: str, run_date: datetime) -> str:
    """Write DataFrame as CSV to the Bronze S3 partition."""
    y = run_date.strftime("%Y")
    m = run_date.strftime("%m")
    d = run_date.strftime("%d")
    key = f"{BRONZE_PFX}/ticker={ticker}/year={y}/month={m}/day={d}/data.csv"

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=BUCKET, Key=key, Body=csv_buffer.getvalue())
    logger.info(f"Uploaded → s3://{BUCKET}/{key}")
    return key


def run(run_date: datetime | None = None):
    """Entry point called by Airflow (or directly for backfills)."""
    run_date = run_date or datetime.utcnow() - timedelta(days=1)   # yesterday by default
    logger.info(f"Ingestion run for date: {run_date.date()}")

    success, skipped = 0, 0
    for ticker in SECTOR_ETFS:
        try:
            df = fetch_daily(ticker, run_date)
            if df.empty:
                skipped += 1
                continue
            upload_to_bronze(df, ticker, run_date)
            success += 1
        except Exception as e:
            logger.error(f"Failed for {ticker}: {e}", exc_info=True)

    logger.info(f"Done — {success} uploaded, {skipped} skipped (market closed)")


if __name__ == "__main__":
    run()
