"""
spark_jobs/silver_to_gold.py
-----------------------------
Reads the full Silver history, computes rolling KPIs per ticker,
and writes the Gold layer to both S3 Parquet and Postgres
(for Apache Superset to query directly).

KPIs computed per ticker per day:
  - daily_return_pct    : (close - prev_close) / prev_close * 100
  - monthly_return_pct  : (close - close_30d_ago) / close_30d_ago * 100
  - ma_50               : 50-day simple moving average of close
  - ma_200              : 200-day simple moving average of close
  - avg_volume_30d      : 30-day rolling average volume
  - volume_spike        : True if volume > 2× avg_volume_30d
  - high_low_range_pct  : (high - low) / low * 100  (intraday range)
"""

import os
import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from dotenv import load_dotenv

load_dotenv("../config/.env")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET      = os.getenv("S3_BUCKET")
SILVER_PFX  = os.getenv("S3_SILVER_PREFIX", "silver/etf")
GOLD_PFX    = os.getenv("S3_GOLD_PREFIX", "gold/etf")
PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_DB       = os.getenv("POSTGRES_DB", "finance_gold")
PG_USER     = os.getenv("POSTGRES_USER", "admin")
PG_PASS     = os.getenv("POSTGRES_PASSWORD", "admin")
PG_TABLE    = "etf_kpis"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SilverToGold")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.hadoop.fs.s3a.access.key",  os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key",  os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint",    "s3.amazonaws.com")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def run(run_date: datetime | None = None):
    run_date = run_date or datetime.utcnow() - timedelta(days=1)
    spark = get_spark()

    # ── Read full Silver history ──────────────────────────────────────────────
    silver_glob = f"s3a://{BUCKET}/{SILVER_PFX}/**/*.parquet"
    logger.info(f"Reading Silver: {silver_glob}")

    df = spark.read.parquet(f"s3a://{BUCKET}/{SILVER_PFX}")
    df = df.filter(F.col("date") <= run_date.strftime("%Y-%m-%d"))

    # ── Window specs ──────────────────────────────────────────────────────────
    by_ticker_date = Window.partitionBy("ticker").orderBy("date")
    rolling_30     = by_ticker_date.rowsBetween(-29, 0)
    rolling_50     = by_ticker_date.rowsBetween(-49, 0)
    rolling_200    = by_ticker_date.rowsBetween(-199, 0)

    # ── Compute KPIs ──────────────────────────────────────────────────────────
    gold = (
        df
        # Daily return
        .withColumn("prev_close",
            F.lag("close", 1).over(by_ticker_date))
        .withColumn("daily_return_pct",
            F.round((F.col("close") - F.col("prev_close")) / F.col("prev_close") * 100, 4))

        # Monthly return (approx 30 trading days)
        .withColumn("close_30d_ago",
            F.lag("close", 30).over(by_ticker_date))
        .withColumn("monthly_return_pct",
            F.round((F.col("close") - F.col("close_30d_ago")) / F.col("close_30d_ago") * 100, 4))

        # Moving averages
        .withColumn("ma_50",  F.round(F.avg("close").over(rolling_50),  2))
        .withColumn("ma_200", F.round(F.avg("close").over(rolling_200), 2))

        # Volume KPIs
        .withColumn("avg_volume_30d", F.round(F.avg("volume").over(rolling_30), 0).cast("long"))
        .withColumn("volume_spike",
            F.when(F.col("volume") > F.col("avg_volume_30d") * 2, True).otherwise(False))

        # Intraday range
        .withColumn("high_low_range_pct",
            F.round((F.col("high") - F.col("low")) / F.col("low") * 100, 4))

        # MA signal: Golden Cross / Death Cross
        .withColumn("ma_signal",
            F.when(F.col("ma_50") > F.col("ma_200"), "bullish")
             .when(F.col("ma_50") < F.col("ma_200"), "bearish")
             .otherwise("neutral"))

        .select(
            "date", "ticker", "open", "high", "low", "close", "volume",
            "daily_return_pct", "monthly_return_pct",
            "ma_50", "ma_200", "ma_signal",
            "avg_volume_30d", "volume_spike",
            "high_low_range_pct",
        )
    )

    # ── Write Gold Parquet to S3 ──────────────────────────────────────────────
    gold_s3 = f"s3a://{BUCKET}/{GOLD_PFX}"
    (
        gold.write
        .partitionBy("ticker")
        .mode("overwrite")
        .parquet(gold_s3)
    )
    logger.info(f"Gold written → {gold_s3}")

    # ── Sink to Postgres for Superset ─────────────────────────────────────────
    jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
    pg_props = {
        "user":     PG_USER,
        "password": PG_PASS,
        "driver":   "org.postgresql.Driver",
    }
    (
        gold.write
        .jdbc(url=jdbc_url, table=PG_TABLE, mode="overwrite", properties=pg_props)
    )
    logger.info(f"Gold synced to Postgres table: {PG_TABLE}")
    spark.stop()


if __name__ == "__main__":
    run()
