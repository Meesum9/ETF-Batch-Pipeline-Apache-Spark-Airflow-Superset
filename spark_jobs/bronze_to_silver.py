"""
spark_jobs/bronze_to_silver.py
-------------------------------
Reads yesterday's raw Bronze CSVs from S3, validates and cleans them,
then writes Parquet to the Silver layer.

Silver schema per partition:
  date | ticker | open | high | low | close | volume | ingest_ts
"""

import os
import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, DateType
from dotenv import load_dotenv

load_dotenv("../config/.env")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET      = os.getenv("S3_BUCKET")
BRONZE_PFX  = os.getenv("S3_BRONZE_PREFIX", "bronze/etf")
SILVER_PFX  = os.getenv("S3_SILVER_PREFIX", "silver/etf")


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BronzeToSilver")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.hadoop.fs.s3a.access.key",  os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key",  os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint",    "s3.amazonaws.com")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def bronze_path(run_date: datetime) -> str:
    y, m, d = run_date.strftime("%Y"), run_date.strftime("%m"), run_date.strftime("%d")
    return f"s3a://{BUCKET}/{BRONZE_PFX}/*/year={y}/month={m}/day={d}/data.csv"


def silver_path(run_date: datetime) -> str:
    y, m, d = run_date.strftime("%Y"), run_date.strftime("%m"), run_date.strftime("%d")
    return f"s3a://{BUCKET}/{SILVER_PFX}/year={y}/month={m}/day={d}"


def run(run_date: datetime | None = None):
    run_date = run_date or datetime.utcnow() - timedelta(days=1)
    spark = get_spark()

    src = bronze_path(run_date)
    logger.info(f"Reading Bronze: {src}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(src)
    )

    if df.rdd.isEmpty():
        logger.warning("No Bronze data found — skipping Silver write")
        spark.stop()
        return

    # ── Cast & rename ─────────────────────────────────────────────────────────
    df = (
        df
        .withColumn("date",      F.col("date").cast(DateType()))
        .withColumn("open",      F.col("open").cast(DoubleType()))
        .withColumn("high",      F.col("high").cast(DoubleType()))
        .withColumn("low",       F.col("low").cast(DoubleType()))
        .withColumn("close",     F.col("close").cast(DoubleType()))
        .withColumn("volume",    F.col("volume").cast(LongType()))
        .withColumn("ticker",    F.col("ticker").cast(StringType()))
        .withColumn("ingest_ts", F.col("ingest_ts").cast(StringType()))
    )

    # ── Data quality checks ───────────────────────────────────────────────────
    before = df.count()
    df = df.dropna(subset=["date", "ticker", "close", "volume"])
    df = df.filter(F.col("close") > 0)
    df = df.filter(F.col("volume") >= 0)
    df = df.dropDuplicates(["date", "ticker"])
    after = df.count()
    logger.info(f"DQ: {before} rows → {after} rows after cleaning")

    # ── Write Silver Parquet ──────────────────────────────────────────────────
    dst = silver_path(run_date)
    (
        df.select("date", "ticker", "open", "high", "low", "close", "volume", "ingest_ts")
        .write
        .mode("overwrite")
        .parquet(dst)
    )
    logger.info(f"Silver written → {dst}")
    spark.stop()


if __name__ == "__main__":
    run()
