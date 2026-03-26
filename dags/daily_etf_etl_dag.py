"""
dags/daily_etf_etl_dag.py
--------------------------
Daily DAG:  Ingest → Bronze → (Spark) Silver → (Spark) Gold → Superset ready

Schedule: every weekday at 18:00 UTC (after US market close at 16:00 ET)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="daily_etf_etl",
    default_args=default_args,
    description="Daily ingest + Spark batch for S&P 500 sector ETFs",
    schedule_interval="0 18 * * 1-5",   # Mon–Fri at 18:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["finance", "etf", "spark", "batch"],
) as dag:

    # ── Task 1: Ingest raw data from yfinance → S3 Bronze ─────────────────────
    def ingest_task(**context):
        from ingestion.ingest import run
        run_date = context["logical_date"]
        run(run_date=run_date)

    ingest = PythonOperator(
        task_id="ingest_yfinance_to_bronze",
        python_callable=ingest_task,
        provide_context=True,
    )

    # ── Task 2: Spark job — Bronze → Silver (clean + validate) ────────────────
    bronze_to_silver = SparkSubmitOperator(
        task_id="spark_bronze_to_silver",
        application="spark_jobs/bronze_to_silver.py",
        conn_id="spark_default",
        name="bronze_to_silver_{{ ds }}",
        executor_cores=2,
        executor_memory="2g",
        driver_memory="1g",
        conf={
            "spark.hadoop.fs.s3a.impl":
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        env_vars={"RUN_DATE": "{{ ds }}"},
    )

    # ── Task 3: Spark job — Silver → Gold (compute KPIs) ─────────────────────
    silver_to_gold = SparkSubmitOperator(
        task_id="spark_silver_to_gold",
        application="spark_jobs/silver_to_gold.py",
        conn_id="spark_default",
        name="silver_to_gold_{{ ds }}",
        executor_cores=2,
        executor_memory="4g",
        driver_memory="2g",
        conf={
            "spark.hadoop.fs.s3a.impl":
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
        },
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.7.3"
        ),
        env_vars={"RUN_DATE": "{{ ds }}"},
    )

    # ── Task 4: Sentinel — pipeline complete ──────────────────────────────────
    done = EmptyOperator(task_id="pipeline_complete")

    # ── Dependencies ──────────────────────────────────────────────────────────
    ingest >> bronze_to_silver >> silver_to_gold >> done
