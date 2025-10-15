from __future__ import annotations

import os
from datetime import date, timedelta

import pendulum
import pandas as pd

from airflow import Dataset
from airflow.sdk import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.exceptions import AirflowException

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from include.DAG1_finance_sales_daily_sql import AGG_SQL_TEMPLATE
from include.DAG1_finance_sales_daily_config import finance_config

conf = finance_config()

# OpenLineage / Datasets (Astronomer observability)
# I have added some OpenLineage inlets & outlets here for Data Lineage as it is a critical business process likely audited
BQ_INLETS = [
    Dataset("bigquery://bigquery-public-data/thelook_ecommerce/order_items"),
    Dataset("bigquery://bigquery-public-data/thelook_ecommerce/products"),
    Dataset("bigquery://bigquery-public-data/thelook_ecommerce/users"),
    Dataset("bigquery://bigquery-public-data/thelook_ecommerce/distribution_centers"),
    Dataset("bigquery://bigquery-public-data/thelook_ecommerce/inventory_items"),
]
SNOWFLAKE_OUTLET = Dataset(f"snowflake://{conf.SNOWFLAKE_DATABASE}/{conf.SNOWFLAKE_SCHEMA}/{conf.SNOWFLAKE_TABLE}")


@dag(
    dag_id="DAG1_thelook_bq_to_snowflake_incremental",
    description="Incremental (order_date) from BigQuery (thelook_ecommerce) to Snowflake via GCS + COPY INTO + MERGE + validation. Creates a DAG with at least 4 tasks that are serially dependent.",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz=conf.TZ),
    catchup=False,
    tags=["Sequential","thelook", "bigquery", "snowflake", "gcs", "incremental", "observability", "validation"],
)
def thelook_bq_to_snowflake_incremental():
    logger = LoggingMixin().log

    @task(inlets=[SNOWFLAKE_OUTLET], outlets=[SNOWFLAKE_OUTLET], task_id="get_max_loaded_date")
    def get_max_loaded_date() -> dict:
        sf = SnowflakeHook(snowflake_conn_id=conf.SNOWFLAKE_CONN_ID)
        max_date = None
        with sf.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {conf.SNOWFLAKE_DATABASE}.{conf.SNOWFLAKE_SCHEMA}.{conf.SNOWFLAKE_TABLE} (
                        ORDER_DATE DATE,
                        DISTRIBUTION_CENTER_ID NUMBER,
                        DISTRIBUTION_CENTER STRING,
                        CATEGORY STRING,
                        DEPARTMENT STRING,
                        --UNITS NUMBER,
                        GROSS_SALES NUMBER
                        --AVG_UNIT_PRICE NUMBER
                    );
                """)
                cur.execute(f"SELECT MAX(ORDER_DATE) FROM {conf.SNOWFLAKE_DATABASE}.{conf.SNOWFLAKE_SCHEMA}.{conf.SNOWFLAKE_TABLE}")
                row = cur.fetchone()
                if row and row[0]:
                    max_date = row[0]  # datetime.date
        logger.info("Current max ORDER_DATE in Snowflake: %s", max_date)
        return {"max_order_date": str(max_date) if max_date else None}

    #This is the task I queued on the bigger worker queue.
    #That's because this is the most intensive task in the entire DAG as it saves results to a pd df and loads it to GCS
    @task(queue = "a10-worker-q",inlets=BQ_INLETS, outlets=[Dataset("gs://" + conf.GCS_BUCKET + "/" + conf.GCS_PREFIX)], task_id="extract_from_bigquery_to_gcs")
    def extract_from_bigquery_to_gcs(meta: dict) -> dict:
        tz_now = pendulum.now(conf.TZ).date()
        window_end_pend = tz_now  # exclusive (yesterday inclusive)

        max_loaded = meta.get("max_order_date")
        if max_loaded:
            start_pend = pendulum.parse(max_loaded).date() + timedelta(days=1)
        else:
            start_pend = date(2019, 1, 1)

        # Coerce pendulum.Date -> datetime.date for BigQuery client
        start = date.fromisoformat(str(start_pend))
        window_end = date.fromisoformat(str(window_end_pend))

        if start >= window_end:
            logger.info("No new data to extract. start=%s >= end=%s", start, window_end)
            return {
                "extracted_rows": 0,
                "start_date": str(start),
                "end_date_exclusive": str(window_end),
                "gcs_uri": None,
                "batch_prefix": None,
            }

        # Use BigQuery client directly via the hook
        bq_hook = BigQueryHook(gcp_conn_id=conf.GCP_CONN_ID, use_legacy_sql=False)
        client: bigquery.Client = bq_hook.get_client()

        job_config = bigquery.QueryJobConfig(
            use_legacy_sql=False,
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start),
                bigquery.ScalarQueryParameter("end_date", "DATE", window_end),
            ],
        )

        logger.info("Executing BQ aggregate for window [%s, %s)", start, window_end)
        job = client.query(AGG_SQL_TEMPLATE, job_config=job_config)

        # Use BQ Storage API if available for speed (falls back if not installed)
        df: pd.DataFrame = job.result().to_dataframe(create_bqstorage_client=True)

        row_count = len(df)
        logger.info("BQ aggregate rows: %s", row_count)

        if row_count == 0:
            return {
                "extracted_rows": 0,
                "start_date": str(start),
                "end_date_exclusive": str(window_end),
                "gcs_uri": None,
                "batch_prefix": None,
            }

        os.makedirs(conf.TMP_DIR, exist_ok=True)
        local_csv = os.path.join(conf.TMP_DIR, conf.GCS_FILENAME)
        df.to_csv(local_csv, index=False)

        batch_dt = str(window_end - timedelta(days=1))
        batch_prefix = f"{conf.GCS_PREFIX}/batch_dt={batch_dt}"
        object_name = f"{batch_prefix}/{conf.GCS_FILENAME}"

        gcs = GCSHook(gcp_conn_id=conf.GCS_CONN_ID)
        gcs.upload(bucket_name=conf.GCS_BUCKET, object_name=object_name, filename=local_csv)

        gcs_uri = f"gs://{conf.GCS_BUCKET}/{object_name}"
        logger.info("Uploaded batch to %s", gcs_uri)

        return {
            "extracted_rows": row_count,
            "start_date": str(start),
            "end_date_exclusive": str(window_end),
            "gcs_uri": gcs_uri,
            "batch_prefix": batch_prefix,
        }


    @task(inlets=[Dataset("gs://"+conf.GCS_BUCKET+"/"+conf.GCS_PREFIX)], outlets=[SNOWFLAKE_OUTLET], task_id="copy_merge_into_snowflake")
    def copy_merge_into_snowflake(meta: dict) -> dict:
        gcs_uri = meta.get("gcs_uri")
        batch_prefix = meta.get("batch_prefix")
        extracted_rows = meta.get("extracted_rows", 0)
        if not gcs_uri or extracted_rows == 0:
            return {"copied": 0, "merged_updated": 0, "merged_inserted": 0, "message": "No new data to load."}

        sf = SnowflakeHook(snowflake_conn_id=conf.SNOWFLAKE_CONN_ID)
        with sf.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                  CREATE OR REPLACE FILE FORMAT {conf.SNOWFLAKE_DATABASE}.{conf.SNOWFLAKE_SCHEMA}.{conf.SNOWFLAKE_FILE_FORMAT}
                  TYPE = CSV
                  PARSE_HEADER = TRUE
                  FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
                  TRIM_SPACE = TRUE
                  NULL_IF = ('', 'NULL')
                """)
                cur.execute(f"""
                    CREATE STAGE IF NOT EXISTS {conf.SNOWFLAKE_DATABASE}.{conf.SNOWFLAKE_SCHEMA}.{conf.SNOWFLAKE_STAGE}
                    URL = 'gcs://{conf.GCS_BUCKET}'
                    STORAGE_INTEGRATION = {conf.SNOWFLAKE_STORAGE_INTEGRATION}
                """)

                staging_table = f"{conf.SNOWFLAKE_DATABASE}.{conf.SNOWFLAKE_SCHEMA}.{conf.SNOWFLAKE_TABLE}_STAGING"
                cur.execute(f"""
                    CREATE OR REPLACE TEMP TABLE {staging_table} LIKE {conf.SNOWFLAKE_DATABASE}.{conf.SNOWFLAKE_SCHEMA}.{conf.SNOWFLAKE_TABLE}
                """)

                pattern = f"^{batch_prefix.replace('/', '\/')}\/{conf.GCS_FILENAME}$"
                copy_sql = f"""
                    COPY INTO {staging_table}
                    FROM @{conf.SNOWFLAKE_DATABASE}.{conf.SNOWFLAKE_SCHEMA}.{conf.SNOWFLAKE_STAGE}
                    FILE_FORMAT = (FORMAT_NAME={conf.SNOWFLAKE_DATABASE}.{conf.SNOWFLAKE_SCHEMA}.{conf.SNOWFLAKE_FILE_FORMAT})
                    PATTERN = '{pattern}'
                    ON_ERROR = 'ABORT_STATEMENT'
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                """
                cur.execute(copy_sql)

                target = f"{conf.SNOWFLAKE_DATABASE}.{conf.SNOWFLAKE_SCHEMA}.{conf.SNOWFLAKE_TABLE}"
                merge_sql = f"""
                    MERGE INTO {target} t
                    USING {staging_table} s
                    ON  t.ORDER_DATE = s.ORDER_DATE
                    AND t.DISTRIBUTION_CENTER_ID = s.DISTRIBUTION_CENTER_ID
                    AND t.CATEGORY = s.CATEGORY
                    AND t.DEPARTMENT = s.DEPARTMENT
                    WHEN MATCHED THEN UPDATE SET
                        t.DISTRIBUTION_CENTER   = s.DISTRIBUTION_CENTER,
                        t.GROSS_SALES            = s.GROSS_SALES
                    WHEN NOT MATCHED THEN INSERT (
                        ORDER_DATE, DISTRIBUTION_CENTER_ID, DISTRIBUTION_CENTER,
                        CATEGORY, DEPARTMENT, GROSS_SALES
                    ) VALUES (
                        s.ORDER_DATE, s.DISTRIBUTION_CENTER_ID, s.DISTRIBUTION_CENTER,
                        s.CATEGORY, s.DEPARTMENT, s.GROSS_SALES
                    );
                """
                cur.execute(merge_sql)
                cur.execute(f"SELECT COUNT(*) FROM {staging_table}")
                staged_rows = cur.fetchone()[0]

        return {
            "copied": int(staged_rows),
            "merged_updated": None,
            "merged_inserted": None,
            "batch_prefix": batch_prefix,
            "gcs_uri": gcs_uri,
            "extracted_rows": int(extracted_rows),
        }

    # --------------------------
    # Step 4 — Validation
    # --------------------------
    @task(inlets=[SNOWFLAKE_OUTLET], outlets=[SNOWFLAKE_OUTLET], task_id="validate_loaded_window")
    def validate_loaded_window(extract_meta: dict, load_meta: dict, tolerance: float = 0.0) -> dict:
        """
        Validates that Snowflake has the expected number of rows for the extracted date window.
        - Compares extracted_rows (from BQ) vs count(*) in FINANCE.SALES.DAILY_SALES for [start_date, end_date).
        - tolerance: fraction allowed difference (e.g., 0.01 = 1%).
        Raises AirflowException if mismatch exceeds tolerance.
        """
        start_date = extract_meta.get("start_date")
        end_date_excl = extract_meta.get("end_date_exclusive")
        expected = int(extract_meta.get("extracted_rows", 0))
        copied = int(load_meta.get("copied", 0))

        # If nothing extracted, just record and exit successfully
        if expected == 0:
            return {
                "validated": True,
                "reason": "No new data for window",
                "start_date": start_date,
                "end_date_exclusive": end_date_excl,
                "expected_rows": expected,
                "snowflake_rows_for_window": 0,
                "copied_from_gcs": copied,
            }

        sf = SnowflakeHook(snowflake_conn_id=conf.SNOWFLAKE_CONN_ID)
        with sf.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(*) 
                    FROM {conf.SNOWFLAKE_DATABASE}.{conf.SNOWFLAKE_SCHEMA}.{conf.SNOWFLAKE_TABLE}
                    WHERE ORDER_DATE >= TO_DATE(%s)
                      AND ORDER_DATE <  TO_DATE(%s)
                """, (start_date, end_date_excl))
                landed = cur.fetchone()[0]

        # Basic consistency: landed should equal expected (within tolerance)
        if expected == 0:
            diff_frac = 0.0
        else:
            diff_frac = abs(landed - expected) / expected

        if diff_frac > tolerance:
            raise AirflowException(
                f"Validation failed for window [{start_date}, {end_date_excl}): "
                f"expected {expected} rows, found {landed} in Snowflake (copied={copied}, tol={tolerance})."
            )

        return {
            "validated": True,
            "start_date": start_date,
            "end_date_exclusive": end_date_excl,
            "expected_rows": expected,
            "snowflake_rows_for_window": int(landed),
            "copied_from_gcs": copied,
            "tolerance": tolerance,
        }

    # Orchestration: 4 sequential steps
    max_meta = get_max_loaded_date()
    extract_meta = extract_from_bigquery_to_gcs(max_meta)
    load_meta = copy_merge_into_snowflake(extract_meta)
    validate_loaded_window(extract_meta, load_meta)


thelook_bq_to_snowflake_incremental()
