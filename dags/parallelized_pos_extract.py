# dags/thelook_exports_hooks.py
from __future__ import annotations

import json
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.stats import Stats

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# ───────────────────────────────────────────────────────────────────────────────
# CONFIG (edit these)
# ───────────────────────────────────────────────────────────────────────────────
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID", default_var="astro-lab-se")
BQ_LOCATION    = Variable.get("BQ_LOCATION", default_var="US")  # "US" for the public dataset
BQ_CONN_ID     = Variable.get("BQ_CONN_ID", default_var="google_default")
GCS_CONN_ID    = Variable.get("GCS_CONN_ID", default_var="google_default")

GCS_BUCKET     = Variable.get("GCS_EXPORT_BUCKET", default_var="retail_media_extract")
GCS_PREFIX     = Variable.get("GCS_EXPORT_PREFIX", default_var="retailer/thelook")
BQ_DATASET     = "bigquery-public-data.thelook_ecommerce"


EXPORT_FORMAT  = "PARQUET"      # or "CSV", "JSON" (Parquet recommended)
MAX_FILE_CEIL  = 1_073_741_824  # 1 GiB sanity ceiling per part (for validation metrics)

SCHEDULE       = "@daily"
START_DATE     = pendulum.datetime(2025, 1, 1, tz="UTC")

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ───────────────────────────────────────────────────────────────────────────────
# Helpers (Hooks-based)
# ───────────────────────────────────────────────────────────────────────────────
def _bq() -> BigQueryHook:
    return BigQueryHook(
        gcp_conn_id=BQ_CONN_ID,
        use_legacy_sql=False,
        location=BQ_LOCATION
    )

def _gcs() -> GCSHook:
    return GCSHook(gcp_conn_id=GCS_CONN_ID)

def _export_sql_to_gcs(sql: str, uri_prefix: str) -> dict:
    """
    Use BigQuery EXPORT DATA ... AS SELECT via BigQueryHook.insert_job().
    """
    uri = f"gs://{GCS_BUCKET}/{uri_prefix}/*.parquet" if EXPORT_FORMAT == "PARQUET" else f"gs://{GCS_BUCKET}/{uri_prefix}/*"
    export_stmt = f"""
    EXPORT DATA OPTIONS(
      uri='{uri}',
      format='{EXPORT_FORMAT}',
      overwrite=true  
    ) AS
    {sql}
    """
    bq = _bq()
    job = bq.insert_job(
        configuration={
            "query": {
                "query": export_stmt,
                "useLegacySql": False,
                "location": BQ_LOCATION,
            }
        }
    )
    bq.wait_for_job(job_id=job["jobReference"]["jobId"])
    # total bytes processed isn’t returned directly here; rely on metrics/validation
    return {
        "job_id": job["jobReference"]["jobId"],
        "destination_uri": uri,
        "state": "DONE",
    }

def _table_row_count(table: str, where: str | None = None) -> int:
    bq = _bq()
    sql = f"SELECT COUNT(*) AS c FROM `{table}`"
    if where:
        sql += f" WHERE {where}"
    row = bq.get_first(sql)
    return int(row[0]) if row else 0

def _list_and_size(prefix: str) -> dict:
    """
    Use GCSHook to list object names; get sizes via hook.get_size().
    """
    gcs = _gcs()
    objects = gcs.list(bucket_name=GCS_BUCKET, prefix=prefix)
    total_bytes = 0
    for obj in objects:
        try:
            total_bytes += gcs.get_size(bucket_name=GCS_BUCKET, object_name=obj) or 0
        except Exception:
            # If a transient error occurs on size fetch, skip that object but continue
            continue
    return {
        "prefix": prefix,
        "file_count": len(objects),
        "total_bytes": total_bytes,
        "examples": objects[:5],
    }

# ───────────────────────────────────────────────────────────────────────────────
# DAG
# ───────────────────────────────────────────────────────────────────────────────
with DAG(
        dag_id="thelook_extract_to_gcs_hooks",
        description="Hooks-based extract of thelook_ecommerce to GCS (daily + monthly, parallelized)",
        default_args=default_args,
        start_date=START_DATE,
        schedule=SCHEDULE,
        catchup=False,
        max_active_runs=1,
        tags=["thelook", "gcs", "bq", "astronomer", "exports", "hooks"],
) as dag:

    # ------------------------ Control & gating ------------------------
    @task(sla=timedelta(minutes=5))
    def is_month_start(execution_date: str) -> bool:
        dt = pendulum.parse(execution_date)
        is_first = dt.day == 1
        if not is_first:
            print(f"Monthly full-load will skip: {dt.to_date_string()}")
        return is_first

    @task
    def guard_monthly(should_run: bool) -> None:
        if not should_run:
            raise AirflowSkipException("Not the first of the month – skipping monthly full extracts.")

    # --------------------- DAILY: order_items (3-day window) --------------------
    @task_group(group_id="daily_order_items")
    def daily_order_items_group():
        @task(sla=timedelta(minutes=15))
        def export_order_items(logical_date: str, data_interval_end: str) -> dict:
            dt_end = pendulum.parse(data_interval_end)          # end-exclusive
            start_date = (dt_end - timedelta(days=3)).date()     # inclusive
            end_date = dt_end.date()                             # exclusive in WHERE

            where = f"DATE(created_at) >= DATE('{start_date}') AND DATE(created_at) < DATE('{end_date}')"
            target_prefix = (
                f"{GCS_PREFIX}/daily/order_items/"
                f"run_date={pendulum.parse(logical_date).to_date_string()}/"
                f"window_start={start_date}/window_end={end_date}"
            )
            sql = f"SELECT * FROM `{BQ_DATASET}.order_items` WHERE {where}"
            summary = _export_sql_to_gcs(sql, target_prefix)

            # Observability
            row_count = _table_row_count(f"{BQ_DATASET}.order_items", where=where)
            Stats.gauge("thelook.daily.order_items.rows", row_count)
            print("order_items export summary:", json.dumps({**summary, "row_count": row_count}, indent=2))
            return {"prefix": target_prefix, **summary, "row_count": row_count}

        @task(trigger_rule=TriggerRule.ALL_DONE)
        def validate_order_items(prefix_summary: dict):
            prefix = prefix_summary["prefix"]
            listing = _list_and_size(prefix)
            if listing["file_count"] == 0:
                Stats.incr("thelook.daily.order_items.validation_empty")
                raise ValueError(f"No files exported under gs://{GCS_BUCKET}/{prefix}")
            if listing["total_bytes"] > 50 * MAX_FILE_CEIL:
                Stats.incr("thelook.daily.order_items.validation_bytes_above_ceiling")
            Stats.gauge("thelook.daily.order_items.total_bytes", listing["total_bytes"])
            Stats.gauge("thelook.daily.order_items.files", listing["file_count"])
            print("order_items validation:", json.dumps(listing, indent=2))

        exported = export_order_items(
            logical_date="{{ logical_date }}",
            data_interval_end="{{ data_interval_end }}",
        )
        validate_order_items(exported)

    # -------------------- MONTHLY: full extracts (3 tables) ---------------------
    @task_group(group_id="monthly_full")
    def monthly_full_group():
        def _monthly_export_task(table: str):
            safe = table.replace(".", "_")

            @task(sla=timedelta(minutes=30))
            def _export(logical_date: str) -> dict:
                run_day = pendulum.parse(logical_date).to_date_string()
                run_month = run_day[:7]  # YYYY-MM
                target_prefix = f"{GCS_PREFIX}/monthly/{safe}/run_month={run_month}"
                sql = f"SELECT * FROM `{table}`"
                summary = _export_sql_to_gcs(sql, target_prefix)
                row_count = _table_row_count(table)
                Stats.gauge(f"thelook.monthly.{table.split('.')[-1]}.rows", row_count)
                print(f"{table} export summary:", json.dumps({**summary, "row_count": row_count}, indent=2))
                return {"prefix": target_prefix, **summary, "row_count": row_count}

            @task(trigger_rule=TriggerRule.ALL_DONE)
            def _validate(prefix_summary: dict):
                prefix = prefix_summary["prefix"]
                listing = _list_and_size(prefix)
                if listing["file_count"] == 0:
                    Stats.incr(f"thelook.monthly.{table.split('.')[-1]}.validation_empty")
                    raise ValueError(f"No files exported under gs://{GCS_BUCKET}/{prefix}")
                Stats.gauge(f"thelook.monthly.{table.split('.')[-1]}.total_bytes", listing["total_bytes"])
                Stats.gauge(f"thelook.monthly.{table.split('.')[-1]}.files", listing["file_count"])
                print(f"{table} validation:", json.dumps(listing, indent=2))

            v = _export(logical_date="{{ logical_date }}")
            _validate(v)

        _monthly_export_task(f"{BQ_DATASET}.products")
        _monthly_export_task(f"{BQ_DATASET}.users")
        _monthly_export_task(f"{BQ_DATASET}.distribution_centers")

    # ------------------------------ Wiring --------------------------------------
    daily = daily_order_items_group()
    monthly_gate = guard_monthly(is_month_start("{{ logical_date }}"))
    monthly = monthly_full_group()

    monthly_gate >> monthly
    # Daily path runs independently to allow parallelism with monthly on day-1
