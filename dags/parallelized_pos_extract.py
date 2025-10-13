from __future__ import annotations

import io
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.param import Param
from airflow.exceptions import AirflowSkipException

# GCP
from google.cloud import bigquery
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

GCP_CONN_ID = "google_default"  # matches your Airflow connection ID


# Metrics (works on Astronomer)
try:
    from airflow.stats import Stats
except Exception:  # older Airflow fallback
    from airflow import stats as Stats  # type: ignore

LONDON = pendulum.timezone("Europe/London")

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
        dag_id="retail_media_thelook_extract",
        description="Extract thelook_ecommerce tables to GCS for third-party sharing",
        default_args=default_args,
        schedule="0 2 * * *",  # 02:00 London daily
        start_date=pendulum.datetime(2025, 1, 1, tz=LONDON),
        catchup=False,
        max_active_runs=1,
        tags=["retail", "bigquery", "gcs", "astronomer", "taskflow"],
        params={
            "gcs_bucket": Param("retail_media_extract", type="string"),
            "daily_window_days": Param(3, type="integer", minimum=1, maximum=7),
            "bq_location": Param("US", type="string"),
            "row_limit": Param(None, type=["null", "integer"]),
        },
        doc_md="""
**Retail Media Extract (thelook_ecommerce → GCS)**

- **Daily**: `order_items` → last N days (default 3), file `transactions/transaction_DDMMYYYY.csv` (append-only)
- **Monthly (1st)**: full refresh of `products`, `users`, `distribution_centers` → `{table}/{table}_DDMMYYYY.csv` (overwrite)
""",
) as dag:

    # ---------- Helpers ----------
    def _bq_client(location: str) -> bigquery.Client:
        hook = GoogleBaseHook(gcp_conn_id=GCP_CONN_ID)
        credentials, project_id = hook.get_credentials_and_project_id()
        # project_id is used for billing the query against your project
        return bigquery.Client(project=project_id, credentials=credentials, location=location)


    def _format_run_date(dt: pendulum.DateTime) -> str:
        return dt.in_timezone(LONDON).strftime("%d%m%Y")

    def _upload_csv_bytes(gcs: GCSHook, bucket: str, object_name: str, data: bytes, overwrite: bool) -> None:
        if not overwrite and gcs.exists(bucket, object_name):
            raise AirflowSkipException(f"Already exists: gs://{bucket}/{object_name}")
        gcs.upload(bucket_name=bucket, object_name=object_name, data=data, mime_type="text/csv", gzip=False)

    def _df_to_csv_bytes(df) -> bytes:
        import pandas as pd  # noqa: F401  (used at runtime)
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        return buf.getvalue().encode("utf-8")

    # ---------- Monthly gate ----------
    @task.short_circuit(task_id="is_first_of_month")
    def is_first_of_month() -> bool:
        ctx = get_current_context()
        logical_date = ctx["logical_date"].in_timezone(LONDON)
        is_first = logical_date.day == 1
        Stats.incr("retail_media.monthly_gate.first_of_month" if is_first else "retail_media.monthly_gate.other_day")
        return is_first

    # ---------- DAILY: order_items (rolling N-day) ----------
    @task(task_id="order_items_daily", retries=1)
    def extract_order_items_daily() -> dict:
        ctx = get_current_context()
        params = ctx["params"]
        logical_date = ctx["logical_date"].in_timezone(LONDON)

        start_ts = pendulum.now()
        Stats.incr("retail_media.order_items.attempt")

        run_date_str = _format_run_date(logical_date)
        window_days = int(params["daily_window_days"])
        bq_location = params["bq_location"]
        bucket = params["gcs_bucket"]
        row_limit = params["row_limit"]

        run_date = logical_date.date()
        start_date = (run_date.subtract(days=window_days - 1)).to_date_string()  # YYYY-MM-DD
        end_date_excl = (run_date.add(days=1)).to_date_string()

        sql = f"""
        SELECT *
        FROM `bigquery-public-data.thelook_ecommerce.order_items`
        WHERE DATE(created_at, 'Europe/London') >= DATE('{start_date}')
          AND DATE(created_at, 'Europe/London') <  DATE('{end_date_excl}')
        """
        if row_limit:
            sql += f"\nLIMIT {int(row_limit)}"

        client = _bq_client(bq_location)
        df = client.query(sql).to_dataframe(create_bqstorage_client=True)

        object_name = f"transactions/transaction_{run_date_str}.csv"
        gcs = GCSHook(gcp_conn_id=GCP_CONN_ID)
        _upload_csv_bytes(gcs, bucket, object_name, _df_to_csv_bytes(df), overwrite=False)

        elapsed_ms = int((pendulum.now() - start_ts).total_seconds() * 1000)
        Stats.timing("retail_media.order_items.runtime_ms", elapsed_ms)
        Stats.incr("retail_media.order_items.success")
        return {"rows": len(df), "gcs_uri": f"gs://{bucket}/{object_name}"}

    # ---------- MONTHLY: full extracts (overwrite) ----------
    def _monthly_full_extract_task(table: str, folder: str, task_id: str):
        @task(task_id=task_id, retries=1)
        def _inner() -> dict:
            ctx = get_current_context()
            params = ctx["params"]
            logical_date = ctx["logical_date"].in_timezone(LONDON)

            start_ts = pendulum.now()
            Stats.incr(f"retail_media.{table}.attempt")

            bq_location = params["bq_location"]
            bucket = params["gcs_bucket"]
            row_limit = params["row_limit"]
            run_date_str = _format_run_date(logical_date)

            sql = f"SELECT * FROM `bigquery-public-data.thelook_ecommerce.{table}`"
            if row_limit:
                sql += f"\nLIMIT {int(row_limit)}"

            client = _bq_client(bq_location)
            df = client.query(sql).to_dataframe(create_bqstorage_client=True)

            object_name = f"{folder}/{folder}_{run_date_str}.csv"
            gcs = GCSHook(gcp_conn_id=GCP_CONN_ID)
            _upload_csv_bytes(gcs, bucket, object_name, _df_to_csv_bytes(df), overwrite=True)

            elapsed_ms = int((pendulum.now() - start_ts).total_seconds() * 1000)
            Stats.timing(f"retail_media.{table}.runtime_ms", elapsed_ms)
            Stats.incr(f"retail_media.{table}.success")
            return {"rows": len(df), "gcs_uri": f"gs://{bucket}/{object_name}"}

        return _inner

    extract_products_monthly = _monthly_full_extract_task("products", "products", "products_monthly")
    extract_users_monthly = _monthly_full_extract_task("users", "users", "users_monthly")
    extract_dc_monthly = _monthly_full_extract_task("distribution_centers", "distribution_centers", "distribution_centers_monthly")

    @task(task_id="all_done", trigger_rule=TriggerRule.ALL_DONE)
    def all_done():
        Stats.incr("retail_media.run.completed")

    # ---------- Wiring (parallel by default) ----------
    daily = extract_order_items_daily()
    monthly_gate = is_first_of_month()
    monthly_products = extract_products_monthly()
    monthly_users = extract_users_monthly()
    monthly_dc = extract_dc_monthly()

    monthly_gate >> [monthly_products, monthly_users, monthly_dc]
    [daily, monthly_products, monthly_users, monthly_dc] >> all_done()
