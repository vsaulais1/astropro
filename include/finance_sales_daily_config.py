from airflow import Dataset

# --------------------------
# Config
# --------------------------
class finance_config:
    def __init__(self):
        self.GCP_CONN_ID = "google_default"
        self.GCS_CONN_ID = "google_default"  # or a dedicated GCS conn id
        self.GCS_BUCKET = "finance_sales"
        self.GCS_PREFIX = "thelook/daily_sales"    # no leading/trailing slash
        self.GCS_FILENAME = "thelook_daily_sales.csv"

        self.SNOWFLAKE_CONN_ID = "snowflake_default"
        self.SNOWFLAKE_DATABASE = "FINANCE"
        self.SNOWFLAKE_SCHEMA = "SALES"
        self.SNOWFLAKE_TABLE = "DAILY_SALES"
        self.SNOWFLAKE_STAGE = "GCS_THELOOK_STAGE"
        self.SNOWFLAKE_FILE_FORMAT = "GCS_THELOOK_CSV_FMT"
        self.SNOWFLAKE_STORAGE_INTEGRATION = "GCS_INT_THELOOK"  # replace with your integration

        self.TZ = "Europe/London"
        self.TMP_DIR = "/tmp"

        # OpenLineage / Datasets (Astronomer observability)
        self.BQ_INLETS = [
            Dataset("bigquery://bigquery-public-data/thelook_ecommerce/order_items"),
            Dataset("bigquery://bigquery-public-data/thelook_ecommerce/products"),
            Dataset("bigquery://bigquery-public-data/thelook_ecommerce/users"),
            Dataset("bigquery://bigquery-public-data/thelook_ecommerce/distribution_centers"),
            Dataset("bigquery://bigquery-public-data/thelook_ecommerce/inventory_items"),
        ]
        self.SNOWFLAKE_OUTLET = Dataset(f"snowflake://{self.SNOWFLAKE_DATABASE}/{self.SNOWFLAKE_SCHEMA}/{self.SNOWFLAKE_TABLE}")

