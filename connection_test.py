from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
h = SnowflakeHook(snowflake_conn_id="snowflake_default")
print(h._get_conn_params())  # should include 'account': 'tb68904.europe-west2.gcp'
h.get_conn().cursor().execute("SELECT 1").fetchone()