# dags/fanout_seven_taskflow_two_ingests.py
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label
from include.DAG3_Rules import dag3_config

conf = dag3_config()

default_args = {"owner": "data-platform","retries": 1,"retry_delay": timedelta(minutes=5)}

with DAG(
        dag_id="DAG3_Crazy_CRM_Deduplication",
        description="A DAG to deduplicate CRM records across 2 Business Units",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        default_args=default_args,
        tags=["CRM","Dedup","Crazy","parallel","dummy"],
) as dag:

    # Visual anchors
    start = EmptyOperator(task_id="start")
    gate_checks = EmptyOperator(task_id="gate_checks")
    gate_post_check = EmptyOperator(task_id="gate_post_check")  # FIX: use this after fan-in
    end = EmptyOperator(task_id="end")

    # BU 1
    @task(task_id="ingest_BU1_CRM")
    def ingest_a():
        return {"source": "BU1_CRM", "version": "v1"}

    @task(task_id="prepare_BU1_CRM")
    def prepare_a(context: dict) -> list[str]:
        _ = context
        return list(conf.match_rules_BU1.keys())

    # BU 2
    @task(task_id="ingest_BU2_CRM")
    def ingest_b():
        return {"source": "BU2_CRM", "version": "v2"}

    @task(task_id="prepare_BU2_CRM")
    def prepare_b(context: dict) -> list[str]:
        _ = context
        return list(conf.match_rules_BU2.keys())

    # Post fan-in
    @task(task_id="aggregate")
    def aggregate(rule_ids_a: list[str], rule_ids_b: list[str]) -> dict:
        combined = sorted(set(rule_ids_a) | set(rule_ids_b), key=lambda x: int(x))
        return {"count": len(combined), "rules_seen": combined, "by_branch": {"BU1": rule_ids_a, "BU2": rule_ids_b}}

    @task(task_id="finalize")
    def finalize(summary: dict) -> str:
        return f"Processed {summary.get('count', 0)} unique rules from two branches."

    @task(task_id="upload_file_bq")
    def upload_file_bq(summary: dict) -> str:
        return f"Processed {summary.get('count', 0)} unique rules from two branches."

    @task(task_id="trigger_sub_dag")
    def trigger_sub_dag(summary: dict) -> str:
        return f"Processed {summary.get('count', 0)} unique rules from two branches."

    # Wiring
    start >> Label("kick off") >> gate_checks

    ctx_a = ingest_a()
    ctx_b = ingest_b()

    gate_checks >> Label("ingest CRM BU1") >> ctx_a
    gate_checks >> Label("ingest CRM BU2") >> ctx_b

    prepared_ids_CRM_BU1 = prepare_a(ctx_a)
    prepared_ids_CRM_BU2 = prepare_b(ctx_b)

    # 7-way fan-out group
    with TaskGroup(group_id="apply_matching_rules", tooltip="Parallel dummy rule applications") as matching_group:
        matching_start = EmptyOperator(task_id="matching_start")
        matching_end = EmptyOperator(task_id="matching_end")

        for rule_id in conf.match_rules_BU1:
            t = EmptyOperator(task_id=f"rule_{rule_id}")
            matching_start >> t >> matching_end

    # Both branches must be ready before the fan-out begins
    [prepared_ids_CRM_BU1, prepared_ids_CRM_BU2] >> Label("fan out to 7 dummy rules") >> matching_start

    # Collapse after fan-out → go to a NEW gate, not back to gate_checks
    matching_end >> Label("collapse / aggregate") >> gate_post_check

    # Ensure aggregation happens after both (a) prepared lists and (b) the fan-in gate
    summary = aggregate(prepared_ids_CRM_BU1, prepared_ids_CRM_BU2)
    gate_post_check >> summary

    # Finalize and subsequent tasks — CALL the TaskFlow tasks to create instances
    result = finalize(summary)
    uploaded = upload_file_bq(summary)
    triggered = trigger_sub_dag(summary)

    # Wire instances
    result >> [uploaded, triggered] >> Label("done") >> end
