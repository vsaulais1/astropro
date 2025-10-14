from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.sensors.date_time import DateTimeSensor
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

# DAG CONFIG

def on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis, *_, **__):
    # Keep it simple; Airflow UI will show SLA misses. This just logs a message.
    print(
        "[SLA MISS] DAG=%s Tasks=%s Blocking=%s Count=%s"
        % (dag.dag_id, task_list, blocking_task_list, len(slas))
    )

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "sla_miss_callback": on_sla_miss,  # global SLA handler
}

MAPPED_CLUSTER_SIZE = 6  # bump for even denser visuals

with DAG(
        dag_id="DAG3_crazy_dag",
        description="Create your own crazy DAG that results in the most complex looking Graph View representation of the DAG as possible. The more lines, dependencies, etc, the better! Feel free to have fun with this one.",
        default_args=DEFAULT_ARGS,
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=False,
        tags=["demo", "visual", "complex", "sensors", "sla", "branching"],
) as dag:

    # TIER 0: ENTRY + SENSOR GATES
    # Add a tiny wait (TimeDeltaSensor) and a run-date gate (DateTimeSensor) to
    # create a 'gatehouse' that always draws attention in the Graph.

    kickoff = EmptyOperator(task_id="kickoff")

    # This forces a short 'warm-up' period after the DAG run starts
    warmup = TimeDeltaSensor(
        task_id="warmup_wait",
        delta=timedelta(seconds=30),
        mode="poke",  # visible as sensor; use 'reschedule' if you prefer
        sla=timedelta(minutes=5),  # SLA to highlight the sensor if it drags
    )

    # Ensure we don’t start until the scheduled run time is reached
    run_gate = DateTimeSensor(
        task_id="run_time_gate",
        target_time="{{ data_interval_end }}",  # visually interesting templated edge
        mode="poke",
    )

    kickoff >> Label("enter") >> warmup >> Label("at scheduled boundary") >> run_gate

    # TIER 1: THREE-WAY FAN-OUT  A -> [B, C, D]

    B = EmptyOperator(task_id="B", sla=timedelta(minutes=10))
    C = EmptyOperator(task_id="C")
    D = EmptyOperator(task_id="D")

    run_gate >> Label("fan-out") >> [B, C, D]

    # TIER 2: INTERLINKED MIDDLE
    # B -> [E, F]; C -> [F, G]; D -> [G, H]

    E = EmptyOperator(task_id="E")
    F = EmptyOperator(task_id="F", sla=timedelta(minutes=15))
    G = EmptyOperator(task_id="G")
    H = EmptyOperator(task_id="H")

    B >> Label("B1") >> E
    B >> Label("B2") >> F
    C >> Label("C1") >> F
    C >> Label("C2") >> G
    D >> Label("D1") >> G
    D >> Label("D2") >> H

    # TIER 2.5: "NOISY" DYNAMIC CLUSTER seeded by C
    @task_group(group_id="mapped_cluster")
    def mapped_cluster():
        @task(task_id="prepare_cluster_input")
        def prepare():
            return list(range(MAPPED_CLUSTER_SIZE))

        @task(task_id="process")
        def process(item: int) -> str:
            return f"done_{item}"

        @task(task_id="consolidate")
        def consolidate(_results: list[str]) -> None:
            return None

        items = prepare()
        results = process.expand(item=items)
        consolidate(results)

    cluster = mapped_cluster()
    C >> Label("seed cluster") >> cluster

    # TIER 2.75: CONDITIONAL BRANCH (BranchPythonOperator)
    # Adds a visually appealing fork with a 'fast_path' vs 'full_path' detour.
    # Both converge later using a forgiving TriggerRule so the graph stays dense.

    def choose_path(**context):
        # Flip a coin by run_id parity purely for demo visuals
        run_id = str(context["run_id"])
        return "full_step_1" if (len(run_id) % 2 == 0) else "fast_path"

    branch = BranchPythonOperator(task_id="choose_path", python_callable=choose_path)

    fast_path = EmptyOperator(task_id="fast_path")
    # A mini detour chain to make the 'full' option look heavier
    full_step_1 = EmptyOperator(task_id="full_step_1")
    full_step_2 = EmptyOperator(task_id="full_step_2", sla=timedelta(minutes=12))
    #full_path = [full_step_1, full_step_2]

    # Branching edges
    [E, F, G, H] >> Label("branching zone") >> branch
    branch >> fast_path
    branch >> full_step_1 >> full_step_2

    # TIER 3: FAN-IN TO [I, J] + cluster joins J
    # Use a relaxed TriggerRule so either branch can proceed without dead-ends.

    I = EmptyOperator(task_id="I", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    J = EmptyOperator(task_id="J", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    # Both paths feed I and J; also pull in the cluster to J
    [fast_path, full_step_2] >> Label("converge") >> [I, J]
    cluster >> Label("cluster→J") >> J

    # TIER 4: FINAL JOIN → K

    K = EmptyOperator(
        task_id="K_finish",
        trigger_rule=TriggerRule.ALL_DONE,  # draw K even if something is skipped
        sla=timedelta(minutes=20),
    )

    I >> Label("finalize") >> K
    J >> Label("finalize") >> K

    # TIER 5: LATEST-ONLY FAN-OUT
    # LatestOnlyOperator keeps backfills quiet but draws eye-catching gates.

    latest_only = LatestOnlyOperator(task_id="latest_only_gate")

    publish_db = EmptyOperator(task_id="publish_db")
    publish_s3 = EmptyOperator(task_id="publish_s3")
    publish_dashboard = EmptyOperator(task_id="publish_dashboard")

    K >> latest_only >> [publish_db, publish_s3, publish_dashboard]

    # A final “always runs” epilogue so the graph gets an extra tail
    wrap_up = EmptyOperator(
        task_id="wrap_up",
        trigger_rule=TriggerRule.ALL_DONE,  # even if latest_only skips on backfills
    )
    [publish_db, publish_s3, publish_dashboard] >> wrap_up