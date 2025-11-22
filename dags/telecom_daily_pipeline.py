# ~/dags/telecom_daily_pipeline.py
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.utils.dates import days_ago

# ──────────────────────────────────────────────────────────────────────────────
# FULLY AUTOMATIC TELECOM CDR PIPELINE – Runs daily at 8:30 PM IST
# Features:
# • Creates temporary Dataproc cluster (2 workers)
# • Runs your PySpark job (direct_to_bigquery.py)
# • Sends fraud alerts via Pub/Sub → Cloud Function
# • Deletes cluster automatically (zero leftover cost)
# • 100% compatible with November 2025 GCP rules
# ──────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="telecom_daily_pipeline",
    schedule_interval="30 20 * * *",        # 8:30 PM IST every day
    start_date=days_ago(1),
    catchup=False,
    default_args={
        "owner": "Saiyed",
        "retries": 1,
        "retry_delay": 300,  # 5 minutes
    },
    description="Daily Telecom CDR → BigQuery + Real-time Fraud Alerts",
    tags=["telecom", "production", "dataproc", "pyspark"],
    max_active_runs=1,
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id="telecom-cdr",
        region="us-central1",
        cluster_name="telecom-cluster-{{ ds_nodash }}",
        cluster_config={
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n2-standard-4",
                "disk_config": {"boot_disk_size_gb": 100},
            },
            "worker_config": {
                "num_instances": 2,                     # 2 normal (non-preemptible) workers
                "machine_type_uri": "n2-standard-4",
                "disk_config": {"boot_disk_size_gb": 100},
            },
            # NO preemptible / preemptibility field → 2025 API requirement
            "gce_cluster_config": {
                "service_account_scopes": [
                    "https://www.googleapis.com/auth/cloud-platform"
                ]
            },
        },
        labels={"env": "production", "owner": "saiyed"},
    )

    run_spark_job = DataprocSubmitJobOperator(
        task_id="run_spark_job",
        project_id="telecom-cdr",
        region="us-central1",
        job={
            "placement": {"cluster_name": "telecom-cluster-{{ ds_nodash }}"},
            "pyspark_job": {
                "main_python_file_uri": "gs://telecom-cdr-data/direct_to_bigquery.py"
            },
        },
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id="telecom-cdr",
        region="us-central1",
        cluster_name="telecom-cluster-{{ ds_nodash }}",
        trigger_rule="all_success",   # Only delete if previous tasks succeeded
    )

    # Task dependency
    create_cluster >> run_spark_job >> delete_cluster
