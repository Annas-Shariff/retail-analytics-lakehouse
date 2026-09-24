from airflow.decorators import dag
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from pendulum import datetime, duration


@dag(
    dag_id="retail_lakehouse_daily",
    schedule="0 18 * * *",
    start_date=datetime(2026, 7, 25, tz="Asia/Kolkata"),
    catchup=False,
    default_args={"retries": 2, "retry_delay": duration(minutes=5)},
    tags=["retail-lakehouse"],
)
def retail_lakehouse_daily():
    trigger_pipeline = DatabricksRunNowOperator(
        task_id="trigger_bronze_silver_gold",
        databricks_conn_id="databricks_default",
        job_id="456954407196275",
        job_parameters={
            "day_folder": "retail_{{ ds }}",
            "business_date": "{{ ds }}",
        },
    )


retail_lakehouse_daily()
