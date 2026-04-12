import sys
sys.path.append("/mnt/d/pharma_pipeline")

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ingestion.generate_data import generate_sales_data,save_raw_data
from src.ingestion.bronze_ingestion import ingest_to_bronze
from src.transforms.silver_transform import transform_to_silver
from src.transforms.gold_transform import transform_to_gold
from src.ml.forecasting import run_forecasting
from config.spark_session import get_spark_session

default_args =  {
    "owner" : "krish",
    "retries" : 2,
    "retry_delay" : timedelta(minutes=5),
    "email_on_failure" : False,
}

dag = DAG(
    dag_id = "pharma_forecast_pipeline",
    default_args = default_args,
    description = "End to end pharma sales forecasting pipeline",
    schedule_interval="0 6 * * *",  # runs daily at 6am
    start_date=datetime(2025, 1, 1),
    catchup=False,                  
    tags=["pharma", "pyspark", "forecasting"]
)

def task_generate_data():
    df = generate_sales_data(num_records=10000)
    save_raw_data(df)
    print("Raw data generated")

def task_bronze():
    spark = get_spark_session("Bronze_Task")
    try:
        ingest_to_bronze(spark)
        print("Bronze complete")
    finally:
        spark.stop()


def task_silver():
    spark = get_spark_session("Silver_Task")
    try:
        transform_to_silver(spark)
        print("Silver complete")
    finally:
        spark.stop()

def task_gold():
    spark = get_spark_session("Gold_Task")
    try:
        transform_to_gold(spark)
        print("Gold complete")
    finally:
        spark.stop()


def task_forecast():
    run_forecasting()
    print("Forecasting complete")


# ── TASK DEFINITIONS ─────────────────────────────────────────
t1 = PythonOperator(
    task_id="generate_data",
    python_callable=task_generate_data,
    dag=dag
)

t2 = PythonOperator(
    task_id="bronze_ingestion",
    python_callable=task_bronze,
    dag=dag
)

t3 = PythonOperator(
    task_id="silver_transform",
    python_callable=task_silver,
    dag=dag
)

t4 = PythonOperator(
    task_id="gold_transform",
    python_callable=task_gold,
    dag=dag
)

t5 = PythonOperator(
    task_id="ml_forecasting",
    python_callable=task_forecast,
    dag=dag
)

# ── DEPENDENCIES ─────────────────────────────────────────────
t1 >> t2 >> t3 >> t4 >> t5