"""
Flight Data Pipeline DAG

Orchestrates the extraction, storage, and transformation of flight data from AviationStack API.

Tasks:
1. Extract flight data from AviationStack API
2. Upload raw data to GCS (date-partitioned)
3. Load data into Snowflake staging table (idempotent)
4. Run dbt transformations
5. Data quality checks

Schedule: Daily at 6 AM UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Import your custom modules
# from src import aviation_stack, gcs, snowflake


# TODO: Configure default args
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# TODO: Create the DAG
dag = DAG(
    "flight_data_pipeline",
    default_args=default_args,
    description="Daily flight data pipeline from AviationStack to Snowflake",
    schedule_interval="0 6 * * *",  # Daily at 6 AM UTC
    catchup=False,
    tags=["aviation", "daily", "etl"],
)


# TODO: Task 1 - Extract flight data from AviationStack API
def extract_flight_data(**context):
    """Extract flight data from AviationStack API."""
    # Implementation here
    pass


# TODO: Task 2 - Upload data to GCS
def upload_to_gcs_task(**context):
    """Upload extracted data to GCS with date partitioning."""
    # Implementation here
    pass


# TODO: Task 3 - Load data to Snowflake (idempotent)
def load_to_snowflake_task(**context):
    """Load data from GCS to Snowflake staging table."""
    # Implementation here
    pass


# TODO: Task 4 - Data quality check
def data_quality_check(**context):
    """Validate data quality after load."""
    # Implementation here
    pass


# TODO: Define task instances and dependencies
# Example:
# extract_task = PythonOperator(
#     task_id='extract_flight_data',
#     python_callable=extract_flight_data,
#     dag=dag,
# )
