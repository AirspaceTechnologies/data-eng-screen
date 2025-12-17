import os
from datetime import datetime

from sqlalchemy import create_engine

# Load Snowflake connection parameters from environment
warehouse = os.getenv("SF_WAREHOUSE")
database = os.getenv("SF_DATABASE")
role = os.getenv("SF_ROLE")
user = os.getenv("SF_USER")
account = os.getenv("SF_ACCOUNT")
password = os.getenv("SF_PASSWORD")

schema = "FLIGHT_DATA"

# Create global engine for reuse
snowflake_uri = f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}"
sf_engine = create_engine(snowflake_uri, pool_pre_ping=True)


def load_flights_to_snowflake(local_path, table_name, file_date=None):
    """
    Load local JSON file into Snowflake table.

    Args:
        local_path: Path to local JSON file
        table_name: Target Snowflake table name
        file_date: Optional date string for the data (YYYY-MM-DD)
    """
    conn = sf_engine.connect()

    if file_date is None:
        file_date = datetime.now().strftime("%Y-%m-%d")

    stage_path = f"@{schema}.flight_stage/{table_name}/{file_date}/"

    try:
        # Upload file to Snowflake internal stage
        put_query = f"PUT file://{local_path} {stage_path}"
        conn.execute(put_query)

        # Copy data from stage into table
        copy_query = f"""
            COPY INTO {schema}.{table_name}
            FROM {stage_path}
            FILE_FORMAT = (TYPE = 'JSON')
            PURGE = TRUE
        """
        result = conn.execute(copy_query)

        rows_loaded = result.rowcount
        print(f"Successfully loaded {rows_loaded} rows into {table_name}")

    finally:
        conn.close()


def truncate_and_load(local_path, table_name):
    """
    Truncate table and load fresh data.
    Use this for full refresh scenarios.
    """
    conn = sf_engine.connect()

    try:
        # Clear existing data
        truncate_query = f"TRUNCATE TABLE {schema}.{table_name}"
        conn.execute(truncate_query)

        # Load new data
        load_flights_to_snowflake(local_path, table_name)

        print(f"Truncated and reloaded {table_name}")

    except Exception as e:
        print(f"Error during truncate and load: {e}")
        raise
    finally:
        conn.close()


def create_flights_table_if_not_exists(table_name):
    """Create the flights table if it doesn't exist."""
    conn = sf_engine.connect()

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
        flight_date DATE,
        flight_status VARCHAR(50),
        departure_airport VARCHAR(100),
        arrival_airport VARCHAR(100),
        airline_name VARCHAR(200),
        load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    """

    conn.execute(create_table_sql)
    conn.close()



