import os

from sqlalchemy import create_engine

warehouse = os.getenv("SF_WAREHOUSE")
database = os.getenv("SF_DATABASE")
role = os.getenv("SF_ROLE")
user = os.getenv("SF_USER")
account = os.getenv("SF_ACCOUNT")
password = os.getenv("SF_PASSWORD")

schema = "FLIGHT_DATA"

snowflake_uri = f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}"

sf_engine = create_engine(snowflake_uri)


def load_flights_to_snowflake(local_path, table_name):
    """Load local JSON file into Snowflake staging table."""
    cs = sf_engine.connect()

    try:
        cs.execute(
            f"""
            PUT file://{local_path} @FLIGHT_DATA_STAGE.{table_name}
        """
        )
        cs.execute(
            f"""
            COPY INTO FLIGHT_DATA.{table_name}
            FROM @FLIGHT_DATA_STAGE.{table_name}
            FILE_FORMAT=(TYPE=JSON STRIP_OUTER_ARRAY=TRUE)
        """
        )
    finally:
        cs.close()



