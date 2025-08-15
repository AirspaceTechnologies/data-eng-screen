import os
import json
from google.cloud import storage
import snowflake.connector
from utils.logging_config import logger
from gcs.gcs_utils import download_from_gcs
from sqlalchemy import create_engine
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import snowflake.sqlalchemy  # noqa: F401
from snowflake.sqlalchemy import (
    URL as sfURL,
)  # avoid name collision with sqlalchemy.URL

warehouse = os.getenv("SF_WAREHOUSE")
database = os.getenv("SF_DATABASE")
role = os.getenv("SF_ROLE")
snowflake_etl_service = {
    "username": os.getenv("SF_ETL_SERVICE_USER"),
    "decryption_key": os.getenv("SF_ETL_SERVICE_KEY_PWD"),
    "account": os.getenv("SF_ACCOUNT"),
    "sf_key": os.getenv("SF_ETL_SERVICE_KEY").replace("\\n", "\n"),
}
schema = "PT"


def engine(
    config=snowflake_etl_service,
    database=database,
    schema=schema,
    role=role,
    warehouse=warehouse,
):
    sf_key = config["sf_key"]
    decryption_key = config["decryption_key"]
    # print(decryption_key)
    p_key = serialization.load_pem_private_key(
        data=sf_key.encode("utf-8"),
        password=decryption_key.encode("utf-8"),
        backend=default_backend(),
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return create_engine(
        sfURL(
            account=config["account"],
            user=config["username"],
            database=database,
            warehouse=warehouse,
            schema=schema,
            role=role,
        ),
        connect_args={
            "private_key": pkb,
        },
    )


def load_flights_to_snowflake(local_path, table_name):
    """Load local JSON file into Snowflake staging table."""
    sf_engine = engine()
    cs = sf_engine.connect()

    try:
        logger.info(f"Creating staging table {table_name}...")
        cs.execute(
            f"""
            CREATE OR REPLACE TABLE tech_screening.flight_data.{table_name} (
                raw VARIANT
            )
        """
        )

        logger.info("Loading JSON into staging table...")
        cs.execute(
            f"""
            PUT file://{local_path} @TECH_SCREENING.FLIGHT_DATA.{table_name}
        """
        )
        cs.execute(
            f"""
            COPY INTO TECH_SCREENING.FLIGHT_DATA.{table_name}
            FROM @TECH_SCREENING.FLIGHT_DATA.{table_name}
            FILE_FORMAT=(TYPE=JSON STRIP_OUTER_ARRAY=TRUE)
        """
        )
    finally:
        cs.close()


def gcs_to_snowflake():
    """Main GCS → Snowflake load."""
    local_file = "/tmp/raw_flights.json"
    download_from_gcs(os.getenv("GCS_BUCKET"), "flights_raw.json", local_file)
    load_flights_to_snowflake(local_file, "STG_FLIGHTS")
