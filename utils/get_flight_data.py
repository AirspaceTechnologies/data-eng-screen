import os
import json
import requests
from gcs.gcs_utils import upload_to_gcs
from utils.logging_config import logger


def fetch_flight_data():
