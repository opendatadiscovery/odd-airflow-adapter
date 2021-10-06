import os


METADATA_SCHEMA_URL = "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/extensions/airflow.json#/definitions/AirflowDataTransformerRunExtension"

DATA_CATALOG_URL = os.getenv("DATA_CATALOG_URL", None)

AIRFLOW_UNIT_ID = os.getenv("AIRFLOW_UNIT_ID", None)
