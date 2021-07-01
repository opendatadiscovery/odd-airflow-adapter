import os

AIRFLOW_SOURCE = "airflow"
POSTGRES_SOURCE = "postgresql"

METADATA_SCHEMA_URL = "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/extensions/airflow.json#/definitions/AirflowDataTransformerRunExtension"

CATALOG_URL = os.getenv("CATALOG_URL", None)

CLOUD_TYPE = os.getenv("CLOUD_TYPE", "aws")
CLOUD_REGION = os.getenv("CLOUD_REGION", "region_id")
CLOUD_ACCOUNT = os.getenv("CLOUD_ACCOUNT", "account_id")

CLOUD = {
    "type": CLOUD_TYPE,
    "region": CLOUD_REGION,
    "account": CLOUD_ACCOUNT
}