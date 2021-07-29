## ODD Airflow adapter

ODD Airflow adapter is used for extracting data transformers and data transformers runs info and metadata from Apache Airflow (versions up to 1.10.15). This adapter is implemetation of push model (see more https://github.com/opendatadiscovery/opendatadiscovery-specification/blob/main/specification/specification.md#discovery-models). After installation, your Airflow will push new data transformer on DAG creation, and data transformer runs on every DAG run.
This service based on Python Flask and Connexion frameworks with APScheduler.

#### Data entities:
| Entity type | Entity source |
|:----------------:|:---------:|
|Data Transformer|DAG|
|Data Transformer run|DAG's runs|

For more information about data entities see https://github.com/opendatadiscovery/opendatadiscovery-specification/blob/main/specification/specification.md#data-model-specification

## Quickstart
#### Installation
```
pip3 install odd-airflow
```
#### Usage
```Python
from odd_airflow import DAG

default_args = {  
	# Your DAG arguments
	"data_catalog_host": "https://yourcatalog.url/ingestion",
	"cloud": {
		"type": "aws",
		"account": "account_id",
		"region": "region_id"
	}
}

dag = DAG(
    dag_id='your_example_dag',
    default_args=default_args,
    schedule_interval=None,
    tags=['example']
)

# Your tasks
```


## Advanced configuration
All configuration must be inside default_args parameter of DAG()
```Python
{
	"data_catalog_host": "https://yourcatalog.url/ingestion", # Data catalog ingestion API url
  	# You must specify one of - "cloud" or "hosts" params for ODDRN generation
	"cloud": {
		"type": "aws",   # Type of cloud (AWS by default)
		"account": "account_id", # Specify account_id
		"region": "region_id" # Specify region_id
	}
  "hosts": ["192.168.0.1", "192.168.0.2"] # Host or hosts of Airflow source
}
```

## Requirements
- Python 3.8
- Airflow  <= 1.10.15
