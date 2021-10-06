import requests
from functools import partial
import json

from typing import List, Callable

from pkg_resources import parse_version

from airflow.models import DAG
from airflow.version import version as AIRFLOW_VERSION
from airflow.utils.db import provide_session

from oddrn import Generator
from odd_airflow.settings import DATA_CATALOG_URL, AIRFLOW_UNIT_ID
from odd_airflow.utils import *
from odd_airflow.extractors.extractors import Extractors

# Corrects path of import for Airflow different versions
if parse_version(AIRFLOW_VERSION) >= parse_version("1.10.11"):
    from airflow import LoggingMixin
else:
    from airflow.utils.log.logging_mixin import LoggingMixin

extractor_mapper = Extractors()

def on_complete_callback(context, callback: Callable, *args, **kwargs):
    dag = context.get('dag', None)
    dag.extract_entities(**context)
    if callback:
        callback()

class DAG(DAG):
    def __init__(self, *args, **kwargs):
        kwargs['on_success_callback'] = partial(on_complete_callback, callback=kwargs.get('on_success_callback', None))
        kwargs['on_failure_callback'] = partial(on_complete_callback, callback=kwargs.get('on_failure_callback', None))
        self.oddrn_generator = Generator(data_source='airflow', host=kwargs.get('default_args', {}).get('unit_id', AIRFLOW_UNIT_ID))
        super().__init__(*args, **kwargs)

    @provide_session
    def create_dagrun(self, *args, **kwargs):
        dagrun = super().create_dagrun(*args, **kwargs)
        self.extract_entities(dag_run=dagrun)
        return dagrun

    @provide_session
    def extract_entities(self, *args, **kwargs):
        session = kwargs["session"]
        dagrun = kwargs.get("dag_run", self.get_last_dagrun())
        reason = kwargs.get("reason", None)
        data = []
        for task_id, operator in self.task_dict.items():
            extractor = extractor_mapper.get_extractor_class(operator.__class__)()
            task_instance = next(
                ti for ti in dagrun.get_task_instances(session=session)
                if ti.task_id == task_id
            )
            session.expunge(task_instance) # Extract object from session for later use
            data.extend([extractor.extract_transformer_run(dagrun=dagrun, operator=operator, task_instance=task_instance, reason=reason), extractor.extract_transformer(operator=operator)])
        self._send_data(data)

    def _send_data(self, data: List[dict]):
        request_data = {
            "data_source_oddrn": self.oddrn_generator.get_base(),
            "items": data
        }
        url = self.default_args.get("data_catalog_url", DATA_CATALOG_URL)
        try:
            r = requests.post(url, json=request_data)
            if r.status_code == 200:
                self.log.info(f"Data transfer success")
            else:
                self.log.error(f"Error on catalog request. Code: {r.status_code}, Message: {r.text}")
        except requests.exceptions.ConnectionError:
            self.log.error(f"Unable to connect to data catalog")
