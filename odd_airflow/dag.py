import requests

from dataclasses import asdict
from typing import List

from pkg_resources import parse_version

from airflow.models import BaseOperator, TaskInstance, DAG, DagRun
from airflow.version import version as AIRFLOW_VERSION
from airflow.utils.db import provide_session

from .metadata import Metadata, EntityTypes
from .settings import CATALOG_URL
from .utils import *
from .oddrn.airflow import get_transformer_oddrn, get_transformer_run_oddrn, get_datasource_oddrn

# Corrects path of import for Airflow different versions
if parse_version(AIRFLOW_VERSION) >= parse_version("1.10.11"):
    from airflow import LoggingMixin
else:
    from airflow.utils.log.logging_mixin import LoggingMixin


class DAG(DAG, LoggingMixin):
    @provide_session
    def create_dagrun(self, *args, **kwargs):
        session = kwargs["session"]
        dagrun = super(DAG, self).create_dagrun(*args, **kwargs)
        data = []
        for task_id, operator in self.task_dict.items():
            if hasattr(operator, "sql"):
                task_instance = next(
                    ti for ti in dagrun.get_task_instances(session=session)
                    if ti.task_id == task_id
                )
                session.expunge(task_instance) # Extract object from session for later use
                data.extend([self._add_run(dagrun, operator, task_instance), self._add_transformer(operator)])
        self._send_data(data)
        return dagrun

    @provide_session
    def handle_callback(self, *args, **kwargs):
        session = kwargs["session"]
        try:
            dagrun = args[0]
            transformer_runs = []
            for task_id, operator in self.task_dict.items():
                task_instance = next(
                    ti for ti in dagrun.get_task_instances(session=session)
                    if ti.task_id == task_id
                )
                session.expunge(task_instance)  # Extract object from session for later use
                transformer_runs.append(self._add_run(dagrun, operator, task_instance))
            self._send_data(transformer_runs)
        except Exception as e:
            self.log.error(
                f'Failed to record dagrun callback: {e} '
                f'dag_id={self.dag_id}',
                exc_info=True
            )

        return super().handle_callback(*args)

    def _send_data(self, data: List[dict]):
        request_data = {
            "data_source_oddrn": get_datasource_oddrn(),
            "items": data
        }
        url = self.default_args.get("data_catalog_host", CATALOG_URL)
        r = requests.post(url, json=request_data)
        if r.status_code == 200:
            self.log.info(f"Data transfer success")
        else:
            self.log.error(f"Error on catalog request. Code: {r.status_code}, Message: {r.text}")

    def _add_transformer(self, operator: BaseOperator) -> dict:
        dag = operator.dag
        sql_parser = Parser(operator)
        inputs, outputs = sql_parser.get_response()
        metadata = Metadata(
            entity_type=EntityTypes.TRANSFORMER,
            operator=operator
        )
        data = {
            "oddrn": get_transformer_oddrn(dag.dag_id, operator.task_id),
            "name": get_transformer_name(dag.dag_id, operator.task_id),
            "description": operator.doc_md if operator.doc_md else dag.description,
            "owner": dag.owner,
            "type": "JOB",
            "data_transformer": {
                "source_code_url": "source",
                "sql": operator.sql,
                "inputs": inputs,
                "outputs": outputs
            },
            "metadata": [asdict(metadata)]
        }
        return data

    def _add_run(self, dagrun: DagRun, operator: BaseOperator, task_instance: TaskInstance) -> dict:
        dag = dagrun.dag
        start_date = task_instance.start_date.isoformat() if task_instance.start_date else None
        end_date = task_instance.end_date.isoformat() if task_instance.end_date else None
        metadata = Metadata(
            entity_type=EntityTypes.TRANSFORMER_RUN,
            task_instance=task_instance
        )

        data = {
            "oddrn": get_transformer_run_oddrn(dag.dag_id, task_instance.task_id, dagrun.run_id),
            "name": get_transformer_run_name(dagrun.run_id, task_instance.task_id),
            "description": operator.doc_md if operator.doc_md else dag.description,
            "owner": dag.owner,
            "type": "JOB_RUN",
            "data_transformer_run": {
                "transformer_oddrn": get_transformer_oddrn(dag.dag_id, task_instance.task_id),
                "start_time": start_date,
                "end_time": end_date,
                "status": "OTHER", # map status,
                "status_reason": "" # get status reason?
            },
            "metadata": [asdict(metadata)]
        }
        return data

