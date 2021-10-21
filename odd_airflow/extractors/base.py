import attr
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any
from dataclasses import asdict

from pkg_resources import parse_version

from airflow.version import version as AIRFLOW_VERSION
from airflow.models import BaseOperator, TaskInstance, DAG, DagRun
from airflow.hooks.base_hook import BaseHook

from odd_airflow.utils import *
from odd_airflow.metadata import Metadata, EntityTypes

if parse_version(AIRFLOW_VERSION) >= parse_version("2.0.0"):
    # Corrects path of import for Airflow versions below 1.10.11
    from airflow.utils.log.logging_mixin import LoggingMixin
elif parse_version(AIRFLOW_VERSION) >= parse_version("1.10.11"):
    from airflow import LoggingMixin
else:
    # Corrects path of import for Airflow versions below 1.10.11
    from airflow.utils.log.logging_mixin import LoggingMixin

DAGRUN_STATE_TO_ODD_STATUS = {
    'success': 'SUCCESS',
    'running': 'RUNNING',
    'failed': 'FAILED'
}

class BaseExtractor(ABC, LoggingMixin):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        raise NotImplementedError()

    def validate(self):
        assert (self.operator.__class__.__name__ in self.get_operator_classnames())

    def get_connection(self, connection_id):
        return BaseHook.get_connection(connection_id)

    def extract_transformer(self, operator: BaseOperator) -> dict:
        dag = operator.dag

        metadata = Metadata(
            entity_type=EntityTypes.TRANSFORMER,
            operator=operator
        )
        data = {
            "oddrn": dag.oddrn_generator.get_task(dag.dag_id, operator.task_id),
            "name": get_transformer_name(dag.dag_id, operator.task_id),
            "description": operator.doc_md if hasattr(operator, 'doc_md') and operator.doc_md else dag.description,
            "owner": dag.owner,
            "type": "JOB",
            "data_transformer": self.extract_transformer_data(operator),
            "metadata": [asdict(metadata)]
        }
        return data

    def extract_transformer_run(self, dagrun: DagRun, operator: BaseOperator, task_instance: TaskInstance, reason: str='') -> dict:
        dag = operator.dag
        start_date = task_instance.start_date.isoformat() if task_instance.start_date else None
        end_date = task_instance.end_date.isoformat() if task_instance.end_date else None
        metadata = Metadata(
            entity_type=EntityTypes.TRANSFORMER_RUN,
            task_instance=task_instance
        )

        data = {
            "oddrn": dag.oddrn_generator.get_task_run(dag.dag_id, task_instance.task_id, dagrun.run_id),
            "name": get_transformer_run_name(dagrun.run_id, task_instance.task_id),
            "description": operator.doc_md if hasattr(operator, 'doc_md') and operator.doc_md else dag.description,
            "owner": dag.owner,
            "type": "JOB_RUN",
            "data_transformer_run": {
                "transformer_oddrn": dag.oddrn_generator.get_task(dag.dag_id, task_instance.task_id),
                "start_time": start_date,
                "end_time": end_date,
                "status": DAGRUN_STATE_TO_ODD_STATUS.get(dagrun.state, 'UNKNOWN'),
                "status_reason": reason
            },
            "metadata": [asdict(metadata)]
        }
        return data

