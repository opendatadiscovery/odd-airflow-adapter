from abc import ABC
from datetime import datetime
from typing import List

import pytz as pytz
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator, TaskInstance, DagRun
from airflow.version import version as AIRFLOW_VERSION
from odd_models.models import DataEntityType, DataEntity, DataTransformerRun, MetadataExtension, Status
from pkg_resources import parse_version

from odd_airflow.metadata import Metadata, EntityTypes
from odd_airflow.utils import *

if parse_version(AIRFLOW_VERSION) >= parse_version("2.0.0"):
    # Corrects path of import for Airflow versions below 1.10.11
    from airflow.utils.log.logging_mixin import LoggingMixin
elif parse_version(AIRFLOW_VERSION) >= parse_version("1.10.11"):
    from airflow import LoggingMixin
else:
    # Corrects path of import for Airflow versions below 1.10.11
    from airflow.utils.log.logging_mixin import LoggingMixin


DAGRUN_STATE_TO_ODD_STATUS = {
    'success': Status.SUCCESS,
    'running': Status.RUNNING,
    'failed': Status.FAILED,
}


class BaseExtractor(ABC, LoggingMixin):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        raise NotImplementedError()

    def validate(self):
        assert (self.operator.__class__.__name__ in self.get_operator_classnames())

    def get_connection(self, connection_id):
        return BaseHook.get_connection(connection_id)

    def extract_transformer(self, operator: BaseOperator) -> DataEntity:
        dag = operator.dag

        metadata = Metadata(
            entity_type=EntityTypes.TRANSFORMER,
            operator=operator
        )
        dag.oddrn_generator.set_oddrn_paths(dags=dag.dag_id, tasks=operator.task_id)
        return DataEntity(
            oddrn=dag.oddrn_generator.get_oddrn_by_path("tasks"),
            name=get_transformer_name(dag.dag_id, operator.task_id),
            description=operator.doc_md if hasattr(operator, 'doc_md') and operator.doc_md else dag.description,
            owner=dag.owner,
            type=DataEntityType.JOB,
            data_transformer=self.extract_transformer_data(operator),
            metadata=[MetadataExtension(schema_url=metadata.schema_url, metadata=metadata.metadata)]
        )

    def extract_transformer_run(self, dagrun: DagRun, operator: BaseOperator, task_instance: TaskInstance, reason: str='') -> DataEntity:
        dag = operator.dag
        start_date = task_instance.start_date.isoformat() if task_instance.start_date else None
        end_date = task_instance.end_date.isoformat() if task_instance.end_date else None
        metadata = Metadata(
            entity_type=EntityTypes.TRANSFORMER_RUN,
            task_instance=task_instance
        )
        dag.oddrn_generator.set_oddrn_paths(dags=dag.dag_id, tasks=operator.task_id, runs=dagrun.run_id)
        return DataEntity(
            oddrn=dag.oddrn_generator.get_oddrn_by_path("runs"),
            name=get_transformer_run_name(dagrun.run_id, task_instance.task_id),
            description=operator.doc_md if hasattr(operator, 'doc_md') and operator.doc_md else dag.description,
            owner=dag.owner,
            type=DataEntityType.JOB_RUN,
            data_transformer_run=DataTransformerRun(
                transformer_oddrn=dag.oddrn_generator.get_oddrn_by_path("tasks", task_instance.task_id),
                # todo: DataEntityType.start_time is required
                start_time=start_date or datetime.now(tz=pytz.utc).isoformat(),
                end_time=end_date,
                status=DAGRUN_STATE_TO_ODD_STATUS.get(dagrun.state, Status.UNKNOWN),
                status_reason=reason or ''
            ),
            metadata=[MetadataExtension(schema_url=metadata.schema_url, metadata=metadata.metadata)]
        )

