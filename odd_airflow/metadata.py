from enum import Enum
from typing import Optional
from dataclasses import dataclass, field

from airflow.models import BaseOperator, TaskInstance

from .settings import METADATA_SCHEMA_URL
from .utils import get_props

class EntityTypes(Enum):
    TRANSFORMER = "data_transformer"
    TRANSFORMER_RUN = "data_transformer_run"

@dataclass
class Metadata:
    metadata: dict = field(default_factory=dict)
    schema_url: str = METADATA_SCHEMA_URL

    def __init__(self, entity_type: EntityTypes, operator: Optional[BaseOperator] = None,
                 task_instance: Optional[TaskInstance] = None):
        if entity_type == EntityTypes.TRANSFORMER:
            if operator:
                self._get_transformer_metadata(operator)
            else:
                raise ValueError(f"for entity_type '{entity_type}' operator param is required")
        elif entity_type == EntityTypes.TRANSFORMER_RUN:
            if task_instance:
                self._get_transformer_run_metadata(task_instance)
            else:
                raise ValueError(f"for entity_type '{entity_type}' task_instance param is required")
        else:
            raise ValueError(f"entity_type must be one of: {[e.value for e in EntityTypes]}")

    def _get_transformer_metadata(self, operator: BaseOperator):
        exclude_list = [
            "log", "logger", "owner", "sql", "ui_color", "ui_fgcolor", "dag", "dag_id", "downstream_list",
            "upstream_list", "template_fields", "template_ext", "start_date", "end_date", "doc_md", "deps"
        ]
        self.metadata = get_props(operator, exclude_list)

    def _get_transformer_run_metadata(self, task_instance: TaskInstance):
        exclude_list = [
            "generate_command", "dag_id", "end_date", "start_date", "log", "logger",
            "metadata", "previous_ti", "previous_ti_success", "task"
        ]
        self.metadata = get_props(task_instance, exclude_list)
