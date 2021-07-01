from oddrn import Generator

from .. import settings

airflow_generator = Generator(data_source=settings.AIRFLOW_SOURCE, cloud=settings.CLOUD)


def get_datasource_oddrn() -> str:
    return airflow_generator.get_base()

def get_transformer_oddrn(dag_id: str, task_id: str) -> str:
    return airflow_generator.get_task(dag_id, task_id)

def get_transformer_run_oddrn(dag_id: str, task_id: str, run_id: str) -> str:
    return airflow_generator.get_task_run(dag_id, task_id, run_id)
