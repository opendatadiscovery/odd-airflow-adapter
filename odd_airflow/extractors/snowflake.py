from typing import List

from oddrn_generator import SnowflakeGenerator

from odd_airflow.extractors.base import BaseExtractor
from odd_airflow.extractors.sql_mixin import SqlMixin


class SnowflakeExtractor(BaseExtractor, SqlMixin):

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SnowflakeOperator']

    def get_oddrn_list(self, task, tables):
        connection = self.get_connection(task.snowflake_conn_id)
        warehouse = task.warehouse or connection.warehouse
        database = task.database or connection.database
        schema = task.schema or connection.schema
        generator = SnowflakeGenerator(
            host_settings=f"{connection.account}.{connection.region}.snowflakecomputing.com",
            warehouses=warehouse, databases=database, schemas=schema
        )
        return [generator.get_oddrn_by_path("tables", table) for table in tables]
