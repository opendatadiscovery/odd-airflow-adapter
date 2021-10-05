from odd_airflow.extractors.sql_mixin import SqlMixin
from typing import List
from odd_airflow.extractors.base import BaseExtractor
from odd_airflow.extractors.sql_mixin import SqlMixin
from oddrn import Generator

from .. import settings

generator = Generator(data_source=settings.SNOWFLAKE_SOURCE)

class SnowflakeExtractor(BaseExtractor, SqlMixin):

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SnowflakeOperator']

    def get_oddrn_list(self, task, tables):
        response = []
        connection = self.get_connection(task.snowflake_conn_id)
        warehouse = task.warehouse or connection.warehouse
        database = task.database or connection.database
        schema = task.schema or connection.schema
        generator = Generator(data_source=settings.POSTGRES_SOURCE, host=f"{connection.account}.{connection.region}.snowflakecomputing.com")
        for table in tables:
            oddrn = generator.get_table(warehouse_name=warehouse, database_name=database, schema_name=schema, table_name=table)
            response.append(oddrn)
        return response