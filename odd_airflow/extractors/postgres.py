from typing import List

from oddrn_generator import PostgresqlGenerator

from odd_airflow.extractors.base import BaseExtractor
from odd_airflow.extractors.sql_mixin import SqlMixin


class PostgresExtractor(BaseExtractor, SqlMixin):

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['PostgresOperator']

    def get_oddrn_list(self, task, tables):
        response = []
        connection = self.get_connection(task.postgres_conn_id)
        generator = PostgresqlGenerator(
            host_settings=f"{connection.host}:{connection.port}", databases=task.database or connection.schema
        )
        for table in tables:
            source = table.split(".")
            schema, table = (source[0], source[1]) if len(source) > 1 else ("public", source[0])
            generator.set_oddrn_paths(schemas=schema, tables=table)
            response.append(generator.get_oddrn_by_path("tables"))
        return response
