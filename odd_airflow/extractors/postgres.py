from typing import List
from odd_airflow.extractors.sql_mixin import SqlMixin
from odd_airflow.extractors.base import BaseExtractor
from odd_airflow.extractors.sql_mixin import SqlMixin
from oddrn import Generator

class PostgresExtractor(BaseExtractor, SqlMixin):

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['PostgresOperator']

    def get_oddrn_list(self, task, tables):
        response = []
        connection = self.get_connection(task.postgres_conn_id)
        generator = Generator(data_source='postgresql', host=f"{connection.host}:{connection.port}")
        database = task.database or connection.schema
        for table in tables:
            source = table.split(".")
            if len(source) > 1:
                # Schema included
                oddrn = generator.get_table(database, source[0], source[1])
            else:
                oddrn = generator.get_table(database, "public", source[0])
            response.append(oddrn)
        return response