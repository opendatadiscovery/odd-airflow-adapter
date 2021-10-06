from typing import List
from odd_airflow.extractors.base import BaseExtractor
from odd_airflow.extractors.sql_mixin import SqlMixin
from oddrn import Generator

class MySqlExtractor(BaseExtractor, SqlMixin):

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['MySqlOperator']

    def get_oddrn_list(self, task, tables):
        response = []
        connection = self.get_connection(task.mysql_conn_id)
        generator = Generator(data_source='mysql', host=f"{connection.host}:{connection.port}")
        database = task.database or connection.schema
        for table in tables:
            source = table.split(".")
            if len(source) > 1:
                # DB name included
                oddrn = generator.get_table(database, source[1])
            else:
                oddrn = generator.get_table(database, source[0])
            response.append(oddrn)
        return response