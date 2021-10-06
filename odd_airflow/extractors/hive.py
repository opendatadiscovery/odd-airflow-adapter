from typing import List
from odd_airflow.extractors.sql_mixin import SqlMixin
from odd_airflow.extractors.base import BaseExtractor
from odd_airflow.extractors.sql_mixin import SqlMixin
from oddrn import Generator

class HiveExtractor(BaseExtractor, SqlMixin):

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['HiveOperator']

    def get_query(self, operator):
        return operator.hql

    def get_oddrn_list(self, task, tables):
        response = []
        connection = self.get_connection(task.hive_cli_conn_id)
        generator = Generator(data_source='hive', host=f"{connection.host}:{connection.port}")
        database = connection.schema
        for table in tables:
            source = table.split(".")
            if len(source) > 1:
                # DB name included
                oddrn = generator.get_table(database, source[1])
            else:
                oddrn = generator.get_table(database, source[0])
            response.append(oddrn)
        return response