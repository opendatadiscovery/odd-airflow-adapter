from typing import List

from oddrn_generator import MysqlGenerator

from odd_airflow.extractors.base import BaseExtractor
from odd_airflow.extractors.sql_mixin import SqlMixin


class MySqlExtractor(BaseExtractor, SqlMixin):

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['MySqlOperator']

    def get_oddrn_list(self, task, tables):
        response = []
        connection = self.get_connection(task.mysql_conn_id)
        generator = MysqlGenerator(
            host_settings=f"{connection.host}", databases=task.database or connection.schema
        )
        for table in tables:
            source = table.split(".")
            table_name = source[1] if len(source) > 1 else source[0]
            response.append(generator.get_oddrn_by_path("tables", table_name))
        return response
