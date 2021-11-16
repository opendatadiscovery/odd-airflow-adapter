from typing import List

from oddrn_generator import HiveGenerator

from odd_airflow.extractors.base import BaseExtractor
from odd_airflow.extractors.sql_mixin import SqlMixin


class HiveExtractor(BaseExtractor, SqlMixin):

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['HiveOperator']

    def get_query(self, operator):
        return operator.hql

    def get_oddrn_list(self, task, tables):
        response = []
        connection = self.get_connection(task.hive_cli_conn_id)
        generator = HiveGenerator(
            host_settings=f"{connection.host}", databases=connection.schema
        )
        for table in tables:
            source = table.split(".")
            table_name = source[1] if len(source) > 1 else source[0]
            response.append(generator.get_oddrn_by_path("tables", table_name))
        return response
