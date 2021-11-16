from airflow.models import BaseOperator
from odd_models.models import DataTransformer

from odd_airflow.utils import SqlParser


class SqlMixin:
    def get_query(self, operator):
        return operator.sql

    def extract_transformer_data(self, operator: BaseOperator):
        sql = ""
        try:
            sql = self.get_query(operator)
            sql_parser = SqlParser(self, operator, sql)
            inputs, outputs = sql_parser.get_response()
        except AttributeError:
            self.log.error(f"Unable to get inputs and outputs for operator {operator.__class__.__name__}")
            inputs, outputs = [], []
        return DataTransformer(
            source_code_url="source",
            sql=sql,
            inputs=inputs,
            outputs=outputs
        )
