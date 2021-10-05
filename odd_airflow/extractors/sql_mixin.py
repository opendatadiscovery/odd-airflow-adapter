from airflow.models import BaseOperator
from odd_airflow.utils import *

class SqlMixin:
    def extract_transformer_data(self, operator: BaseOperator):
        try:
            sql_parser = Parser(self, operator)
            inputs, outputs = sql_parser.get_response()
        except AttributeError:
            self.log.error(f"Unable to get inputs and outputs for operator {operator.__class__.__name__}")
            inputs, outputs = [], []
        return {
                "source_code_url": "source",
                "sql": operator.sql,
                "inputs": inputs,
                "outputs": outputs
            }
