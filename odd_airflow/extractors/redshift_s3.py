from odd_airflow.extractors.sql_mixin import SqlMixin
from typing import List

from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from airflow.operators.redshift_to_s3_operator import RedshiftToS3Transfer
from airflow.hooks.postgres_hook import PostgresHook

from odd_airflow.extractors.base import BaseExtractor
from oddrn import Generator

class RedshiftS3Extractor(BaseExtractor):

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['RedshiftToS3Transfer', 'S3ToRedshiftTransfer']

    def extract_transformer_data(self, operator: RedshiftToS3Transfer or S3ToRedshiftTransfer):
        return {
            "source_code_url": "source",
            "inputs": [self.get_table_oddrn(operator)] if isinstance(operator, RedshiftToS3Transfer) else [],
            "outputs": [self.get_table_oddrn(operator)] if isinstance(operator, S3ToRedshiftTransfer) else [],
        }

    def get_table_oddrn(self, operator) -> str:
        connection = self.get_connection(operator.redshift_conn_id)
        generator = Generator(data_source='redshift', host=f"{connection.host}:{connection.port}")
        return generator.get_table(database_name=connection.schema, schema_name=operator.schema, table_name=operator.table)