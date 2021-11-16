from typing import List

from airflow.operators.redshift_to_s3_operator import RedshiftToS3Transfer
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from odd_models.models import DataTransformer
from oddrn_generator import RedshiftGenerator

from odd_airflow.extractors.base import BaseExtractor


class RedshiftS3Extractor(BaseExtractor):

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['RedshiftToS3Transfer', 'S3ToRedshiftTransfer']

    def extract_transformer_data(self, operator: RedshiftToS3Transfer or S3ToRedshiftTransfer):
        return DataTransformer(
            source_code_url="source",
            inputs=[self.get_table_oddrn(operator)] if isinstance(operator, RedshiftToS3Transfer) else [],
            outputs=[self.get_table_oddrn(operator)] if isinstance(operator, S3ToRedshiftTransfer) else [],
        )

    def get_table_oddrn(self, operator) -> str:
        connection = self.get_connection(operator.redshift_conn_id)
        generator = RedshiftGenerator(
            host_settings=f"{connection.host}",
            databases=connection.schema, schemas=operator.schema, tables=operator.table
        )
        return generator.get_oddrn_by_path("tables")
