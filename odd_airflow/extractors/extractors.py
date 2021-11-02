from typing import Type, Optional

from airflow.version import version as AIRFLOW_VERSION
from pkg_resources import parse_version

from odd_airflow.extractors.base import BaseExtractor
from odd_airflow.extractors.hive import HiveExtractor
from odd_airflow.extractors.mysql import MySqlExtractor
from odd_airflow.extractors.postgres import PostgresExtractor
from odd_airflow.extractors.redshift_s3 import RedshiftS3Extractor
from odd_airflow.extractors.snowflake import SnowflakeExtractor

if parse_version(AIRFLOW_VERSION) >= parse_version("2.0.0"):
    # Corrects path of import for Airflow versions below 1.10.11
    from airflow.utils.log.logging_mixin import LoggingMixin
elif parse_version(AIRFLOW_VERSION) >= parse_version("1.10.11"):
    from airflow import LoggingMixin
else:
    # Corrects path of import for Airflow versions below 1.10.11
    from airflow.utils.log.logging_mixin import LoggingMixin


_extractors = [
    PostgresExtractor,
    MySqlExtractor,
    RedshiftS3Extractor,
    SnowflakeExtractor,
    HiveExtractor
]


class Extractors(LoggingMixin):
    """
    This exposes implemented extractors, while hiding ones that require additional, unmet
    dependency. Patchers are a category of extractor that needs to hook up to operator's
    internals during DAG creation.
    """
    def __init__(self):
        self.extractors = {}

        for extractor in _extractors:
            for operator_class in extractor.get_operator_classnames():
                self.extractors[operator_class] = extractor

    def get_extractor_class(self, cls: Type) -> Optional[Type[BaseExtractor]]:
        name = cls.__name__
        if name in self.extractors:
            self.log.info(f"Extractor found for {name}: {self.extractors[name]}")
            return self.extractors[name]
        self.log.error(f"No extractor found for {name}. Using BaseExtractor")
        return BaseExtractor
