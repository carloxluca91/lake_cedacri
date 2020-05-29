import configparser

from abc import ABC
from pyspark.sql import DataFrame, SparkSession


class AbstractBancllLoader(ABC):

    def __init__(self, job_properties: configparser.ConfigParser):
        import logging

        self.__logger = logging.getLogger(__name__)
        self.__job_properties: configparser.ConfigParser = job_properties
        self.__spark_session: SparkSession = SparkSession \
            .builder \
            .getOrCreate()

        self.__mapping_specification_df: DataFrame = self._load_mapping_specification()
        self.__logger.info(f"Successfully loaded mapping specification DataFrame")

    def _load_mapping_specification(self):

        mapping_specification_file_path: str = self.__job_properties["paths"]["specification_file_path"]
        mapping_specification_separator: str = self.__job_properties["spark"]["specification_file_delimiter"]
        mapping_specification_header: str = self.__job_properties["spark"]["specification_file_header"]
        mapping_specification_header_option: bool = True if mapping_specification_header.lower() == "true" else False

        self.__logger.info(f"Specification file for ingestion process at {mapping_specification_file_path}")
        self.__logger.info(f"Separator to be used: {mapping_specification_separator}")
        self.__logger.info(f"Header option: {mapping_specification_header_option}")

        return self.__spark_session\
            .read.format("csv")\
            .option("sep", mapping_specification_separator)\
            .option("header", mapping_specification_header_option)\
            .csv(mapping_specification_file_path, schema=[])
