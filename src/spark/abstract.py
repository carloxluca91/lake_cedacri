import configparser

from abc import ABC
from datetime import datetime, timedelta
from pyspark.sql import DataFrame, SparkSession
from typing import Any, List


def _get_specification_schema(json_schema_str: str):

    import json

    from src.spark.spark_types import DATA_TYPE_DICT
    from pyspark.sql.types import StructField, StructType

    mapping_specification_schema_dict = json.loads(json_schema_str, encoding="UTF-8")
    return StructType(list(
        map(lambda x: StructField(x, DATA_TYPE_DICT[mapping_specification_schema_dict[x]]),
            mapping_specification_schema_dict)))


class AbstractBancllLoader(ABC):

    def __init__(self, job_properties: configparser.ConfigParser, n_records: int):

        import logging
        import numpy as np

        from numpy.random import RandomState

        self.__n_records: int = n_records
        self.__logger = logging.getLogger(__name__)
        self.__job_properties: configparser.ConfigParser = job_properties
        self._spark_session: SparkSession = SparkSession \
            .builder \
            .getOrCreate()

        self.__logger.info(f"Successfully created SparkSession")
        self.__logger.info(f"Spark application UI url: {self._spark_session.sparkContext.uiWebUrl}")

        self.__rng: RandomState = RandomState(int(datetime.now().timestamp()))
        self.__lower_bound_date: datetime = datetime(1910, 1, 1, 0, 0)
        self.__upper_bound_date: datetime = datetime.now()
        self.__time_delta: timedelta = self.__upper_bound_date - self.__lower_bound_date

        self._mapping_specification_df: DataFrame = self._load_mapping_specification()
        self.__logger.info(f"Successfully loaded mapping specification DataFrame using following schema")
        self._mapping_specification_df.printSchema()
        self.__RD_STATIC_COLUMN_TYPES: dict = {

            "INDICE": np.arange(1, self.__n_records + 1),
            "COD_ISTITUTO": self._sample_from_range(["1", "13", "27", "46", "94", "95", "122"]),
            "NDG": self._sample_from_range(list(map(lambda x: str(x), range(27910, 51840)))),
            "TIPO_NDG": self._sample_from_range(["PRIV", "DIP", "CO", "CODIP"]),
            "NOME": self._get_name_list(name_type="first"),
            "COGNOME": self._get_name_list(name_type="last"),
            "DATA": self._generate_datetime_list,
            "TIMESTAMP": self._generate_datetime_list,
            "STATO_NDG": self._sample_from_range(["ATTIVO", "NON ATTIVO"])
        }

    def _load_mapping_specification(self):

        # RETRIEVE SETTINGS FOR READ SPECIFICATION FILE
        mapping_specification_file_path: str = self.__job_properties["paths"]["specification_file_path"]
        mapping_specification_separator: str = self.__job_properties["spark"]["specification_file_delimiter"]
        mapping_specification_header: str = self.__job_properties["spark"]["specification_file_header"]
        mapping_specification_schema_json: str = self.__job_properties["spark"]["specification_file_schema"]

        mapping_specification_header_option: bool = True if mapping_specification_header.lower() == "true" else False

        self.__logger.info(f"Specification file for ingestion process at {mapping_specification_file_path}")
        self.__logger.info(f"Separator to be used: {mapping_specification_separator}")
        self.__logger.info(f"Header option: {mapping_specification_header_option}")

        # READ THE FILE
        return self._spark_session\
            .read.format("csv")\
            .option("sep", mapping_specification_separator)\
            .option("header", mapping_specification_header_option)\
            .csv(mapping_specification_file_path,
                 schema=_get_specification_schema(mapping_specification_schema_json))

    def _generate_datetime_list(self, output_date_format: str, size: int = None) -> List[str]:

        effective_size: int = self.__n_records if size is None else size
        random_array = self.__rng.random_sample(effective_size)
        return list(map(lambda x: x.strftime(output_date_format),
                        map(lambda y: self.__lower_bound_date + self.__time_delta * y, random_array)))

    def _generate_data_for_column(self, column_description: str, date_column_format: str = None) -> List[Any]:

        return self.__RD_STATIC_COLUMN_TYPES[column_description] if column_description not in ["DATA", "TIMESTAMP"] else \
            self.__RD_STATIC_COLUMN_TYPES[column_description](date_column_format)

    def _sample_from_range(self, value_range: List, size: int = None):

        effective_size: int = self.__n_records if size is None else size
        return self.__rng.choice(value_range, effective_size)

    def _get_name_list(self, name_type: str, size: int = None):

        import names

        effective_size: int = self.__n_records if size is None else size
        return [names.get_first_name() for _ in range(effective_size)] if name_type == "first" else \
            [names.get_last_name() for _ in range(effective_size)]

    def _save_dataframe_at_path(self, dataframe: DataFrame, bancll_name: str, dt_business_date: str):

        import os

        # RETRIEVE SETTINGS FOR SAVING DATAFRAME
        lake_cedacri_data_dir_path: str = self.__job_properties["paths"]["data_dir_path"]
        raw_dataframe_save_full_path: str = os.path.join(lake_cedacri_data_dir_path, bancll_name, f"dt_business_date={dt_business_date}")
        raw_dataframe_file_format: str = self.__job_properties["spark"]["raw_dataframe_file_format"]
        raw_dataframe_savemode: str = self.__job_properties["spark"]["raw_dataframe_savemode"]

        self.__logger.info(f"Starting to save dataframe at path {raw_dataframe_save_full_path} "
                           f"using format {raw_dataframe_file_format} "
                           f"and savemode {raw_dataframe_savemode}")

        self.__logger.info("Dataframe schema:")
        dataframe.printSchema()

        # SAVE THE DATAFRAME
        dataframe.write\
            .format(raw_dataframe_file_format)\
            .mode(raw_dataframe_savemode)\
            .option("path", raw_dataframe_save_full_path)\
            .save()

        self.__logger.info(f"Successfully saved dataframe at path {raw_dataframe_save_full_path} "
                           f"using format {raw_dataframe_file_format} "
                           f"and savemode {raw_dataframe_savemode}")
