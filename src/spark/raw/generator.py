import logging
from datetime import datetime, timedelta
from typing import List

from numpy.random import RandomState
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, StructType
from src.spark.types import DATA_TYPE_DICT
from src.spark.engine.source_load import ColumnSpecification


class RawDataGenerator:

    def __init__(self, n_records: int):

        self.__logger = logging.getLogger(__name__)
        self.__n_records: int = n_records
        self.__rng: RandomState = RandomState(int(datetime.now().timestamp()))
        self.__lower_bound_date: datetime = datetime(1910, 1, 1, 0, 0)
        self.__upper_bound_date: datetime = datetime.now()
        self.__time_delta: timedelta = self.__upper_bound_date - self.__lower_bound_date

        self.__cd_istituto_range: List[str] = ["1", "27", "94", "95"]
        self.__cd_istituto_weights: List[float] = [0.33, 0.01, 0.33, 0.33]

        self.__ndg_range: List[str] = list(map(lambda x: str(x), range(270, 5184)))

        self.__tipo_ndg_range: List[str] = ["PRIV", "DIP", "CO", "CODIP"]
        self.__tipo_ndg_weights: List[float] = [0.50, 0.01, 0.48, 0.01]

        self.__stato_ndg_range: List[str] = ["ATTIVO", "NON ATTIVO", "SOSPESO"]
        self.__stato_ndg_weights: List[float] = [0.95, 0.04, 0.01]

        self.__COLUMN_DESCRIPTIONS: dict = {

            "indice": range(1, self.__n_records + 1),
            "cod_istituto": self._sample_from_range(self.__cd_istituto_range, weights=self.__cd_istituto_weights),
            "ndg": self._sample_from_range(self.__ndg_range),
            "tipo_ndg": self._sample_from_range(self.__tipo_ndg_range, weights=self.__tipo_ndg_weights),
            "data": self._generate_date_or_datetime_list,
            "timestamp": self._generate_date_or_datetime_list,
            "stato_ndg": self._sample_from_range(self.__stato_ndg_range, weights=self.__stato_ndg_weights)
        }

    def _generate_date_or_datetime_list(self, flag_date: bool, output_date_format: str, size: int = None) -> List[str]:

        effective_size: int = self.__n_records if size is None else size
        random_array = self.__rng.random_sample(effective_size)
        return list(map(lambda x: x.date().strftime(output_date_format) if flag_date else x.strftime(output_date_format),
                        map(lambda y: self.__lower_bound_date + self.__time_delta * y, random_array)))

    def _sample_from_range(self, value_range: List, size: int = None,  weights: List[float] = None):

        effective_size: int = self.__n_records if size is None else size
        return self.__rng.choice(value_range, size=effective_size, p=weights)

    def get_bancll_raw_dataframe(self, spark_session: SparkSession, column_specifications: List[ColumnSpecification]) -> DataFrame:

        raw_data_dict = {}
        raw_data_struct_type: StructType = StructType()
        for (index, column_specification) in enumerate(column_specifications):

            column_name: str = column_specification.column_name.lower()
            column_desc: str = column_specification.column_desc.lower()
            column_type: str = column_specification.column_type.lower()
            column_date_format: str = column_specification.date_format.lower()

            self.__logger.info(f"Processing column # {index} (name: \'{column_name}\', "
                               f"desc: \'{column_desc}\', type: {column_type}, [format: \'{column_date_format}\'])")

            # UPDATE DATA_DICT AND STRUCTTYPE
            raw_data_dict[column_name] = self.__COLUMN_DESCRIPTIONS[column_desc] if column_desc not in ["data", "timestamp"] else \
                self.__COLUMN_DESCRIPTIONS[column_desc](column_date_format)
            raw_data_struct_type = raw_data_struct_type.add(StructField(column_name, DATA_TYPE_DICT[column_type]))

            self.__logger.info(f"Successfully added data related to column # {index} (name: \'{column_name}\', "
                               f"desc: \'{column_desc}\', type: {column_type}, [format: \'{column_date_format}\'])")

        self.__logger.info("Trying to create raw pyspark.sql.DataFrame")
        raw_dataframe: DataFrame = spark_session.createDataFrame(raw_data_dict, raw_data_struct_type)
        self.__logger.info("Successfully created raw pyspark.sql.DataFrame")
        raw_dataframe.printSchema()
        return raw_dataframe
