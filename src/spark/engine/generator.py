import logging
import random

from datetime import date, datetime, timedelta
from typing import Any, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, StructType
from pyspark.sql.functions import lit
from src.spark.types import DATA_TYPE_DICT
from src.spark.time import BUSINESS_DATE_FORMAT, JAVA_TO_PYTHON_FORMAT


class RawDataGenerator:

    def __init__(self, n_records: int):

        self.__logger = logging.getLogger(__name__)
        self.__n_records: int = n_records
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

        random.seed()

    def _generate_date_or_datetime_list(self, output_date_format: str) -> List[str]:

        random_array = [random.random() for _ in range(self.__n_records)]
        return list(map(lambda x: x.strftime(output_date_format),
                        map(lambda y: self.__lower_bound_date + self.__time_delta * y,
                            random_array)))

    def _sample_from_range(self, value_range: List, weights: List[float] = None) -> List[Any]:

        return random.choices(value_range, weights=weights, k=self.__n_records)

    def get_raw_dataframe(self,
                          spark_session: SparkSession,
                          column_specifications: List[Tuple],
                          dt_business_date: str) -> DataFrame:

        raw_data_dict = {}
        raw_data_struct_type: StructType = StructType()
        for (index, column_specification) in enumerate(column_specifications):

            column_name: str = column_specification[0].lower()
            column_type: str = column_specification[1].lower()
            column_desc: str = column_specification[2].lower()

            if column_desc in ["data", "timestamp"]:

                column_date_format: str = column_specification[3]
                self.__logger.info(f"Processing column # {index} (name: \'{column_name}\', desc: \'{column_desc}\', type: \'{column_type}\', "
                                   f"format: \'{column_date_format}\')")

                raw_data_dict[column_name] = self.__COLUMN_DESCRIPTIONS[column_desc](JAVA_TO_PYTHON_FORMAT[column_date_format])

                self.__logger.info(f"Successfully added data related to column # {index} (name: \'{column_name}\', "
                                   f"desc: \'{column_desc}\', type: {column_type}, format: \'{column_date_format}\')")

            else:

                self.__logger.info(f"Processing column # {index} (name: \'{column_name}\', desc: \'{column_desc}\', type: \'{column_type}\')")

                raw_data_dict[column_name] = self.__COLUMN_DESCRIPTIONS[column_desc]

                self.__logger.info(f"Successfully added data related to column # {index} (name: \'{column_name}\', desc: \'{column_desc}\', "
                                   f"type: \'{column_type}\'")

            raw_data_struct_type = raw_data_struct_type.add(StructField(column_name, DATA_TYPE_DICT[column_type]))

        raw_data_tuple_list: List[Tuple] = [tuple(raw_data_dict[key][i] for key in list(raw_data_dict.keys())) for i in range(self.__n_records)]
        business_date_format: str = JAVA_TO_PYTHON_FORMAT[BUSINESS_DATE_FORMAT]
        business_date_date: date = datetime.strptime(dt_business_date, business_date_format).date()
        self.__logger.info("Trying to create raw pyspark.sql.DataFrame")

        raw_dataframe: DataFrame = spark_session.createDataFrame(raw_data_tuple_list, raw_data_struct_type)\
            .withColumn("dt_business_date", lit(business_date_date))

        self.__logger.info("Successfully created raw pyspark.sql.DataFrame")
        return raw_dataframe
