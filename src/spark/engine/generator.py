import logging
import random

from datetime import date, datetime, timedelta
from typing import Any, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, StructType
from pyspark.sql.functions import lit, monotonically_increasing_id
from src.spark.types import DATA_TYPE_DICT
from src.spark.time import DT_RIFERIMENTO_DATE, PYTHON_FORMAT


def _get_random_binaries(n: int, one_probability: float = 0.005) -> List[int]:

    return random.choices([0, 1], weights=[1 - one_probability, one_probability], k=n)


def _replace_randomly_with_none(original_data: List[Any], none_probability: float = 0.005) -> List[Any]:

    # CREATE A RANDOM ARRAY OF ZEROS AND ONES, ASSIGNING PICKING PROBABILITIES
    return list(map(lambda value, probability: value if probability == 0 else None,
                    original_data,
                    _get_random_binaries(len(original_data), none_probability)))


def _get_random_datetime_format() -> str:

    random_java_format: str = random.choice(list(PYTHON_FORMAT.keys()))
    return PYTHON_FORMAT[random_java_format]


class RawDataGenerator:

    def __init__(self, n_records: int):

        self.__logger = logging.getLogger(__name__)
        self.__n_records: int = n_records
        self.__lower_bound_date: datetime = datetime(1910, 1, 1, 0, 0)
        self.__upper_bound_date: datetime = datetime.now()
        self.__time_delta: timedelta = self.__upper_bound_date - self.__lower_bound_date

        self.__cd_istituto_range: List[str] = ["1", "27", "94", "95", None]
        self.__cd_istituto_weights: List[float] = [0.33, 0.05, 0.33, 0.33, 0.05]

        self.__ndg_range: List[str] = list(map(lambda x: str(x), range(270, 5184)))

        self.__tipo_ndg_range: List[str] = ["PRIV", "DIP", "CO", "CODIP", None]
        self.__tipo_ndg_weights: List[float] = [0.50, 0.04, 0.48, 0.04, 0.02]

        self.__stato_ndg_range: List[str] = ["ATTIVO", "NON ATTIVO", "SOSPESO", None]
        self.__stato_ndg_weights: List[float] = [0.95, 0.02, 0.02, 0.01]

        self.__COLUMN_DESCRIPTIONS: dict = {

            "cod_istituto": self._sample_from_range(self.__cd_istituto_range, weights=self.__cd_istituto_weights),
            "ndg": self._sample_from_range(self.__ndg_range),
            "tipo_ndg": self._sample_from_range(self.__tipo_ndg_range, weights=self.__tipo_ndg_weights),
            "data": self._generate_date_or_datetime_list,
            "timestamp": self._generate_date_or_datetime_list,
            "stato_ndg": self._sample_from_range(self.__stato_ndg_range, weights=self.__stato_ndg_weights)
        }

        random.seed()

    def _generate_date_or_datetime_list(self, output_date_format: str) -> List[str]:

        # FIRST GENERATE PURE RANDOM DATETIMES
        random_array = [random.random() for _ in range(self.__n_records)]
        initial_list_of_datetimes: List[datetime] = list(map(lambda x: self.__lower_bound_date + self.__time_delta * x, random_array))

        # COMPUTE PROBABILITIES OF USING A DIFFERENT OUTPUT FORMAT
        use_randomic_format_probabilities: List[int] = _get_random_binaries(len(initial_list_of_datetimes))

        # FORMAT ORIGINAL PURE RANDOM DATETIMES
        list_of_datetimes_with_different_formats: List[str] = \
            list(map(lambda x, y: x.strftime(output_date_format) if y == 0 else x.strftime(_get_random_datetime_format()),
                     initial_list_of_datetimes,
                     use_randomic_format_probabilities))

        # AND GET THEM DIRTY WITH SOME NONE
        return _replace_randomly_with_none(list_of_datetimes_with_different_formats)

    def _sample_from_range(self, value_range: List, weights: List[float] = None) -> List[Any]:

        return random.choices(value_range, weights=weights, k=self.__n_records)

    def get_raw_dataframe(self,
                          spark_session: SparkSession,
                          column_specifications: List[Tuple],
                          dt_riferimento: str) -> DataFrame:

        raw_data_dict = {}
        raw_data_struct_type: StructType = StructType()
        for (index, column_specification) in enumerate(column_specifications):

            column_name: str = column_specification[0].lower()
            column_type: str = column_specification[1].lower()
            column_desc: str = column_specification[2].lower()

            if column_desc in ["data", "timestamp"]:

                column_date_format: str = column_specification[3]
                self.__logger.info(f"Processing column # {index} (name: '{column_name}', desc: '{column_desc}', type: '{column_type}', "
                                   f"format: '{column_date_format}')")

                raw_data_dict[column_name] = self.__COLUMN_DESCRIPTIONS[column_desc](PYTHON_FORMAT[column_date_format])

                self.__logger.info(f"Successfully added data related to column # {index} (name: '{column_name}', "
                                   f"desc: '{column_desc}', type: {column_type}, format: '{column_date_format}')")

            else:

                self.__logger.info(f"Processing column # {index} (name: '{column_name}', desc: '{column_desc}', type: '{column_type}')")

                raw_data_dict[column_name] = self.__COLUMN_DESCRIPTIONS[column_desc]

                self.__logger.info(f"Successfully added data related to column # {index} (name: '{column_name}', desc: '{column_desc}', "
                                   f"type: '{column_type}'")

            raw_data_struct_type = raw_data_struct_type.add(StructField(column_name, DATA_TYPE_DICT[column_type]))

        raw_data_tuple_list: List[Tuple] = [tuple(raw_data_dict[key][i] for key in list(raw_data_dict.keys())) for i in range(self.__n_records)]
        dt_riferimento_date: date = datetime.strptime(dt_riferimento, PYTHON_FORMAT[DT_RIFERIMENTO_DATE]).date()
        self.__logger.info("Trying to create raw pyspark.sql.DataFrame")

        raw_dataframe: DataFrame = spark_session.createDataFrame(raw_data_tuple_list, raw_data_struct_type) \
            .withColumn("row_id", monotonically_increasing_id()) \
            .withColumn("ts_inserimento", lit(datetime.now())) \
            .withColumn("dt_inserimento", lit(datetime.now().date())) \
            .withColumn("dt_riferimento", lit(dt_riferimento_date))

        self.__logger.info("Successfully created raw pyspark.sql.DataFrame")
        return raw_dataframe \
            .orderBy("row_id") \
            .select(["row_id"] + list(filter(lambda x: not x.lower() == "row_id", raw_dataframe.columns)))
