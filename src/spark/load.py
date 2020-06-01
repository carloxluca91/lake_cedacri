import configparser

from src.spark.abstract import AbstractBancllLoader
from src.spark.time import JAVA_TO_PYTHON_FORMAT
from pyspark.sql import DataFrame, Row
from typing import List, Set, Tuple


class BancllLoader(AbstractBancllLoader):

    def __init__(self, job_properties: configparser.ConfigParser, n_records: int):

        import logging

        super().__init__(job_properties, n_records)
        self.__logger = logging.getLogger(__name__)

    def run(self, bancll_name: str, dt_business_date: str):

        import pandas as pd
        from pyspark.sql import functions as f

        # FILTER SPECIFICATION TABLE ACCORDING TO PROVIDED BANCLL
        mapping_specification_df: DataFrame = self._mapping_specification_df
        bancll_specification_rows: List[Row] = mapping_specification_df\
            .filter(f.col("flusso") == bancll_name)\
            .collect()

        # NO CONFIGURATION FOUND ?
        if len(bancll_specification_rows) == 0:

            from src.spark.exceptions import UndefinedBANCLLError

            self.__logger.error(f"No specification found for BANCLL \"{bancll_name}\"")
            raise UndefinedBANCLLError(bancll_name)

        self.__logger.info(f"Identified {len(bancll_specification_rows)} rows related to BANCLL \"{bancll_name}\"")
        self.__logger.info(f"Starting to validate specifications stated for BANCLL \"{bancll_name}\"")

        self._validate_bancll_specification(bancll_name, bancll_specification_rows)

        self.__logger.info(f"Successfully validated specification for BANCLL \"{bancll_name}\"")
        bancll_specification_tuples: List[Tuple] = list(map(
            lambda x: (x["colonna_rd"], x["tipo_colonna_rd"], x["descrizione_colonna_rd"], x["formato_data"], x["posizione_iniziale"]),
            bancll_specification_rows))

        # SORT THE TUPLES BY 'posizione_iniziale'
        bancll_specification_tuples_sorted = sorted(bancll_specification_tuples, key=lambda x: x[4])

        raw_dataframe_dict = {}
        for t in bancll_specification_tuples_sorted:

            raw_dataframe_dict[t[0]] = self._generate_data_for_column(t[2], t[3])
            self.__logger.info(f"Successfully added data related to column \"{t[0]}\" of type \"{t[1]}\"")

        # noinspection PyUnresolvedReferences
        raw_dataframe: DataFrame = self._spark_session.createDataFrame(pd.DataFrame.from_dict(raw_dataframe_dict))\
            .withColumn("dt_business_date", f.lit(dt_business_date))

        self.__logger.info(f"Successfully created pyspark.sql.DataFrame for BANCLL \"{bancll_name}\" with following schema")
        raw_dataframe.printSchema()

        self._save_dataframe_at_path(raw_dataframe, bancll_name, dt_business_date)

    def _validate_bancll_specification(self, bancll_name, bancll_specification_rows):

        from src.spark.spark_types import SPARK_ALLOWED_TYPES
        import src.spark.exceptions as exceptions

        def log_and_raise_exception(exception: Exception):

            self.__logger.error(f"{exception}")
            raise exception

        # VALIDATION OF BANCLL SPECIFICATION
        # [a] IS COLUMN 'sorgente_rd' UNIQUE FOR THE PROVIDED BANCLL ?
        bancll_raw_table_names: Set[str] = set(map(lambda x: x["sorgente_rd"], bancll_specification_rows))
        if len(bancll_raw_table_names) > 1:

            log_and_raise_exception(exceptions.InvalidBANCLLSourceError(bancll_name, bancll_raw_table_names))

        bancll_raw_column_names: List[str] = list(map(lambda x: x["colonna_rd"], bancll_specification_rows))
        bancll_raw_column_types: List[str] = list(map(lambda x: x["tipo_colonna_rd"], bancll_specification_rows))
        bancll_raw_column_positions: List[int] = list(map(lambda x: x["posizione_iniziale"], bancll_specification_rows))
        bancll_raw_column_input_formats: List[str] = list(map(lambda x: x["formato_data"], bancll_specification_rows))

        # [b] DUPLICATED COLUMNS IN 'colonna_rd' ?
        if len(bancll_raw_column_names) > len(set(bancll_raw_column_names)):

            bancll_raw_column_names_duplicated: Set[str] = set(filter(lambda x: bancll_raw_column_names.count(x) > 1, bancll_raw_column_names))
            log_and_raise_exception(exceptions.DuplicateColumnError(bancll_name, bancll_raw_column_names_duplicated))

        # [c] CORRECT DATA_TYPES IN 'tipo_colonna_rd' ?
        unknown_data_types: List[str] = list(filter(lambda x: x not in SPARK_ALLOWED_TYPES, set(bancll_raw_column_types)))
        if len(unknown_data_types) > 0:

            log_and_raise_exception(exceptions.UnknownDataTypeError(bancll_name, unknown_data_types))

        # [d] CORRECT ORDERING STATED IN 'posizione_iniziale' ?
        # [d.1] ARE BOTH MIN_INDEX AND MAX_INDEX NON-NEGATIVE ?

        min_initial_position: int = min(bancll_raw_column_positions)
        max_initial_position: int = max(bancll_raw_column_positions)
        if min_initial_position < 0 or max_initial_position < 0:

            negative_index: int = min_initial_position if min_initial_position < 0 else max_initial_position
            log_and_raise_exception(exceptions.NegativeColumnIndexError(bancll_name, negative_index))

        # [d.2] IS MIN_INDEX CORRECT ?
        if min_initial_position != 1:

            log_and_raise_exception(exceptions.InvalidMinColumnIndexError(bancll_name, min_initial_position))

        # [d.3] IS MAX_INDEX GREATER (OR EQUAL AT LEAST) THAN MIN_INDEX
        if max_initial_position < min_initial_position:

            log_and_raise_exception(exceptions.InvalidMaxColumnIndexError(bancll_name, max_initial_position, min_initial_position))

        # [d.4] IS THE DEFINED RANGE CONTINUOUS ?
        missing_positions: List[int] = list(filter(lambda x: x not in bancll_raw_column_positions,
                                                   range(min_initial_position, max_initial_position + 1)))
        if len(missing_positions) > 0:

            log_and_raise_exception(exceptions.NonContinuousRangeError(bancll_name, missing_positions))

        # [e] ARE ALL DATE_FORMATS CORRECTLY DEFINED ?
        date_formats_set: Set[str] = set(bancll_raw_column_input_formats)
        undefined_formats: List[str] = list(
            filter(lambda y: JAVA_TO_PYTHON_FORMAT.get(y) is None,
                filter(lambda x: x is not None, date_formats_set)))

        if len(undefined_formats) > 0:

            log_and_raise_exception(exceptions.UnmatchedDateFormatError(bancll_name, undefined_formats))
