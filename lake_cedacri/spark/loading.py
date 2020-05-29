import configparser

from lake_cedacri.spark.abstract import AbstractBancllLoader
from lake_cedacri.time.formats import JAVA_TO_PYTHON_FORMAT
from pyspark.sql import DataFrame, Row
from typing import List, Set


def _validate_bancll_specification(bancll_name, bancll_specification_rows):

    from lake_cedacri.spark.types import SPARK_TYPES

    # VALIDATION OF BANCLL SPECIFICATION
    # [a] IS COLUMN 'sorgente_rd' UNIQUE FOR THE PROVIDED BANCLL ?
    bancll_raw_table_names: Set[str] = set(map(lambda x: x["sorgente_rd"], bancll_specification_rows))
    if len(bancll_raw_table_names) > 1:

        from lake_cedacri.spark.exceptions import InvalidBANCLLSourceError
        raise InvalidBANCLLSourceError(bancll_name, bancll_raw_table_names)

    # EXTRACT INFO RELATED TO RAW_COLUMN SPECIFICATIONS
    bancll_raw_column_specs: List[Row] = bancll_specification_rows\
        .selectExpr("colonna_rd", "tipo_colonna_rd", "posizione_iniziale", "formato_input")\
        .collect()

    bancll_raw_column_names: List[str] = list(map(lambda x: x["colonna_rd"], bancll_raw_column_specs))
    bancll_raw_column_types: List[str] = list(map(lambda x: x["tipo_colonna_rd"], bancll_raw_column_specs))
    bancll_raw_column_positions: List[int] = list(map(lambda x: x["posizione_iniziale"], bancll_raw_column_specs))
    bancll_raw_column_input_formats: List[str] = list(map(lambda x: x["formato_input"], bancll_raw_column_specs))

    # [b] DUPLICATED COLUMNS IN 'colonna_rd' ?
    if len(bancll_raw_column_names) > len(set(bancll_raw_column_names)):

        from lake_cedacri.spark.exceptions import DuplicateColumnError
        bancll_raw_column_names_duplicated: Set[str] = set(filter(lambda x: bancll_raw_column_names.count(x) > 1, bancll_raw_column_names))
        raise DuplicateColumnError(bancll_name, bancll_raw_column_names_duplicated)

    # [c] CORRECT DATA_TYPES IN 'tipo_colonna_rd' ?
    unknown_data_types: List[str] = list(filter(lambda x: x not in SPARK_TYPES, set(bancll_raw_column_types)))
    if len(unknown_data_types) > 0:

        from lake_cedacri.spark.exceptions import UnknownDataTypeError
        raise UnknownDataTypeError(bancll_name, unknown_data_types)

    # [d] CORRECT ORDERING STATED IN 'posizione_iniziale' ?
    # [d.1] ARE BOTH MIN_INDEX AND MAX_INDEX NON-NEGATIVE ?

    min_initial_position: int = min(bancll_raw_column_positions)
    max_initial_position: int = max(bancll_raw_column_positions)
    if min_initial_position < 0 or max_initial_position < 0:

        from lake_cedacri.spark.exceptions import NegativeColumnIndexError
        negative_index: int = min_initial_position if min_initial_position < 0 else max_initial_position
        raise NegativeColumnIndexError(bancll_name, negative_index)

    # [d.2] IS MIN_INDEX CORRECT ?
    if min_initial_position != 0:

        from lake_cedacri.spark.exceptions import InvalidMinColumnIndexError
        raise InvalidMinColumnIndexError(bancll_name, min_initial_position)

    # [d.3] IS MAX_INDEX GREATER (OR EQUAL AT LEAST) THAN MIN_INDEX
    if max_initial_position < min_initial_position:

        from lake_cedacri.spark.exceptions import InvalidMaxColumnIndexError
        raise InvalidMaxColumnIndexError(bancll_name, max_initial_position, min_initial_position)

    # [d.4] IS THE DEFINED RANGE CONTINUOUS ?
    missing_positions: List[int] = list(filter(lambda x: x not in bancll_raw_column_positions,
                                               range(min_initial_position, max_initial_position + 1)))
    if len(missing_positions) > 0:

        from lake_cedacri.spark.exceptions import NonContinuousRangeError
        raise NonContinuousRangeError(bancll_name, missing_positions)

    # [e] ARE ALL DATE_FORMATS CORRECTLY DEFINED ?
    undefined_formats: List[str] = list(filter(lambda x: x is not None,
                                               list(map(lambda x: JAVA_TO_PYTHON_FORMAT.get(x), bancll_raw_column_input_formats))))

    if len(undefined_formats) > 1:

        from lake_cedacri.spark.exceptions import UndefinedDateFormatError
        raise UndefinedDateFormatError(bancll_name, undefined_formats)


class BancllLoader(AbstractBancllLoader):

    def __init__(self, job_properties: configparser.ConfigParser):

        import logging

        super().__init__(job_properties)
        self.__logger = logging.getLogger(__name__)

    def run(self, bancll_name, dt_business_date):

        from pyspark.sql import functions as f

        # FILTER SPECIFICATION TABLE ACCORDING TO PROVIDED BANCLL

        mapping_specification_df: DataFrame = self.__mapping_specification_df
        bancll_specification_rows = mapping_specification_df\
            .filter(f.col("flusso") == bancll_name)\
            .collect()

        # NO CONFIGURATION FOUND ?
        if len(bancll_specification_rows) == 0:

            from lake_cedacri.spark.exceptions import UndefinedBANCLLError
            raise UndefinedBANCLLError(bancll_name)

        _validate_bancll_specification(bancll_name, bancll_specification_rows)

