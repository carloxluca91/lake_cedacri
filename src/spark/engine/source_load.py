from functools import partial
from typing import Callable, Tuple

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as f

from src.spark.branch import Branch
from src.spark.engine.abstract import AbstractEngine
from src.spark.engine.generator import RawDataGenerator
from src.spark.exceptions import *
from src.spark.time import PYTHON_FORMAT
from src.spark.types import SPARK_ALLOWED_TYPES


class SourceLoadEngine(AbstractEngine):

    def __init__(self, job_ini_file: str, number_of_records: int):

        import logging

        super().__init__(job_ini_file)
        self.__logger = logging.getLogger(__name__)
        self.__raw_data_generator: RawDataGenerator = RawDataGenerator(number_of_records)

    def run(self, bancll_name: str, dt_riferimento: str):

        target_database: str = self._job_properties["spark"]["database"]
        specification_table: str = self._job_properties["spark"]["specification_table_name"]

        self.__logger.info(f"Spark target database: '{target_database}'")
        self.__logger.info(f"Spark mapping specification table: '{specification_table}'")

        if self._table_exists(target_database, specification_table):

            # PARTIAL FUNCTION FOR MORE AGILE LOGGING
            insert_application_log: Callable = partial(
                self._insert_application_log,
                application_branch=Branch.SOURCE_LOAD.value,
                bancll_name=bancll_name,
                dt_riferimento=dt_riferimento)

            try:

                # FILTER SPECIFICATION TABLE ACCORDING TO PROVIDED BANCLL
                self.__logger.info(f"Table '{target_database}'.'{specification_table}' exists. So, trying to read it")
                bancll_specification_rows: List[Row] = self._read_from_jdbc(target_database, specification_table) \
                    .filter((f.col("flusso") == bancll_name) & f.col("colonna_rd").isNotNull()) \
                    .collect()

                # CHECK IF SOME CONFIGURATION CAN BE FOUND
                if len(bancll_specification_rows) == 0:

                    self.__logger.error(f"No specification found for BANCLL '{bancll_name}'")
                    raise UndefinedBANCLLError(bancll_name)

                self.__logger.info(f"Identified {len(bancll_specification_rows)} rows related to BANCLL '{bancll_name}'")
                self.__logger.info(f"Starting to validate specifications stated for BANCLL '{bancll_name}'")

                # VALIDATION OF BANCLL SPECIFICATION
                self._validate_bancll_specification(bancll_name, bancll_specification_rows)

                self.__logger.info(f"Successfully validated specification for BANCLL '{bancll_name}'")
                raw_actual_table_name: str = set(map(lambda x: x["sorgente_rd"], bancll_specification_rows)).pop().lower()
                raw_historical_table_name: str = f"{raw_actual_table_name}_h"
                column_specifications: List[Tuple] = list(map(lambda x: (x["colonna_rd"],
                                                                         x["tipo_colonna_rd"],
                                                                         x["descrizione_colonna_rd"],
                                                                         x["formato_data"],
                                                                         x["posizione_iniziale"]),
                                                              bancll_specification_rows))

                # FILTER THE TUPLES SUCH THAT 'colonna_rd' is Not None, SORT THEM BY 'posizione_iniziale'
                filtered_and_sorted_specifications: List[Tuple] = sorted(
                    filter(lambda x: x[0] is not None, column_specifications),
                    key=lambda x: x[4])

                # AND USE THEM TO CREATE A DATAFRAME
                raw_dataframe: DataFrame = self.__raw_data_generator \
                    .get_raw_dataframe(self._spark_session,
                                       filtered_and_sorted_specifications,
                                       dt_riferimento)

                self._try_to_write_to_jdbc(raw_dataframe, target_database, raw_historical_table_name, "append", insert_application_log)
                self._try_to_write_to_jdbc(raw_dataframe, target_database, raw_actual_table_name, "overwrite", insert_application_log)

            except Exception as e:

                self.__logger.exception(f"Got an error while trying to create data for BANCLL '{bancll_name}', reference date '{dt_riferimento}'")
                insert_application_log(impacted_table=None, exception_message=str(e))

        else:

            initial_load_branch: str = Branch.INITIAL_LOAD.value
            self.__logger.warning(f"Table '{target_database}'.'{specification_table}' does not exist yet")
            self.__logger.warning(f"Thus, no data will be uploaded. You should first run '{initial_load_branch}' branch")

    def _try_to_write_to_jdbc(self, dataframe: DataFrame, target_database: str, target_table: str, savemode: str, insert_log_record: Callable):

        try:

            self._write_to_jdbc(dataframe, target_database, target_table, savemode)

        except Exception as e:

            self.__logger.exception(f"Got an error while trying to load data for table '{target_database}'.'{target_table}'")
            insert_log_record(impacted_table=target_table, exception_message=repr(e))

        else:

            insert_log_record(impacted_table=target_table)

    def _validate_bancll_specification(self, bancll_name, bancll_specification_rows):

        def log_and_raise_exception(exception: Exception):

            self.__logger.error(f"{exception}")
            raise exception

        # VALIDATION OF BANCLL SPECIFICATION
        # [a] IS COLUMN 'sorgente_rd' UNIQUE FOR THE PROVIDED BANCLL ?
        bancll_raw_table_names: Set[str] = set(map(lambda x: x["sorgente_rd"], bancll_specification_rows))
        if len(bancll_raw_table_names) > 1:

            log_and_raise_exception(InvalidBANCLLSourceError(bancll_name, bancll_raw_table_names))

        bancll_raw_column_names: List[str] = list(map(lambda x: x["colonna_rd"], bancll_specification_rows))
        bancll_raw_column_types: List[str] = list(map(lambda x: x["tipo_colonna_rd"], bancll_specification_rows))
        bancll_raw_column_positions: List[int] = list(map(lambda x: x["posizione_iniziale"], bancll_specification_rows))
        bancll_raw_column_input_formats: List[str] = list(map(lambda x: x["formato_data"], bancll_specification_rows))

        # [b] DUPLICATED COLUMNS IN 'colonna_rd' ?
        if len(bancll_raw_column_names) > len(set(bancll_raw_column_names)):

            bancll_raw_column_names_duplicated: Set[str] = set(filter(lambda x: bancll_raw_column_names.count(x) > 1, bancll_raw_column_names))
            log_and_raise_exception(DuplicateColumnError(bancll_name, bancll_raw_column_names_duplicated))

        # [c] CORRECT DATA_TYPES IN 'tipo_colonna_rd' ?
        unknown_data_types: List[str] = list(filter(lambda x: x not in SPARK_ALLOWED_TYPES, set(bancll_raw_column_types)))
        if len(unknown_data_types) > 0:

            log_and_raise_exception(UnknownDataTypeError(bancll_name, unknown_data_types))

        # [d] CORRECT ORDERING STATED IN 'posizione_iniziale' ?
        # [d.1] ARE BOTH MIN_INDEX AND MAX_INDEX NON-NEGATIVE ?

        min_initial_position: int = min(bancll_raw_column_positions)
        max_initial_position: int = max(bancll_raw_column_positions)
        if min_initial_position < 0 or max_initial_position < 0:

            negative_index: int = min_initial_position if min_initial_position < 0 else max_initial_position
            log_and_raise_exception(NegativeColumnIndexError(bancll_name, negative_index))

        # [d.2] IS MIN_INDEX CORRECT ?
        if min_initial_position != 1:

            log_and_raise_exception(InvalidMinColumnIndexError(bancll_name, min_initial_position))

        # [d.3] IS MAX_INDEX GREATER (OR EQUAL AT LEAST) THAN MIN_INDEX
        if max_initial_position < min_initial_position:

            log_and_raise_exception(InvalidMaxColumnIndexError(bancll_name, max_initial_position, min_initial_position))

        # [d.4] IS THE DEFINED RANGE CONTINUOUS ?
        missing_positions: List[int] = list(filter(lambda x: x not in bancll_raw_column_positions,
                                                   range(min_initial_position, max_initial_position + 1)))
        if len(missing_positions) > 0:

            log_and_raise_exception(NonContinuousRangeError(bancll_name, missing_positions))

        # [e] ARE ALL DATE_FORMATS CORRECTLY DEFINED ?
        date_formats_set: Set[str] = set(bancll_raw_column_input_formats)
        undefined_formats: List[str] = list(
            filter(lambda y: PYTHON_FORMAT.get(y) is None,
                   filter(lambda x: x is not None, date_formats_set)))

        if len(undefined_formats) > 0:

            log_and_raise_exception(UnmatchedDateFormatError(bancll_name, undefined_formats))
