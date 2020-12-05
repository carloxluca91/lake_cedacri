from functools import partial
from typing import Callable, Tuple, List

from pyspark.sql import DataFrame, Row, Column
from pyspark.sql import functions as f

from lake_cedacri.utils.branch import Branch
from lake_cedacri.engine.abstract import AbstractEngine


class SourceLoadEngine(AbstractEngine):

    def __init__(self, job_ini_file: str, number_of_records: int):

        import logging

        super().__init__(job_ini_file)
        self._logger = logging.getLogger(__name__)

    def run(self, bancll_name: str, dt_riferimento: str) -> None:

        target_database: str = self._job_properties["lake_cedacri"]["database"]
        specification_table: str = self._job_properties["lake_cedacri"]["specification_table_name"]

        self._logger.info(f"Spark target database: '{target_database}'")
        self._logger.info(f"Spark mapping specification table: '{specification_table}'")

        if self._table_exists(target_database, specification_table):

            insert_application_log: Callable = partial(
                self._insert_application_log,
                application_branch=Branch.SOURCE_LOAD.value,
                bancll_name=bancll_name,
                dt_riferimento=dt_riferimento)

            try:

                # Filter specification table according to provided bancll
                self._logger.info(f"Table '{target_database}'.'{specification_table}' exists. So, trying to read it")
                specification_filter_column: Column = f.trim(f.lower(f.col("flusso"))) == bancll_name
                bancll_specification_rows: List[Row] = self._read_from_jdbc(target_database, specification_table) \
                    .filter(specification_filter_column) \
                    .collect()

                # Check if some configuration have been found
                if len(bancll_specification_rows) == 0:

                    error_msg: str = f"No specification found for BANCLL '{bancll_name}'. Thus, nothing will be triggered"
                    self._logger.warning(error_msg)
                    raise ValueError(error_msg)

                self._logger.info(f"Identified {len(bancll_specification_rows)} rows related to BANCLL '{bancll_name}'")

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
                raw_dataframe: DataFrame = self._raw_data_generator \
                    .get_raw_dataframe(self._spark_session,
                                       filtered_and_sorted_specifications,
                                       dt_riferimento)

                self._try_to_write_to_jdbc(raw_dataframe, target_database, raw_historical_table_name, "append", insert_application_log)
                self._try_to_write_to_jdbc(raw_dataframe, target_database, raw_actual_table_name, "overwrite", insert_application_log)

            except Exception as e:

                self._logger.exception(f"Got an error while trying to create data for BANCLL '{bancll_name}', reference date '{dt_riferimento}'")
                insert_application_log(impacted_table=None, exception_message=str(e))

        else:

            initial_load_branch: str = Branch.INITIAL_LOAD.value
            self._logger.warning(f"Table '{target_database}'.'{specification_table}' does not exist yet")
            self._logger.warning(f"Thus, no data will be uploaded. You should first run '{initial_load_branch}' branch")

    def _try_to_write_to_jdbc(self, dataframe: DataFrame, target_database: str, target_table: str, savemode: str, insert_log_record: Callable):

        try:

            self._write_to_jdbc(dataframe, target_database, target_table, savemode)

        except Exception as e:

            self._logger.exception(f"Got an error while trying to load data for table '{target_database}'.'{target_table}'")
            insert_log_record(impacted_table=target_table, exception_message=repr(e))

        else:

            insert_log_record(impacted_table=target_table)
