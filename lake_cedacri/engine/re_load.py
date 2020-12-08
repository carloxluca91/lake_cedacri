import logging
from functools import partial
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from time_utils import TimeUtils

from lake_cedacri.engine import AbstractEngine
from lake_cedacri.utils import Branch


class ReloadEngine(AbstractEngine):

    def __init__(self, overwrite_flag: bool, job_ini_file: str):

        super().__init__(job_ini_file)
        self._logger = logging.getLogger(__name__)
        self._overwrite_flag: bool = overwrite_flag
        self._insert_reload_log_record: Callable = partial(
            self._insert_application_log,
            application_branch=Branch.RE_LOAD.value,
            bancll_name=None,
            dt_riferimento=None)

    def run(self):

        specification_table: str = self._job_properties["spark"]["specification_table_name"]
        specification_historical_table: str = self._job_properties["spark"]["specification_historical_table_name"]
        database: str = self._job_properties["spark"]["database"]

        try:

            self._reload_mapping_specification(database, specification_table, specification_historical_table)

        except Exception as e:

            self._logger.exception(f"Unable to overwrite table '{database}.{specification_table}'")
            self._insert_reload_log_record(impacted_table=specification_table, exception_message=repr(e))
            self._insert_reload_log_record(impacted_table=specification_historical_table, exception_message=repr(e))

        else:

            self._insert_reload_log_record(impacted_table=specification_table)
            self._insert_reload_log_record(impacted_table=specification_historical_table)

    def _reload_mapping_specification(self, database: str, specification_actual_table: str, specification_historical_table: str):

        """
        https://spark.apache.org/docs/2.2.3/sql-programming-guide.html#jdbc-to-other-databases
        truncate: This is a JDBC writer related option.
        When SaveMode.Overwrite is enabled, this option causes Spark to truncate an existing table instead of dropping and recreating it.
        This can be more efficient, and prevents the table metadata (e.g., indices) from being removed.
        However, it will not work in some cases, such as when the new data has a different schema.
        It defaults to false. This option applies only to writing.
        """

        # When input_overwrite_option is True, the user wants to drop the table and then recreate it (useful if schema has changed)
        # so, according to above documentation, truncate = false

        # When input_overwrite_option is False, the user wants to just fill the table with new data
        # so, according to above documentation, truncate = true

        jdbc_overwrite_option: bool = not self._overwrite_flag

        # Read old specifications and insert them into the historical specifications
        old_specification_df: DataFrame = self._read_from_jdbc(database, specification_actual_table) \
            .withColumn("ts_fine_validita", lit(TimeUtils.datetime_now())) \
            .withColumn("dt_fine_validita", lit(TimeUtils.date_now()))

        self._write_to_jdbc(old_specification_df, database, specification_historical_table, "append")

        old_version_number: str = old_specification_df.selectExpr("versione") \
            .distinct() \
            .collect()[0][0]

        new_version_number: str = f"{float(old_version_number) + 0.1:.1f}"
        self._logger.info(f"Updating version number from {old_version_number} to {new_version_number}")

        # Overwrite old specifications
        new_specification_df: DataFrame = self._read_mapping_specification_from_file() \
            .withColumn("versione", lit(new_version_number).cast("string"))
        self._write_to_jdbc(new_specification_df, database, specification_actual_table, "overwrite", jdbc_overwrite_option)
