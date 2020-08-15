import logging
from datetime import datetime
from functools import partial
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from src.spark.branch import Branch
from src.spark.engine.abstract import AbstractEngine


class ReloadEngine(AbstractEngine):

    def __init__(self, overwrite_flag: bool, job_ini_file: str):

        super().__init__(job_ini_file)
        self.__logger = logging.getLogger(__name__)
        self.__overwrite_flag: bool = overwrite_flag

    def run(self):

        database: str = self._job_properties["spark"]["database"]
        specification_table: str = self._job_properties["spark"]["specification_table_name"]
        specification_historical_table: str = self._job_properties["spark"]["specification_historical_table_name"]
        insert_reload_log_record: Callable = partial(
            self._insert_application_log,
            application_branch=Branch.RE_LOAD.value,
            bancll_name=None,
            dt_business_date=None)

        try:

            self.__reload_mapping_specification(database, specification_table, specification_historical_table)

        except Exception as e:

            self.__logger.exception(f"Unable to overwrite table '{database}.{specification_table}'")
            insert_reload_log_record(impacted_table=specification_table, exception_message=repr(e))
            insert_reload_log_record(impacted_table=specification_historical_table, exception_message=repr(e))

        else:

            insert_reload_log_record(impacted_table=specification_table)
            insert_reload_log_record(impacted_table=specification_historical_table)

    def __reload_mapping_specification(self, database: str, specification_actual_table: str, specification_historical_table: str):

        """
        https://spark.apache.org/docs/2.2.3/sql-programming-guide.html#jdbc-to-other-databases
        truncate: This is a JDBC writer related option.
        When SaveMode.Overwrite is enabled, this option causes Spark to truncate an existing table instead of dropping and recreating it.
        This can be more efficient, and prevents the table metadata (e.g., indices) from being removed.
        However, it will not work in some cases, such as when the new data has a different schema.
        It defaults to false. This option applies only to writing.
        """

        # WHEN input_overwrite_option == True, THE USER WANTS TO DROP THE TABLE AND THEN RECREATE IT (USEFUL IF SCHEMA HAS CHANGED)
        # SO, ACCORDING TO ABOVE DOCUMENTATION, truncate = false

        # WHEN input_overwrite_option == False, THE USER WANTS TO JUST FILL THE TABLE WITH NEW DATA
        # SO, ACCORDING TO ABOVE DOCUMENTATION, truncate = true

        jdbc_overwrite_option: bool = not self.__overwrite_flag
        date_time_now: datetime = datetime.now()

        # READ OLD SPECIFICATIONS AND INSERT THEM INTO THE HISTORICAL SPECIFICATIONS
        old_specification_df: DataFrame = self._read_from_jdbc(database, specification_actual_table)\
            .withColumn("ts_fine_validita", lit(date_time_now))\
            .withColumn("dt_fine_validita", lit(date_time_now.date()))

        self._write_to_jdbc(old_specification_df, database, specification_historical_table, "append")

        old_version_number: float = old_specification_df.selectExpr("versione")\
            .distinct()\
            .collect()[0][0]

        new_version_number: float = old_version_number + 0.1
        self.__logger.info(f"Updating version number from {old_version_number:.1f} to {new_version_number:.1f}")

        # OVERWRITE OLD SPECIFICATIONS
        new_specification_df: DataFrame = self._read_mapping_specification_from_file()\
            .withColumn("versione", lit(new_version_number).cast("double"))
        self._write_to_jdbc(new_specification_df, database, specification_actual_table, "overwrite", jdbc_overwrite_option)
