import logging

from pyspark.sql import DataFrame
from src.spark.engine.abstract import AbstractEngine
from src.spark.branch import Branch


class ReloadEngine(AbstractEngine):

    def __init__(self, overwrite_flag: bool, job_ini_file: str):

        """
        :param overwrite_flag: flag to control how to reload mapping_specification table.
        :param job_ini_file: .ini file holding Spark job useful information
        """

        super().__init__(job_ini_file)
        self.__logger = logging.getLogger(__name__)
        self.__overwrite_flag: bool = overwrite_flag

    def run(self):

        database: str = self._job_properties["spark"]["database"]
        table: str = self._job_properties["spark"]["specification_table_name"]

        try:

            self.__reload_mapping_specification(database, table)

        except Exception as e:

            self.__logger.error(f"Unable to overwrite table \'{database}.{table}\'")
            self.__logger.error(f"Message: {str(e)}")
            self._insert_application_log(application_branch=Branch.INITIAL_LOAD.name,
                                         bancll_name=None,
                                         dt_business_date=None,
                                         impacted_table=table,
                                         exception_message=str(e))

        else:

            self._insert_application_log(application_branch=Branch.RE_LOAD.name,
                                         bancll_name=None,
                                         dt_business_date=None,
                                         impacted_table=table)

    def __reload_mapping_specification(self, database: str, table: str):

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
        specification_df_from_file: DataFrame = self._read_mapping_specification_from_file()
        self._write_to_jdbc(specification_df_from_file, database, table, "overwrite", jdbc_overwrite_option)
