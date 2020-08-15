import logging

from functools import partial
from typing import Callable, List
from pyspark.sql import DataFrame
from src.spark.branch import Branch
from src.spark.engine.abstract import AbstractEngine


class InitialLoadEngine(AbstractEngine):

    def __init__(self, job_ini_file):

        super().__init__(job_ini_file)
        self.__logger = logging.getLogger(__name__)

    def run(self):

        database_to_create: str = self._job_properties["spark"]["database"]
        table_to_create: str = self._job_properties["spark"]["specification_table_name"]
        self.__create_database_if_not_exists(database_to_create)
        insert_initial_load_log: Callable = partial(self._insert_application_log,
            application_branch=Branch.INITIAL_LOAD.value,
            bancll_name=None,
            dt_business_date=None,
            impacted_table=table_to_create)

        try:

            self.__load_mapping_specification(database_to_create, table_to_create)

        except Exception as e:

            self.__logger.exception(f"Unable to save data into table '{database_to_create}.{table_to_create}'")
            insert_initial_load_log(exception_message=repr(e))

        else:

            insert_initial_load_log()

    def __create_database_if_not_exists(self, database_to_create: str):

        self.__logger.info(f"Checking existence of DB '{database_to_create}'")
        self._mysql_cursor.execute("SHOW DATABASES")

        # GET LIST OF EXISTING DATABASES
        existing_databases: List[str] = list(map(lambda x: x[0], self._mysql_cursor))
        existing_databases_str: str = ", ".join(map(lambda x: f"'{x}'", existing_databases))
        self.__logger.info(f"Existing DB(s): {existing_databases_str}")

        # CHECK IF GIVEN DATABASE ALREADY EXISTS
        if database_to_create not in existing_databases:

            self.__logger.warning(f"DB '{database_to_create}' does not exist yet. Attempting to create it now")
            self._mysql_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_to_create}")
            self.__logger.info(f"Successuflly created DB '{database_to_create}'")

        else:

            self.__logger.info(f"DB '{database_to_create}' already exists. So, not much to do :)")

    def __load_mapping_specification(self, database_to_use: str, table_to_create: str):

        from pyspark.sql.functions import lit

        # CHECK IF THE GIVEN TABLE EXISTS WITHIN GIVEN DATABASE
        self.__logger.info(f"Table to search (and eventually create) within DB '{database_to_use}': '{table_to_create}'")
        if not self._table_exists(database_to_use, table_to_create):

            self.__logger.warning(f"DB '{database_to_use}' does not contain table '{table_to_create}' yet. Attempting to create it now")
            specification_df_from_file: DataFrame = self._read_mapping_specification_from_file()\
                .withColumn("versione", lit(1.0).cast("double"))

            self._write_to_jdbc(specification_df_from_file, database_to_use, table_to_create, "overwrite")

        else:

            self.__logger.info(f"DB '{database_to_use}' already contains table '{table_to_create}'. So, not much to do :)")
