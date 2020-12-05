import logging

from functools import partial
from typing import Callable, List
from pyspark.sql import DataFrame

from lake_cedacri.utils.branch import Branch
from lake_cedacri.engine.abstract import AbstractEngine


class InitialLoadEngine(AbstractEngine):

    def __init__(self, job_ini_file):

        super().__init__(job_ini_file)
        self._logger = logging.getLogger(__name__)
        self._insert_initial_load_log: Callable = partial(
            self._insert_application_log,
            application_branch=Branch.INITIAL_LOAD.value,
            bancll_name=None,
            dt_riferimento=None)

    def run(self) -> None:

        database_to_create: str = self._job_properties["lake_cedacri"]["database"]
        table_to_create: str = self._job_properties["lake_cedacri"]["specification_table_name"]
        try:

            self._create_database_if_not_exists(database_to_create)
            self._load_mapping_specification(database_to_create, table_to_create)

        except Exception as e:

            self._logger.exception(f"Unable to save data into table '{database_to_create}'.'{table_to_create}'")
            self._insert_initial_load_log(table_to_create=table_to_create, exception_message=repr(e))

        else:

            self._insert_initial_load_log(table_to_create=table_to_create)

    def _create_database_if_not_exists(self, database_to_create: str):

        self._logger.info(f"Checking existence of DB '{database_to_create}'")
        self._mysql_cursor.execute("SHOW DATABASES")

        # Get list of existing databases
        existing_databases: List[str] = list(map(lambda x: x[0], self._mysql_cursor))
        existing_databases_str: str = ", ".join(map(lambda x: f"'{x}'", existing_databases))
        self._logger.info(f"Existing DB(s): {existing_databases_str}")

        # Check if given database exists
        if database_to_create not in existing_databases:

            self._logger.warning(f"DB '{database_to_create}' does not exist yet. Attempting to create it now")
            self._mysql_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_to_create}")
            self._logger.info(f"Successfully created DB '{database_to_create}'")

        else:

            self._logger.info(f"DB '{database_to_create}' already exists. So, not much to do :)")

    def _load_mapping_specification(self, database_to_use: str, table_to_create: str):

        from pyspark.sql.functions import lit

        # check if the given table exists within given database
        self._logger.info(f"Table to search (and eventually create) within DB '{database_to_use}' is '{table_to_create}'")
        if not self._table_exists(database_to_use, table_to_create):

            self._logger.warning(f"DB '{database_to_use}' does not contain table '{table_to_create}' yet. Attempting to create it now")
            specification_df_from_file: DataFrame = self._read_mapping_specification_from_file() \
                .withColumn("versione", lit("0.1"))

            self._write_to_jdbc(specification_df_from_file, database_to_use, table_to_create, "overwrite")

        else:

            self._logger.info(f"DB '{database_to_use}' already contains table '{table_to_create}'. So, not much to do :)")
