import logging
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.types import StructField, StructType

from src.spark.engine.abstract import AbstractEngine


class InitialLoadEngine(AbstractEngine):

    def __init__(self, job_ini_file: str):

        super().__init__(job_ini_file)
        self.__logger = logging.getLogger(__name__)

    def run(self):

        database_to_create: str = self._job_properties["spark"]["database"]
        self.__create_database_if_not_exists(database_to_create)

    def __create_database_if_not_exists(self, database_to_create: str):

        self.__logger.info(f"Checking existence of DB \'{database_to_create}\'")
        self._mysql_cursor.execute("SHOW DATABASES")

        # GET LIST OF EXISTING DATABASES
        existing_databases: List[str] = list(map(lambda x: x[0], self._mysql_cursor))
        self.__logger.info(f"""Existing DB(s): {", ".join(map(lambda x: "'{}'".format(x), existing_databases))}""")

        # CHECK IF GIVEN DATABASE ALREADY EXISTS
        if database_to_create not in existing_databases:

            self.__logger.warning(f"DB \'{database_to_create}\' does not exist yet")
            self.__logger.info("Attempting to create it now")
            self._mysql_cursor.execute(f"CREATE DATABASE IF NOT EXIST {database_to_create}")
            self.__logger.info(f"Successuflly created DB \'{database_to_create}\'")

        else:

            self.__logger.info(f"DB \'{database_to_create}\' already exists. So, not much to do :)")

    def __load_mapping_specification_if_not_exists(self, database_where_to_search: str):

        table_to_search: str = self._job_properties["spark"]["specification_table_name"]
        self.__logger.info(f"Table to search for (and eventually create) within DB \'{database_where_to_search}\': \'{table_to_search}\'")
        self._mysql_cursor.execute(f"USE {database_where_to_search}")
        self._mysql_cursor.execute("SHOW TABLES")

        # GET LIST OF EXISTING TABLES WITHIN GIVEN DATABASE
        existing_tables: List[str] = list(map(lambda x: x[0], self._mysql_cursor))
        self.__logger.info(f"""Existing tables within DB \'{database_where_to_search}\': 
                            {", ".join(map(lambda x: "'{}'".format(x), existing_tables))}""")

        # CHECK IF THE GIVEN TABLE EXISTS WITHIN GIVEN DATABASE
        if table_to_search not in existing_tables:

            self.__logger.warning(f"DB \'{database_where_to_search}\' does not contain table \'{table_to_search}\' yet")
            self.__logger.warning(f"Attempting to create table \'{database_where_to_search}\'.\'{table_to_search}\' now")

            # RETRIEVE SETTINGS FOR FILE READING
            specification_file_path: str = self._job_properties["path"]["specification_file_path"]
            specification_file_sep: str = self._job_properties["spark"]["specification_file_delimiter"]
            specification_file_header_string: str = self._job_properties["spark"]["specification_file_header"]
            specification_file_header: bool = True if specification_file_header_string.lower() == "true" else False
            specification_file_schema: str = self._job_properties["spark"]["specification_file_schema"]

            def from_json_to_struct_type(json_string: str) -> StructType:

                import json

                column_type_dict: dict = {

                    "string": StringType,
                    "int": IntegerType,
                }

                column_list: List[dict] = json.loads(json_string, encoding="UTF-8")["schema"]
                return StructType(list(map(lambda x: StructField(x["name"],
                                                                 column_type_dict[x["type"]](),
                                                                 True if x["nullable"].lower() == "true" else False), column_list)))

            self.__logger.info(f"Attempting to load file at path \'{specification_file_path}\' as a pyspark.sql.DataFrame")

            # READ THE FILE USING ACTIVE SPARK_SESSION
            specification_df: DataFrame = self._spark_session.read\
                .format("csv")\
                .option("path", specification_file_path)\
                .option("sep", specification_file_sep)\
                .option("header", specification_file_header)\
                .option("schema", from_json_to_struct_type(specification_file_schema))\
                .load()

            self.__logger.info(f"Successfully loaded file at path \'{specification_file_path}\' as a pyspark.sql.DataFrame")
            specification_df.printSchema()

            # WRITE THE DATAFRAME TO MySQL SERVER
            self._write_to_jdbc(specification_df, database_where_to_search, table_to_search, "overwrite")

        else:

            self.__logger.info(f"DB \'{database_where_to_search}\' already contains table \'{table_to_search}\'. So, not much to do :)")
