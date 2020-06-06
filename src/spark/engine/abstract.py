import configparser
import logging
import mysql.connector
import pandas as pd

from abc import ABC
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql.types import StructField, StructType
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import DataFrame, DataFrameReader, SparkSession
from typing import List, Union


class AbstractEngine(ABC):

    def __init__(self, job_ini_file: str):

        self.__logger = logging.getLogger(__name__)
        self._job_properties: configparser.ConfigParser = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        with open(job_ini_file, "r") as f:

            self._job_properties.read(f)
            self.__logger.info("Successfully loaded job properties dict")

        self._spark_session: SparkSession = SparkSession \
            .builder \
            .getOrCreate()

        self.__logger.info(f"Successfully created SparkSession")
        self.__logger.info(f"Spark application UI url: {self._spark_session.sparkContext.uiWebUrl}")

        jdbc_host = self._job_properties["jdbc"]["host"]
        jdbc_port = int(self._job_properties["jdbc"]["port"])
        jdbc_url = self._job_properties["jdbc"]["url"]
        jdbc_user = self._job_properties["jdbc"]["user"]
        jdbc_password = self._job_properties["jdbc"]["password"]
        jdbc_driver = self._job_properties["jdbc"]["driver"]
        jdbc_use_ssl = self._job_properties["jdbc"]["useSSL"].lower()

        self.__logger.info(f"JDBC host: {jdbc_host}")
        self.__logger.info(f"JDBC port: {jdbc_port}")
        self.__logger.info(f"JDBC url: {jdbc_url}")
        self.__logger.info(f"JDBC user: {jdbc_user}")
        self.__logger.info(f"JDBC password: {jdbc_password}")
        self.__logger.info(f"JDBC driver: {jdbc_driver}")
        self.__logger.info(f"JDBC useSSL: {jdbc_use_ssl}")

        self._spark_jdbc_options: dict = {

            "url": jdbc_url,
            "driver": jdbc_driver,
            "user": jdbc_user,
            "password": jdbc_password,
            "useSSL": jdbc_use_ssl
        }

        self._connector_options: dict = {

            "host": jdbc_host,
            "port": jdbc_port,
            "user": jdbc_user,
            "password": jdbc_password,
            "ssl_disabled": False if jdbc_use_ssl == "true" else True,
            "raise_on_warnings": True,
        }

        # MySQL Python CONNECTOR
        self._mysql_connection: mysql.connector.MySQLConnection = mysql.connector.connect(** self._connector_options)
        self.__logger.info(f"""Successfully estabilished connection to 
                            \'{f"{self._connector_options['host']}:{str(self._connector_options['port'])}"}\'
                            with credentials (\'{self._connector_options['user']}\', \'{self._connector_options['password']}\')""")

        # MySQL Python CURSOR (FOR QUERY EXECUTION)
        self._mysql_cursor: mysql.connector.connection.MySQLCursor = self._mysql_connection.cursor()

        # SPARK JDBC READER
        self._spark_jdbc_reader: DataFrameReader = self._spark_session.read\
            .format("jdbc")\
            .options(self._spark_jdbc_options)

    def _insert_application_log(self, application_branch: str,
                                bancll_name: Union[str, None],
                                dt_business_date: Union[str, None],
                                impacted_table: str,
                                exception_message: Union[str, None] = None):

        spark_context: SparkContext = self._spark_session.sparkContext
        logging_record_dict: dict = {

            "application_id": spark_context.applicationId(),
            "application_name": spark_context.appName,
            "application_branch": application_branch,
            "application_start_time": datetime.fromtimestamp(spark_context.startTime / 1000),
            "application_end_time": datetime.now(),
            "bancll_name": bancll_name if bancll_name is not None else str(None),
            "dt_business_date": dt_business_date if dt_business_date is not None else str(None),
            "impacted_table": impacted_table,
            "exception_message": exception_message if exception_message is not None else str(None),
            "application_finish_code": -1 if exception_message is not None else 0,
            "application_finish_status": "FAILED" if exception_message is not None else "SUCCESSED"
        }

        logging_record_df: DataFrame = self._spark_session.createDataFrame(pd.DataFrame.from_dict(logging_record_dict))

        database_name: str = self._job_properties["spark"]["database"]
        table_name: str = self._job_properties["spark"]["application_log_table_name"]
        if self._table_exists(database_name, table_name):

            self.__logger.info(f"Logging table \'{database_name}.{table_name}\' already exists")

        else:

            self.__logger.warning(f"Logging table \'{database_name}.{table_name}\' does not exists. So, creating it now")

        self._write_to_jdbc(logging_record_df, database_name, table_name, "append")

    def _read_mapping_specification_from_file(self) -> DataFrame:

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
        specification_df: DataFrame = self._spark_session.read \
            .format("csv") \
            .option("path", specification_file_path) \
            .option("sep", specification_file_sep) \
            .option("header", specification_file_header) \
            .option("schema", from_json_to_struct_type(specification_file_schema)) \
            .load()

        self.__logger.info(f"Successfully loaded file at path \'{specification_file_path}\' as a pyspark.sql.DataFrame")
        specification_df.printSchema()
        return specification_df

    def _table_exists(self, database_name: str, table_name: str) -> bool:

        self._mysql_cursor.execute(f"USE {database_name}")
        self._mysql_cursor.execute("SHOW TABLES")

        # GET LIST OF EXISTING TABLES WITHIN GIVEN DATABASE
        existing_tables: List[str] = list(map(lambda x: x[0].lower(), self._mysql_cursor))
        self.__logger.info(f"""Existing tables within DB \'{database_name}\': 
                                    {", ".join(map(lambda x: "'{}'".format(x), existing_tables))}""")

        return table_name.lower() in existing_tables

    def _write_to_jdbc(self, dataframe: DataFrame, database_name: str, table_name: str, savemode: str, truncate: bool = False):

        full_table_name: str = f"{database_name}.{table_name}"
        truncate_option: str = "true" if savemode.lower() == "overwrite" and truncate else "false"
        self.__logger.info(f"Starting to insert data into table \'{full_table_name}\' using savemode \'{savemode}\'")

        dataframe.write\
            .format("jdbc")\
            .options(self._spark_jdbc_options)\
            .option("dbtable", full_table_name)\
            .option("truncate", truncate_option)\
            .mode(savemode)\
            .save()

        self.__logger.info(f"Successfully inserted data into table \'{full_table_name}\' using savemode \'{savemode}\'")
