import configparser
import logging
import os
from abc import ABC
# from datetime import date, datetime
from typing import List, Tuple, Union

import mysql.connector
from pyspark import SparkContext
from pyspark.sql import DataFrame, DataFrameReader
from pyspark.sql.functions import lit
from pyspark_utils.sql import DataFrameUtils, SparkSessionUtils
from time_utils import TimeUtils

from lake_cedacri.utils import SparkUtils


class AbstractEngine(ABC):

    def __init__(self, job_ini_file: str):

        self._logger = logging.getLogger(__name__)
        self._job_properties: configparser.ConfigParser = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        if not os.path.exists(job_ini_file):

            self._logger.error(f"File {job_ini_file} does not exist (or cannot be found)")
            raise FileNotFoundError(job_ini_file)

        else:

            self._logger.info(f"File {job_ini_file} exists. Trying to load it as a dict")

        with open(job_ini_file, mode="r", encoding="UTF-8") as f:

            self._job_properties.read_file(f)
            self._logger.info(f"Successfully loaded job properties dict. Job properties sections: {self._job_properties.sections()}")

        self._spark_session = SparkSessionUtils.get_or_create()

        jdbc_host = self._job_properties["jdbc"]["host"]
        jdbc_port = int(self._job_properties["jdbc"]["port"])
        jdbc_url = self._job_properties["jdbc"]["url"]
        jdbc_user = self._job_properties["jdbc"]["user"]
        jdbc_password = self._job_properties["jdbc"]["password"]
        jdbc_driver = self._job_properties["jdbc"]["driver"]
        jdbc_use_ssl = self._job_properties["jdbc"]["useSSL"].lower()

        self._logger.info(f"JDBC host:port = '{jdbc_host}:{jdbc_port}'")
        self._logger.info(f"JDBC url = '{jdbc_url}'. User = '{jdbc_user}'. Password = '{jdbc_password}")
        self._logger.info(f"JDBC driver = '{jdbc_driver}'. UseSSL flag = '{jdbc_use_ssl}'")

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

        # MySQL Python connector
        self._mysql_connection: mysql.connector.MySQLConnection = mysql.connector.connect(** self._connector_options)
        self._logger.info(f"Successfully estabilished connection to '{self._connector_options['host']}:{str(self._connector_options['port'])}' "
                           f"with credentials ('{self._connector_options['user']}', '{self._connector_options['password']}')")

        # MySQL Python cursor (for SQL statements execution)
        self._mysql_cursor: mysql.connector.connection.MySQLCursor = self._mysql_connection.cursor()

        # Spark JDBC reader
        self._spark_jdbc_reader: DataFrameReader = self._spark_session.read \
            .format("jdbc") \
            .options(** self._spark_jdbc_options)

    def _insert_application_log(self, application_branch: str,
                                bancll_name: Union[str, None],
                                dt_riferimento: Union[str, None],
                                impacted_table: Union[str, None],
                                exception_message: Union[str, None] = None):

        spark_context: SparkContext = self._spark_session.sparkContext
        dt_riferimento_date = TimeUtils.to_date(dt_riferimento, TimeUtils.java_default_dt_format()) if dt_riferimento is not None else None

        logging_record_tuple_list: List[Tuple] = [(
            spark_context.applicationId,
            spark_context.appName,
            TimeUtils.datetime_from_epoch_millis(spark_context.startTime),
            TimeUtils.date_from_epoch_millis(spark_context.startTime),
            application_branch,
            bancll_name,
            dt_riferimento_date,
            impacted_table,
            TimeUtils.datetime_now(),
            TimeUtils.date_now(),
            -1 if exception_message is not None else 0,
            "KO" if exception_message is not None else "OK",
            exception_message)]

        logging_table_schema: str = self._job_properties["path"]["application_log_schema_file_path"]
        logging_record_df: DataFrame = self._spark_session.createDataFrame(
            logging_record_tuple_list,
            SparkUtils.to_struct_type(logging_table_schema))

        database_name: str = self._job_properties["spark"]["database"]
        table_name: str = self._job_properties["spark"]["application_log_table_name"]
        if self._table_exists(database_name, table_name):

            self._logger.info(f"Logging table '{database_name}.{table_name}' already exists")

        else:

            self._logger.warning(f"Logging table '{database_name}.{table_name}' does not exists. So, creating it now")

        self._write_to_jdbc(logging_record_df, database_name, table_name, "append")

    def _read_from_jdbc(self, database_name: str, table_name: str) -> DataFrame:

        self._logger.info(f"Starting to load table '{database_name}.{table_name}'")

        dataframe: DataFrame = self._spark_jdbc_reader \
            .option("dbtable", f"{database_name}.{table_name}") \
            .load()

        self._logger.info(f"Successfully loaded table '{database_name}.{table_name}'")
        return dataframe

    def _read_mapping_specification_from_file(self) -> DataFrame:

        # Retrieve setting for reading tsv file
        specification_tsv_file_path: str = self._job_properties["path"]["specification_tsv_file_path"]
        specification_schema_file_path: str = self._job_properties["path"]["specification_schema_file_path"]

        specification_tsv_file_sep: str = self._job_properties["spark"]["specification_tsv_file_delimiter"]
        specification_tsv_file_header_str: str = self._job_properties["spark"]["specification_tsv_file_header"]
        specification_tsv_file_header_bool: bool = True if specification_tsv_file_header_str.lower() == "true" else False

        self._logger.info(f"Attempting to load file at path '{specification_tsv_file_path}' as a pyspark.sql.DataFrame")

        # Read the file
        specification_df: DataFrame = self._spark_session.read \
            .format("csv") \
            .option("sep", specification_tsv_file_sep) \
            .option("header", specification_tsv_file_header_bool) \
            .load(specification_tsv_file_path, schema=SparkUtils.to_struct_type(specification_schema_file_path))

        self._logger.info(f"Successfully loaded file at path '{specification_tsv_file_path}' as a pyspark.sql.DataFrame")
        self._logger.info(f"Dataframe original schema (provided): \n{DataFrameUtils.schema_tree_string(specification_df)}")

        return specification_df \
            .withColumn("ts_inizio_validita", lit(TimeUtils.datetime_now())) \
            .withColumn("dt_inizio_validita", lit(TimeUtils.date_now()))

    def _table_exists(self, database_name: str, table_name: str) -> bool:

        self._logger.info(f"Checking existence of table '{database_name}'.'{table_name}'")
        self._mysql_cursor.execute(f"SHOW TABLES IN {database_name}")

        # Get list of existing tables
        existing_tables: List[str] = list(map(lambda x: x[0].lower(), self._mysql_cursor))
        existing_tables_str: str = ", ".join(map(lambda x: f"'{x}'", existing_tables))
        self._logger.info(f"Existing tables within DB '{database_name}': {existing_tables_str}")

        return table_name.lower() in existing_tables

    def _write_to_jdbc(self, dataframe: DataFrame, database_name: str, table_name: str, savemode: str, truncate: bool = False):

        full_table_name: str = f"{database_name}.{table_name}"
        truncate_option: str = "true" if savemode.lower() == "overwrite" and truncate else "false"
        self._logger.info(f"Starting to insert data into table '{full_table_name}' using savemode '{savemode}'. "
                           f"Value of 'truncate' option: {truncate_option}")
        self._logger.info(f"DataFrame to be written has schema: {DataFrameUtils.schema_tree_string(dataframe)}")

        dataframe.write \
            .format("jdbc") \
            .options(** self._spark_jdbc_options) \
            .option("dbtable", full_table_name) \
            .option("truncate", truncate_option) \
            .mode(savemode) \
            .save()

        self._logger.info(f"Successfully inserted data into table '{full_table_name}' using savemode '{savemode}'")
