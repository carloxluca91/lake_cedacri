import os
import configparser
import logging
import mysql.connector

from abc import ABC
from datetime import date, datetime
from pyspark import SparkContext
from pyspark.sql import DataFrame, DataFrameReader, SparkSession
from pyspark.sql.types import StructField, StructType
from src.spark.types import DATA_TYPE_DICT
from src.spark.time import BUSINESS_DATE_FORMAT, JAVA_TO_PYTHON_FORMAT
from typing import List, Tuple, Union


def from_json_to_struct_type(json_string: str) -> StructType:

    import json

    column_list: List[dict] = json.loads(json_string, encoding="UTF-8")["schema"]
    return StructType(list(map(lambda x: StructField(name=x["name"],
                                                     dataType=DATA_TYPE_DICT[x["type"]],
                                                     nullable=True if x["nullable"].lower() == "true" else False),
                               column_list)))


class AbstractEngine(ABC):

    def __init__(self, job_ini_file: str):

        self.__logger = logging.getLogger(__name__)
        self._job_properties: configparser.ConfigParser = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        if not os.path.exists(job_ini_file):

            self.__logger.error(f"File {job_ini_file} does not exist (or cannot be found)")
            raise FileNotFoundError(job_ini_file)

        else:

            self.__logger.info(f"File {job_ini_file} exists. Trying to load it as a dict")

        with open(job_ini_file, mode="r", encoding="UTF-8") as f:

            self._job_properties.read_file(f)
            self.__logger.info("Successfully loaded job properties dict")
            self.__logger.info(f"Job properties sections: {self._job_properties.sections()}")

        self.__logger.info(f"Trying to get or create SparkSession")
        self._spark_session: SparkSession = SparkSession \
            .builder \
            .getOrCreate()

        self.__logger.info(f"Successfully got or created SparkSession")
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
        host_port_str: str = f"{self._connector_options['host']}:{str(self._connector_options['port'])}"
        username_password_str: str = f"({self._connector_options['user']}, {self._connector_options['password']})"
        self.__logger.info(f"Successfully estabilished connection to {host_port_str} with credentials {username_password_str}")

        # MySQL Python CURSOR (FOR QUERY EXECUTION)
        self._mysql_cursor: mysql.connector.connection.MySQLCursor = self._mysql_connection.cursor()

        # SPARK JDBC READER
        self._spark_jdbc_reader: DataFrameReader = self._spark_session.read\
            .format("jdbc")\
            .options(** self._spark_jdbc_options)

    def _insert_application_log(self, application_branch: str,
                                bancll_name: Union[str, None],
                                dt_business_date: Union[str, None],
                                impacted_table: str,
                                exception_message: Union[str, None] = None):

        spark_context: SparkContext = self._spark_session.sparkContext

        business_date_format: str = JAVA_TO_PYTHON_FORMAT[BUSINESS_DATE_FORMAT]
        dt_business_date: date = datetime.strptime(dt_business_date, business_date_format).date() if dt_business_date is not None else None

        logging_record_tuple_list: List[Tuple] = [(
            spark_context.applicationId,
            spark_context.appName,
            application_branch,
            datetime.fromtimestamp(spark_context.startTime / 1000),
            datetime.now(),
            bancll_name if bancll_name is not None else None,
            dt_business_date if dt_business_date is not None else None,
            impacted_table,
            exception_message if exception_message is not None else None,
            -1 if exception_message is not None else 0,
            "FAILED" if exception_message is not None else "SUCCESSED")]

        logging_table_schema: str = self._job_properties["spark"]["application_log_schema"]
        logging_record_df: DataFrame = self._spark_session\
            .createDataFrame(logging_record_tuple_list,
                             from_json_to_struct_type(logging_table_schema))

        database_name: str = self._job_properties["spark"]["database"]
        table_name: str = self._job_properties["spark"]["application_log_table_name"]
        if self._table_exists(database_name, table_name):

            self.__logger.info(f"Logging table \'{database_name}.{table_name}\' already exists")

        else:

            self.__logger.warning(f"Logging table \'{database_name}.{table_name}\' does not exists. So, creating it now")

        self._write_to_jdbc(logging_record_df, database_name, table_name, "append")

    def _read_from_jdbc(self, database_name: str, table_name: str) -> DataFrame:

        self.__logger.info(f"Starting to load table \'{database_name}\'.\'{table_name}\'")

        dataframe: DataFrame = self._spark_jdbc_reader\
            .option("dbtable", f"{database_name}.{table_name}")\
            .load()

        self.__logger.info(f"Successfully loaded table \'{database_name}\'.\'{table_name}\'")
        return dataframe

    def _read_mapping_specification_from_file(self) -> DataFrame:

        # RETRIEVE SETTINGS FOR FILE READING
        specification_file_path: str = self._job_properties["path"]["specification_file_path"]
        specification_file_sep: str = self._job_properties["spark"]["specification_file_delimiter"]
        specification_file_header_string: str = self._job_properties["spark"]["specification_file_header"]
        specification_file_header: bool = True if specification_file_header_string.lower() == "true" else False
        specification_file_schema: str = self._job_properties["spark"]["specification_file_schema"]

        self.__logger.info(f"Attempting to load file at path \'{specification_file_path}\' as a pyspark.sql.DataFrame")

        # READ THE FILE USING ACTIVE SPARK_SESSION
        specification_df: DataFrame = self._spark_session.read \
            .format("csv") \
            .option("sep", specification_file_sep) \
            .option("header", specification_file_header) \
            .load(specification_file_path, schema=from_json_to_struct_type(specification_file_schema))

        self.__logger.info(f"Successfully loaded file at path \'{specification_file_path}\' as a pyspark.sql.DataFrame")
        specification_df.printSchema()
        return specification_df

    def _table_exists(self, database_name: str, table_name: str) -> bool:

        self._mysql_cursor.execute(f"SHOW TABLES IN {database_name}")

        # GET LIST OF EXISTING TABLES WITHIN GIVEN DATABASE
        existing_tables: List[str] = list(map(lambda x: x[0].lower(), self._mysql_cursor))
        existing_tables_str: str = ", ".join(map(lambda x: f"\'{x}\'", existing_tables))
        self.__logger.info(f"Existing tables within DB \'{database_name}\': {existing_tables_str}")

        return table_name.lower() in existing_tables

    def _write_to_jdbc(self, dataframe: DataFrame, database_name: str, table_name: str, savemode: str, truncate: bool = False):

        full_table_name: str = f"{database_name}.{table_name}"
        truncate_option: str = "true" if savemode.lower() == "overwrite" and truncate else "false"
        self.__logger.info(f"Value of \'truncate\' option: {truncate_option}")
        self.__logger.info(f"Starting to insert data into table \'{full_table_name}\' using savemode \'{savemode}\'")

        dataframe.write\
            .format("jdbc")\
            .options(** self._spark_jdbc_options)\
            .option("dbtable", full_table_name)\
            .option("truncate", truncate_option)\
            .mode(savemode)\
            .save()

        self.__logger.info(f"Successfully inserted data into table \'{full_table_name}\' using savemode \'{savemode}\'")
