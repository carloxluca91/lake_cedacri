import logging
import configparser
import mysql.connector

from abc import ABC
from pyspark.sql import DataFrame, DataFrameReader, SparkSession


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

    def _write_to_jdbc(self, dataframe: DataFrame, database_name: str, table_name: str, savemode: str):

        full_table_name: str = f"{database_name}.{table_name}"
        self.__logger.info(f"Starting to insert data into table \'{full_table_name}\' using savemode \'{savemode}\'")

        # noinspection PyBroadException
        try:

            dataframe.write\
                .format("jdbc")\
                .options(self._spark_jdbc_options)\
                .option("dbtable", full_table_name)\
                .mode(savemode)\
                .save()

            self.__logger.info(f"Successfully inserted data into table \'{full_table_name}\' using savemode \'{savemode}\'")

        except Exception as e:

            self.__logger.error(f"Unable to save data into table \'{full_table_name}\' using savemode \'{savemode}\'")
            self.__logger.error(f"Message: {str(e)}")
            self.__logger.exception(e)
