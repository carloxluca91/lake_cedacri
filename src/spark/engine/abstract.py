import logging
import configparser
import mysql.connector

from abc import ABC
from pyspark.sql import SparkSession


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

        self._jdbc_config: dict = {

            "host": self._job_properties["jdbc"]["host"],
            "port": self._job_properties["jdbc"]["port"],
            "user": self._job_properties["jdbc"]["user"],
            "password": self._job_properties["jdbc"]["password"],
            "ssl_disabled": bool(self._job_properties["jdbc"]["useSSL"])
        }

        self._mysql_connection: mysql.connector.MySQLConnection = mysql.connector.connect(** self._jdbc_config)
