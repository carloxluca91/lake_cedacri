
class IngestionRunner:

    def __init__(self, job_properties_dict):

        import logging

        from pyspark import SparkContext, SparkConf
        from pyspark.sql import HiveContext

        self.__logger = logging.getLogger(__name__)
        self.__job_properties_dict = job_properties_dict

        # INITIALIZE SPARKCONTEXT AND SQLCONTEXT
        self.__spark_context = SparkContext(SparkConf())
        self.__sql_context = HiveContext(self.__spark_context)
        self.__logger.info("Successfully initialized both SparkContext and SqlContext")

    def run(self, bancll_names, dt_business_date):

        from lake_cedacri.spark.loader import BancllLoader

        try:

            # INTIALIZE ENGINE FOR BANCCL INGESTION
            bancll_loader = BancllLoader(self.__spark_context, self.__sql_context, self.__job_properties_dict)
            self.__logger.info("Successfully initialized engine for BANCLL ingestion")
            for (bancll_index, bancll_name) in enumerate(bancll_names):

                self.__logger.info("Starting to ingest data for BANCLL #{} ({}), dt_business_date = {}".format(
                    bancll_index, bancll_name, dt_business_date))

                # RUN INGESTION FOR THE CURRENT BANCLL
                bancll_loader.run(bancll_name, dt_business_date)

                self.__logger.info("Successfully ingested data for BANCLL #{} ({}), dt_business_date = {}".format(
                    bancll_index, bancll_name, dt_business_date))

                # DEFINE A LOGGING RECORD REPORTING THE SUCCESSED INGESTION
                self.__insert_logging_record(True)

        except Exception as exception:

            # DEFINE A LOGGING RECORD REPORTING THE EXCEPTION
            self.__logger.error("Caught exception while ingesting data")
            self.__insert_logging_record(False, exception)

        finally:

            self.__logger.info("Exiting the application")

    def __insert_logging_record(self, has_application_successed, exception=None):

        import pandas as pd
        from datetime import datetime

        log_table_database = self.__job_properties_dict["database"]["trusted"]
        log_table_name = self.__job_properties_dict["table"]["log"]
        log_table_full_name = "{}.{}".format(log_table_database, log_table_name)
        self.__logger.info("Starting to insert logging record into table {}".format(log_table_full_name))

        log_record_dict = {

            "application_id": [self.__spark_context.applicationId],
            "application_name": [self.__spark_context.appName],
            "application_start_time": [datetime.fromtimestamp(self.__spark_context.startTime / 1000)],
            "application_end_time": [datetime.now()],
            "application_exit_code": [0 if has_application_successed else -1],
            "application_exit_message": ["SUCCESSED" if has_application_successed else "FAILED"],
            "application_exception_type": [None if has_application_successed else type(exception)],
            "application_exception_message": [None if has_application_successed else exception.message]
        }

        log_record_pandas_df = pd.DataFrame.from_dict(log_record_dict)
        log_record_spark_df = self.__sql_context.createDataFrame(log_record_pandas_df)
        self.__logger.info("Successfully created logging record for table {}".format(log_table_full_name))

        # IF LOG TABLE ALREADY EXISTS, WE JUST NEED TO INSERT INTO
        if log_table_name in self.__sql_context.tableNames(log_table_database):

            self.__logger.info("Logging table {} already exists".format(log_table_full_name))
            self.__logger.info("Starting to insert logging record into table {}".format(log_table_full_name))
            log_record_spark_df.write.insertInto(log_table_full_name)
            self.__logger.info("Successfully inserted logging record into table {}".format(log_table_full_name))

        else:

            self.__logger.warning("Logging table {} does not exist yet. Creating it now".format(log_table_full_name))

            # TODO: INDAGARE SAVE_AS_TABLE
            log_record_spark_df.write.format("parquet").saveAsTable(log_table_full_name)
            self.__logger.info("Successfully created logging table {}".format(log_table_full_name))
