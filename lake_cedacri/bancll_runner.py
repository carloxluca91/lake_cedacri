import logging


class BancllRunner:

    def __init__(self, bancll_names, dt_business_date, spark_job_yaml_file):

        self.logger = logging.getLogger(__name__)
        self.bancll_names = bancll_names
        self.dt_business_date = dt_business_date
        self.spark_job_yaml_file = spark_job_yaml_file

    def run(self):

        from datetime import datetime
        from lake_cedacri.spark.loader import BancllLoader
        from lake_cedacri.time.formats import CLASSIC_FORMATS, JAVA_TO_PYTHON_FORMAT
        from pyspark import SparkContext, SparkConf
        from pyspark.sql import HiveContext

        spark_context = SparkContext(SparkConf())
        sql_context = HiveContext(spark_context)

        try:

            bancll_loader = BancllLoader(spark_context, sql_context, self.spark_job_yaml_file)
            for (bancll_index, bancll_name) in enumerate(self.bancll_names):

                self.logger.info("Starting to load data for BANCLL #{} ({}), dt_business_date = {}".format(
                    bancll_index, bancll_name, self.dt_business_date))

                bancll_loader.run(bancll_name, self.dt_business_date)

        except Exception as exception:

            self.logger.error("Caught exception while loading data for BANCLL")

            # TODO: creazione riga per log di errore

            timestamp_format = JAVA_TO_PYTHON_FORMAT[CLASSIC_FORMATS["timestamp"]]
            application_id = spark_context.applicationId
            application_name = spark_context.appName
            application_start_time_in_epoch_seconds = spark_context.startTime / 1000
            application_start_time_str = datetime \
                .fromtimestamp(application_start_time_in_epoch_seconds) \
                .strftime(timestamp_format)

            application_end_time = datetime.now().strftime(timestamp_format)
            application_status = "FAILED"
            application_exception_type = type(exception)
            application_failure_message = exception.message
