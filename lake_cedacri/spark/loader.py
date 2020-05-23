import logging
import yaml


class BancllLoader:

    def __init__(self, spark_context, sql_context, spark_job_yaml_file):

        self.logger = logging.getLogger(__name__)
        self.job_properties = {}
        with open(spark_job_yaml_file, "r") as f:

            self.job_properties = yaml.safe_load(f.read())

        self.logger.info("Successfully loaded job properties dict")
        self.raw_database_name = self.job_properties["database"]["raw"]
        self.trusted_database_name = self.job_properties["database"]["trusted"]

        self.t_trd_mapping_specification_name = self.job_properties["table"]["specification"]
        self.t_trd_mapping_specification_full_name = "{}.{}".format(self.trusted_database_name, self.t_trd_mapping_specification_name)

        self.raw_dataload_log_name = self.job_properties["table"]["log"]
        self.raw_dataload_log_full_name = "{}.{}".format(self.trusted_database_name, self.raw_dataload_log_name)

        self.spark_context = spark_context
        self.sql_context = sql_context
        self.t_trd_mapping_specification_df = self.sql_context.table(self.t_trd_mapping_specification_full_name)

    def __get_table_if_exists(self, database_name, table_name):

        if table_name in self.sql_context.tableNames(database_name):

            return self.sql_context.table("{}.{}".format(database_name, table_name))
        else:

            raise

    def run(self, bancll_name, dt_business_date):

        pass
