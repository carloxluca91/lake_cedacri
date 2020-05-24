
class BancllLoader:

    def __init__(self, spark_context, sql_context, job_properties):

        import logging

        self.__logger = logging.getLogger(__name__)
        self.__job_properties = job_properties
        self.raw_database_name = self.__job_properties["database"]["raw"]
        self.trusted_database_name = self.__job_properties["database"]["trusted"]

        self.mapping_specification_name = self.__job_properties["table"]["specification"]
        self.mapping_specification_full_name = "{}.{}".format(self.trusted_database_name, self.mapping_specification_name)

        self.dataload_log_name = self.__job_properties["table"]["log"]
        self.dataload_log_full_name = "{}.{}".format(self.trusted_database_name, self.dataload_log_name)

        self.__spark_context = spark_context
        self.__sql_context = sql_context
        self.__mapping_specification_df = self.__sql_context.table(self.mapping_specification_full_name)

    def __get_table_if_exists(self, database_name, table_name):

        from lake_cedacri.spark.exceptions import UnexistingTableError

        if table_name in self.__sql_context.tableNames(database_name):

            return self.__sql_context.table("{}.{}".format(database_name, table_name))

        else:

            raise UnexistingTableError(database_name, table_name)

    def run(self, bancll_name, dt_business_date):

        from pyspark.sql import SparkSession
        from pyspark.sql import functions
        from lake_cedacri.spark.exceptions import InvalidSourceError

        mapping_specification_filtered = SparkSession.createDataFrame()
        # mapping_specification_filtered = self.__mapping_specification_df.filter(functions.col("flusso") == bancll_name)
        bancll_raw_table_name_rows = mapping_specification_filtered.selectExpr("sorgente_rd").distinct().collect
        if len(bancll_raw_table_name_rows) > 1:

            bancll_raw_sources = list(map(lambda x: x["sorgente_rd"], bancll_raw_table_name_rows))
            raise InvalidSourceError(bancll_name, bancll_raw_sources)

        bancll_raw_column_specs = mapping_specification_filtered.selectExpr("colonna_rd", "tipo_colonna_rd", "posizione_iniziale").collect()
        bancll_raw_column_names = list(map(lambda x: x["colonna_rd"], bancll_raw_column_specs))
        bancll_raw_column_types = list(map(lambda x: x["tipo_colonna_rd"], bancll_raw_column_specs))
        bancll_raw_column_posiions = list(map(lambda x: x["posizione_iniziale"], bancll_raw_column_specs))

        # TODO: implement checks (and related exceptions):
        # 1. uniqueness of column names
        # 2. data type
        # 3. continuity of column ordering
