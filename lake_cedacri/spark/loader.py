
def _validate_bancll_specification(bancll_name, bancll_specification_rows):

    from lake_cedacri.spark.types import SPARK_TYPES

    # NO CONFIGURATION FOUND ?
    if len(bancll_specification_rows) == 0:

        from lake_cedacri.spark.exceptions import UndefinedBANCLLError
        raise UndefinedBANCLLError(bancll_name)

    # VALIDATION OF BANCLL SPECIFICATION
    # [a] IS COLUMN 'sorgente_rd' UNIQUE FOR THE PROVIDED BANCLL ?
    bancll_raw_table_names = set(map(lambda x: x["sorgente_rd"], bancll_specification_rows))
    if len(bancll_raw_table_names) > 1:

        from lake_cedacri.spark.exceptions import InvalidBANCLLSourceError
        raise InvalidBANCLLSourceError(bancll_name, bancll_raw_table_names)

    # EXTRACT INFO RELATED TO RAW_COLUMN SPECIFICATIONS
    bancll_raw_column_specs = bancll_specification_rows\
        .selectExpr("colonna_rd", "tipo_colonna_rd", "posizione_iniziale", "formato_input")\
        .collect()

    bancll_raw_column_names = list(map(lambda x: x["colonna_rd"], bancll_raw_column_specs))
    bancll_raw_column_types = list(map(lambda x: x["tipo_colonna_rd"], bancll_raw_column_specs))
    bancll_raw_column_positions = list(map(lambda x: x["posizione_iniziale"], bancll_raw_column_specs))
    bancll_raw_column_input_formats = list(map(lambda x: x["formato_input"], bancll_raw_column_specs))

    # [b] DUPLICATED COLUMNS IN 'colonna_rd' ?
    if len(bancll_raw_column_names) > len(set(bancll_raw_column_names)):

        from lake_cedacri.spark.exceptions import DuplicateColumnError
        bancll_raw_column_names_duplicated = set(filter(lambda x: bancll_raw_column_names.count(x) > 1, bancll_raw_column_names))
        raise DuplicateColumnError(bancll_name, bancll_raw_column_names_duplicated)

    # [c] CORRECT DATA_TYPES IN 'tipo_colonna_rd' ?
    unknown_data_types = filter(lambda x: x not in SPARK_TYPES, set(bancll_raw_column_types))
    if len(unknown_data_types) > 0:

        from lake_cedacri.spark.exceptions import UnknownDataTypeError
        raise UnknownDataTypeError(bancll_name, unknown_data_types)

    # [d] CORRECT ORDERING STATED IN 'posizione_iniziale' ?
    # [d.1] ARE BOTH MIN_INDEX AND MAX_INDEX NON-NEGATIVE ?
    min_initial_position, max_initial_position = min(bancll_raw_column_positions), max(bancll_raw_column_positions)
    if min_initial_position < 0 or max_initial_position < 0:

        from lake_cedacri.spark.exceptions import NegativeColumnIndexError
        negative_index = min_initial_position if min_initial_position < 0 else max_initial_position
        raise NegativeColumnIndexError(bancll_name, negative_index)

    # [d.2] IS MIN_INDEX CORRECT ?
    if min_initial_position != 0:

        from lake_cedacri.spark.exceptions import InvalidMinColumnIndexError
        raise InvalidMinColumnIndexError(bancll_name, min_initial_position)

    # [d.3] IS MAX_INDEX GREATER (OR EQUAL AT LEAST) THAN MIN_INDEX
    if max_initial_position < min_initial_position:

        from lake_cedacri.spark.exceptions import InvalidMaxColumnIndexError
        raise InvalidMaxColumnIndexError(bancll_name, max_initial_position, min_initial_position)

    # [d.4] IS THE DEFINED RANGE CONTINUOUS ?
    missing_positions = filter(lambda x: x not in bancll_raw_column_positions, range(min_initial_position, max_initial_position + 1))
    if len(missing_positions) > 0:

        from lake_cedacri.spark.exceptions import NonContinuousRangeError
        raise NonContinuousRangeError(bancll_name, missing_positions)

    # [e] ARE ALL DATE_FORMATS CORRECTLY DEFINED ?
    # TODO


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

        if table_name in self.__sql_context.tableNames(database_name):

            self.__logger.info("Table {} exists within database {}".format(table_name, database_name))
            return self.__sql_context.table("{}.{}".format(database_name, table_name))

        else:

            from lake_cedacri.spark.exceptions import UnexistingTableError
            raise UnexistingTableError(database_name, table_name)

    def run(self, bancll_name, dt_business_date):

        from pyspark.sql import functions
        from lake_cedacri.spark.types import SPARK_TYPES

        # FILTER SPECIFICATION TABLE ACCORDING TO PROVIDED BANCLL
        bancll_specification_rows = self.__mapping_specification_df\
            .filter(functions.col("flusso") == bancll_name)\
            .collect()

        _validate_bancll_specification(bancll_name, bancll_specification_rows)

