import logging
import random
from collections import namedtuple
from typing import List, Any

from pyspark.sql import Row, SparkSession, functions, Column
from pyspark.sql.types import StructType, StructField
from pyspark_utils.sql import DataTypeUtils, DataFrameUtils, SQLParser
from time_utils import TimeUtils

from lake_cedacri.data.random import RandomChoice, RandomNumber, RandomDate
from lake_cedacri.data.enum import RandomFunctionEnum

SPECIFICATION_RECORD_COLUMNS = ["flusso", "sorgente_rd", "colonna_rd", "posizione",
                                "funzione", "tipo_colonna_rd", "flag_nullable", "funzione_spark"]
SpecificationRecord = namedtuple("SpecificationRecord", SPECIFICATION_RECORD_COLUMNS)


class DataFactory:

    _logger = logging
    _FUNCTIONS = {

        RandomFunctionEnum.RANDOM_CHOICE: RandomChoice,
        RandomFunctionEnum.RANDOM_NUMBER: RandomNumber,
        RandomFunctionEnum.RANDOM_DATE: RandomDate
    }

    @classmethod
    def create_data(cls, spark_session: SparkSession, specification_rows: List[Row], number_of_records: int, dt_riferimento: str):

        # Turn generic rows into namedtuples
        specification_records: List[SpecificationRecord] = list(map(lambda x: SpecificationRecord(**x.asDict()), specification_rows))
        cls._logger.info(f"Successfully turned {len(specification_rows)} {Row.__name__}(s) into {SpecificationRecord.__name__} object(s)")

        # Initialize random data and schema with 'row_id' column
        random_raw_dataframe: List[List[Any]] = [list(range(1, number_of_records + 1))]
        random_dataframe_schema = StructType([StructField("row_id", DataTypeUtils.spark_datatype("int"), False)])
        for i, sr in enumerate(specification_records):

            # Match Python function to apply
            function_str: str = sr.funzione
            matching_functions: List[RandomFunctionEnum] = list(filter(lambda x: x.match(function_str), [r for r in RandomFunctionEnum]))
            if len(matching_functions) == 0:

                value_error_msg: str = f"Unable to match function expression '{function_str}'. " \
                                       f"Suggest to look for the nearest regex within {RandomFunctionEnum.__name__} " \
                                       f"and compare it with the unmatching expression using https://regex101.com/ ;)"
                raise ValueError(value_error_msg)

            else:

                # Create random data
                random_function = cls._FUNCTIONS[matching_functions[0]](function_str)
                cls._logger.info(f"Matched random function <{random_function.to_string}>")
                random_data: List[Any] = random_function.create_data(number_of_records)

                # If nullable flag is on, replace some data with None
                nullable: bool = sr.flag_nullable is not None and sr.flag_nullable.lower() == "y"
                random_data_maybe_nullable: List[Any] = random_data if not nullable else \
                    [datum if not prob else None for (datum, prob) in
                     zip(random_data, random.choices([0, 1], weights=[0.99, 0.01], k=number_of_records))]

                # Update data collection and related StructType
                random_raw_dataframe.append(random_data_maybe_nullable)
                random_dataframe_schema.add(sr.colonna_rd,
                                             DataTypeUtils.spark_datatype(sr.tipo_colonna_rd),
                                             nullable=True)

                cls._logger.info(f"Successfully created data for column # {i} '{sr.colonna_rd}', "
                                 f"dataType = '{sr.tipo_colonna_rd}', "
                                 f"flag_nullable = '{sr.flag_nullable}'")

        # Create dataframe with random data
        list_of_random_rows: List[Row] = [Row(*t) for t in zip(*random_raw_dataframe)]
        random_dataframe = spark_session.createDataFrame(list_of_random_rows, schema=random_dataframe_schema)\
            .withColumn("ts_inserimento", functions.lit(TimeUtils.datetime_now()))\
            .withColumn("dt_inserimento", functions.lit(TimeUtils.date_now()))\
            .withColumn("dt_riferimento", functions.lit(TimeUtils.to_date(dt_riferimento, TimeUtils.java_default_dt_format())))

        cls._logger.info(f"Successfully created random dataframe. Schema: {DataFrameUtils.schema_tree_string(random_dataframe)}")

        # Transform columns according to provided Spark SQL function, if any
        specification_records_with_spark_transformation: List[SpecificationRecord] = list(
            filter(lambda x: x.funzione_spark is not None, specification_records))

        if len(specification_records_with_spark_transformation) > 0:

            for i, sr_with_transformation in enumerate(specification_records_with_spark_transformation):

                transformed_column: Column = SQLParser.parse(sr_with_transformation.funzione_spark)
                random_dataframe = random_dataframe.withColumn(sr_with_transformation.colonna_rd, transformed_column)
                cls._logger.info(f"Successfully transformed column '{sr_with_transformation.colonna_rd}'")

            cls._logger.info(f"Successfully applied each Spark transformation. Schema: {DataFrameUtils.schema_tree_string(random_dataframe)}")

        else:

            cls._logger.info(f"No transformation to apply to random dataframe")

        return random_dataframe
