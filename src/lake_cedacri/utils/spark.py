import json
import logging
import os
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType


class SparkUtils:

    _DATA_TYPE_DICT = {

        "string": StringType(),
        "int": IntegerType(),
        "double": DoubleType(),
        "date": DateType(),
        "timestamp": TimestampType()
    }

    @staticmethod
    def schema_tree_string(dataframe: DataFrame) -> str:

        schema_json: dict = dataframe.schema.jsonValue()
        schema_str_list: List[str] = list(map(lambda x: f" |-- {x['name']}: {x['type']} (nullable: {str(x['nullable']).lower()})",
                                              schema_json["fields"]))
        schema_str_list.insert(0, "\nroot")
        schema_str_list.append("\n")

        return "\n".join(schema_str_list)

    @classmethod
    def to_struct_type(cls, json_file_path: str) -> StructType:

        logger = logging.getLogger(__name__)
        if os.path.exists(json_file_path):

            logger.info(f"File '{json_file_path}' exists, trying to parse it in order to detect schema")

        else:

            raise FileNotFoundError

        with open(json_file_path, "r", encoding="UTF-8") as f:

            json_content: dict = json.load(f)

        structtype_from_json: StructType = StructType(list(map(
            lambda x: StructField(name=x["name"],
                                  dataType=cls._DATA_TYPE_DICT[x["type"]],
                                  nullable=True if x["nullable"].lower() == "true" else False),
            json_content["schema"])))

        logger.info(f"Successfully retrieved StructType from file '{json_file_path}'")
        return structtype_from_json
