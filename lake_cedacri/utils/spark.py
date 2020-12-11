import json
import logging
import os

from pyspark.sql.types import StructType, StructField
from pyspark_utils.sql import DataTypeUtils


class SparkUtils:

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
                                  dataType=DataTypeUtils.spark_datatype(x["type"]),
                                  nullable=x["nullable"]),
            json_content["schema"])))

        logger.info(f"Successfully retrieved StructType from file '{json_file_path}'")
        return structtype_from_json
