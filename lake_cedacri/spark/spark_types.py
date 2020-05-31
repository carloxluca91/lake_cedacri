from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.types import DoubleType, TimestampType

SPARK_ALLOWED_TYPES = ("int", "string", "timestamp", "double")

DATA_TYPE_DICT = {

    "string": StringType(),
    "int": IntegerType(),
    "double": DoubleType(),
    "timestamp": TimestampType()
}