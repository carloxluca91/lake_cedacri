from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.types import DateType, TimestampType

SPARK_ALLOWED_TYPES = ("string", "int", "double", "date", "timestamp")

DATA_TYPE_DICT = {

    "string": StringType(),
    "int": IntegerType(),
    "double": DoubleType(),
    "date": DateType(),
    "timestamp": TimestampType()
}