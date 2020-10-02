import unittest


class UtilsTestCase(unittest.TestCase):

    def test_json_reading(self):

        from pyspark.sql.types import StructType
        from src.spark.engine.abstract import from_json_file_to_struct_type
        from tests.paths import SPECIFICATION_SCHEMA_FILE_PATH

        structType: StructType = from_json_file_to_struct_type(SPECIFICATION_SCHEMA_FILE_PATH)
        self.assertEqual(len(structType), 7)


if __name__ == '__main__':

    unittest.main()
