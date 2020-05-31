### Lake cedacri (Py)

Python project that outputs .`parquet` file by using `pyspark.sql` API, 
according to a set of specification stated within a `.tsv` file. 

The output files serve as `HDFS` parquet files to be used for feeding a set of
`Hive` tables.

Such tables represent the raw data layer for another `Scala`-based `Spark` project

