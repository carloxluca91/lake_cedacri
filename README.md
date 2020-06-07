### Lake cedacri

`Python` `Spark` `MySQL`

Python project that populates a set of tables hosted by a `MySQL` server using `pyspark.sql` API, 
according to a set of specifications stated in a table hosted by the same server. 

The application presents the following functionalities (or _branches_)

`[1]` an `INITIAL_LOAD` _branch_, which takes care of creating all databases and tables 
needed by the `SOURCE_LOAD` _branch_ (such as the _specification table_) 

`[2]` a `SOURCE_LOAD` _branch_. The core _branch_. Given the name of a source, loads the corresponding data 
into a table within the database previously created by the `INITIAL_LOAD` _branch_

`[3]` a `RE_LOAD` _branch_, used for rewriting the _specification table_ 

The tables populated by the `SOURCE_LOAD` _branch_ represent the raw data layer for another `Scala`-based `Spark` project

