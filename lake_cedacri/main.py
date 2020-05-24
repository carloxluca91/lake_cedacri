
if __name__ == "__main__":

    # TODO: check presence of PyYaml package
    import yaml
    import argparse
    import logging

    from logging import config
    from datetime import datetime
    from lake_cedacri.spark.ingestion import IngestionRunner
    from lake_cedacri.time.formats import JAVA_TO_PYTHON_FORMAT

    # LOGGING CONFIGURATION
    with open("logging.yaml", "r") as f:
        logging_config = yaml.safe_load(f.read())
        config.dictConfig(logging_config)

    logger = logging.getLogger(__name__)
    logger.info("Successfully loaded logging configuration")
    parser = argparse.ArgumentParser()

    # OPTION -b, --bancll
    parser.add_argument("-b", "--bancll", type=str, nargs="+", dest="bancll_names", metavar="source",
                        help="list of raw sources (BANCLL) to be loaded (separated by blank space if more than one)",
                        required=True)

    # OPTION -d, --dt-business--date
    parser.add_argument("-d", "--dt-business--date", type=str, dest="dt_business_date", metavar="date",
                        help="dt_business_date to be used for data loading (format yyyy-MM-dd")

    # OPTION -f, --file
    parser.add_argument("-f", "--file", type=str, dest="spark_job_yaml_file", metavar="example.yaml",
                        help=".yaml file holding spark job information supplied via spark-submit --files option", required=True)

    parsed_arguments = parser.parse_args()

    # INPUT PARAMETERS
    bancll_names = parsed_arguments.bancll_names
    input_dt_business_date = parsed_arguments.dt_business_date
    spark_job_yaml_file = parsed_arguments.spark_job_yaml_file

    # CHECK
    # [a] THAT BUSINESS DATE IS CORRECT (IF PROVIDED)
    dt_business_date_format = JAVA_TO_PYTHON_FORMAT["yyyy-MM-dd"]
    if input_dt_business_date is not None:

        try:
            dt_business_date = datetime.strptime(input_dt_business_date, dt_business_date_format)

        except Exception:

            raise ValueError("Invalid dt_business_date: {}. It should follow format {}".format(
                input_dt_business_date, dt_business_date_format))

    else:

        dt_business_date = datetime.now().strftime(dt_business_date_format)

    # [b] THAT THE PROVIDED FILE IS A .yaml
    if not spark_job_yaml_file.endswith(".yaml"):

        raise ValueError("Invalid file for spark job: {}. It should be a .yaml file".format(spark_job_yaml_file))

    logger.info("Provided {} BANCLL(s): {}".format(len(bancll_names), bancll_names))
    logger.info("Working business date: {}".format(dt_business_date))
    logger.info("Spark job .yaml file name: {}".format(spark_job_yaml_file))

    with open(spark_job_yaml_file, "r") as f:

        job_properties_dict = yaml.safe_load(f.read())

    logger.info("Successfully loaded job properties dict")
    IngestionRunner(job_properties_dict).run(bancll_names, dt_business_date)
