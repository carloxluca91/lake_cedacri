
if __name__ == "__main__":

    import configparser
    import argparse
    import logging

    from logging import config
    from typing import List
    from datetime import datetime
    from lake_cedacri.spark.loading import BancllLoader
    from lake_cedacri.time.formats import JAVA_TO_PYTHON_FORMAT

    # LOGGING CONFIGURATION
    with open("logging.ini", "r") as f:

        config.fileConfig(f)

    logger = logging.getLogger(__name__)
    logger.info("Successfully loaded logging configuration")
    parser = argparse.ArgumentParser()

    # OPTION -b, --bancll
    parser.add_argument("-b", "--bancll", type=str, nargs="+", dest="bancll_names", metavar="source",
                        help="list of raw sources (BANCLL) to be loaded (separated by blank space if more than one)",
                        required=True)

    # OPTION -d, --dt-business--date
    parser.add_argument("-d", "--dt-business--date", type=str, dest="dt_business_date", metavar="date",
                        help="dt_business_date to be used for data loading (format yyyy-MM-dd", required=False)

    # OPTION -f, --file
    parser.add_argument("-f", "--file", type=str, dest="spark_job_ini_file", metavar="example.ini",
                        help=".ini file holding spark job information supplied via spark-submit --files option", required=True)

    # OPTION -n, --number-of-records
    # OPTION -f, --file
    parser.add_argument("-n", "--number-of-records", type=int, dest="number_of_records", metavar="number",
                        help="number_of_records to be generated", required=False)

    parsed_arguments = parser.parse_args()

    # INPUT PARAMETERS
    bancll_names: List[str] = parsed_arguments.bancll_names
    input_dt_business_date: str = parsed_arguments.dt_business_date
    spark_job_ini_file: str = parsed_arguments.spark_job_ini_file
    number_of_records: int = parsed_arguments.number_of_records

    # CHECK
    # [a] THAT BUSINESS DATE IS CORRECT (IF PROVIDED)
    dt_business_date_format: str = JAVA_TO_PYTHON_FORMAT["yyyy-MM-dd"]

    try:

        dt_business_date: str = datetime.now().strftime(dt_business_date_format) if input_dt_business_date is None \
            else datetime.strptime(input_dt_business_date, dt_business_date_format)

        logger.info("Successfully parsed dt_business_date")

    except ValueError:

        raise Exception(f"Invalid business date. Provided \"{input_dt_business_date}\", should follow {dt_business_date_format}") from None

    # [b] THAT THE PROVIDED FILE IS A .ini
    if not spark_job_ini_file.endswith(".ini"):

        raise ValueError(f"Invalid file for spark job: {spark_job_ini_file}. It should be a .ini file")

    logger.info(f"Provided {len(bancll_names)} BANCLL(s): {bancll_names}")
    logger.info(f"Working business date: {dt_business_date}")
    logger.info(f"Spark job .yaml file name: {spark_job_ini_file}")

    with open(spark_job_ini_file, "r") as f:

        # SET INTERPOLATION TO EXTENDE IN ORDER TO INTERPOLATE FROM OTHER SECTIONS AS WELL
        job_properties: configparser.ConfigParser = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        job_properties.read(spark_job_ini_file)
        logger.info("Successfully loaded job properties dict")

    default_number_of_records: int = job_properties["spark"]["raw_dataframe_count"]
    number_of_records = default_number_of_records if number_of_records is None else number_of_records
    logger.info(f"Number of records to generate: {number_of_records}")

    bancll_loader: BancllLoader = BancllLoader(job_properties, number_of_records)
    for (bancll_index, bancll_name) in enumerate(bancll_names):

        logger.info(f"Starting to load raw data for BANCLL # {bancll_index} ({bancll_name})")

        bancll_loader.run(bancll_name, dt_business_date)

        logger.info(f"Successfully loaded data for BANCLL # {bancll_index} ({bancll_name})")
