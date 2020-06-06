
if __name__ == "__main__":

    import configparser
    import argparse
    import logging

    from logging import config
    from typing import List
    from datetime import datetime
    from branch.enum import Branch
    from spark.engine.source_load import SourceLoadEngine
    from src.spark.time import JAVA_TO_PYTHON_FORMAT

    # LOGGING CONFIGURATION
    with open("src/conf/logging.ini", "r") as f:

        config.fileConfig(f)

    logger = logging.getLogger(__name__)
    logger.info("Successfully loaded logging configuration")
    branch_parser = argparse.ArgumentParser()

    # OPTION -b, --branch
    branch_parser.add_argument("-b", "--branch", type=str, dest="branch_name", metavar="branch",
                        help="application branch to be run", required=True)

    (parsed_arguments, unknown_arguments) = branch_parser.parse_known_args()
    branch_name: str = parsed_arguments.branch_name.upper()
    logger.info(f"Parsed branch name: {branch_name}")

    if branch_name == Branch.INITIAL_LOAD.name:

        initial_load_parser = argparse.ArgumentParser()

        # OPTION -p, --properties
        initial_load_parser.add_argument("-p", "--properties", type=str, dest="spark_job_ini_file", metavar="example.ini",
                                         help=".ini file holding spark job information supplied via spark-submit --files option",
                                         required=True)

        # RETRIEVE ARGUMENTS
        (parsed_arguments, unknown_arguments) = initial_load_parser.parse_known_args()
        spark_job_ini_file: str = parsed_arguments.spark_job_ini_file
        logger.info(f"Provided spark job file: {spark_job_ini_file}")

        # TODO: class for initial load branch

    elif branch_name == Branch.SOURCE_LOAD.name:

        source_load_parser = argparse.ArgumentParser()

        # OPTION -p, --properties
        source_load_parser.add_argument("-p", "--properties", type=str, dest="spark_job_ini_file", metavar="example.ini",
                                         help=".ini file holding spark job information supplied via spark-submit --files option",
                                         required=True)

        # OPTION -s, --sources
        source_load_parser.add_argument("-s", "--sources", type=str, nargs="+", dest="bancll_names", metavar="source",
                            help="list of raw sources (BANCLL) to be loaded (separated by blank space if more than one)",
                            required=True)

        # OPTION -d, --business--date
        business_date_format: str = JAVA_TO_PYTHON_FORMAT["yyyy-MM-dd"]
        source_load_parser.add_argument("-d", "--business--date", type=str, dest="dt_business_date", metavar="date",
                            help=f"dt_business_date to be used for data loading (format {business_date_format})", required=False)

        # OPTION -n, --number-of-records
        source_load_parser.add_argument("-n", "--n-records", type=int, dest="number_of_records", metavar="number",
                            help="number_of_records to be generated", required=False)

        # RETRIEVE ARGUMENTS
        (parsed_arguments, unknown_arguments) = source_load_parser.parse_known_args()
        spark_job_ini_file: str = parsed_arguments.spark_job_ini_file
        bancll_names: List[str] = parsed_arguments.bancll_names
        input_dt_business_date: str = parsed_arguments.dt_business_date
        number_of_records: int = parsed_arguments.number_of_records

        # CHECK THAT BUSINESS DATE IS CORRECT (IF PROVIDED)
        try:

            dt_business_date: str = datetime.now().strftime(business_date_format) if input_dt_business_date is None \
                else datetime.strptime(input_dt_business_date, business_date_format)

            logger.info("Successfully parsed dt_business_date")

        except ValueError:

            raise Exception(f"Invalid business date. Provided \"{input_dt_business_date}\", should follow {business_date_format}") from None

        # CHECK THAT THE PROVIDED FILE IS A .ini
        if not spark_job_ini_file.endswith(".ini"):

            raise ValueError(f"Invalid file for spark job: {spark_job_ini_file}. It should be a .ini file")

        logger.info(f"Provided {len(bancll_names)} BANCLL(s): {bancll_names}")
        logger.info(f"Working business date: {dt_business_date}")
        logger.info(f"Spark job .ini file name: {spark_job_ini_file}")

        with open(spark_job_ini_file, "r") as f:

            # SET INTERPOLATION TO EXTENDE IN ORDER TO INTERPOLATE FROM OTHER SECTIONS AS WELL
            job_properties: configparser.ConfigParser = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
            job_properties.read(f)
            logger.info("Successfully loaded job properties dict")

        default_number_of_records: int = int(job_properties["spark"]["raw_dataframe_count"])
        number_of_records = default_number_of_records if number_of_records is None else number_of_records
        logger.info(f"Number of records to generate: {number_of_records}")

        bancll_loader: SourceLoadEngine = SourceLoadEngine(job_properties, number_of_records)
        for (bancll_index, bancll_name) in enumerate(bancll_names):

            logger.info(f"Starting to load raw data for BANCLL # {bancll_index} ({bancll_name})")

            bancll_loader.run(bancll_name, dt_business_date)

            logger.info(f"Successfully loaded data for BANCLL # {bancll_index} ({bancll_name})")
