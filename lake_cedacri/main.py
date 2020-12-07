
if __name__ == "__main__":

    import argparse
    import logging

    from logging import config
    from typing import List
    from datetime import datetime
    from time_utils import TimeUtils

    from lake_cedacri.engine import InitialLoadEngine
    from lake_cedacri.engine import ReloadEngine
    from lake_cedacri.engine import SourceLoadEngine
    from lake_cedacri.utils import Branch

    # Logging configuration
    with open("lake_cedacri/logging.ini", "r") as f:

        config.fileConfig(f)

    logger = logging.getLogger(__name__)
    logger.info("Successfully loaded logging configuration")
    initial_parser = argparse.ArgumentParser()

    # Option -b, --branch
    initial_parser.add_argument("-b", "--branch",
                                type=str,
                                dest="branch_name",
                                metavar="branch",
                                help="application branch to be run",
                                required=True)

    # Option -p, --properties
    initial_parser.add_argument("-p", "--properties",
                                type=str,
                                dest="spark_job_ini_file",
                                metavar="example.ini",
                                help=".ini file holding lake_cedacri job information supplied via spark-submit --files option",
                                required=True)

    (parsed_arguments, unknown_arguments) = initial_parser.parse_known_args()
    branch_name: str = parsed_arguments.branch_name.upper()
    spark_job_ini_file: str = parsed_arguments.spark_job_ini_file

    logger.info(f"Parsed branch name: '{branch_name}'")
    logger.info(f"Provided lake_cedacri job file: '{spark_job_ini_file}'")

    if branch_name == Branch.INITIAL_LOAD.value:

        InitialLoadEngine(spark_job_ini_file).run()

    elif branch_name == Branch.RE_LOAD.value:

        re_load_parser = argparse.ArgumentParser()

        # Option -o, --overwrite
        re_load_parser.add_argument("-o", "--overwrite",
                                    dest="overwrite_flag",
                                    action="store_true",
                                    help="wheter or not a complete overwrite of specification table is needed. Default: false if not specified",
                                    required=False)

        (parsed_arguments, unknown_arguments) = re_load_parser.parse_known_args()
        overwrite_flag: bool = parsed_arguments.overwrite_flag

        logger.info(f"Complete overwrite flag: '{overwrite_flag}'")

        ReloadEngine(overwrite_flag, spark_job_ini_file).run()

    elif branch_name == Branch.SOURCE_LOAD.value:

        source_load_parser = argparse.ArgumentParser()

        # Option -s, --sources
        source_load_parser.add_argument("-s", "--sources",
                                        type=str,
                                        nargs="+",
                                        dest="bancll_names",
                                        metavar="source",
                                        help="list of raw sources (BANCLL) to be loaded (separated by blank space if more than one)",
                                        required=True)

        # Option -d, --business--date
        dt_riferimento_format: str = TimeUtils.java_default_dt_format()
        source_load_parser.add_argument("-d", "--dt--riferimento",
                                        type=str,
                                        dest="dt_riferimento",
                                        metavar="date",
                                        help=f"reference date to be used for data loading (format {dt_riferimento_format})",
                                        required=False,
                                        default=TimeUtils.to_string(datetime.now().date(), dt_riferimento_format))

        # Option -n, --number-of-records
        source_load_parser.add_argument("-n", "--n-records",
                                        type=int,
                                        dest="number_of_records",
                                        metavar="number",
                                        help="number of records to be generated",
                                        required=False,
                                        default=1000)

        # Parse input arguments
        (parsed_arguments, unknown_arguments) = source_load_parser.parse_known_args()
        bancll_names: List[str] = parsed_arguments.bancll_names
        input_dt_riferimento: str = parsed_arguments.dt_riferimento
        number_of_records: int = parsed_arguments.number_of_records

        logger.info(f"Provided {len(bancll_names)} BANCLL(s): {bancll_names}")
        logger.info(f"Provided dt_riferimento: '{input_dt_riferimento}'")
        logger.info(f"Number of records: {number_of_records}")

        # Check validity of dt_riferimento (if provided)
        try:

            TimeUtils.to_date(input_dt_riferimento, dt_riferimento_format)

        except ValueError:

            raise ValueError(f"Invalid reference date. Provided '{input_dt_riferimento}', should follow '{dt_riferimento_format}'") from None

        else:

            logger.info("Successfully parsed dt_riferimento")

        bancll_loader: SourceLoadEngine = SourceLoadEngine(spark_job_ini_file, number_of_records)
        for (bancll_index, bancll_name) in enumerate(bancll_names):

            logger.info(f"Starting to load raw data for BANCLL # {bancll_index + 1} ('{bancll_name}')")
            bancll_loader.run(bancll_name, input_dt_riferimento)
            logger.info(f"Successfully loaded data for BANCLL # {bancll_index + 1} ('{bancll_name}')")
