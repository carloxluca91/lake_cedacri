
if __name__ == "__main__":

    import argparse
    import logging

    from logging import config
    from typing import List
    from datetime import datetime
    from src.spark.branch import Branch
    from src.spark.engine.initial_load import InitialLoadEngine
    from src.spark.engine.re_load import ReloadEngine
    from src.spark.engine.source_load import SourceLoadEngine
    from src.spark.time import DT_RIFERIMENTO_DATE, PYTHON_FORMAT

    # LOGGING CONFIGURATION
    with open("src/logging.ini", "r") as f:

        config.fileConfig(f)

    logger = logging.getLogger(__name__)
    logger.info("Successfully loaded logging configuration")
    initial_parser = argparse.ArgumentParser()

    # OPTION -b, --branch
    initial_parser.add_argument("-b", "--branch",
                               type=str,
                               dest="branch_name",
                               metavar="branch",
                               help="application branch to be run",
                               required=True)

    # OPTION -p, --properties
    initial_parser.add_argument("-p", "--properties",
                                type=str,
                                dest="spark_job_ini_file",
                                metavar="example.ini",
                                help=".ini file holding spark job information supplied via spark-submit --files option",
                                required=True)

    (parsed_arguments, unknown_arguments) = initial_parser.parse_known_args()
    branch_name: str = parsed_arguments.branch_name.upper()
    spark_job_ini_file: str = parsed_arguments.spark_job_ini_file

    logger.info(f"Parsed branch name: '{branch_name}'")
    logger.info(f"Provided spark job file: '{spark_job_ini_file}'")

    if branch_name == Branch.INITIAL_LOAD.value:

        InitialLoadEngine(spark_job_ini_file).run()

    elif branch_name == Branch.RE_LOAD.value:

        re_load_parser = argparse.ArgumentParser()

        # OPTION -o, --overwrite
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

        # OPTION -s, --sources
        source_load_parser.add_argument("-s", "--sources",
                                        type=str,
                                        nargs="+",
                                        dest="bancll_names",
                                        metavar="source",
                                        help="list of raw sources (BANCLL) to be loaded (separated by blank space if more than one)",
                                        required=True)

        # OPTION -d, --business--date
        dt_riferimento_format: str = PYTHON_FORMAT[DT_RIFERIMENTO_DATE]
        source_load_parser.add_argument("-d", "--dt--riferimento",
                                        type=str,
                                        dest="dt_riferimento",
                                        metavar="date",
                                        help=f"reference date to be used for data loading (format {dt_riferimento_format})",
                                        required=False,
                                        default=datetime.now().strftime(dt_riferimento_format))

        # OPTION -n, --number-of-records
        source_load_parser.add_argument("-n", "--n-records",
                                        type=int,
                                        dest="number_of_records",
                                        metavar="number",
                                        help="number of records to be generated",
                                        required=False,
                                        default=1000)

        # RETRIEVE ARGUMENTS
        (parsed_arguments, unknown_arguments) = source_load_parser.parse_known_args()
        bancll_names: List[str] = parsed_arguments.bancll_names
        input_dt_riferimento: str = parsed_arguments.dt_riferimento
        number_of_records: int = parsed_arguments.number_of_records

        logger.info(f"Provided {len(bancll_names)} BANCLL(s): {bancll_names}")
        logger.info(f"Provided data reference date: '{input_dt_riferimento}'")
        logger.info(f"Number of records: {number_of_records}")

        # CHECK THAT DT_RIFERIMENTO IS CORRECT (IF PROVIDED)
        try:

            datetime.strptime(input_dt_riferimento, dt_riferimento_format)

        except ValueError:

            raise Exception(f"Invalid reference date. Provided '{input_dt_riferimento}', should follow '{dt_riferimento_format}'") from None

        else:

            logger.info("Successfully parsed dt_riferimento")

        bancll_loader: SourceLoadEngine = SourceLoadEngine(spark_job_ini_file, number_of_records)
        for (bancll_index, bancll_name) in enumerate(bancll_names):

            logger.info(f"Starting to load raw data for BANCLL # {bancll_index + 1} ('{bancll_name}')")
            bancll_loader.run(bancll_name, input_dt_riferimento)
            logger.info(f"Successfully loaded data for BANCLL # {bancll_index + 1} ('{bancll_name}')")
