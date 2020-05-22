

if __name__ == "__main__":

    import argparse
    import logging
    from logging import config
    from datetime import datetime

    # TODO: check presence of yaml package
    import yaml

    # LOGGING CONFIGURATION
    with open("logging.yaml", "r") as f:
        logging_config = yaml.safe_load(f.read())
        config.dictConfig(logging_config)

    logger = logging.getLogger(__name__)
    logger.info("Successfully loaded logging configuration")

    parser = argparse.ArgumentParser()

    # OPTION -a, --all
    parser.add_argument("-r", "--raw-source", type=str, nargs="+", dest="raw_sources", metavar="source",
                        help="list of raw sources (BANCLL) to be loaded (separated by blank space if more than one)",
                        required=True)

    # OPTION -s, --steps
    parser.add_argument("-b", "--business--date", type=str, nargs="?", dest="business_date", metavar="date",
                        help="dt_business_date to be used for data loading (format yyyy-MM-dd")

    parsed_arguments = parser.parse_args()
    bancll_names = parsed_arguments.raw_sources
    (dt_business_date, provided) = (datetime.now().strftime("%Y-%m-%d"), False) if parsed_arguments.business_date is None \
        else (parsed_arguments.business_date, True)

    logger.info("Provided {} BANCLL(s): {}".format(len(bancll_names), bancll_names))
    dt_business_date_log_string = "Working business date: {} (provided)".format(dt_business_date) if provided \
        else "Working business date: {} (not provided)".format(dt_business_date)
    logger.info(dt_business_date_log_string)
