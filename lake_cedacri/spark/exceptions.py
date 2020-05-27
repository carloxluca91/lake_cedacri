INVALID_SPECIFICATION_STRING = "Invalid specification for BANCLL {0}"


def prefix(bancll_name):

    return INVALID_SPECIFICATION_STRING.format(bancll_name)


class UndefinedBANCLLError(Exception):

    def __init__(self, bancll_name):

        self.message = "Unable to find specification for BANCLL {}".format(bancll_name)

    def __str__(self): return self.message


class UnexistingTableError(Exception):

    def __init__(self, database_name, table_name):

        self.message = "Unable to find table {} within database {}".format(table_name, database_name)

    def __str__(self): return self.message


class InvalidBANCLLSourceError(Exception):

    def __init__(self, bancll_name, raw_sources):

        message_suffix = "Specified more than 1 source table ({})".format(", ".join(raw_sources))
        self.message = "{}. {}".format(prefix(bancll_name), message_suffix)

    def __str__(self): return self.message


class DuplicateColumnError(Exception):

    def __init__(self, bancll_name, duplicated_columns):

        message_suffix = "Duplicated column name(s) ({})".format(", ".join(duplicated_columns))
        self.message = "{}. {}".format(prefix(bancll_name), message_suffix)

    def __str__(self): return self.message


class UnknownDataTypeError(Exception):

    def __init__(self, bancll_name, unknown_types):

        message_suffix = "Unknown data type(s): () ".format(", ".join(unknown_types))
        self.message = "{}. {}".format(prefix(bancll_name), message_suffix)

    def __str__(self): return self.message


class NegativeColumnIndexError(Exception):

    def __init__(self, bancll_name, negative_index):

        message_suffix = "Negative column index: ({}) ".format(negative_index)
        self.message = "{}. {}".format(prefix(bancll_name), message_suffix)

    def __str__(self): return self.message


class InvalidMinColumnIndexError(Exception):

    def __init__(self, bancll_name, invalid_index):

        message_suffix = "Invalid min column index: ({}). Should be 0".format(invalid_index)
        self.message = "{}. {}".format(prefix(bancll_name), message_suffix)

    def __str__(self): return self.message


class InvalidMaxColumnIndexError(Exception):

    def __init__(self, bancll_name, max_index, min_index):

        message_suffix = "Invalid max column index: ({}). Should be greater or equal than {}".format(max_index, min_index)
        self.message = "{}. {}".format(prefix(bancll_name), message_suffix)

    def __str__(self): return self.message


class NonContinuousRangeError(Exception):

    def __init__(self, bancll_name, missing_index):

        message_suffix = "Missing column index : ({}) ".format(missing_index)
        self.message = "{}. {}".format(prefix(bancll_name), message_suffix)

    def __str__(self): return self.message
