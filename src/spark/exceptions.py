from typing import List, Set, Union

INVALID_SPECIFICATION_STRING = "Invalid specification for BANCLL \'{0}\'"


def _prefix(bancll_name):

    return INVALID_SPECIFICATION_STRING.format(bancll_name)


def _wrap_with_single_quotes(strings: Union[List[str], Set[str]]) -> List[str]:

    return list(map(lambda x: f"\'{x}\'", strings))


class UndefinedBANCLLError(Exception):

    def __init__(self, bancll_name: str):

        self.message = f"Unable to find specification for BANCLL \'{bancll_name}\'"

    def __str__(self): return self.message


class InvalidBANCLLSourceError(Exception):

    def __init__(self, bancll_name: str, raw_sources: Set[str]):

        message_suffix = "Specified more than 1 source table ({})".format(", ".join(_wrap_with_single_quotes(raw_sources)))
        self.message = f"{_prefix(bancll_name)}. {message_suffix}"

    def __str__(self): return self.message


class DuplicateColumnError(Exception):

    def __init__(self, bancll_name: str, duplicated_columns: Set[str]):

        message_suffix = "Duplicated column name(s) ({})".format(", ".join(_wrap_with_single_quotes(duplicated_columns)))
        self.message = f"{_prefix(bancll_name)}. {message_suffix}"

    def __str__(self): return self.message


class UnknownDataTypeError(Exception):

    def __init__(self, bancll_name: str, unknown_types: List[str]):

        message_suffix = "Unknown data type(s): ({}) ".format(", ".join(_wrap_with_single_quotes(unknown_types)))
        self.message = f"{_prefix(bancll_name)}. {message_suffix}"

    def __str__(self): return self.message


class NegativeColumnIndexError(Exception):

    def __init__(self, bancll_name: str, negative_index: int):

        message_suffix = f"Negative column index: ({negative_index}) "
        self.message = f"{_prefix(bancll_name)}. {message_suffix}"

    def __str__(self): return self.message


class InvalidMinColumnIndexError(Exception):

    def __init__(self, bancll_name: str, invalid_index: int):

        message_suffix = f"Invalid min column index: ({invalid_index}). Should be 0"
        self.message = f"{_prefix(bancll_name)}. {message_suffix}"

    def __str__(self): return self.message


class InvalidMaxColumnIndexError(Exception):

    def __init__(self, bancll_name: str, max_index: int, min_index: int):

        message_suffix = f"Invalid max column index: ({max_index}). Should be greater or equal than {min_index}"
        self.message = f"{_prefix(bancll_name)}. {message_suffix}"

    def __str__(self): return self.message


class NonContinuousRangeError(Exception):

    def __init__(self, bancll_name: str, missing_indexes: List[int]):

        missing_indexes_str: str = ", ".join(map(lambda x: str(x), missing_indexes))
        message_suffix = f"Missing column index : ({missing_indexes_str}) "
        self.message = f"{_prefix(bancll_name)}. {message_suffix}"

    def __str__(self): return self.message


class UnmatchedDateFormatError(Exception):

    def __init__(self, bancll_name: str, undefined_formats: List[str]):

        message_suffix = "Unable to match date formats : ({}) ".format(", ".join(_wrap_with_single_quotes(undefined_formats)))
        self.message = f"{_prefix(bancll_name)}. {message_suffix}"

    def __str__(self): return self.message
