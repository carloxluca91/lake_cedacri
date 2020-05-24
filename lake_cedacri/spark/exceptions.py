INVALID_SPECIFICATION_STRING = "Invalid specification for BANCLL {0}"


def prefix(bancll_name):

    return INVALID_SPECIFICATION_STRING.format(bancll_name)


class InvalidSourceError(Exception):

    def __init__(self, bancll_name, raw_sources):

        message_suffix = "Specified more than 1 source table ({})".format(", ".join(raw_sources))
        self.message = "{}. {}".format(prefix(bancll_name), message_suffix)

    def __str__(self):

        return self.message


class UnexistingTableError(Exception):

    def __init__(self, database_name, table_name):

        self.message = "Unable to find table {} within database {}".format(table_name, database_name)

    def __str__(self):

        return self.message


# raise InvalidSourceError("aaa", ["1", "2"])
# raise UnexistingTableError("db", "tb1")
