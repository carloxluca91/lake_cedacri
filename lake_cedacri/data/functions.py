from enum import Enum, unique


@unique
class Functions(Enum):

    RANDOM_CHOICE = r"^(choice)\(\[(.+)\], \[(.+)\]\)$"
    RANDOM_NUMBER = r"^(randomNumber)\((\d+), (\d+)\)$"
    RANDOM_DATE = r"^(randomDate)\('(.+)', '(.+)', '(.+)'\)$"
