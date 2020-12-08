import re
from enum import Enum, unique


@unique
class RandomFunctionEnum(Enum):

    RANDOM_CHOICE = r"^(randomChoice)\(\[(.+)\], \[(.+)\]\)$"
    RANDOM_NUMBER = r"^(randomNumber)\((\d+), (\d+)\)$"
    RANDOM_DATE = r"^(randomDate)\('(.+)', '(.+)'\)$"

    def __init__(self, regex: str):
        self._regex = regex

    @property
    def regex(self) -> str:
        return self._regex

    def match(self, expression: str):
        return re.match(self.regex, expression)
