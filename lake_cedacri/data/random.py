import logging
import random
import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, List

from time_utils import TimeUtils

from lake_cedacri.data.enum import RandomFunctionEnum


class AbstractRandomFunction(ABC):

    def __init__(self, string: str, random_function: RandomFunctionEnum):

        self._logger = logging.getLogger(__name__)
        self._string = string
        self._random_function = random_function
        self._match = re.match(random_function.regex, string)
        self._function_name: str = self.group(1)

    def group(self, i: int) -> str:
        return None if self._match is None else self._match.group(i)

    @property
    def function_name(self):
        return self._function_name

    @property
    @abstractmethod
    def to_string(self) -> str:
        pass

    @abstractmethod
    def create_data(self, number_of_records: int) -> List[Any]:
        pass


class RandomChoice(AbstractRandomFunction):

    def __init__(self,
                 string: str,
                 random_function: RandomFunctionEnum = RandomFunctionEnum.RANDOM_CHOICE):

        super().__init__(string, random_function)
        self._values = self.group(2).split(", ")
        self._weights = self.group(3).split(", ")

    @property
    def to_string(self) -> str:
        return f"{self.function_name}(values = [{', '.join(self._values)}], weights = [{', '.join(self._weights)}])"

    def create_data(self, number_of_records: int) -> List[Any]:

        weights = list(map(lambda x: float(x), self._weights))
        return random.choices(self._values, weights=weights, k=number_of_records)


class RandomDate(AbstractRandomFunction):

    def __init__(self,
                 string: str,
                 random_function: RandomFunctionEnum = RandomFunctionEnum.RANDOM_DATE):

        super().__init__(string, random_function)
        self._lower_dt = self.group(2)
        self._upper_dt = self.group(3)

    @property
    def to_string(self) -> str:
        return f"{self.function_name}('{self._lower_dt}', '{self._upper_dt}')"

    def create_data(self, number_of_records: int) -> List[Any]:

        lower_dt: datetime = TimeUtils.to_datetime(self._lower_dt, TimeUtils.java_default_dt_format())
        upper_dt: datetime = TimeUtils.to_datetime(self._upper_dt, TimeUtils.java_default_dt_format())
        time_delta = upper_dt - lower_dt
        return [lower_dt + (time_delta * random.random()) for _ in range(number_of_records)]


class RandomNumber(AbstractRandomFunction):

    def __init__(self,
                 string: str,
                 random_function: RandomFunctionEnum = RandomFunctionEnum.RANDOM_NUMBER):

        super().__init__(string, random_function)
        self._lower = int(self.group(2))
        self._upper = int(self.group(3))

    @property
    def to_string(self) -> str:
        return f"{self.function_name}({self._lower}, {self._upper})"

    def create_data(self, number_of_records: int) -> List[Any]:

        delta = self._upper - self._lower
        return [self._lower + (delta * random.random()) for _ in range(number_of_records)]
