import logging
from abc import ABC
from typing import Any, List


class AbstractRandomFunction(ABC):

    _logger = logging

    @classmethod
    def create(cls, string: str) -> List[Any]:
        pass


class RandomChoice(AbstractRandomFunction):

    @classmethod
    def create(cls, string: str) -> List[Any]:
        pass


class RandomDate(AbstractRandomFunction):

    @classmethod
    def create(cls, string: str) -> List[Any]:
        pass



