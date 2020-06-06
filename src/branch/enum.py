from enum import Enum


class Branch(Enum):

    INITIAL_LOAD = "INITIAL_LOAD"
    SOURCE_LOAD = "SOURCE_LOAD"
    RE_LOAD = "RE_LOAD"

    def __init__(self, branch_name: str):

        self.name: str = branch_name
