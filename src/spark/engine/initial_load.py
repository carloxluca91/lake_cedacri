from src.spark.engine.abstract import AbstractEngine


class InitialLoadEngine(AbstractEngine):

    def __init__(self, job_ini_file: str):

        super().__init__(job_ini_file)

