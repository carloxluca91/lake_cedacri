import logging

from collections import namedtuple
from typing import List, Any

from pyspark.sql import Row, SparkSession

SpecificationRecord = namedtuple("SpecificationRecord", [
    "flusso", "flusso", "tabella_rd", "nome_colonna", "tipo_colonna",
    "posizione", "funzione", "flag_nullable", "funzione_spark"])


class DataFactory:

    _logger = logging
    _function_dict = {

        r"^(choice\()"
    }

    @classmethod
    def create_data(cls, spark_session: SparkSession, specification_rows: List[Row], dt_riferimento: str):

        specification_record_list: List[SpecificationRecord] = list(map(lambda x: SpecificationRecord(**x), specification_rows))
        cls._logger.info(f"Successfully turned {len(specification_rows)} {Row.__name__}(s) into {SpecificationRecord.__name__} object(s)")
        custom_record_columns: List[str] = list(set(map(lambda x: x.nome_colonna,
                                                        sorted(specification_record_list, key=lambda x: x.posizione))))

        cls._logger.info(f"Defining a custom record structure with column names: {', '.join(custom_record_columns)}")
        CustomRecord = namedtuple("CustomRecord", custom_record_columns)
        custom_record_data: List[List[Any]] = []
        for index, specification_record in enumerate(specification_record_list):

            function_str: str = specification_record.funzione
