from datetime import datetime, date


class TimeUtils:

    _PYTHON_FORMAT_CONVERTER = {

        # DATE
        "yyyy-MM-dd": "%Y-%m-%d",
        "yyyy/MM/dd": "%Y/%m/%d",
        "yyyy_MM_dd": "%Y_%m_%d",

        "dd-MM-yyyy": "%d-%m-%Y",
        "dd/MM/yyyy": "%d/%m/%Y",
        "dd_MM_yyyy": "%d_%m_%Y",

        "dd-MM-yy": "%d-%m-%y",

        # TIME
        "HH:mm:ss": "%H:%M:%S",

        # TIMESTAMP
        "yyyy-MM-dd HH:mm:ss": "%Y-%m-%d %H:%M:%S",
        "dd/MM/yyyy HH:mm:ss": "%d/%m/%Y %H:%M:%S",
        "dd_MM_yyyy HH:mm:ss": "%d_%m_%Y %H:%M:%S",

    }

    _DT_RIFERIMENTO_FORMAT: str = "yyyy-MM-dd"

    @classmethod
    def dt_riferimento_format(cls) -> str:
        return cls._DT_RIFERIMENTO_FORMAT

    @classmethod
    def to_python_format(cls, java_format: str) -> str:
        return cls._PYTHON_FORMAT_CONVERTER[java_format]

    @classmethod
    def to_date(cls, date_string: str, string_format: str) -> date:
        return datetime.strptime(date_string, cls.to_python_format(string_format))

    @classmethod
    def to_datetime(cls, datetime_string: str, string_format: str) -> datetime:
        return datetime.strptime(datetime_string, cls.to_python_format(string_format))
