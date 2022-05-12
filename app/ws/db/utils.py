from datetime import datetime


def date_str_to_int(date_in_str: str, data_format: str = "%Y-%m-%d") -> int:
    time = int(datetime.strptime(date_in_str, data_format).timestamp() * 1000)
    return time


def datetime_to_int(date_value: datetime) -> int:
    time = int(date_value.timestamp() * 1000)
    return time
