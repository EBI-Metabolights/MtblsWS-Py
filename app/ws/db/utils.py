from datetime import datetime


def date_str_to_int(date_in_str: str, data_format: str = "%Y-%m-%d") -> int:
    if date_in_str:
        time = int(datetime.strptime(date_in_str, data_format).timestamp() * 1000)
        return time
    return 0


def datetime_to_int(date_value: datetime) -> int:
    if date_value:
        time = int(date_value.timestamp() * 1000)
        return time
    return 0