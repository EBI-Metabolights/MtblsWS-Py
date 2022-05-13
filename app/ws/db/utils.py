from datetime import datetime


def date_str_to_int(date_in_str: str, data_format: str = "%Y-%m-%d") -> int:
    try:
        time = int(datetime.strptime(date_in_str, data_format).timestamp() * 1000)
        return time
    except Exception:
        return 0


def datetime_to_int(date_value: datetime) -> int:
    try:
        time = int(date_value.timestamp() * 1000)
        return time
    except Exception:
        return 0