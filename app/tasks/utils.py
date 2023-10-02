
import datetime


UTC_SIMPLE_DATE_FORMAT='%Y-%m-%d %H:%M:%S'
def get_current_utc_time_string():
    return datetime.datetime.now(datetime.timezone.utc).strftime(UTC_SIMPLE_DATE_FORMAT)

def get_utc_time_string(input: datetime.datetime):
    return input.strftime(UTC_SIMPLE_DATE_FORMAT)

def get_utc_time_string_from_timestamp(input: int):
    if input > 0:
        input_time = datetime.datetime.fromtimestamp(input)
        return input_time.strftime(UTC_SIMPLE_DATE_FORMAT)
    return ""

