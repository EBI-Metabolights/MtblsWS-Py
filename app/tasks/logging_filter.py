from logging import Filter, LogRecord


class CeleryWorkerLogFilter(Filter):
    """
    Define the filter to stop logging for visualization endpoint
    which will be called very frequently
    and log file will be flooded with this endpoint request logs
    """

    def filter(self, record: LogRecord) -> bool:
        if (
            hasattr(record, "data")
            and record.data
            and "name" in record.data
            and "heartbeat.ping" in record.data["name"]
        ):
            return False
        return True
