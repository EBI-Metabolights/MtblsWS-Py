import logging
import socket
from app.tasks.datamover_worker_utils import restart_datamover_worker

logger = logging.getLogger("wslog_datamover")

if __name__ == "__main__":
    worker_name = socket.gethostname()

    if not worker_name:
        logger.error("Invalid worker name.")
        exit(1)
    success = restart_datamover_worker(worker_name)

    if not success:
        logger.error("Datamover start task failed.")
        exit(1)
