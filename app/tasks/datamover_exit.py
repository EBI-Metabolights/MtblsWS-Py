import logging
import socket

from app.config import get_settings
from app.tasks.datamover_worker_utils import delete_current_workers, delete_queue

logger = logging.getLogger("wslog_datamover")

if __name__ == "__main__":
    worker_name = socket.gethostname()

    if not worker_name:
        logger.error("Invalid worker name.")
        exit(1)
    project_name = get_settings().hpc_cluster.datamover.job_prefix
    name = f"{project_name}_{worker_name}"
    delete_queue(name)
    delete_current_workers(worker_name)
