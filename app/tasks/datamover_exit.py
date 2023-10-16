


import os
import sys
import time
from app.config import get_settings

from app.tasks.datamover_worker_utils import delete_current_workers, delete_queue, ping_datamover_worker


if __name__ == "__main__":
    worker_name = os.getenv('WORKER_NAME')
    
    if not worker_name:
        print("Invalid worker name.")
        exit(1)
    project_name = get_settings().hpc_cluster.configuration.job_project_name
    name = f"{project_name}_{worker_name}"
    delete_queue(name)
    delete_current_workers(worker_name)