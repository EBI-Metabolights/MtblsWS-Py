


import os
import sys
import time
from app.config import get_settings

from app.tasks.datamover_worker_utils import delete_current_workers, delete_queue, ping_datamover_worker
from app.tasks.worker import report_internal_technical_issue


if __name__ == "__main__":
    
    worker_name = os.getenv('WORKER_HOST_NAME')
    
    report_internal_technical_issue(f"{worker_name} queue will be deleted", "Queue will be deleted")
    if not worker_name:
        print("Invalid worker name.")
        exit(1)
    project_name = get_settings().hpc_cluster.configuration.job_project_name
    name = f"{project_name}_{worker_name}"
    delete_queue(name)
    report_internal_technical_issue(f"{worker_name} will be killed", "Worker will be removed from datamover")

    delete_current_workers(worker_name)