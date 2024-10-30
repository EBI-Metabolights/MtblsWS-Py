import logging
import os
import pathlib
import socket
import time

import app as current_app
from app.config import get_settings
from app.tasks.datamover_worker_utils import (
    delete_current_workers,
    delete_queue,
    ping_datamover_worker,
)
from app.tasks.worker import report_internal_technical_issue
from app.utils import current_time

logger = logging.getLogger('wslog_datamover')

if __name__ == "__main__":
    worker_name = socket.gethostname()

    if not worker_name:
        print("Invalid worker name.")
        exit(1)
    max_retry_count = 4
    period = 30
    current_retry_count = 0
    status_file_path = "/tmp/healtz.log"
    status_file = pathlib.Path(status_file_path)
    try:
        while True:
            worker_version = ping_datamover_worker(worker_name, retry=1, timeout=5, wait_period=5, initial_wait_period=5)
            if not worker_version:
                current_retry_count += 1
            else:
                current_retry_count = 0
                if worker_version != current_app.__api_version__:
                    print(
                        f"Versions are not same. App version: {current_app.__api_version__}, " +
                        "Datamover worker version: {worker_version}"
                    )
                current = current_time().strftime("%Y-%m-%d %H:%M:%S")
                status_file.write_text(f"{worker_name}, version: {worker_version}, last_update: {current}")
            if current_retry_count >= max_retry_count:
                print("There is no response from datamover. Exiting")
                raise Exception(f"There is no response from datamover. Maximum retry count exceeded {max_retry_count}.")

            time.sleep(period)
    except Exception as ex:
        report_internal_technical_issue(
            f"{str(ex)}: {worker_name} queue will be deleted", "Queue will be deleted"
        )
        project_name = get_settings().hpc_cluster.datamover.job_prefix
        name = f"{project_name}_{worker_name}"
        delete_queue(name)
        delete_current_workers(worker_name)
        if status_file.exists() and status_file.is_file():
            os.remove(status_file_path)
        exit(1)
