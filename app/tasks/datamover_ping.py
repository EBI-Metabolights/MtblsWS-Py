import logging
import socket

import app as current_app
from app.tasks.datamover_worker_utils import ping_datamover_worker

logger = logging.getLogger('wslog_datamover')

if __name__ == "__main__":
    worker_name = socket.gethostname()

    if not worker_name:
        print("Invalid worker name.")
        exit(1)
        

    try:
        worker_version = ping_datamover_worker(worker_name)
        if not worker_version:
            print(f"Liveness test failed. No response.")
            exit(1)
        if worker_version == current_app.__api_version__:
            print(f"Liveness test successful.")
        else:
            print(
                f"Liveness test successful. but versions are not same. App version: {current_app.__api_version__}, "
                f"Datamover worker version: {worker_version}"
            )

    except Exception as ex:
        print(f"Liveness test failed: {str(ex)}")
        exit(1)
