


import sys
import time
import app as current_app
from app.tasks.datamover_worker_utils import ping_datamover_worker, restart_datamover_worker


if __name__ == "__main__":
    worker_name = "sample"
    if len(sys.argv) > 1 and sys.argv[1]:         
        worker_name = sys.argv[1]
    if not worker_name:
        print("Invalid worker name.")
        exit(1)
    max_retry_count = 5
    current_retry_count = 0
    while True: 
        worker_version = ping_datamover_worker(worker_name)
        if not worker_version:
            current_retry_count += 1
        else:
            if worker_version == current_app.__api_version__:
                current_retry_count = 0 
            else:
                print(f"Versions are not same. App version: {current_app.__api_version__}, Datamover worker version: {worker_version}")
        if current_retry_count >= max_retry_count:
            print("There is no response from datamover. Exiting")
            exit(1)
        
        time.sleep(10)
