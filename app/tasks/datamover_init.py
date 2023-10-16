


import os
import sys

from app.tasks.datamover_worker_utils import restart_datamover_worker


if __name__ == "__main__":
    worker_name = os.getenv('WORKER_NAME')
    
    if not worker_name:
        print("Invalid worker name.")
        exit(1)
    success = restart_datamover_worker(worker_name)
        
    if not success:
        print("Datamover start task failed.")
        exit(1)