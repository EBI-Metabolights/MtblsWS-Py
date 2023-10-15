


import sys

from app.tasks.datamover_worker_utils import restart_datamover_worker


if __name__ == "__main__":
    worker_name = "sample"
    if len(sys.argv) > 1 and sys.argv[1]:         
        worker_name = sys.argv[1]
    if not worker_name:
        print("Invalid worker name.")
        exit(1)
    success = restart_datamover_worker(worker_name)
        
    if not success:
        print("Datamover start task failed.")
        exit(1)