import os
import socket
import time
from typing import List

import kombu

from app.config import get_settings
from app.tasks.hpc_client import HpcClient, HpcJob
from app.tasks.hpc_utils import get_new_hpc_client
from app.tasks.system_monitor_tasks import heartbeat
from app.tasks.worker import celery as app


def get_status(worker_name: str):
    client: HpcClient = get_new_hpc_client()
    project_name = get_settings().hpc_cluster.configuration.job_project_name
    name = f"{project_name}_{worker_name}"
    jobs: List[HpcJob] = client.get_job_status([name])
    return jobs
    
def create_datamover_worker(worker_name: str):
    project_name = get_settings().hpc_cluster.configuration.job_project_name
    name = f"{project_name}_{worker_name}"
    client: HpcClient = get_new_hpc_client()
    settings = get_settings()
    worker_config = settings.workers.datamover_workers
    command = os.path.join(
        worker_config.singularity_image_configuration.docker_deployment_path,
        worker_config.start_datamover_worker_script,
    )
    args = worker_config.broker_queue_names.split(",")
    # args.append(name)
    sif_image_file_url = os.environ.get("SINGULARITY_IMAGE_FILE_URL")

    job_id, _ = client.run_singularity(
        name, command, ",".join(args) + f" {name}", 
        unique_task_name=False,
        sif_image_file_url=sif_image_file_url
    )

    return job_id

def create_queue(name: str):
    if not app.conf.task_queues:
        app.conf.task_queues = []
    queue_exists = False
    for item in app.conf.task_queues:
        queue: kombu.Queue = item
        if queue.name == name:
            queue_exists = True
            break
    if not queue_exists:
        print(f"Queue is created for datamover worker: {name}")
        app.conf.task_queues.append(kombu.Queue(name=name, routing_key="heartbeat"))


def delete_queue(name: str):
    if not app.conf.task_queues:
        return
    target = None
    for item in app.conf.task_queues:
        queue: kombu.Queue = item
        if queue.name == name:
            target = queue
            break
    if not target:
        print(f"Queue will be deleted for datamover worker: {name}")
        app.conf.task_queues.remove(queue)


def delete_current_workers(worker_name: str):
    kill_old_worker_error = False
    for _ in range(3):
        jobs: List[HpcJob] = get_status(worker_name)
        if jobs:
            job_ids = [job.job_id for job in jobs]
            client: HpcClient = get_new_hpc_client()
            killed_ids, stdout, stderr = client.kill_jobs(
                job_ids, failing_gracefully=True
            )
            if len(killed_ids) != len(job_ids):
                print(f"{stdout}, {stderr}")
                kill_old_worker_error = True
            else:
                print("Current workers were killed")
                break
        else:
            print("No worker runs on datamover")
            break
    if kill_old_worker_error:
        print("Current worker still running on datamover")
        return False
    return True


def start_worker(worker_name: str):
    up = False
    for _ in range(5):
        create_datamover_worker(worker_name)
        time.sleep(5)
        started = False
        for _ in range(3):
            jobs = get_status(worker_name)
            if jobs and "RUN" in jobs[0].status.upper():
                started = True
                break
            time.sleep(10)
        if started:
            up = True
            break
    if not up:
        print("Datamover worker failed.")
        return False
    else:
        print("Datamover worker is running.")
        return True


def restart_datamover_worker(worker_name: str):
    project_name = get_settings().hpc_cluster.configuration.job_project_name
    name = f"{project_name}_{worker_name}"

    success = delete_current_workers(worker_name)
    if not success:
        return False

    success = start_worker(worker_name)
    if not success:
        return False

    create_queue(name)

    worker_version = ping_datamover_worker(worker_name)
    if not worker_version:
        print("Datamover worker is not active.")
        return False
    else:
        print(f"Datamover worker '{name}' is active now.")
        return True


def ping_datamover_worker(worker_name: str, retry=1, timeout=5, wait_period=1):
    project_name = get_settings().hpc_cluster.configuration.job_project_name
    name = f"{project_name}_{worker_name}"

    input_value = socket.gethostname()
    for _ in range(retry):
        task = heartbeat.ping.apply_async(queue=name, args=[input_value])
        try:
            result = task.get(timeout=timeout)
            if result and "reply_for" in result and result["reply_for"] == input_value:
                if result and "worker_version" in result:
                    return result["worker_version"]
                else:
                    return None
            else:
                time.sleep(wait_period)
        except Exception as ex:
            print(f"No response from datamover worker {name}: {str(ex)}")

    return None
