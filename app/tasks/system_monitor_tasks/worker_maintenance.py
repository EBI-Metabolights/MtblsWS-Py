import logging
import os
import shutil
import socket
from typing import Any, Dict, List, Union
from app.config import get_settings
from app.tasks.bash_client import BashClient

from app.tasks.lsf_client import LsfClient
from app.tasks.worker import MetabolightsTask, celery
from app.ws.settings.utils import get_cluster_settings

logger = logging.getLogger("beat")


@celery.task(bind=True, base=MetabolightsTask, name="app.tasks.system_monitor_tasks.worker_maintenance.check_all_workers")
def check_all_workers(
    self,
    number_of_datamover_workers: int = -1,
    number_of_additional_localhost_workers=-1,
    remote_vm_hostnames: Union[str, List[str]] = None,
    number_of_remote_vm_workers=-1,
    maximum_shutdown_signal_per_time: int = -1,
):
    results = {}
    registered_workers = celery.control.inspect().stats()
    result_1 = check_datamover_workers(
        number_of_workers=number_of_datamover_workers,
        maximum_shutdown_signal_per_time=maximum_shutdown_signal_per_time,
        registered_workers=registered_workers,
    )

    result_2 = check_additional_vm_workers(
        number_of_additional_localhost_workers=number_of_additional_localhost_workers,
        remote_vm_hostnames=remote_vm_hostnames,
        number_of_remote_vm_workers=number_of_remote_vm_workers,
        maximum_shutdown_signal_per_time=maximum_shutdown_signal_per_time,
        registered_workers=registered_workers,
    )
    results.update(result_1)
    results.update(result_2)
    return results


@celery.task(bind=True, base=MetabolightsTask, name="app.tasks.system_monitor_tasks.worker_maintenance.check_additional_vm_workers")
def check_additional_vm_workers(
    self,
    number_of_additional_localhost_workers: int = -1,
    number_of_remote_vm_workers: int = -1,
    remote_vm_hostnames: Union[str, List[str]] = None,
    maximum_shutdown_signal_per_time: int = -1,
    registered_workers: Dict[str, Any] = None,
    start_maximum_worker_on_vm_per_time=1,
) -> Dict[str, str]:
    results = {}
    started_localhost_workers = 0
    started_remote_vm_workers = {}
    cluster_settings = get_cluster_settings()
    if number_of_additional_localhost_workers < 0:
        number_of_additional_localhost_workers = max(cluster_settings.number_of_additional_localhost_workers, 0)

    if not registered_workers:
        registered_workers = celery.control.inspect().stats()

    if maximum_shutdown_signal_per_time < 0:
        maximum_shutdown_signal_per_time = max(cluster_settings.maximum_shutdown_signal_per_time, 0)

    if not remote_vm_hostnames:
        remote_vm_hostnames = cluster_settings.vm_worker_hostnames.split(",")

    if number_of_remote_vm_workers < 0:
        number_of_remote_vm_workers = max(cluster_settings.number_of_remote_vm_workers, 0)

    local_hostname = socket.gethostname()
    worker_name_prefix = f"{cluster_settings.job_project_name}_vm_worker"
    for i in range(number_of_additional_localhost_workers):
        messages = []
        index = i + 1
    
        full_worker_name = f"{worker_name_prefix}_{index}@{local_hostname}"
        logger.info(f"Check {full_worker_name}")
        worker_name = f"{worker_name_prefix}_{index}"
        
        
        if full_worker_name not in registered_workers:
            if started_localhost_workers < start_maximum_worker_on_vm_per_time:
                paramters = {
                    "application_deployment_path": os.getcwd(),
                    "worker_name": worker_name,
                    "worker_queue": "common-tasks",
                    "conda_environment": cluster_settings.localhost_conda_environment,
                }
                file_path = BashClient.prepare_script_from_template(
                    cluster_settings.start_vm_worker_script_template_name, **paramters
                )
                result = BashClient.execute_command(f"bash < {file_path}")
                try:
                    os.remove(file_path)
                except Exception as exc:
                    logger.warning(f"Error while deleting temp file {file_path}")
                        
                started_localhost_workers += 1
                messages.append("Worker was started.")
            else:
                messages.append("Worker will start next time.")
        else:
            messages.append("Worker is already registered.")

        results[full_worker_name] = " ".join(messages)

    for remote_hostname in remote_vm_hostnames:
        started_remote_vm_workers[remote_hostname] = 0
        for i in range(number_of_remote_vm_workers):
            messages = []
            index = i + 1
            worker_name = f"{worker_name_prefix}_{index}"
            full_worker_name = f"{worker_name_prefix}_{index}@{remote_hostname}"
            if full_worker_name not in registered_workers:
                if started_remote_vm_workers[remote_hostname] < start_maximum_worker_on_vm_per_time:
                    ssh_command = BashClient.build_ssh_command(hostname=remote_hostname, username=get_settings().hpc_cluster.datamover.connection.username)
                    paramters = {
                        "application_deployment_path": cluster_settings.remote_vm_deployment_path,
                        "worker_name": worker_name,
                        "worker_queue": "common-tasks",
                        "conda_environment": cluster_settings.remote_vm_conda_environment,
                    }
                    file_path = BashClient.prepare_script_from_template(
                        cluster_settings.start_vm_worker_script_template_name, **paramters
                    )
                    result = BashClient.execute_command(f"{ssh_command} bash < {file_path}")
                    try:
                        os.remove(file_path)
                    except Exception as exc:
                        logger.warning(f"Error while deleting temp file {file_path}. {str(exc)}")
                    started_remote_vm_workers[remote_hostname] += 1
                    messages.append("Worker was started.")
                else:
                    messages.append("Worker will start next time.")
            else:
                messages.append("Worker is already registered.")
            
            results[full_worker_name] = " ".join(messages)

    return results


@celery.task(bind=True, base=MetabolightsTask, name="app.tasks.system_monitor_tasks.worker_maintenance.check_datamover_workers")
def check_datamover_workers(
    self,
    number_of_workers: int = -1,
    maximum_shutdown_signal_per_time: int = 1,
    registered_workers: Dict[str, Any] = None,
) -> Dict[str, str]:
    results = {}
    cluster_settings = get_cluster_settings()
    if number_of_workers < 0:
        number_of_workers = max(get_settings().hpc_cluster.datamover.worker.number_of_datamover_workers, 0)
    if maximum_shutdown_signal_per_time < -1:
        maximum_shutdown_signal_per_time = max(cluster_settings.maximum_shutdown_signal_per_time, 0)

    if not registered_workers:
        registered_workers = celery.control.inspect().stats()

    uptimes = {}
    for key in registered_workers:
        uptimes[key] = 0
        stat = registered_workers[key]
        if "uptime" in stat:
            uptime = stat["uptime"]
            uptimes[key] = uptime
    shutdown_signal_count = 0
    worker_name_prefix = (
        f"{cluster_settings.job_project_name}_{get_settings().hpc_cluster.datamover.queue_name}_worker"
    )

    client = LsfClient()
    try:
        jobs = client.get_job_status()
    except Exception as exc:
        logger.info("There is no datamover job")
        jobs = []
    current_workers = []
    for job in jobs:
        if job[1].startswith(worker_name_prefix):
            current_workers.append(job)

    for i in range(number_of_workers):
        index = i + 1
        running_worker_names = set()
        send_shutdown_signal = False
        worker_name = f"{worker_name_prefix}_{index}"
        results[worker_name] = ""
        running_instances = []
        pending_instances = []
        finished_instances = []

        for worker in current_workers:
            if worker[1] == worker_name:
                if worker[2] == "RUN":
                    running_instances.append(worker)
                    for registered_worker in registered_workers:
                        if worker_name in registered_worker:
                            running_worker_names.add(registered_worker)
                elif worker[2] == "PEND":
                    pending_instances.append(worker)
                else:
                    finished_instances.append(worker)
        kill_job_candidates = []
        create_new_job = False
        selected_candidate = None
        if len(finished_instances) > 0:
            kill_job_candidates.extend([x[0] for x in finished_instances])

        if len(running_instances) > 0:
            if len(pending_instances) > 0:
                kill_job_candidates.extend([x[0] for x in pending_instances])

            if len(running_instances) > 1:
                # stop all instances if there are multiple worker with same name
                send_shutdown_signal = True
            else:
                if shutdown_signal_count < maximum_shutdown_signal_per_time:
                    max_uptime = get_settings().hpc_cluster.datamover.worker.datamover_worker_maximum_uptime_in_seconds
                    for name in running_worker_names:
                        send_shutdown_signal = True if name in uptimes and uptimes[name] > max_uptime else False
                if not send_shutdown_signal:
                    selected_candidate = running_instances[0]
        else:
            if len(pending_instances) > 0:
                selected_candidate = pending_instances[0]
                kill_job_candidates.extend(
                    [x[0] for x in pending_instances if x != selected_candidate],
                )
            else:
                create_new_job = True
        messages = []
        if selected_candidate:
            messages.append(f"Selected job id {selected_candidate[0]}, its state {selected_candidate[2]}")
        if kill_job_candidates:
            try:
                client.kill_jobs(kill_job_candidates, failing_gracefully=True)
                messages.append(f"Killed jobs {', '.join(kill_job_candidates)}")
            except Exception as exc:
                message = f"Exception after kill jobs command. {str(exc)}"
                logger.warning(message)
                messages.append(message)
        if send_shutdown_signal:
            if shutdown_signal_count < maximum_shutdown_signal_per_time:
                shutdown_signal_count += 1
            celery.control.broadcast("shutdown", destination=list(running_worker_names))
            messages.append(f"Shutdown signal was sent to {', '.join(running_worker_names)}")

        if create_new_job:
            settings = get_settings()
            docker_config = settings.hpc_cluster.singularity
            command = os.path.join(docker_config.docker_deployment_path, settings.hpc_cluster.datamover.worker.start_datamover_worker_script)
            command_arguments = settings.hpc_cluster.datamover.worker.broker_queue_names
            client.run_singularity(worker_name, command, command_arguments, unique_task_name=False)
            
        results[worker_name] = " ".join(messages)

    return results

if __name__ == "__main__":
    check_additional_vm_workers()
    # cluster_settings = get_cluster_settings()
    # project_name = cluster_settings.job_project_name
    # worker_name_prefix = (
    #     f"{cluster_settings.job_project_name}_{get_settings().hpc_cluster.datamover.queue_name}_worker"
    # )
    # local_hostname = socket.gethostname()
    # results = celery.control.inspect().stats()
    # for key in results:
    #     if f"{worker_name_prefix}_1" in key:
    #         celery.control.broadcast("shutdown", destination=[key])
    #     print(results[key])
