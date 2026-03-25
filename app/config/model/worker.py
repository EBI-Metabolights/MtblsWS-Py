from typing import List

from pydantic import BaseModel


class SingularityImageConfiguration(BaseModel):
    docker_deployment_path: str = "/app-root"
    run_singularity_script_template_name: str = "run_singularity.sh.j2"
    worker_deployment_root_path: str
    gitlab_api_token: str
    user_home_binding_source_path: str
    user_home_binding_target_path: str
    logs_path: str
    config_file_path: str = "datamover-config.yaml"
    secrets_path: str = ".datamover-secrets"
    shared_paths: List[str]


class DatamoverWorkerSettings(BaseModel):
    monitor_task_status_key: str = (
        "metabolights:system_monitor_tasks.datamover_worker_maintenance"
    )
    monitor_task_timeout: int = 40
    shutdown_signal_wait_key_prefix: str = (
        "metabolights:datamover_workers:shutdown_signal"
    )
    shutdown_signal_wait_time: int = 3600
    minimum_datamover_workers: int = 3
    maximum_datamover_workers: int = 5
    worker_memory_in_mb: int = 2 * 1024
    worker_cpu: int = 2
    start_datamover_worker_script: str = "start_datamover_worker.sh"
    maximum_uptime_in_seconds: int = 259200
    broker_queue_names: str = "datamover-tasks"
    singularity_image_configuration: SingularityImageConfiguration


class HostWorkerConfiguration(BaseModel):
    hostname: str
    worker_queue_names: str
    mininum_vm_workers: int
    maximim_vm_workers: int
    deployment_path: str
    conda_environment: str


class VmWorkerSettings(BaseModel):
    monitor_task_status_key_prefix: str = (
        "metabolights:system_monitor_tasks.vm_worker_maintenance"
    )
    monitor_task_timeout: int = 40
    start_vm_worker_script_template_name: str = "start_vm_worker_template.sh.j2"
    initiate_vm_worker_key_prefix: str = "metabolights:vm_workers:initiated"
    initiate_vm_worker_wait_timeout: int = 120
    hosts: List[HostWorkerConfiguration] = []


class WorkerSettings(BaseModel):
    vm_workers: VmWorkerSettings = VmWorkerSettings()
    datamover_workers: DatamoverWorkerSettings
