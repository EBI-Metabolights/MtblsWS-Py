from typing import List
from pydantic import BaseModel, Field


class SingularityImageConfiguration(BaseModel):
    docker_deployment_path: str
    run_singularity_script_template_name: str
    worker_deployment_root_path: str
    gitlab_api_token: str
    user_home_binding_source_path: str
    user_home_binding_target_path: str
    logs_path: str
    config_file_path: str
    secrets_path: str
    shared_paths: List[str]


class DatamoverWorkerSettings(BaseModel):
    monitor_task_status_key: str
    monitor_task_timeout: int
    shutdown_signal_wait_key_prefix: str
    shutdown_signal_wait_time: int
    minimum_datamover_workers: int
    maximum_datamover_workers: int
    start_datamover_worker_script: str
    maximum_uptime_in_seconds: int
    broker_queue_names: str
    singularity_image_configuration: SingularityImageConfiguration


class HostWorkerConfiguration(BaseModel):
    hostname: str
    worker_queue_names: str
    mininum_vm_workers: int
    maximim_vm_workers: int
    deployment_path: str
    conda_environment: str


class VmWorkerSettings(BaseModel):
    monitor_task_status_key_prefix: str
    monitor_task_timeout: int
    start_vm_worker_script_template_name: str
    initiate_vm_worker_key_prefix: str
    initiate_vm_worker_wait_timeout: int
    hosts: List[HostWorkerConfiguration]


class WorkerSettings(BaseModel):
    vm_workers: VmWorkerSettings
    datamover_workers: DatamoverWorkerSettings
