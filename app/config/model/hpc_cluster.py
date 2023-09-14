from enum import Enum
from typing import List
from pydantic import BaseModel



class HpcClusterConfiguration(BaseModel):
    job_project_name: str = "metabolights-ws"
    
    job_submit_command: str = "bsub"
    job_running_command: str = "bjobs"
    job_kill_command: str = "bkill"
    job_track_log_location: str
    job_status_read_timeout: int = 10
    task_get_timeout_in_seconds: int = 10

    maximum_shutdown_signal_per_time: int = 1
    number_of_additional_localhost_workers: int = 1
    number_of_remote_vm_workers: int = 0

    vm_worker_hostnames: str
    start_vm_worker_script_template_name: str = "start_vm_worker_template.sh.j2"
    remote_vm_deployment_path: str
    remote_vm_conda_environment: str
    localhost_conda_environment: str
    fella_pathway_script_path: str


class HpcConnection(BaseModel):
    host: str
    username: str


class DataMoverPathConfiguration(BaseModel):
    cluster_study_metadata_files_root_path: str
    cluster_study_internal_files_root_path: str
    cluster_study_audit_files_root_path: str
    cluster_study_readonly_files_root_path: str
    cluster_study_readonly_audit_files_root_path: str
    
    cluster_study_readonly_files_actual_root_path: str
    cluster_study_readonly_audit_files_actual_root_path: str
    cluster_study_readonly_metadata_files_root_path: str
    cluster_study_readonly_public_metadata_versions_root_path: str
    cluster_study_readonly_integrity_check_files_root_path: str

    cluster_readonly_storage_recycle_bin_root_path: str
    cluster_rw_storage_recycle_bin_root_path: str

    cluster_private_ftp_root_path: str
    cluster_private_ftp_recycle_bin_root_path: str
    cluster_public_ftp_root_path: str
    cluster_public_ftp_recycle_bin_root_path: str
    
    cluster_legacy_study_files_root_path: str
    cluster_reports_root_path: str
    cluster_compounds_root_path: str
    
    
class SingularityConfiguration(BaseModel):
    docker_deployment_path: str = "/app-root"
    run_singularity_script_template_name: str
    worker_deployment_root_path: str
    singularity_image: str
    singularity_docker_username: str
    singularity_docker_password: str
    user_home_binding_source_path: str
    user_home_binding_target_path: str
    logs_path: str
    config_file_path: str
    secrets_path: str
    shared_paths: List[str]

class WorkerConfiguration(BaseModel):
    number_of_datamover_workers: int = 1
    start_datamover_worker_script: str = "start_datamover_worker.sh"
    datamover_worker_maximum_uptime_in_seconds: int = 3 * 24 * 60 * 60
    broker_queue_names: str

    
class HpcDataMoverSettings(BaseModel):
    connection: HpcConnection
    queue_name: str
    cluster_private_ftp_user_home_path: str
    mounted_paths: DataMoverPathConfiguration
    worker: WorkerConfiguration

class HpcComputeSettings(BaseModel):
    connection: HpcConnection
    standard_queue: str ="standard"
    long_process_queue: str ="long"
    default_queue: str = "standard"


class HpcClusterSettings(BaseModel):
    datamover: HpcDataMoverSettings
    compute: HpcComputeSettings
    ssh_command: str
    configuration: HpcClusterConfiguration
    singularity: SingularityConfiguration

