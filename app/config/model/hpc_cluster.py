from enum import Enum
from typing import List
from pydantic import BaseModel


class HpcClusterConfiguration(BaseModel):
    cluster_lsf_host: str
    cluster_lsf_host_user: str
    cluster_lsf_datamover_user: str

    cluster_lsf_host_ssh_command: str = "/usr/bin/ssh"
    cluster_lsf_bsub_default_queue: str = "short"
    cluster_lsf_bsub_datamover_queue: str = "datamover"
    cluster_lsf_bsub_standard_queue: str = "standard"
    cluster_lsf_bsub_long_process_queue: str = "long"

    cluster_study_metadata_files_root_path: str
    cluster_study_internal_files_root_path: str
    cluster_study_audit_files_root_path: str

    cluster_study_readonly_files_root_path: str
    cluster_study_readonly_audit_files_root_path: str
    cluster_study_readonly_metadata_files_root_path: str
    cluster_study_readonly_public_metadata_versions_root_path: str
    cluster_study_readonly_integrity_check_files_root_path: str

    cluster_readonly_storage_recycle_bin_root_path: str
    cluster_rw_storage_recycle_bin_root_path: str

    cluster_private_ftp_root_path: str
    cluster_private_ftp_recycle_bin_root_path: str
    cluster_public_ftp_root_path: str
    cluster_public_ftp_recycle_bin_root_path: str

    job_submit_command: str = "bsub"
    job_running_command: str = "bjobs"
    job_kill_command: str = "bkill"
    job_track_email: str
    job_track_log_location: str
    job_status_read_timeout: int = 10

    job_project_name: str = "metabolights-ws"
    number_of_datamover_workers: int = 1
    datamover_worker_maximum_uptime_in_seconds: int = 3 * 24 * 60 * 60
    datamover_job_submission_script_template_name: str
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
    private_ftp_study_root_path: str
    public_ftp_study_root_path: str
    study_metadata_root_path: str
    study_data_root_path: str
    

class WorkerConfiguration(BaseModel):
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
    
    
class HpcDataMoverSettings(BaseModel):
    connection: HpcConnection
    queue_name: str
    paths: DataMoverPathConfiguration
    worker: WorkerConfiguration

class HpcComputeSettings(BaseModel):
    connection: HpcConnection
    standard_queue: str
    long_process_queue: str
    default_queue: str


class HpcClusterSettings(BaseModel):
    datamover: HpcDataMoverSettings
    compute: HpcComputeSettings
    ssh_command: str
    configuration: HpcClusterConfiguration
