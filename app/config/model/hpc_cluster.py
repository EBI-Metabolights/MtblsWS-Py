from enum import Enum
from typing import List, Union
from pydantic import BaseModel, Field



class HpcClusterConfiguration(BaseModel):
    job_status_read_timeout: int = 30
    task_get_timeout_in_seconds: int = 30
    fella_pathway_script_path: str = ""


class SshConnection(BaseModel):
    host: str
    username: str
    identity_file: str = "~/.ssh/id_rsa"

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
    
    cluster_reports_root_path: str
    cluster_compounds_root_path: str

class HpcClusterDefaultSettings(BaseModel):
    use_ssh_tunnel: bool = False
    ssh_tunnel: Union[None, SshConnection] = None
    connection: SshConnection
    default_queue: str
    workload_manager: str
    job_default_cpu: Union[float, int] = 1
    job_default_memory_in_mb: int = 2 * 1024 
    job_default_runtime_limit_in_secs: int = 60 * 60
    job_prefix: str = "mtbls-ws-ns"
    job_prefix_demimeter: str = "---"
    job_track_email: str = ""
    job_track_log_location: str
    stdout_datetime_format: str = "%Y-%m-%dT%H:%M:%S"
    shared_path: str = ""

class HpcDataMoverSettings(HpcClusterDefaultSettings):
    cluster_private_ftp_user_home_path: str
    mounted_paths: DataMoverPathConfiguration
    workload_manager: str = "slurm"

class HpcComputeSettings(HpcClusterDefaultSettings):
    standard_queue: str ="standard"
    long_process_queue: str ="long"
    workload_manager: str = "slurm"


class HpcClusterSettings(BaseModel):
    datamover: HpcDataMoverSettings
    compute: HpcComputeSettings
    ssh_command: str = "ssh"
    configuration: HpcClusterConfiguration

