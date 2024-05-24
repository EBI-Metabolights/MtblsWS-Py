from enum import Enum
from typing import List
from pydantic import BaseModel



class HpcClusterConfiguration(BaseModel):
    job_project_name: str = "metabolights-ws"
    
    job_submit_command: str = "srun"
    job_running_command: str = "sacct"
    job_kill_command: str = "scancel"
    job_default_memory: str = "2G"
    job_default_walltime_limit: str = "60:00"
    job_track_email: str
    job_track_log_location: str
    job_status_read_timeout: int = 10
    task_get_timeout_in_seconds: int = 10
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

    
class HpcDataMoverSettings(BaseModel):
    connection: HpcConnection
    queue_name: str
    cluster_private_ftp_user_home_path: str
    mounted_paths: DataMoverPathConfiguration

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

