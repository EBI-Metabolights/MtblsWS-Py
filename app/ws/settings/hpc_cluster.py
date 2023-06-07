from functools import lru_cache

from app.ws.settings.base import MetabolightsBaseSettings


class HpcClusterSettings(MetabolightsBaseSettings):
    cluster_lsf_host:str
    cluster_lsf_host_user:str
    cluster_lsf_datamover_user:str
    
    cluster_lsf_host_ssh_command:str="/usr/bin/ssh"
    cluster_lsf_bsub_default_queue:str="short"
    cluster_lsf_bsub_datamover_queue:str="datamover"
    cluster_lsf_bsub_standard_queue:str="standard"
    cluster_lsf_bsub_long_process_queue:str="long"

    cluster_study_metadata_files_root_path: str
    cluster_study_internal_files_root_path: str
    cluster_study_audit_files_root_path:str
    
    cluster_study_readonly_files_root_path:str
    cluster_study_readonly_audit_files_root_path:str
    cluster_study_readonly_metadata_files_root_path:str
    cluster_study_readonly_public_metadata_versions_root_path:str
    cluster_study_readonly_integrity_check_files_root_path:str
    
    cluster_ftp_private_root_path: str
    cluster_ftp_public_root_path:str
    
    job_submit_command:str = "bsub"
    job_running_command:str = "bjobs"
    job_kill_command:str = "bjobs"
    job_track_email:str
    job_track_log_location:str
    job_status_read_timeout:int=10