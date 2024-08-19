from pydantic import BaseModel


class StudyMountedPaths(BaseModel):
    study_metadata_files_root_path: str
    study_internal_files_root_path: str
    study_audit_files_root_path: str
    study_readonly_files_actual_root_path: str
    study_readonly_audit_files_actual_root_path: str
    study_readonly_files_root_path: str
    
    study_readonly_audit_files_root_path: str
    study_readonly_metadata_files_root_path: str
    study_readonly_public_metadata_versions_root_path: str
    study_readonly_integrity_check_files_root_path: str

    readonly_storage_recycle_bin_root_path: str
    rw_storage_recycle_bin_root_path: str
    
    legacy_study_files_root_path: str
    
    private_ftp_root_path: str = ""
    private_ftp_recycle_bin_root_path: str = ""
    public_ftp_root_path: str = ""
    public_ftp_recycle_bin_root_path: str = ""
    public_ftp_download_path:str = "ftp://ftp.ebi.ac.uk/pub/databases/metabolights/studies/public"
    
    reports_root_path: str
    compounds_root_path: str
    
class StudySettings(BaseModel):
    mounted_paths: StudyMountedPaths
    
    check_and_use_legacy_study_files_storage_if_it_exists: bool = True
    
    private_ftp_user_home_path: str = ""
    max_study_in_submitted_status: int = 2
    min_study_creation_interval_in_mins: int = 5
    audit_files_symbolic_link_name: str = "AUDIT_FILES"
    internal_files_symbolic_link_name: str = "INTERNAL_FILES"
    readonly_files_symbolic_link_name: str = "FILES"

    audit_folder_name: str = "audit"
    readonly_audit_folder_symbolic_name: str  = "ARCHIVED_AUDIT_FILES"
    metadata_summary_file_name: str = "metadata_summary.tsv"
    data_files_summary_file_name: str = "data_files_summary.tsv"
    study_folder_maintenance_log_file_name: str = "maintenance_log.tsv"
    metadata_files_signature_file_name: str = "metadata_files_signature.txt"
    data_files_maintenance_file_name: str = "data_files_summary.txt"

    template_sample_file_name: str = "s_Sample.txt"
    
    files_list_json_file_name: str = "files-all.json"
    files_list_json_file_creation_timeout: int = 900

    investigation_file_name: str = "i_Investigation.txt"
    internal_logs_folder_name: str = "logs"
    internal_temp_folder_name: str = "temp"
    internal_backup_folder_name: str = "internal-backup"

    validation_report_file_name: str = "validation_report.json"
    validation_files_json_name: str = "validation_files.json"
    validation_files_limit: int = 10000
    validation_script: str = "/nfs/www-prod/web_hx2/cm/metabolights/scripts/cluster_scripts/val/validation.sh"
    missing_files_name: str = "missing_files.txt"
    max_validation_messages_count_in_response: int = 50
    metabolights_website_link: str = "https://www.ebi.ac.uk/metabolights"
