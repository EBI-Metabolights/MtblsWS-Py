from functools import lru_cache

from app.ws.settings.base import MetabolightsBaseSettings


class StudySettings(MetabolightsBaseSettings):
    max_study_in_submitted_status: int = 2
    min_study_creation_interval_in_mins: int = 5
    
    study_metadata_files_root_path:str
    study_internal_files_root_path:str
    study_audit_files_root_path:str
    
    study_readonly_files_root_path:str
    study_readonly_audit_files_root_path:str
    study_readonly_metadata_files_root_path:str
    study_readonly_public_metadata_versions_root_path:str
    study_readonly_integrity_check_files_root_path:str
    
    
    audit_files_symbolic_link_name:str = "AUDIT_FILES" 
    internal_files_symbolic_link_name:str = "INTERNAL_FILES"
    readonly_files_symbolic_link_name:str = "FILES"
    
    audit_folder_name:str = "audit"
    
    study_default_template_path:str = "./resources/templates/study/default"
    study_partner_metabolon_template_path:str = "./resources/templates/study/metabolon"

    report_mariana_folder_name: str
    report_base_folder_name: str
    files_list_json_file_name: str = "files-all.json"
    files_list_json_file_creation_timeout: int = 900
    
    investigation_file_name:str = "i_Investigation.txt"
    internal_logs_folder_name:str = "logs"
    
    mzml_xsd_schema_file_path: str = "./resources/mzML1.1.1_idx.xsd"
    
    validation_report_file_name: str = "validation_report.json"
    validation_files_json_name: str = "validation_files.json"
    validation_files_limit:int = 10000
    validations_file:str = "./resources/validation_schema.json"
    validation_script: str = "/nfs/www-prod/web_hx2/cm/metabolights/scripts/cluster_scripts/val/validation.sh"
    missing_files_name: str = "missing_files.txt"