from functools import lru_cache

from app.ws.settings.base import MetabolightsBaseSettings


class StudySettings(MetabolightsBaseSettings):
    max_study_in_submitted_status: int = 2
    min_study_creation_interval_in_mins: int = 5
    
    study_metadata_files_root_path:str = ""
    study_readonly_files_root_path:str = ""
    study_audit_files_root_path:str = ""
    study_internal_files_root_path:str = ""
    
    study_default_template_path:str = "./resources/templates/study/default"
    study_partner_metabolon_template_path:str = "./resources/templates/study/metabolon"

    report_mariana_folder_name: str = ""
    report_base_folder_name: str = ""
    files_list_json_file_name: str = "files-all.json"
    files_list_json_file_creation_timeout: int = 900