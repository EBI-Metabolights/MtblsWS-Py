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
    
    readonly_storage_recycle_bin_root_path: str
    rw_storage_recycle_bin_root_path: str
    
    cluster_study_metadata_files_root_path:str
    cluster_study_internal_files_root_path:str
    cluster_study_audit_files_root_path:str
    
    cluster_study_readonly_files_root_path:str
    cluster_study_readonly_audit_files_root_path:str
    cluster_study_readonly_metadata_files_root_path:str
    cluster_study_readonly_public_metadata_versions_root_path:str
    cluster_study_readonly_integrity_check_files_root_path:str
    
    cluster_readonly_storage_recycle_bin_root_path: str
    cluster_rw_storage_recycle_bin_root_path: str

    cluster_private_ftp_root_path:str
    cluster_private_ftp_recycle_bin_root_path:str
    
    cluster_public_ftp_root_path:str
    cluster_public_ftp_recycle_bin_root_path:str    
    cluster_private_ftp_user_path: str
    
    audit_files_symbolic_link_name:str = "AUDIT_FILES" 
    internal_files_symbolic_link_name:str = "INTERNAL_FILES"
    readonly_files_symbolic_link_name:str = "FILES"
    
    audit_folder_name:str = "audit"
    chebi_annotation_sub_folder:str = "chebi_pipeline_annotations"
    
    metadata_summary_file_name: str = "metadata_summary.tsv"
    data_files_summary_file_name: str = "data_files_summary.tsv"
    study_folder_maintenance_log_file_name: str = "maintenance_log.tsv"
    metadata_files_signature_file_name: str = "metadata_files_signature.txt"
    data_files_maintenance_file_name: str = "data_files_summary.txt"
    
    metadata_summary_file_name: str = "metadata_summary.tsv"
    data_files_summary_file_name: str = "data_files_summary.tsv"
    study_folder_maintenance_log_file_name: str = "maintenance_log.tsv"
    metadata_files_signature_file_name: str = "metadata_files_signature.txt"
    data_files_maintenance_file_name: str = "data_files_summary.txt"
    
    study_default_template_path:str = "./resources/templates/study/default"
    template_sample_file_name:str = "s_Sample.txt"
    study_partner_metabolon_template_path:str = "./resources/templates/study/metabolon"
    study_mass_spectrometry_maf_file_template_path = "./resources/m_metabolite_profiling_mass_spectrometry_v2_maf.tsv"
    study_nmr_spectroscopy_maf_file_template_path = "./resources/m_metabolite_profiling_NMR_spectroscopy_v2_maf.tsv"
    report_root_path:str
    report_mariana_folder_name: str
    report_base_folder_name: str
    report_global_folder_name: str = "global"
    files_list_json_file_name: str = "files-all.json"
    files_list_json_file_creation_timeout: int = 900
    
    investigation_file_name:str = "i_Investigation.txt"
    internal_logs_folder_name:str = "logs"
    internal_backup_folder_name: str = "internal-backup"
    mzml_xsd_schema_file_path: str = "./resources/mzML1.1.1_idx.xsd"
    
    validation_report_file_name: str = "validation_report.json"
    validation_files_json_name: str = "validation_files.json"
    validation_files_limit:int = 10000
    validations_file:str = "./resources/validation_schema.json"
    validation_script: str = "/nfs/www-prod/web_hx2/cm/metabolights/scripts/cluster_scripts/val/validation.sh"
    missing_files_name: str = "missing_files.txt"
    
    reference_folder: str