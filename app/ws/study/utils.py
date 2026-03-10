import os

from app.config import get_settings


def get_study_metadata_path(study_id: str):
    study_settings = get_settings().study
    return os.path.join(
        study_settings.mounted_paths.study_metadata_files_root_path, study_id
    )


def get_study_audit_files_path(study_id: str):
    study_settings = get_settings().study
    return os.path.join(
        study_settings.mounted_paths.study_audit_files_root_path, study_id, "audit"
    )


def get_study_internal_files_path(study_id: str):
    study_settings = get_settings().study
    return os.path.join(
        study_settings.mounted_paths.study_internal_files_root_path, study_id
    )


def get_cluster_study_data_files_path(study_id: str, obfuscation_code: str):
    mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths
    folder_path = f"{study_id.lower()}-{obfuscation_code}"
    return os.path.join(mounted_paths.cluster_private_ftp_root_path, folder_path)
