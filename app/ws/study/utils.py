import os

from app.config import get_settings


def get_study_metadata_path(study_id: str):
    study_settings = get_settings().study
    return os.path.join(
        study_settings.mounted_paths.study_metadata_files_root_path, study_id
    )
