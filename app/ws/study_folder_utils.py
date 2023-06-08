
import os

from app.file_utils import make_dir_with_chmod
from app.utils import MetabolightsFileOperationException
from app.ws.isaApiClient import IsaApiClient
from app.ws.settings.utils import get_study_settings
from app.ws.utils import (copy_files_and_folders, get_year_plus_one, read_tsv,
                          update_correct_sample_file_name, write_tsv)

iac = IsaApiClient()

def create_initial_study_folder(folder_name, app):
    settings = get_study_settings()
    study_metadata_root_path = settings.study_metadata_files_root_path
    
    from_path = settings.study_default_template_path
    study_metadata_location = os.path.join(study_metadata_root_path, folder_name)
    to_path = study_metadata_location
    if os.path.exists(to_path):
        raise MetabolightsFileOperationException(f'Study folder {folder_name} already exists.')
    
    result, message = copy_files_and_folders(from_path, to_path,
                                                include_raw_data=True,
                                                include_investigation_file=True)
    if not result:
        raise MetabolightsFileOperationException(
            'Could not copy files from {0} to {1}'.format(from_path, to_path))
    return to_path
