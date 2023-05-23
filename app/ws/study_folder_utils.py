
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



def copy_initial_study_files(folder_name, app):    
    settings = get_study_settings()
    study_metadata_root_path = settings.study_metadata_files_root_path
    from_path = settings.study_default_template_path
    study_metadata_location = os.path.join(study_metadata_root_path, folder_name)
    to_path = study_metadata_location
    if not os.path.exists(to_path):
        raise MetabolightsFileOperationException(f'Study folder {folder_name} does not exist.')
    
    log_path = os.path.join(settings.study_internal_files_root_path, folder_name, 'logs')
    make_dir_with_chmod(log_path, 0o777)

    result, message = copy_files_and_folders(from_path, to_path,
                                                include_raw_data=True,
                                                include_investigation_file=True)
    if not result:
        raise MetabolightsFileOperationException(
            'Could not copy files from {0} to {1}'.format(from_path, to_path))
    return to_path


def prepare_rw_study_folder_structure(study_id: str):
    study_acc = study_id
    study_settings = get_study_settings()
    internal_file_path = os.path.join(study_settings.study_internal_files_root_path, study_acc)
    make_dir_with_chmod(internal_file_path, 0o755)
    
    log_path = os.path.join(internal_file_path, study_settings.internal_logs_folder_name)
    make_dir_with_chmod(log_path, 0o777)
    
    audit_path = os.path.join(study_settings.study_audit_files_root_path, study_acc, study_settings.audit_folder_name)
    make_dir_with_chmod(audit_path, 0o755)

    read_only_files_path =  os.path.join(study_settings.study_readonly_files_root_path, study_acc)
    
    readonly_files_symbolic_link_path =  os.path.join(study_settings.study_metadata_files_root_path, study_acc, study_settings.readonly_files_symbolic_link_name)
    audit_folder_symbolic_link_path:str = os.path.join(study_settings.study_metadata_files_root_path, study_acc, study_settings.audit_files_symbolic_link_name)
    internal_file_symbolic_link_path:str = os.path.join(study_settings.study_metadata_files_root_path, study_acc, study_settings.internal_files_symbolic_link_name)
    
    os.symlink(read_only_files_path, readonly_files_symbolic_link_path)
    os.symlink(audit_path, audit_folder_symbolic_link_path)
    os.symlink(internal_file_path, internal_file_symbolic_link_path)

def update_initial_study_files(study_folder_path, study_acc, user_token):
    settings = get_study_settings()
    if os.path.isfile(os.path.join(study_folder_path, settings.investigation_file_name)):
        # Get the ISA documents so we can edit the investigation file
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id=study_acc, api_key=user_token,
                                                            skip_load_tables=True, study_location=study_folder_path)

        # Also make sure the sample file is in the standard format of 's_MTBLSnnnn.txt'
        isa_study, sample_file_name = update_correct_sample_file_name(isa_study, study_folder_path, study_acc)

        # Set publication date to one year in the future
        study_date = get_year_plus_one(isa_format=True)
        isa_study.public_release_date = study_date

        # Updated the files with the study accession
        iac.write_isa_study(
            inv_obj=isa_inv, api_key=user_token, std_path=study_folder_path,
            save_investigation_copy=False, save_samples_copy=False, save_assays_copy=False
        )

        # For ISA-API to correctly save a set of ISA documents, we need to have one dummy sample row
        file_name = os.path.join(study_folder_path, sample_file_name)
        try:
            sample_df = read_tsv(file_name)

            try:
                sample_df = sample_df.drop(sample_df.index[0])  # Drop the first dummy row, if there is one
            except IndexError:
                pass
                # logger.info("No empty rows in the default sample sheet template, so nothing to remove")

            write_tsv(sample_df, file_name)
        except FileNotFoundError as ex:
            raise MetabolightsFileOperationException(message="The file " + file_name + " was not found", http_code=400, exception=ex)
    else:
        raise MetabolightsFileOperationException(message="Could not find ISA-Tab investigation template for study {0}".format(study_acc), http_code=409)