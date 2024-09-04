from pathlib import Path
import re
from typing import Union

from pydantic import BaseModel


class StudyPaths(BaseModel):
    rw_metadata_files: Union[Path, None] = None
    rw_internal_files: Union[Path, None] = None
    rw_audit_files: Union[Path, None] = None
    readonly_metadata_files: Union[Path, None] = None
    actual_readonly_files: Union[Path, None] = None
    actual_readonly_audit_files: Union[Path, None] = None
    readonly_files: Union[Path, None] = None
    readonly_audit_files: Union[Path, None] = None
    readonly_public_metadata_versions: Union[Path, None] = None
    readonly_integrity_check_files: Union[Path, None] = None
    private_ftp_files: Union[Path, None] = None
    public_ftp_files: Union[Path, None] = None


class StudyManagedFiles(BaseModel):
    validation_report: Union[Path, None] = None
    
    
class StudyManagedFolders(BaseModel):
    files_link: Union[Path, None] = None
    internal_files_link: Union[Path, None] = None
    audit_files_link: Union[Path, None] = None
    
    rw_audit_folder: Union[Path, None] = None
    rw_logs_folder: Union[Path, None] = None
    rw_temp_folder: Union[Path, None] = None
    rw_integrity_check_folder: Union[Path, None] = None
    chebi_pipeline_annotations_folder: Union[Path, None] = None
    readonly_audit_folder: Union[Path, None] = None
    rw_study_recycle_bin_folder: Union[Path, None] = None
    readonly_study_recycle_bin_folder: Union[Path, None] = None
    private_ftp_study_recycle_bin_folder: Union[Path, None] = None
    public_ftp_recycle_bin_folder: Union[Path, None] = None