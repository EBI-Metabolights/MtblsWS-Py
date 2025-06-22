from pydantic import BaseModel


class StudySubmissionError(Exception): ...


class RevalidateStudyParameters(BaseModel):
    study_id: str
    current_status: int
    obfuscation_code: str
    test: bool = False
    api_token: str = ""
    current_private_ftp_permission: None | int = None
    make_ftp_folder_readonly_task_status: None | bool = None
    sync_metadata_files_task_status: None | bool = None
    index_data_files_task_status: None | bool = None
    validate_study_task_status: None | bool = None
    reindex_study_task_status: None | bool = None


class MakeStudyPrivateParameters(RevalidateStudyParameters):
    make_study_private_task_status: None | bool = None


class MakeStudyPublicParameters(RevalidateStudyParameters):
    revision_comment: None | str = None
    created_by: None | str = None
    prepare_revision_task_status: None | bool = None
    sync_public_revision_task_status: None | bool = None
