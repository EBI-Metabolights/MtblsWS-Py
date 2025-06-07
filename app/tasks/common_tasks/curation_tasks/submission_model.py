from pydantic import BaseModel


class StudySubmissionError(Exception): ...


class MakeStudyPrivateParameters(BaseModel):
    study_id: str
    obfuscation_code: str
    test: bool = False
    current_private_ftp_permission: None | int = None
    make_ftp_folder_readonly_task_status: None | bool = None
    index_data_files_task_status: None | bool = None
    validate_study_task_status: None | bool = None
    make_study_private_task_status: None | bool = None
