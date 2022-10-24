import os
from typing import List


class DataMoverAvailableStorage():

    def __init__(self, name, ftp_user_home_path, relative_ftp_private_studies_root_path, absolute_studies_root_path):
        self.name = name
        self.ftp_user_home_path = ftp_user_home_path
        self.relative_studies_root_path = relative_ftp_private_studies_root_path.lstrip('/')
        self.ftp_private_studies_path = os.path.join(ftp_user_home_path, relative_ftp_private_studies_root_path.rstip('/'))
        self.absolute_studies_root_path = absolute_studies_root_path

    def sync_from_studies_folder(self, source_study_folder: str, target_ftp_folder: str, ignore_list: List[str] = None, **kwargs):
        source_study_path = self._get_absolute_study_path(source_study_folder)
        target_ftp_path = self._get_absolute_ftp_private_path(target_ftp_folder)
        pass

    def sync_from_ftp_folder (self, source_ftp_folder: str, target_study_folder: str, ignore_list: List[str] = None, **kwargs):
        target_study_path = self._get_absolute_study_path(target_study_folder)
        source_ftp_path = self._get_absolute_ftp_private_path(source_ftp_folder)
        pass

    def create_ftp_folder(self, study_folder_name: str, chmod=0o770, exist_ok: bool = True) -> bool:
        """
        Create FTP folder and RAW_FILES and DERIVED_FILES folders
        """
        study_folder_path = self._get_absolute_ftp_private_path(study_folder_name)

        pass

    def delete_ftp_folder(self, study_folder_name: str) -> bool:
        """
        Delete FTP study folder
        """
        study_folder_path = self._get_absolute_ftp_private_path(study_folder_name)
        pass

    def move_ftp_folder(self, study_folder_name: str, target_path) -> bool:
        """
        Move FTP study folder to other path
        """
        study_folder_path = self._get_absolute_ftp_private_path(study_folder_name)
        target_study_folder_path = self._get_absolute_ftp_private_path(target_path)
        pass

    def get_folder_permission(self, source: str) -> str:
        pass

    def does_folder_exist(self, source: str) -> bool:
        pass

    def update_ftp_folder_permission(self, study_folder_name: str, chmod) -> bool:

        study_folder_path = self._get_absolute_ftp_private_path(study_folder_name)
        pass

    def _get_absolute_ftp_private_path(self, relative_path: str) -> str:
        return os.path.join(self.ftp_private_studies_path, relative_path.lstrip('/'))

    def _get_absolute_study_path(self, relative_path: str) -> str:
        return os.path.join(self.absolute_studies_root_path, relative_path.lstrip('/'))

    def _get_absolute_path(self, root_path: str, relative_path: str) -> str:
        return os.path.join(root_path, relative_path.lstrip('/'))