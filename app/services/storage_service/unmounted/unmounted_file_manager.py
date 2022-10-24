from app.services.storage_service.acl import Acl
from app.services.storage_service.file_manager import FileManager
from app.services.storage_service.unmounted.data_mover_client import DataMoverAvailableStorage


class UnmountedVolumeFileManager(FileManager):

    def __init__(self, name, app=None):
        super(UnmountedVolumeFileManager, self).__init__(name=name)
        absolute_studies_root_path = app.config.get("STUDY_PATH")
        ftp_private_user_home = app.config.get("PRIVATE_FTP_USER_HOME_PATH")
        relative_ftp_private_studies_root_path = app.config.get("PRIVATE_FTP_RELATIVE_STUDIES_ROOT_PATH")
        self.remote_job_manager = DataMoverAvailableStorage(name,
                                                            ftp_private_user_home,
                                                            relative_ftp_private_studies_root_path,
                                                            absolute_studies_root_path)

    def create_folder(self, target: str, acl: Acl = Acl.AUTHORIZED_READ_WRITE, exist_ok: bool = True) -> bool:
        try:
            result = self.remote_job_manager.create_ftp_folder(target, chmod=acl, exist_ok=exist_ok)
        except (OSError, Exception):
            return False
        return result

    def delete_folder(self, target: str) -> bool:
        try:
            result = self.remote_job_manager.delete_ftp_folder(target)
        except (OSError, Exception):
            return False
        return result

    def get_folder_permission(self, source: str) -> Acl:
        try:
            chmod = self.remote_job_manager.get_folder_permission(source)
            permission = Acl(chmod)
        except (OSError, Exception):
            permission = Acl.UNKNOWN

        return permission

    def update_folder_permission(self, source: str, acl: Acl = Acl.AUTHORIZED_READ_WRITE) -> bool:
        try:
            result = self.remote_job_manager.update_ftp_folder_permission(source, chmod=acl)
        except (OSError, Exception):
            return False
        return result

    def does_folder_exist(self, source: str) -> bool:
        try:
            result = self.remote_job_manager.does_folder_exist(source)
        except (OSError, Exception):
            return False
        return result