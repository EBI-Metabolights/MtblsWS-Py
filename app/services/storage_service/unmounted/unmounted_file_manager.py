from app.services.storage_service.acl import Acl
from app.services.storage_service.file_manager import FileManager
from app.services.storage_service.unmounted.data_mover_client import DataMoverAvailableStorage
from app.utils import MetabolightsException


class UnmountedVolumeFileManager(FileManager):

    def __init__(self, name, app=None):
        self.app = app
        super(UnmountedVolumeFileManager, self).__init__(name=name)

    def create_folder(self, target: str, acl: Acl = Acl.AUTHORIZED_READ_WRITE, exist_ok: bool = True) -> bool:
        study_id = self.get_study_id(target)
        if not study_id:
            raise MetabolightsException("Invalid study id")
        try:
            remote_job_manager = DataMoverAvailableStorage("create_folder", study_id, self.app)
            result = remote_job_manager.create_ftp_folder(target, acl, exist_ok)
        except (OSError, Exception):
            return False

        return result

    def delete_folder(self, target: str) -> bool:
        study_id = self.get_study_id(target)
        if not study_id:
            raise MetabolightsException("Invalid study id")
        try:
            remote_job_manager = DataMoverAvailableStorage("delete_folder", study_id, self.app)
            result = remote_job_manager.delete_ftp_folder(target)
        except (OSError, Exception):
            return False
        return result

    def get_folder_permission(self, source: str) -> Acl:
        try:
            study_id = self.get_study_id(source)
            if not study_id:
                raise MetabolightsException("Invalid study id")
            remote_job_manager = DataMoverAvailableStorage("get_folder_permission", study_id, self.app)
            chmod = remote_job_manager.get_ftp_folder_permission(source)
            if len(chmod) > 3:
                chmod = chmod[-3:]
            chmod_int = int(chmod, 8)
            permission = Acl(chmod_int)
        except (OSError, Exception):
            permission = Acl.UNKNOWN

        return permission

    def update_folder_permission(self, source: str, acl: Acl = Acl.AUTHORIZED_READ_WRITE) -> bool:
        try:
            study_id = self.get_study_id(source)
            if not study_id:
                raise MetabolightsException("Invalid study id")
            remote_job_manager = DataMoverAvailableStorage("update_folder_permission", study_id, self.app)
            result = remote_job_manager.update_ftp_folder_permission(source, chmod=acl)
        except (OSError, Exception):
            return False
        return result

    def does_folder_exist(self, source: str) -> bool:
        try:
            study_id = self.get_study_id(source)
            if not study_id:
                raise MetabolightsException("Invalid study id")
            remote_job_manager = DataMoverAvailableStorage("does_folder_exist", study_id, self.app)
            result = remote_job_manager.does_folder_exist(source)
        except OSError:
            return False
        return result
    
    @staticmethod
    def get_study_id(value: str):
        if not value:
            return None
        splitted_value = value.split('-')

        if len(splitted_value) > 1 and splitted_value[0]:
            return splitted_value[0].upper()
        return None