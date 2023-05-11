import os

from app.services.storage_service.mounted.local_storage import LocalStorage
from app.services.storage_service.storage import Storage
from app.services.storage_service.unmounted.unmounted_storage import UnmountedStorage


class StorageService(object):
    storages = {'ftp_private_storage': None,
                'ftp_public_storage': None,
                'studies_storage': None}

    @staticmethod
    def get_studies_storage(app=None) -> Storage:
        return StorageService._get_local_storage('studies_storage', "STUDY_PATH", app=app)

    @staticmethod
    def get_ftp_private_storage(app=None) -> Storage:
        mount_type = app.config.get("FTP_PRIVATE_MOUNT_TYPE")
        if mount_type and mount_type.lower() == "mounted":
            return StorageService._get_local_storage('ftp_private_storage', "MOUNTED_FTP_PRIVATE_ROOT_PATH", app=app)
        if mount_type and mount_type.lower() == "unmounted":
            return UnmountedStorage('ftp_private_storage', app=app)
        raise NotImplementedError(f"Mounted type {mount_type} is not defined.")

    @staticmethod
    def get_ftp_public_storage(app=None) -> Storage:
        mount_type = app.config.get("FTP_PUBLIC_MOUNT_TYPE")
        if mount_type and mount_type.lower() == "mounted":
            return StorageService._get_local_storage('ftp_public_storage', "MOUNTED_FTP_PUBLIC_ROOT_PATH", app=app)
        if mount_type and mount_type.lower() == "unmounted":
            return UnmountedStorage('ftp_public_storage', app=app)
        raise NotImplementedError(f"Mounted type {mount_type} is not defined.")

    @staticmethod
    def get_report_storage(app=None) -> Storage:
        return StorageService._get_local_storage('report_storage', "REPORTING_ROOT_PATH", app=app)

    @staticmethod
    def _get_local_storage(name: str, environment_variable, app=None) -> Storage:
        if name in StorageService.storages and StorageService.storages[name]:
            return StorageService.storages[name]

        if app:
            remote_path = app.config.get(environment_variable)
        else:
            remote_path = os.getenv(environment_variable)
            if not remote_path:
                raise Exception(f'environment variable {environment_variable} is not found')

        manager = LocalStorage(name, remote_folder=remote_path)
        StorageService.storages[name] = manager
        return manager
    