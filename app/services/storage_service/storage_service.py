from app.config import get_settings

from app.services.storage_service.mounted.local_storage import LocalStorage
from app.services.storage_service.remote_worker.remote_ftp_storage import RemoteFtpStorage
from app.services.storage_service.storage import Storage
from app.services.storage_service.unmounted.unmounted_storage import UnmountedStorage


class StorageService(object):
    storages = {'ftp_private_storage': None,
                'ftp_public_storage': None,
                'studies_storage': None}

    @staticmethod
    def get_studies_storage() -> Storage:
        return StorageService._get_local_storage('studies_storage', get_settings().study.mounted_paths.study_metadata_files_root_path)

    @staticmethod
    def get_ftp_private_storage() -> Storage:
        mount_type = get_settings().ftp_server.private.configuration.mount_type
        if mount_type and mount_type.lower() == "mounted":
            return StorageService._get_local_storage('ftp_private_storage', get_settings().study.mounted_paths.private_ftp_root_path)
        if mount_type and mount_type.lower() == "unmounted":
            return UnmountedStorage('ftp_private_storage')
        if mount_type and mount_type.lower() == "remote_worker":
            private_ftp_folder_root_path = get_settings().hpc_cluster.datamover.mounted_paths.cluster_private_ftp_root_path
            return RemoteFtpStorage('remote_worker', remote_folder=private_ftp_folder_root_path)
        raise NotImplementedError(f"Mounted type {mount_type} is not defined.")

    @staticmethod
    def get_ftp_public_storage() -> Storage:
        mount_type = get_settings().ftp_server.public.configuration.mount_type
        if mount_type and mount_type.lower() == "mounted":
            return StorageService._get_local_storage('ftp_public_storage', get_settings().ftp_server.public.configuration.mounted_public_ftp_folders_root_path)
        if mount_type and mount_type.lower() == "unmounted":
            return UnmountedStorage('ftp_public_storage')
        raise NotImplementedError(f"Mounted type {mount_type} is not defined.")

    @staticmethod
    def get_report_storage() -> Storage:
        return StorageService._get_local_storage('report_storage', get_settings().study.mounted_paths.reports_root_pathe)

    @staticmethod
    def _get_local_storage(name: str, path: str) -> Storage:
        if name in StorageService.storages and StorageService.storages[name]:
            return StorageService.storages[name]

        manager = LocalStorage(name, remote_folder=path)
        StorageService.storages[name] = manager
        return manager
    