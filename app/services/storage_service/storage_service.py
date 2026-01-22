from app.config import get_settings
from app.services.storage_service.remote_worker.remote_ftp_storage import (
    RemoteFtpStorage,
)
from app.services.storage_service.storage import Storage
from app.services.storage_service.unmounted.unmounted_storage import UnmountedStorage


class StorageService(object):
    storages = {
        "ftp_private_storage": None,
        "ftp_public_storage": None,
        "studies_storage": None,
    }

    @staticmethod
    def get_ftp_private_storage() -> Storage:
        mount_type = get_settings().ftp_server.private.configuration.mount_type
        if mount_type and mount_type.lower() == "unmounted":
            return UnmountedStorage("ftp_private_storage")
        if mount_type and mount_type.lower() == "remote_worker":
            private_ftp_folder_root_path = get_settings().hpc_cluster.datamover.mounted_paths.cluster_private_ftp_root_path
            return RemoteFtpStorage(
                "remote_worker", remote_folder=private_ftp_folder_root_path
            )
        raise NotImplementedError(f"Mounted type {mount_type} is not defined.")

    @staticmethod
    def get_ftp_public_storage() -> Storage:
        mount_type = get_settings().ftp_server.public.configuration.mount_type
        if mount_type and mount_type.lower() == "unmounted":
            return UnmountedStorage("ftp_public_storage")
        raise NotImplementedError(f"Mounted type {mount_type} is not defined.")
