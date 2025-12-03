from abc import ABC, abstractmethod
from typing import List

from app.services.storage_service.file_manager import FileManager
from app.services.storage_service.models import (
    SyncCalculationTaskResult,
    SyncTaskResult,
)


class Storage(ABC):
    def __init__(self, name: str, remote_file_manager: FileManager):
        self._name = name
        self._remote_file_manager: FileManager = remote_file_manager

    def get_name(self):
        return self._name

    def _get_remote_file_manager(self) -> FileManager:
        return self._remote_file_manager

    remote: FileManager = property(fget=_get_remote_file_manager)

    @abstractmethod
    def sync_from_local(
        self,
        source_local_folder: str,
        target_folder: str,
        ignore_list: List[str] = None,
        **kwargs,
    ):
        pass

    @abstractmethod
    def sync_to_public_ftp(
        self,
        source_local_folder: str,
        target_folder: str,
        ignore_list: List[str] = None,
        **kwargs,
    ):
        pass

    @abstractmethod
    def sync_from_storage(
        self,
        source_path: str,
        target_local_path: str,
        ignore_list: List[str] = None,
        **kwargs,
    ):
        pass

    @abstractmethod
    def calculate_sync_status(
        self,
        study_id: str,
        obfuscation_code: str,
        target_local_path: str,
        force: bool = True,
        ignore_list: List[str] = None,
    ) -> SyncCalculationTaskResult:
        pass

    @abstractmethod
    def check_folder_sync_status(
        self, study_id: str, obfuscation_code: str, target_local_path: str
    ) -> SyncTaskResult:
        pass
