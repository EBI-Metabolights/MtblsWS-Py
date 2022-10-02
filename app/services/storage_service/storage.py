from abc import ABC, abstractmethod
from typing import List

from app.services.storage_service.file_manager import FileManager


class Storage(ABC):

    def __init__(self, name: str, local_file_manager: FileManager, remote_file_manager: FileManager):
        self._name = name
        self._local_file_manager = local_file_manager
        self._remote_file_manager = remote_file_manager

    def get_name(self):
        return self._name

    def _get_local_file_manager(self):
        return self._local_file_manager

    def _get_remote_file_manager(self):
        return self._remote_file_manager

    local = property(fget=_get_local_file_manager)
    remote = property(fget=_get_remote_file_manager)

    @abstractmethod
    def download_file(self, source_file: str, target_local_folder: str, new_name: str = None) -> str:
        pass

    @abstractmethod
    def download_folder(self, source_folder: str, target_local_folder: str) -> str:
        pass

    @abstractmethod
    def upload_file(self, source_file: str, target_remote_folder: str, target_file_name: str = None) -> List[str]:
        pass

    @abstractmethod
    def upload_files(self, files: List[str], target_parent_folder:str) -> List[str]:
        pass

    @abstractmethod
    def upload_folder(self, source_folder: str, target_remote_folder: str = None) -> str:
        pass

    @abstractmethod
    def sync_from_local(self, source_local_folder: str, target_folder: str, ignore_list: List[str] = None, **kwargs):
        pass

    @abstractmethod
    def sync_from_storage(self, source_path: str, target_local_path: str, ignore_list: List[str] = None, **kwargs):
        pass
