from abc import ABC, abstractmethod
from typing import List

from app.services.storage_service.acl import Acl
from app.services.storage_service.file_descriptor import FileDescriptor


class FileManager(ABC):

    def __init__(self, name: str):
        self._name = name

    def _get_name(self):
        return self._name

    name = property(fget=_get_name)

    @abstractmethod
    def get_uri(self, source):
        pass

    @abstractmethod
    def get_base_uri(self, source):
        pass

    @abstractmethod
    def create_folder(self, folder: str, acl: Acl = Acl.AUTHORIZED_READ_WRITE, exist_ok: bool = True) -> bool:
        pass

    @abstractmethod
    def delete(self, target: str) -> bool:
        pass

    @abstractmethod
    def exists(self, source: str) -> bool:
        pass

    @abstractmethod
    def is_file(self, source: str) -> bool:
        pass

    @abstractmethod
    def is_folder(self, source: str) -> bool:
        pass

    @abstractmethod
    def update_permission(self, source: str, acl: Acl = Acl.AUTHORIZED_READ_WRITE) -> bool:
        pass

    @abstractmethod
    def get_permission(self, source: str) -> Acl:
        pass

    @abstractmethod
    def list_folder(self, folder: str, filter_pattern=None) -> List[FileDescriptor]:
        pass

    @abstractmethod
    def get_file_descriptor(self, source: str) -> FileDescriptor:
        pass
