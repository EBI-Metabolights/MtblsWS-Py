from abc import ABC, abstractmethod
from typing import List, Union

from app.services.storage_service.acl import Acl


class FileManager(ABC):

    def __init__(self, name: str):
        self._name = name

    def _get_name(self):
        return self._name

    name = property(fget=_get_name)

    @abstractmethod
    def create_folder(self, folder_path_list: Union[str, List[str]], acl: Acl = Acl.AUTHORIZED_READ_WRITE, exist_ok: bool = True) -> bool:
        pass

    @abstractmethod
    def delete_folder(self, target: str) -> bool:
        pass

    @abstractmethod
    def update_folder_permission(self, source: str, acl: Acl = Acl.AUTHORIZED_READ_WRITE) -> bool:
        pass

    @abstractmethod
    def get_folder_permission(self, source: str) -> Acl:
        pass

    @abstractmethod
    def does_folder_exist(self, source: str) -> bool:
        pass
