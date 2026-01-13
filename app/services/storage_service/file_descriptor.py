from enum import Enum
from typing import List, Union


class FileType(Enum):
    UNKNOWN = 0
    FILE = 1
    FOLDER = 2


class FilePermission(Enum):
    PRIVATE = 0
    PUBLIC = 1


class FileDescriptor(object):
    def __init__(
        self,
        name: str,
        folder: str,
        file_type: FileType,
        hash_value: Union[None, str] = None,
        size_in_bytes: int = -1,
        is_public: bool = False,
        created_time=-1,
        modified_time=-1,
        tags: Union[None, List[str]] = None,
    ):
        self.name = name
        self.folder = folder
        self.file_type: FileType = file_type
        self.hash_value = hash_value
        self.size_in_bytes = size_in_bytes
        self.is_public = is_public
        self.created_time = created_time
        self.modified_time = modified_time
        self.tags = tags if tags else []
