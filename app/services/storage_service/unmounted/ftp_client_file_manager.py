import datetime
import hashlib
import os
import random
import shutil
from distutils.dir_util import copy_tree
from pathlib import Path
from typing import List

import ftputil

from app.services.storage_service.acl import Acl
from app.services.storage_service.exceptions import StorageServiceException
from app.services.storage_service.file_descriptor import FileDescriptor, FileType
from app.services.storage_service.file_manager import FileManager


class FtpClientFileManager(FileManager):

    def __init__(self, name, relative_studies_root_path):
        super(FtpClientFileManager, self).__init__(name=name)
        self.relative_studies_root_path = relative_studies_root_path
        self.ftp_client = ftputil.FTPHost("ftp-private.ebi.ac.uk", "mtblight", "gs4qYabh")

    def get_uri(self, source):
        self._validate_path(source)
        path = os.path.join('.', source.lstrip('.').lstrip(os.sep))
        return path

    def get_base_uri(self, source):
        return ''

    def create_folder(self, target: str, acl: Acl = Acl.AUTHORIZED_READ_WRITE, exist_ok: bool = True) -> bool:
        try:
            path = os.path.join('.', target.lstrip('.').lstrip(os.sep))
            self.ftp_client.chdir(self.relative_studies_root_path)
            self.ftp_client.makedirs(path, mode=acl.value, exist_ok=exist_ok)
        except (OSError, FileExistsError):
            return False
        return True

    def delete(self, target: str) -> bool:
        path = os.path.join('.', target.lstrip('.').lstrip(os.sep))
        if not self.ftp_client.path.exists(path):
            return True
        try:
            if self.ftp_client.path.isfile(path):
                self.ftp_client.remove(path)
            elif self.ftp_client.path.isdir(path):
                self.ftp_client.rmtree(path, ignore_errors=False)
            else:
                return False
            return True
        except (OSError, Exception):
            return False

    def exists(self, source: str) -> bool:
        source_path = os.path.join('.', source.lstrip('.').lstrip(os.sep))
        if self.ftp_client.path.exists(source_path):
            return self.ftp_client.path.isfile(source_path) or self.ftp_client.path.isdir(source_path)
        return False

    def is_file(self, source: str) -> bool:
        source_path = os.path.join('.', source.lstrip('.').lstrip(os.sep))
        if self.ftp_client.path.exists(source_path):
            return self.ftp_client.path.isfile(source_path)
        return False

    def is_folder(self, source: str) -> bool:
        source_path = os.path.join('.', source.lstrip('.').lstrip(os.sep))
        if self.ftp_client.path.exists(source_path):
            return self.ftp_client.path.isdir(source_path)
        return False

    def get_permission(self, source: str) -> Acl:
        source_path = os.path.join('.', source.lstrip('.').lstrip(os.sep))

        chmod = int(oct(self.ftp_client.stat(source_path).st_mode & 0o770), 8)
        try:
            permission = Acl(chmod)
        except:
            permission = Acl.UNKNOWN

        return permission

    def update_permission(self, source: str, acl: Acl = Acl.AUTHORIZED_READ_WRITE) -> bool:
        source_path = os.path.join('.', source.lstrip('.').lstrip(os.sep))
        chmod = acl.value
        return self._update_chmod(source_path, chmod)

    def list_folder(self, source: str, filter_pattern=None) -> List[FileDescriptor]:
        source_path = os.path.join('.', source.lstrip('.').lstrip(os.sep))
        file_list = []
        entries = self.ftp_client.listdir(source_path)

        for entry in entries:
            file_type = FileType.UNKNOWN
            size = -1
            created_time = None
            modified_time = self.ftp_client.path.getmtime(entry)
            if self.ftp_client.path.isfile(entry):
                file_type = FileType.FILE
                size = self.ftp_client.path.getsize(entry)
            elif self.ftp_client.path.isdir(entry):
                file_type = FileType.FOLDER
            path = os.path.join('.', entry.lstrip('.').lstrip(os.sep))
            parent_folder = os.path.dirname(path)
            base_name = os.path.basename(path)
            fd = FileDescriptor(base_name, parent_folder, file_type,
                                created_time=created_time, modified_time=modified_time, size_in_bytes=size)
            file_list.append(fd)

        return file_list

    def get_file_descriptor(self, source: str) -> FileDescriptor:
        source_path = os.path.join('.', source.lstrip('.').lstrip(os.sep))
        file_type = FileType.UNKNOWN
        size = -1
        created_time = None
        modified_time = self.ftp_client.path.getmtime(source_path)
        if self.ftp_client.path.isfile(source_path):
            file_type = FileType.FILE
            size = self.ftp_client.path.getsize(source_path)
        elif os.path.isdir(source_path):
            file_type = FileType.FOLDER
        path = source_path
        parent_folder = os.path.dirname(path)
        base_name = os.path.basename(path)
        fd = FileDescriptor(base_name, parent_folder, file_type,
                            created_time=created_time, modified_time=modified_time, size_in_bytes=size)
        return fd

    def _update_chmod(self, file, chmod):
        try:
            self.ftp_client.chmod(file, chmod)
        except (OSError, Exception):
            return False
        return True

    def _get_validated_path(self, path: str):
        source_path = os.path.join('.', path.lstrip('.').lstrip(os.sep))
        if not self.ftp_client.path.exists(source_path):
            message = f'Source {path} does not exist'
            raise StorageServiceException(StorageServiceException.ERR_CODE_FILE_NOT_EXIST, message)
        return source_path

    @staticmethod
    def _validate_path(source: str):
        if not source or source.startswith('..'):
            message = f'{source} is not allowed'
            raise StorageServiceException(StorageServiceException.ERR_CODE_NOT_ALLOWED_FILE, message)

    def _get_abs_path(self, source):
        self._validate_path(source)
        if source == os.sep:
            return self.mounted_root_folder

        if not source.startswith(self.mounted_root_folder):
            return os.path.join(self.mounted_root_folder, source.lstrip('.').lstrip(os.sep))
        return source

    def _get_relative_path(self, source):
        self._validate_path(source)
        if source.startswith(self.mounted_root_folder):
            return source.replace(self.mounted_root_folder, '', 1)
        if source.startswith(os.sep):
            return source.lstrip(os.sep)
        return source
