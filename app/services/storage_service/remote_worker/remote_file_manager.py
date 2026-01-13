import os
from typing import List, Union

from app.services.storage_service.acl import Acl
from app.services.storage_service.file_manager import FileManager
from app.tasks.datamover_tasks.basic_tasks import file_management
from app.ws.settings.utils import get_cluster_settings


class RemoteFileManager(FileManager):
    def __init__(self, name, mounted_root_folder=None):
        super(RemoteFileManager, self).__init__(name=name)
        self.mounted_root_folder = mounted_root_folder

    def get_absolute_path(self, path: Union[str, List[str]]) -> str:
        if self.mounted_root_folder:
            if isinstance(path, list):
                paths = []
                for item in path:
                    if item.strip().strip(os.sep):
                        if item.strip().startswith(self.mounted_root_folder):
                            paths.append(item.strip())
                        else:
                            paths.append(
                                os.path.join(
                                    self.mounted_root_folder, path.strip().strip(os.sep)
                                )
                            )
            else:
                if path.strip().strip(os.sep):
                    if path.strip().startswith(self.mounted_root_folder):
                        return path.strip()
                    return os.path.join(
                        self.mounted_root_folder, path.strip().strip(os.sep)
                    )
                else:
                    return ""
        if path.strip():
            return path.strip()
        return ""

    def create_folder(
        self,
        folder_path: Union[str, List[str]],
        acl: Acl = Acl.AUTHORIZED_READ_WRITE,
        exist_ok: bool = True,
    ) -> bool:
        folder_path = self.get_absolute_path(folder_path)
        inputs = {"folder_paths": folder_path, "acl": acl, "exist_ok": exist_ok}
        task = file_management.create_folders.apply_async(kwargs=inputs, expires=60 * 5)
        cluster_settings = get_cluster_settings()
        output = task.get(timeout=cluster_settings.task_get_timeout_in_seconds)
        if not output:
            return False

        for item in output:
            if "status" not in output[item] or not output[item]["status"]:
                return False
        return True

    def delete_folder(self, folder_path: str) -> bool:
        absolute_folder_path = self.get_absolute_path(folder_path)
        root_path = absolute_folder_path.replace(folder_path, "", 1).strip(os.sep)
        inputs = {
            "root_path": root_path,
            "folder_paths": absolute_folder_path,
        }
        task = file_management.delete_folders.apply_async(kwargs=inputs, expires=60 * 5)
        cluster_settings = get_cluster_settings()
        output = task.get(timeout=cluster_settings.task_get_timeout_in_seconds * 2)
        if not output:
            return False

        for item in output:
            if not item["status"]:
                return False
        return True

    def move(self, source_path: str, target_path: str, timeout=None) -> bool:
        source_path = self.get_absolute_path(source_path)
        target_path = self.get_absolute_path(target_path)
        inputs = {"source_path": source_path, "target_path": target_path}
        task = file_management.move.apply_async(kwargs=inputs, expires=60 * 5)
        cluster_settings = get_cluster_settings()
        if not timeout:
            output = task.get(timeout=cluster_settings.task_get_timeout_in_seconds * 2)
        else:
            output = task.get(timeout=timeout)
        if not output:
            return False

        for item in output:
            if not item["status"]:
                return False
        return True

    def does_folder_exist(self, folder_path: str) -> bool:
        folder_path = self.get_absolute_path(folder_path)
        inputs = {"source_path": folder_path}
        task = file_management.exists.apply_async(kwargs=inputs, expires=60 * 5)
        cluster_settings = get_cluster_settings()

        output = task.get(timeout=cluster_settings.task_get_timeout_in_seconds)
        return output

    def get_folder_permission(self, source_path: str) -> Acl:
        source_path = self.get_absolute_path(source_path)
        inputs = {"source_path": source_path}
        task = file_management.get_permission.apply_async(kwargs=inputs, expires=60 * 5)
        cluster_settings = get_cluster_settings()
        output = task.get(timeout=cluster_settings.task_get_timeout_in_seconds)

        if output and "status" in output and output["status"]:
            permission_int = output["value"]
            chmod = int(oct(permission_int & 0o770), 8)
            try:
                permission = Acl(chmod)
            except:
                permission = Acl.UNKNOWN
        else:
            return Acl.UNKNOWN
        return permission

    def is_file(self, source_path: str) -> bool:
        source_path = self.get_absolute_path(source_path)
        inputs = {"source_path": source_path}
        task = file_management.isfile.apply_async(kwargs=inputs, expires=60 * 5)
        cluster_settings = get_cluster_settings()
        output = task.get(timeout=cluster_settings.task_get_timeout_in_seconds)
        return output

    def is_folder(self, source_path: str) -> bool:
        source_path = self.get_absolute_path(source_path)
        inputs = {"source_path": source_path}
        task = file_management.isdir.apply_async(kwargs=inputs, expires=60 * 5)
        cluster_settings = get_cluster_settings()
        output = task.get(timeout=cluster_settings.task_get_timeout_in_seconds)
        return output

    def update_folder_permission(
        self, paths: str, acl: Acl = Acl.AUTHORIZED_READ_WRITE
    ) -> bool:
        paths = self.get_absolute_path(paths)
        inputs = {"paths": paths, "acl": acl}
        task = file_management.chmod.apply_async(kwargs=inputs, expires=60 * 5)
        cluster_settings = get_cluster_settings()
        output = task.get(timeout=cluster_settings.task_get_timeout_in_seconds)
        if not output:
            return False

        for item in output:
            if "status" not in output[item] or not output[item]["status"]:
                return False
        return True
