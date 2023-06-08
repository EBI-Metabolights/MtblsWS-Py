import logging
import os
import shutil
from typing import List, Union

from app.file_utils import make_dir_with_chmod
from app.services.storage_service.acl import Acl
from app.tasks.worker import MetabolightsTask, celery

logger = logging.getLogger("datamover_worker")


@celery.task(
    bind=True, base=MetabolightsTask, name="app.tasks.datamover_tasks.basic_tasks.file_management.create_folders"
)
def create_folders(
    self, folder_paths: Union[str, List[str]], acl: Union[int, Acl] = Acl.AUTHORIZED_READ_WRITE, exist_ok: bool = True
):
    results = {}
    input_paths = get_input_paths(folder_paths)
    permission = get_input_permission_value(acl)

    for path_item in input_paths:
        path_exist = os.path.exists(path_item)
        print(path_item)
        if path_exist:
            if not os.path.isdir(path_item):
                results[path_item] = {"status": False, "message": f"Path '{path_item}' is not a folder"}
            else:
                results[path_item] = update_permission(path_item, permission)
        else:
            try:
                make_dir_with_chmod(path_item, permission)
                last_status = os.path.exists(path_item)
                if last_status:
                    result = update_permission(path_item, permission)
                    if not result["status"]:
                        results[path_item] = {
                            "status": False,
                            "message": f"Path '{path_item}' created but permission could not updated.",
                        }
                    else:
                        results[path_item] = {"status": True, "message": f"Path '{path_item}' was created."}
                else:
                    results[path_item] = {"status": False, "message": f"Path '{path_item}' could not be created."}

            except Exception as ex:
                results[path_item] = {
                    "status": False,
                    "message": f"Path '{path_item}' could not be created. Root cause: {str(ex)}",
                }
    return results


@celery.task(
    bind=True,
    base=MetabolightsTask,
    name="app.tasks.datamover_tasks.basic_tasks.file_management.chmod",
)
def chmod(self, paths: Union[str, List[str]], acl: Union[int, Acl] = Acl.AUTHORIZED_READ_WRITE):
    results = {}

    input_paths = get_input_paths(paths)
    permission = get_input_permission_value(acl)

    for path_item in input_paths:
        results[path_item] = update_permission(path_item, permission)

    return results


@celery.task(
    bind=True,
    base=MetabolightsTask,
    name="app.tasks.datamover_tasks.basic_tasks.file_management.delete_folders",
)
def delete_folders(self, folder_paths: Union[str, List[str]]):
    results = {}

    input_paths = get_input_paths(folder_paths)

    for path_item in input_paths:
        if os.path.exists(path_item):
            if os.path.isdir(path_item):
                try:
                    shutil.rmtree(path_item)
                    results[path_item] = {"status": False, "message": f"'{path_item}' was deleted."}
                except Exception as ex:
                    results[path_item] = {
                        "status": False,
                        "message": f"Path '{path_item}' could not be deleted. Root cause: {str(ex)}",
                    }
            else:
                results[path_item] = {"status": False, "message": f"'{path_item}' is not a folder or does not exist."}
        else:
            results[path_item] = {"status": False, "message": f"There is no folder '{path_item}'."}
    return results


@celery.task(
    bind=True,
    base=MetabolightsTask,
    name="app.tasks.datamover_tasks.basic_tasks.file_management.move",
)
def move(self, source_path: str, target_path: str):
    input_path = source_path
    new_path = target_path

    if not input_path or not new_path:
        return {"status": False, "message": f"inputs are not valid."}

    if os.path.exists(input_path):
        if not os.path.exists(new_path):
            try:
                shutil.move(input_path, new_path)
                if not os.path.exists(input_path) and os.path.exists(new_path):
                    return {"status": True, "message": f"'{input_path}' was moved to {new_path}."}
                else:
                    return {
                        "status": False,
                        "message": f"Unexpected status: {input_path} could not be moved to {new_path} successfully.",
                    }
            except Exception as ex:
                return {
                    "status": False,
                    "message": f"Path '{input_path}' could not be moved to {new_path}. Root cause: {str(ex)}",
                }
        else:
            return {"status": False, "message": f"'{new_path}' already exists."}
    else:
        return {"status": False, "message": f"'{new_path}'  does not exist."}


@celery.task(
    bind=True,
    base=MetabolightsTask,
    name="app.tasks.datamover_tasks.basic_tasks.file_management.isfile",
)
def isfile(self, source_path: str) -> bool:
    if source_path and os.path.exists(source_path) and os.path.isfile(source_path):
        return True
    return False


@celery.task(
    bind=True,
    base=MetabolightsTask,
    name="app.tasks.datamover_tasks.basic_tasks.file_management.isdir",
)
def isdir(self, source_path: str) -> bool:
    if source_path and os.path.exists(source_path) and os.path.isdir(source_path):
        return True
    return False


@celery.task(
    bind=True,
    base=MetabolightsTask,
    name="app.tasks.datamover_tasks.basic_tasks.file_management.exists",
)
def exists(self, source_path: str) -> bool:

    if source_path and os.path.exists(source_path):
        return True
    return False


@celery.task(
    bind=True,
    base=MetabolightsTask,
    name="app.tasks.datamover_tasks.basic_tasks.file_management.get_permission",
)
def get_permission(self, source_path: str):
    try:
        if source_path and os.path.exists(source_path):
            current_permission = os.stat(source_path).st_mode
            current_permission_str = oct(current_permission).replace("0o", "")
            result = {"status": True, "value": current_permission, "octal_value": current_permission_str}
            return result
        else:
            return {"status": False, "message": f"'{source_path}' does not exist.", "value": -1, "octal_value": -1}

    except Exception as ex:
        return {
            "status": False,
            "message": f"Permission check error for '{source_path}'. Root cause: {str(ex)}",
            "value": -1,
            "octal_value": -1,
        }


def get_input_paths(folder_paths: Union[str, List[str]]) -> List[str]:
    input_paths: List[str] = []
    if not folder_paths:
        return input_paths

    if isinstance(folder_paths, str):
        input_paths.append(folder_paths)
    else:
        input_paths = folder_paths
    return input_paths


def get_input_permission_value(acl: Union[int, Acl] = Acl.AUTHORIZED_READ_WRITE) -> int:
    if isinstance(acl, Acl):
        permission = acl.value
    else:
        permission = acl
    return permission


def update_permission(path_item, permission):
    current_permission = os.stat(path_item).st_mode & 0o777
    current_permission_str = oct(current_permission).replace("0o", "")
    permission_str = oct(permission & 0o777).replace("0o", "")
    if permission != current_permission:
        try:
            os.chmod(path_item, mode=permission)
            last_permission = os.stat(path_item).st_mode & 0o777
            if last_permission & 0o777 == permission & 0o777:
                result = {
                    "status": True,
                    "message": f"Path '{path_item}' permission was updated from {current_permission_str} to {permission_str}.",
                }
            else:
                result = {
                    "status": False,
                    "message": f"Path '{path_item}'  already exists. Permission could not be updated from {current_permission_str} to {permission_str}.",
                }
        except Exception as ex:
            result = {
                "status": False,
                "message": f"Path '{path_item}' permission could not be updated from {current_permission_str} to {permission_str}. Root cause: {str(ex)}",
            }
            raise ex
    else:
        result = {"status": True, "message": f"Path '{path_item}' permission is already {current_permission_str}."}
    return result
