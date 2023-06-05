import logging
import os
from typing import List, Tuple, Union
import celery
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
    input_paths = get_input_folder_paths(folder_paths)
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
                make_dir_with_chmod(path_item, mode=permission)
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
                raise ex
    return results


@celery.task(
    bind=True,
    base=MetabolightsTask,
    name="app.tasks.datamover_tasks.basic_tasks.file_management.update_folder_permission",
)

def update_folder_permission(
    self, folder_paths: Union[str, List[str]], acl: Union[int, Acl] = Acl.AUTHORIZED_READ_WRITE
):
    results = {}
        
    input_paths = get_input_folder_paths(folder_paths)
    permission = get_input_permission_value(acl)

    for path_item in input_paths:
        results[path_item] = update_permission(path_item, permission)
    
    return results

def get_input_folder_paths(folder_paths: Union[str, List[str]]) -> List[str]:
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
    permission_str = oct(permission).replace("0o", "")
    if permission != current_permission:
        try:
            os.chmod(path_item, mode=permission)
            last_permission = os.stat(path_item).st_mode & 0o777
            if last_permission == permission:
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
