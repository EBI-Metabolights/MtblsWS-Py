import logging
import os
from typing import List, Union
import celery
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
    if not folder_paths:
        return results
    input_paths = []
    if isinstance(folder_paths, str):
        input_paths.append(folder_paths)
    else:
        input_paths = folder_paths

    if isinstance(acl, Acl):
        permission = acl.value
    else:
        permission = acl

    for path_item in input_paths:
        path_exist = os.path.exists(path_item)
        print(path_item)
        if path_exist:
            if os.path.isdir(path_item):
                results[path_item] = {"status": False, "message": f"Path '{path_item}' is not a folder"}
            else:
                current_permission = os.stat(path_item).st_mode & 0o777
                if permission != current_permission:
                    try:
                        os.chmod(path_item, mode=permission)
                        last_permission = os.stat(path_item).st_mode & 0o777
                        if last_permission == permission:
                            results[path_item] = {
                                "status": True,
                                "message": f"Path '{path_item}'  already exists. Folder permission was updated",
                            }
                        else:
                            results[path_item] = {
                                "status": True,
                                "message": f"Path '{path_item}'  already exists. Permission could not be updated.",
                            }
                    except Exception as ex:
                        results[path_item] = {
                            "status": False,
                            "message": f"Path '{path_item}'  already exists. Permission could not be updated. Exception: {str(ex)}",
                        }
                else:
                    results[path_item] = {"status": True, "message": f"Path '{path_item}' already exists."}
        else:
            try:
                os.makedirs(path_item, mode=permission, exist_ok=exist_ok)

                last_status =  os.path.exists(path_item)
                if last_status:
                    results[path_item] = {"status": True, "message": f"Path '{path_item}' was created."}
                else:
                    results[path_item] = {"status": False, "message": f"Path '{path_item}' could not be created."}
            except Exception as ex:
                results[path_item] = {
                    "status": False,
                    "message": f"Path '{path_item}' could not be created. Exception: {str(ex)}",
                }
                
    return results
