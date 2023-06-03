
import datetime
import glob
import json
import logging
import os
import time
from typing import List, Union
import celery
from app.services.storage_service.acl import Acl
from app.tasks.worker import (MetabolightsTask, celery, get_flask_app, send_email)
from flask.json import jsonify
from app.tasks.worker import MetabolightsTask
from app.utils import MetabolightsException

from app.ws.db_connection import update_release_date
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.settings.utils import get_study_settings
from app.ws.utils import validate_mzml_files, convert_to_isa, copy_file, read_tsv, write_tsv, \
    update_correct_sample_file_name, get_year_plus_one

logger = logging.getLogger('datamover_worker')


@celery.task(bind=True, base=MetabolightsTask, name="app.tasks.datamover_tasks.basic_tasks.file_management.create_folders")
def create_folders(self, folder_paths: Union[str, List[str]], acl: Union[int, Acl] = Acl.AUTHORIZED_READ_WRITE, exist_ok: bool = True):
    results = {}
    if not folder_paths:
        return results
    paths = []
    if isinstance(folder_paths, str):
        paths.append(folder_paths)
    else:
        paths = folder_paths
    
    if isinstance(acl, Acl):
        permission = acl.value
    else:
        permission = acl
           
    for path_item in paths:
        exist = True if os.path.exists(path_item) else False
        is_dir = True if exist and os.path.isdir(path_item) else False
        
        if exist:
            if not is_dir:
                results[path_item] = {"status": False, "message": f"Path '{path_item}' is not a folder"}
            else:
                current_permission = os.stat(path_item).st_mode & 0o777
                if permission != current_permission:
                    try:
                        os.chmod(path_item, mode=permission)
                        last_permission = os.stat(path_item).st_mode & 0o777
                        if last_permission == permission:
                            results[path_item] = {"status": True, "message": f"Path '{path_item}'  already exists. Folder permission was updated"}
                        else:
                            results[path_item] = {"status": True, "message": f"Path '{path_item}'  already exists. Permission could not be updated."}
                    except Exception as exc:
                        results[path_item] = {"status": False, "message": f"Path '{path_item}'  already exists. Permission could not be updated. Exception: {str(exc)}"}
                else:
                    results[path_item] = {"status": True, "message": f"Path '{path_item}' already exists."}
        else:
            try:
                os.makedirs(path_item, mode=permission, exist_ok=exist_ok)
            
                last_status = True if os.path(path_item) else False
                if last_status:
                    results[path_item] = {"status": True, "message": f"Path '{path_item}' was created."}
                else:
                    results[path_item] = {"status": False, "message": f"Path '{path_item}' could not be created."}
            except Exception as exc:
                results[path_item] = {"status": False, "message": f"Path '{path_item}' could not be created. Exception: {str(exc)}"}            
    return results