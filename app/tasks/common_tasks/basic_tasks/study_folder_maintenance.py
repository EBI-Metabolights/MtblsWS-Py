import datetime
import json
import logging
import os

import pandas as pd
from app.services.storage_service.remote_worker.remote_file_manager import RemoteFileManager

from app.tasks.worker import (MetabolightsTask, celery, get_flask_app,
                              send_email)
from app.utils import MetabolightsDBException
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study, User
from app.ws.db.types import StudyStatus
from app.ws.folder_maintenance import MaintenanceAction, MaintenanceActionLog, StudyFolderMaintenanceTask
from app.ws.settings.utils import get_study_settings
from app.ws.study.user_service import UserService

logger = logging.getLogger(__name__)
from app.tasks.worker import get_flask_app

@celery.task(base=MetabolightsTask, name="app.tasks.common_tasks.basic_tasks.study_folder_maintenance.maintain_rw_storage_study_folders")
def maintain_rw_storage_study_folders(user_token: str, send_email_to_submitter=False, study_id: str=None, force_to_maintain=False):
    all_results = []
    headers = ["STUDY_ID", "STUDY STATUS", "STATUS", "ACTION", "ITEM", "MESSAGE", "PARAMETERS"]
    try:
        flask_app = get_flask_app()
        UserService.get_instance(flask_app).validate_user_has_curator_role(user_token)
        with flask_app.app_context():
            with DBManager.get_instance(flask_app).session_maker() as db_session:
                user = db_session.query(User.email).filter(User.apitoken == user_token).first()
                if not user:
                    raise MetabolightsDBException("No user")
                
                email = user.email
                if study_id:
                    studies = db_session.query(Study).filter(Study.acc == study_id).all()
                else:
                    studies = db_session.query(Study).all()
                    
                if not studies:
                    raise MetabolightsDBException(f"No study found on db.") 
                results = {}
                
                for item in studies:
                    try:
                        study: Study = item
                        results[study.acc] = {}
                        study_status = StudyStatus(study.status)
                        maintenance_task = StudyFolderMaintenanceTask(
                            study.acc,
                            study_status,
                            study.releasedate,
                            study.submissiondate,
                            recycle_bin_folder_name=f"_INITIAL_{study.acc}",
                            delete_unreferenced_metadata_files=False,
                            settings=get_study_settings(),
                            apply_future_actions=False,
                            force_to_maintain=force_to_maintain,
                        )
                        results = maintenance_task.maintain_study_rw_storage_folders()
                        rows = []
                        for action in maintenance_task.actions:
                            success = action.successful
                            action_name = action.action.name
                            item = action.item
                            message = action.message
                            parameters = action.parameters
                            rows.append(
                                [f"{study_id}", f"{study_status.name}", f"{success}", f"{action_name}", f"{item}", f"{message}", f"{parameters}"]
                            )
                        
                        all_results.extend(rows)
                        
                    except Exception as exc:
                        results[study.acc]["exception"] = exc
                        all_results.append(f"{study_id}", "{study_status.name}", "ERROR", "ERROR_MESSAGE", "{study_id}", f"{str(exc)}", "")
                df = pd.DataFrame(all_results, columns=headers)
                
                result = {"time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                "executed_on":  os.uname().nodename,
                "result": "Listed Below" 
                }

            
            if send_email_to_submitter:
                result_str = json.dumps(result, indent=4)
                result_str = result_str + "<p>" + df.to_html().replace('border="1"','border="0"')
                send_email("Result of the task: maintain MetaboLights study folders", result_str, None, email, None)                        


    except Exception as ex:
        if send_email_to_submitter:
            result_str = str(ex).replace("\n", "<p>")
            if all_results:
                df = pd.DataFrame(all_results, columns=headers)
                result_str = result_str + "<p>" + df.to_html().replace('border="1"','border="0"')

            send_email("A task was failed: maintain MetaboLights study folders", result_str, None, email, None)
        raise ex                    



@celery.task(base=MetabolightsTask, name="app.tasks.common_tasks.basic_tasks.study_folder_maintenance.maintain_readonly_storage_study_folders")
def maintain_readonly_storage_study_folders(user_token: str, send_email_to_submitter=False, study_id: str=None, force_to_maintain=False):
    all_results = []
    headers = ["STUDY_ID", "STUDY STATUS", "STATUS", "ACTION", "ITEM", "MESSAGE", "PARAMETERS"]
    try:
        flask_app = get_flask_app()
        UserService.get_instance(flask_app).validate_user_has_curator_role(user_token)
        with flask_app.app_context():
            with DBManager.get_instance(flask_app).session_maker() as db_session:
                user = db_session.query(User.email).filter(User.apitoken == user_token).first()
                if not user:
                    raise MetabolightsDBException("No user")
                
                email = user.email
                if study_id:
                    studies = db_session.query(Study).filter(Study.acc == study_id).all()
                else:
                    studies = db_session.query(Study).all()
                    
                if not studies:
                    raise MetabolightsDBException(f"No study found on db.") 
                results = {}
                
                for item in studies:
                    try:
                        study: Study = item
                        results[study.acc] = {}
                        study_status = StudyStatus(study.status)
                        maintenance_task = StudyFolderMaintenanceTask(
                            study.acc,
                            study_status,
                            study.releasedate,
                            study.submissiondate,
                            recycle_bin_folder_name=f"_INITIAL_{study.acc}",
                            delete_unreferenced_metadata_files=False,
                            settings=get_study_settings(),
                            apply_future_actions=False,
                            force_to_maintain=force_to_maintain,
                        )
                        results = maintenance_task.create_maintenace_actions_for_study_data_files()
                        rows = []
                        file_manager = RemoteFileManager("remote_client", mounted_root_folder="/")
                        for action in maintenance_task.future_actions:
                            success = execute_action(file_manager, action)
                            # success = action.successful
                            action_name = action.action.name
                            item = action.item
                            message = action.message
                            parameters = action.parameters
                            rows.append(
                                [f"{study_id}", f"{study_status.name}", f"{success}", f"{action_name}", f"{item}", f"{message}", f"{parameters}"]
                            )
                        
                        all_results.extend(rows)
                        
                    except Exception as exc:
                        results[study.acc]["exception"] = exc
                        all_results.append(f"{study_id}", "{study_status.name}", "ERROR", "ERROR_MESSAGE", "{study_id}", f"{str(exc)}", "")
                df = pd.DataFrame(all_results, columns=headers)
                
                result = {"time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                "executed_on":  os.uname().nodename,
                "result": "Listed Below" 
                }

            
            if send_email_to_submitter:
                result_str = json.dumps(result, indent=4)
                result_str = result_str + "<p>" + df.to_html().replace('border="1"','border="0"')
                send_email("Result of the task: maintain MetaboLights study folders", result_str, None, email, None)                        


    except Exception as ex:
        if send_email_to_submitter:
            result_str = str(ex).replace("\n", "<p>")
            if all_results:
                df = pd.DataFrame(all_results, columns=headers)
                result_str = result_str + "<p>" + df.to_html().replace('border="1"','border="0"')

            send_email("A task was failed: maintain MetaboLights study folders", result_str, None, email, None)
        raise ex                    


def execute_action(file_manager: RemoteFileManager, action_log: MaintenanceActionLog):
    if action_log.action == MaintenanceAction.CREATE:
        permission = None
        try:
            permission = int(action_log.parameters["acl"], 8)
        except Exception as ex:
            pass
        if permission:
            return file_manager.create_folder(action_log.item, acl=permission)
        else:
            return file_manager.create_folder(action_log.item)
    elif action_log.action == MaintenanceAction.MOVE or action_log.action == MaintenanceAction.RENAME:
        target_path = None
        if "target" in  action_log.parameters and action_log.parameters["target"]:
            target_path = action_log.parameters["target"]
        
        return file_manager.move(action_log.item, target_path)
    return False