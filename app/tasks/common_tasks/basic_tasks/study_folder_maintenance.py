import datetime
import json
import logging
import os

import pandas as pd

from app.tasks.worker import (MetabolightsTask, celery, get_flask_app,
                              send_email)
from app.utils import MetabolightsDBException
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study, User
from app.ws.db.types import StudyStatus
from app.ws.folder_maintenance import StudyFolderMaintenanceTask
from app.ws.settings.utils import get_study_settings
from app.ws.study.user_service import UserService

logger = logging.getLogger(__name__)
from app.tasks.worker import get_flask_app


def sort_by_study_id(key: str):
    if key:
        val = os.path.basename(key).upper().replace("MTBLS", "")
        if val.isnumeric():
            return int(val)
    return -1

@celery.task(base=MetabolightsTask, name="app.tasks.common_tasks.basic_tasks.study_folder_maintenance.maintain_metadata_study_folders")
def maintain_metadata_study_folders(user_token: str, send_email_to_submitter=False, study_id: str=None, force_to_maintain=False):
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
