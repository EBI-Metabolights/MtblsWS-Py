import datetime
import glob
import json
import logging
import os
import shutil
from app.tasks.common.elasticsearch import reindex_study
from app.tasks.common.remote_folder_operations import create_readonly_study_folders

from app.tasks.worker import (MetabolightsTask, celery, get_flask_app,
                              send_email)
from app.utils import MetabolightsDBException
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study, User
from app.ws.settings.utils import get_study_settings
from app.ws.study.user_service import UserService
from app.ws.study_folder_utils import (copy_initial_study_files,
                                       create_initial_study_folder, prepare_rw_study_folder_structure,
                                       update_initial_study_files)

logger = logging.getLogger(__name__)
from app.tasks.worker import get_flask_app


def sort_by_study_id(key: str):
    if key:
        val = os.path.basename(key).upper().replace("MTBLS", "")
        if val.isnumeric():
            return int(val)
    return -1

@celery.task(base=MetabolightsTask, name="app.tasks.periodic_tasks.study_folder.maintain_study_folders")
def maintain_study_folders(user_token: str, send_email_to_submitter=False):
    try:
        flask_app = get_flask_app()
        UserService.get_instance(flask_app).validate_user_has_curator_role(user_token)
        with flask_app.app_context():
            studies_dict = {}    
            with DBManager.get_instance(flask_app).session_maker() as db_session:
                user = db_session.query(User.email).filter(User.apitoken == user_token).first()
                if not user:
                    raise MetabolightsDBException("No user")
                
                email = user.email
                
                result = db_session.query(Study.acc, Study.updatedate).all()

                if not result:
                    raise MetabolightsDBException(f"No study found on db.") 
                for study in result:
                    studies_dict[study["acc"]] = study["updatedate"] 
                    
            current_study_folders = {} 
            nonexist_study_folders = []
            uncompleted_studies = {}
            studies_not_in_db = []
            settings = get_study_settings()
            
            root_path = settings.study_metadata_files_root_path
            search_pattern = "MTBLS*"
            files_found = glob.glob(os.path.join(root_path, search_pattern))
            study_folders = [f for f in files_found if os.path.isdir(f)]
            files_found = None
            study_folders.sort(key=sort_by_study_id)
            
            for folder in study_folders:
                study_id = os.path.basename(folder)
                current_study_folders[study_id] = {"investigation_file": settings.investigation_file_name, "sample_file": ""}
                if study_id not in studies_dict:
                    studies_not_in_db.append(study_id)
                    continue
                sample_file = ''
                investigation_file_path = os.path.join(folder, settings.investigation_file_name)
                if not os.path.exists(investigation_file_path):
                    current_study_folders[study_id]["investigation_file"] = ""
                    uncompleted_studies[study_id] = current_study_folders[study_id]
                elif os.stat(investigation_file_path).st_size == 0:
                    try:
                        os.remove(investigation_file_path)
                    except OSError as ex:
                        pass
                    current_study_folders[study_id]["investigation_file"] = ""
                    uncompleted_studies[study_id] = current_study_folders[study_id]
                
                sample_files = glob.glob(os.path.join(folder, "s_*.txt"))
                
                if  len(sample_files) == 0:
                    uncompleted_studies[study_id] = current_study_folders[study_id]
                else:
                    for sample_file in sample_files:
                        file_name = os.path.basename(sample_file)
                        current_study_folders[study_id]["sample_file"] = file_name
                
            for db_study in studies_dict:
                if db_study not in current_study_folders:
                    nonexist_study_folders.append(db_study)
                    
            created_study_folders = set()   
            updated_study_folders = set()
            failures = {}
            try:                
                for item in nonexist_study_folders:
                    try:
                        create_initial_study_folder(item, flask_app)
                        study_folder = os.path.join(root_path, item)
                        update_initial_study_files(study_folder, item, user_token)
                        created_study_folders.add(item)
                        
                        prepare_rw_study_folder_structure(item)
                        study_acc = item
                        inputs = {"study_id": study_acc}
                        create_study_folders_task = create_readonly_study_folders.apply_async(kwargs=inputs)
                        logger.info(f"Create read only study folders task is started for study {study_acc} with task id: {create_study_folders_task.id}")

                    except Exception as ex:
                        failures[item] = str(ex)

                for item in uncompleted_studies:
                    try:
                        from_path = settings.study_default_template_path
                        study_folder = os.path.join(root_path, item)
                        files_updated = False
                        if not uncompleted_studies[item]["investigation_file"] and not uncompleted_studies[item]["sample_file"]: 
                            copy_initial_study_files(item, flask_app)
                            files_updated = True
                        elif not uncompleted_studies[item]["sample_file"]:
                            target = os.path.join(study_folder, f"s_{item}.txt")
                            source = os.path.join(from_path, "s_Sample.txt")
                            shutil.copy(source, target)
                            files_updated = True
                        elif not uncompleted_studies[item]["investigation_file"]:
                            target = os.path.join(study_folder, settings.investigation_file_name)
                            source = os.path.join(from_path, settings.investigation_file_name)
                            shutil.copy(source, target)
                            files_updated = True
                            
                        if files_updated:
                            update_initial_study_files(study_folder, item, user_token)
                            updated_study_folders.add(item)
                        else:
                            failures[item] = f"Manual review is reqeired for inital study files: investigation file='{uncompleted_studies[item]['investigation_file']}', sample file='{uncompleted_studies[item]['sample_file']}' "
                    except Exception as ex:
                        failures[item] = str(ex)
                        
            except Exception as exc:
                raise exc
        
        updated = True
        if not uncompleted_studies and not nonexist_study_folders:
            updated = False
        warnings = True
        if not failures and not studies_not_in_db:
            warnings = False
        
        status = "NO CHANGE" if not updated else "UPDATED"
        status = f"{status} WITH WARNING" if warnings else status
        
        result = {"time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                "status": f'{status}',
                "executed_on":  os.uname().nodename,
                "created_folders": str(nonexist_study_folders),
                "updated_folders": str(updated_study_folders),
                "failures": str(failures),
                "unexpected_study_folders": str(studies_not_in_db)}
        
        if send_email_to_submitter:
            result_str = json.dumps(result, indent=4)
            result_str = result_str.replace("\n", "<p>")
            send_email("Result of the task: maintain MetaboLights study folders", result_str, None, email, None)
        
        return result
    except Exception as ex:
        if send_email_to_submitter:
            result_str = str(ex).replace("\n", "<p>")
            send_email("A task was failed: maintain MetaboLights study folders", result_str, None, email, None)
        raise ex        