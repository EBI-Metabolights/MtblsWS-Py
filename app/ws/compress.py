import logging
import os
import time
from app.config import get_settings
from app.tasks.bash_client import BashClient
from typing import Dict, List, Set, Tuple

from flask import request, current_app as app
from flask_restful import Resource, reqparse, abort
from flask_restful_swagger import swagger
from app.services.cluster.hpc_client import HpcClient
from app.services.cluster.hpc_utils import get_new_hpc_datamover_client
from app.ws.db.models import SimplifiedUserModel

from app.ws.study.user_service import UserService
logger = logging.getLogger('wslog')

def compress_raw_data_folders(task_name: str, study_id: str, filename_pattern: str, email: str):
    settings = get_settings()
    client: HpcClient = get_new_hpc_datamover_client()
    messages = []
    inputs = {
            "ROOT_PATH": settings.hpc_cluster.datamover.mounted_paths.cluster_study_readonly_files_actual_root_path,
            "STUDY_ID": study_id,
            "FILE_NAME_PATTERN": filename_pattern,
        }
        
    hpc_queue_name = settings.hpc_cluster.datamover.default_queue
    
    script_template = "compress_raw_data_folders.sh.j2"
    script_path = BashClient.prepare_script_from_template(script_template, **inputs)
    study_log_path = os.path.join(settings.study.mounted_paths.study_internal_files_root_path, study_id, settings.study.internal_logs_folder_name)
    task_log_path = os.path.join(study_log_path, f"{study_id}_{task_name}")
    os.makedirs(task_log_path, exist_ok=True)
    out_log_path = os.path.join(task_log_path, f"{task_name}_out.log")
    err_log_path = os.path.join(task_log_path, f"{task_name}_err.log")
    
    try:
        submission_result = client.submit_hpc_job(
                    script_path, task_name, output_file=out_log_path, error_file=err_log_path, queue=hpc_queue_name, account=email
                )
        job_id = submission_result.job_ids[0] if submission_result and submission_result[0] else  None
        
        messages.append(f"New job was submitted with job id {job_id} for {task_name}")
        return job_id, messages
    except Exception as exc:
        message = f"Exception after job submission. {str(exc)}"
        logger.warning(message)
        messages.append(message)
        return None, messages
    finally:
        if script_path and os.path.exists(script_path):
            os.remove(script_path)


class CompressRawDataFolders(Resource):
    @swagger.operation(
        summary="Compress study raw data folders (curators only)",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "filename_pattern",
                "description": "folder name pattern. For example: *.raw, *.d",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["*.d", "*.raw"]
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication. "
                           "Please provide a study id and a valid user token"
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token"
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def post(self, study_id):
        # param validation
        if study_id is None:
            logger.info('No study_id given')
            abort(404, message="invalid study id")
        study_id = study_id.upper()

        user_token = ""
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        if not user_token:
            abort(404, message="invalid user token")
        
        UserService.get_instance().validate_user_has_curator_role(user_token)
        
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('filename_pattern', help='Filename pattern')
        filename_pattern = None
        
        if request.args:
            args = parser.parse_args(req=request)
            filename_pattern = args['filename_pattern'] if "filename_pattern" in args and args['filename_pattern'] else None

        if filename_pattern is None:
            logger.info('No filename pattern is not given')
            abort(404, message="invalid filename pattern")

        user: SimplifiedUserModel = UserService.get_instance().get_simplified_user_by_token(user_token)
        email = user.email
        date_format = "%Y-%m-%d_%H-%M-%S"
        task_name =  f"COMPRESS_RAW_DATA_FILES_{time.strftime(date_format)}" 
        job_id, messages = compress_raw_data_folders(task_name=task_name, study_id=study_id, filename_pattern=filename_pattern, email=email)
        if job_id:
            result = {"content": f"Task has been started on codon. Result will be sent by email with task id {job_id}", "message": messages, "err": None}
            return result
        else:
            result = {"content": f"Task failed", "message": None, "err": messages}