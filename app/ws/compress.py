import logging
import os
import time

from flask import request
from flask_restful import Resource, abort
from flask_restful_swagger import swagger

from app.config import get_settings
from app.services.cluster.hpc_client import HpcClient
from app.services.cluster.hpc_utils import get_new_hpc_datamover_client
from app.tasks.bash_client import BashClient
from app.utils import MetabolightsDBException, metabolights_exception_handler
from app.ws.auth.permissions import validate_user_has_curator_role
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study

logger = logging.getLogger("wslog")


def compress_raw_data_folders(
    task_name: str, study_id: str, filename_pattern: str, email: str
):
    with DBManager.get_instance().session_maker() as db_session:
        study: Study = db_session.query(Study).filter(Study.acc == study_id).first()
        if not study:
            raise MetabolightsDBException("No study found on db.")
        folder_name = f"{study.acc.lower()}-{study.obfuscationcode}"

    settings = get_settings()
    client: HpcClient = get_new_hpc_datamover_client()
    messages = []
    inputs = {
        "ROOT_PATH": settings.hpc_cluster.datamover.mounted_paths.cluster_private_ftp_root_path,
        "STUDY_ID": study_id,
        "FILE_NAME_PATTERN": filename_pattern,
        "FTP_PRIVATE_FOLDER": folder_name,
    }

    hpc_queue_name = settings.hpc_cluster.datamover.default_queue

    script_template = "compress_raw_data_folders.sh.j2"
    script_path = BashClient.prepare_script_from_template(script_template, **inputs)
    study_log_path = os.path.join(
        settings.study.mounted_paths.study_internal_files_root_path,
        study_id,
        settings.study.internal_logs_folder_name,
    )
    task_log_path = os.path.join(study_log_path, f"{study_id}_{task_name}")
    os.makedirs(task_log_path, exist_ok=True)
    out_log_path = os.path.join(task_log_path, f"{task_name}_out.log")
    err_log_path = os.path.join(task_log_path, f"{task_name}_err.log")

    try:
        submission_result = client.submit_hpc_job(
            script_path,
            task_name,
            output_file=out_log_path,
            error_file=err_log_path,
            queue=hpc_queue_name,
            account=email,
        )
        job_id = (
            submission_result.job_ids[0]
            if submission_result and submission_result.job_ids
            else None
        )

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
                "dataType": "string",
            },
            {
                "name": "filename_pattern",
                "description": "folder name pattern. For example: *.raw, *.d",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["*.d", "*.raw"],
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication. "
                "Please provide a study id and a valid user token",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def post(self, study_id):
        result = validate_user_has_curator_role(request, study_required=True)
        study_id = result.context.study_id
        username = result.context.username

        filename_pattern = None

        if request.args:
            filename_pattern = (
                request.args.get("filename_pattern")
                if "filename_pattern" in request.args
                and request.args.get("filename_pattern")
                else None
            )

        if filename_pattern is None:
            logger.info("No filename pattern is not given")
            abort(404, message="invalid filename pattern")

        email = username
        date_format = "%Y-%m-%d_%H-%M-%S"
        task_name = f"COMPRESS_RAW_DATA_FILES_{study_id}_{time.strftime(date_format)}"
        job_id, messages = compress_raw_data_folders(
            task_name=task_name,
            study_id=study_id,
            filename_pattern=filename_pattern,
            email=email,
        )
        if job_id:
            result = {
                "content": f"Task has been started on codon. Result will be sent by email with task id {job_id}",
                "message": messages,
                "err": None,
            }
            return result
        else:
            result = {"content": "Task failed", "message": None, "err": messages}
