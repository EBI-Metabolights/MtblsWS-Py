import datetime
import glob
import json
import logging
import os
import pathlib
import re
from typing import Dict, Tuple
import uuid
from flask import request
from flask_restful_swagger import swagger
from flask_restful import Resource

from app.config import get_settings
from app.config.utils import get_private_ftp_relative_root_path
from app.tasks.common_tasks.curation_tasks.study_revision import (
    delete_study_revision,
    sync_study_revision,
)
from app.tasks.common_tasks.curation_tasks.submission_model import (
    MakeStudyPublicParameters,
)
from app.tasks.common_tasks.curation_tasks.submission_pipeline import (
    start_new_public_revision_pipeline,
)
from app.utils import MetabolightsException, metabolights_exception_handler
from app.ws.db.schemes import User, Study
from app.ws.db.types import CurationRequest, StudyRevisionStatus, StudyStatus, UserRole
from app.ws.study.study_revision_service import StudyRevisionService
from app.ws.study.study_service import StudyService
from app.ws.study.user_service import UserService
from app.ws.study_actions import ValidationResultFile
from app.ws.tasks.db_track import create_task, get_task

logger = logging.getLogger("wslog")


class StudyRevisions(Resource):
    @swagger.operation(
        summary="Create new study revision.",
        nickname="Create new study revision.",
        notes="""Start a revision pipeline. The revision date will be set to the current date.""",
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
                "name": "revision-comment",
                "description": "Revision comment",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def post(self, study_id: str):
        user_token = request.headers.get("user-token", None)
        revision_comment = request.headers.get("revision-comment", None)

        user = UserService.get_instance().validate_user_has_write_access(
            user_token, study_id
        )
        study: Study = StudyService.get_instance().get_study_by_acc(study_id)
        if study.revision_number > 0:
            revision = StudyRevisionService.get_study_revision(
                study_id, study.revision_number
            )
            if not revision:
                raise Exception(
                    f"Study revision {study.revision_number} is not defined. Fix the revision."
                )
            if revision.status != StudyRevisionStatus.COMPLETED:
                raise Exception(
                    f"Study revision {study.revision_number} is still in progress."
                )

        study_status = StudyStatus(study.status)
        user_role = UserRole(user.role)
        if user_role in {UserRole.ANONYMOUS, UserRole.REVIEWER}:
            raise Exception("User is not allowed to create a new revision")

        if user_role == UserRole.ROLE_SUBMITTER and study_status not in {
            StudyStatus.PRIVATE,
            StudyStatus.INREVIEW,
        }:
            raise Exception(
                "User is not allowed to create a new revision for this study."
            )

        if study_status not in {
            StudyStatus.PRIVATE,
            StudyStatus.PUBLIC,
        }:
            raise Exception(
                f"User is not allowed to create a new revision for study {study.acc}. "
                "Make study Private and try again."
            )
        if user_role == UserRole.ROLE_SUBMITTER:
            if study.revision_number > 0:
                raise MetabolightsException(
                    http_code=403,
                    message="Submitter can only create first public revision of studies.",
                )

            validated, message = self.has_validated(study_id)
            if not validated:
                if "not ready" in message:
                    raise MetabolightsException(
                        http_code=403,
                        message="Please run validation and fix any problems before attempting to change study status.",
                    )
                elif "Metadata files are updated" in message:
                    raise MetabolightsException(
                        http_code=403,
                        message="Metadata files are updated after validation. Please re-run validation and fix any issues before attempting to change study status.",
                    )
                else:
                    raise MetabolightsException(
                        http_code=403,
                        message="There are validation errors in the latest validation report. Please fix any issues before attempting to change study status.",
                    )
        study = StudyService.get_instance().get_study_by_req_or_mtbls_id(study_id)
        current_task = get_task(
            study_id=study.reserved_submission_id, task_name="UPDATE_STUDY_STATUS"
        )
        if current_task:
            raise MetabolightsException(
                f"There is a study status update task running. {current_task.id}"
            )
        if study.reserved_accession:
            current_task = get_task(
                study_id=study.reserved_accession, task_name="UPDATE_STUDY_STATUS"
            )
            if current_task:
                raise MetabolightsException(
                    f"There is a study status update task running. {current_task.id}"
                )
        task_id = str(uuid.uuid4())
        try:
            create_task(
                study_id=study_id, task_name="UPDATE_STUDY_STATUS", message=task_id
            )
        except Exception as ex:
            raise MetabolightsException(
                f"Failed to start status update task. {str(ex)}"
            )

        service_user_token = get_settings().auth.service_account.api_token
        inputs = MakeStudyPublicParameters(
            study_id=study_id,
            obfuscation_code=study.obfuscationcode,
            api_token=service_user_token,
            revision_comment=revision_comment,
            current_status=study.status,
            target_status=StudyStatus.PUBLIC.value,
            created_by=user[1],
        )

        try:
            start_new_public_revision_pipeline.apply_async(
                kwargs={"params": inputs.model_dump()}, task_id=task_id
            )
        except Exception as ex:
            raise MetabolightsException(
                f"Failed to start status update task. {str(ex)}"
            )
        # task = prepare_study_revision.apply_async(kwargs={"study_id": study_id, "user_token": service_user_token})

        # revision_model = StudyRevisionModel.model_validate(revision, from_attributes=True)
        # return revision_model.model_dump()

        current_curation_request = CurationRequest(study.curation_request)
        current_status = StudyStatus(study.status)
        ftp_private_relative_root_path = get_private_ftp_relative_root_path()
        ftp_private_study_folder = study.acc.lower() + "-" + study.obfuscationcode
        ftp_private_folder_path = os.path.join(
            ftp_private_relative_root_path, ftp_private_study_folder
        )

        release_date = (
            study.first_public_date.strftime("%Y-%m-%d")
            if study.first_public_date
            else study.releasedate.strftime("%Y-%m-%d")
        )
        return {
            "release-date": release_date,
            "curation_request": current_curation_request.to_camel_case_str(),
            "assigned_study_id": study.acc,
            "assigned_status": current_status.to_camel_case_str(),
            "assigned_status_code": current_status.value,
            "curation_request_code": current_curation_request,
            "ftp_folder_path": ftp_private_folder_path,
            "obfuscation_code": study.obfuscationcode,
            "study_table_id": study.id,
            "task_id": task_id,
            "async_task": True,
        }

    def get_all_metadata_files(self, study_metadata_files_path: str):
        metadata_files = []
        if not os.path.exists(study_metadata_files_path):
            return metadata_files
        patterns = ["a_*.txt", "s_*.txt", "i_*.txt", "m_*.tsv"]
        for pattern in patterns:
            metadata_files.extend(
                glob.glob(
                    os.path.join(study_metadata_files_path, pattern), recursive=False
                )
            )
        return metadata_files

    def get_validation_summary_result_files_from_history(
        self, study_id: str
    ) -> Dict[str, Tuple[str, ValidationResultFile]]:
        internal_files_root_path = pathlib.Path(
            get_settings().study.mounted_paths.study_internal_files_root_path
        )
        files = {}
        validation_history_path: pathlib.Path = internal_files_root_path / pathlib.Path(
            f"{study_id}/validation-history"
        )
        validation_history_path.mkdir(exist_ok=True)
        result = [
            x for x in validation_history_path.glob("validation-history__*__*.json")
        ]
        for item in result:
            match = re.match(r"(.*)validation-history__(.+)__(.+).json$", str(item))
            if match:
                groups = match.groups()
                definition = ValidationResultFile(
                    validation_time=groups[1], task_id=groups[2]
                )
                files[groups[2]] = (item, definition)
        return files

    def has_validated(self, study_id: str) -> Tuple[bool, str]:
        if not study_id:
            return None, "study_id is not valid."
        metadata_root_path = (
            get_settings().study.mounted_paths.study_metadata_files_root_path
        )
        study_path = os.path.join(metadata_root_path, study_id)
        metadata_files = self.get_all_metadata_files(study_path)
        last_modified = -1
        for file in metadata_files:
            modified_time = os.path.getmtime(file)
            if modified_time > last_modified:
                last_modified = modified_time

        result: Dict[str, Tuple[str, ValidationResultFile]] = (
            self.get_validation_summary_result_files_from_history(study_id)
        )

        result_file = ""
        validation_time = ""
        if result:
            try:
                sorted_result = [result[x] for x in result]
                sorted_result.sort(
                    key=lambda x: x[1].validation_time if x and x[1] else "",
                    reverse=True,
                )
                latest_validation = sorted_result[0]

                result_file = latest_validation[0]

                content = json.loads(
                    pathlib.Path(result_file).read_text(encoding="utf-8")
                )
                start_time = datetime.datetime.fromisoformat(
                    content["start_time"]
                ).timestamp()
                # 1 sec threshold
                if start_time < last_modified:
                    return (
                        False,
                        "Metadata files are updated after the last validation. Re-run validation.",
                    )

                if not content["study_id"]:
                    return (
                        False,
                        "Validation file content is not valid. Study id is different.",
                    )
                if content["status"] == "ERROR":
                    return (
                        False,
                        "There are validation errors. Update metadata and data files and re-run validation",
                    )
                return True, "There is no validation errors"
            except Exception as exc:
                message = f"Validation file read error. {validation_time}: {str(exc)}"
                logger.error(message)
                return False, message
        else:
            return False, "Validation report is not ready. Run validation."

    @swagger.operation(
        summary="Get all study revisions",
        nickname="Get all study revisions",
        notes="""Get all study revisions.""",
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
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def get(self, study_id: str):
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        revisions = StudyRevisionService.get_study_revisions(study_id)
        revisions_dump = [x.model_dump() for x in revisions]
        return revisions_dump


class StudyRevision(Resource):
    @swagger.operation(
        summary="Update study revision task (Curator only)",
        nickname="Update study revision task (Curator only)",
        notes="""Update study revision task.""",
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
                "name": "revision_number",
                "description": "Revision number",
                "paramType": "path",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "status",
                "description": "Task status: Initiated, In Progress, Failed, Completed",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "task-started-at",
                "description": "Task start time in ISO format",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "task-completed-at",
                "description": "Task completed time in ISO format",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "task-message",
                "description": "Task message",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "revision-comment",
                "description": "Revision comment",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def put(self, study_id: str, revision_number: int):
        user_token = request.headers.get("user-token", None)
        status_str = request.headers.get("status", None)
        mapping = {"initiated": 0, "in progress": 1, "failed": 2, "completed": 3}
        status_item = status_str.lower().replace("_", " ")
        if status_item not in mapping:
            raise Exception("Invalid status")
        status = StudyRevisionStatus(mapping[status_item]) if status_str else None
        task_completed_at = request.headers.get("task-completed-at", None)
        task_started_at = request.headers.get("task-started-at", None)
        task_message = request.headers.get("task-message", None)
        revision_comment = request.headers.get("revision-comment", None)

        task_completed_at = (
            datetime.datetime.fromisoformat(task_completed_at)
            if task_completed_at
            else None
        )
        task_started_at = (
            datetime.datetime.fromisoformat(task_started_at)
            if task_started_at
            else None
        )

        UserService.get_instance().validate_user_has_curator_role(user_token)
        revision = StudyRevisionService.update_study_revision_task_status(
            study_id=study_id,
            revision_number=revision_number,
            status=status,
            user_token=user_token,
            task_completed_at=task_completed_at,
            task_started_at=task_started_at,
            task_message=task_message,
            revision_comment=revision_comment,
        )
        return revision.model_dump()

    @swagger.operation(
        summary="Get study revision information",
        nickname="Get study revision information",
        notes="""Get study revision information.""",
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
                "name": "revision_number",
                "description": "Revision number",
                "paramType": "path",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def get(self, study_id: str, revision_number: str):
        user_token = request.headers.get("user-token", None)
        UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        revision = StudyRevisionService.get_study_revision(
            study_id, int(revision_number)
        )
        if not revision:
            raise ("Revision not found")
        return revision.model_dump()

    @swagger.operation(
        summary="Delete a study revision (Curator only)",
        nickname="Delete a study revision (Curator only)",
        notes="""Delete study revision """,
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
                "name": "revision_number",
                "description": "Revision number",
                "paramType": "path",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def delete(self, study_id: str, revision_number: str):
        user_token = request.headers.get("user-token", None)
        UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        task = delete_study_revision.apply_async(
            kwargs={"study_id": study_id, "revision_number": revision_number}
        )
        return {"message": "Revision delete task started", "task_id": task.id}


class StudyRevisionSyncTask(Resource):
    @swagger.operation(
        summary="Start Study revisions sync task. (Curator only)",
        nickname="Start Study revisions sync task.",
        notes="""Start Study revisions sync task.""",
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
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def post(self, study_id: str):
        user_token = request.headers.get("user-token", None)

        UserService.get_instance().validate_user_has_curator_role(user_token)

        study: Study = StudyService.get_instance().get_study_by_acc(study_id)
        study_status = StudyStatus(study.status)
        if study_status not in {
            StudyStatus.PRIVATE,
            StudyStatus.INREVIEW,
            StudyStatus.PUBLIC,
        }:
            raise Exception(
                f"User is not allowed to sync FTP folder for study {study.acc}."
            )

        task = sync_study_revision.apply_async(
            kwargs={
                "study_id": study_id,
                "user_token": user_token,
                "latest_revision": study.revision_number,
            }
        )
        # task = sync_study_revision.apply_async(kwargs={"study_id": study_id, "user_token": user_token})

        return {"task": task.id, "message": "Task started."}
