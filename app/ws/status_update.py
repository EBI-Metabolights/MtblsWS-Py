import os
import uuid
from flask import request
from app.config.utils import get_private_ftp_relative_root_path
from app.tasks.common_tasks.curation_tasks.submission_model import (
    RevalidateStudyParameters,
)
from app.tasks.common_tasks.curation_tasks.submission_pipeline import (
    start_make_study_private_pipeline,
)
from app.utils import MetabolightsException, metabolights_exception_handler
from flask_restful import Resource
from flask_restful_swagger import swagger

from app.ws.db.schemes import StudyTask
from app.ws.db.types import CurationRequest, StudyStatus, StudyTaskStatus
from app.ws.study.study_service import StudyService
from app.ws.study.user_service import UserService
from app.ws.study_status_utils import StudyStatusHelper
from app.ws.tasks.db_track import create_task, delete_task, get_task


class PrivateStudy(Resource):
    @swagger.operation(
        summary="Change study status to private",
        nickname="Change study status",
        notes="""Change study status from 'Provisional' to 'Private'""",
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
                "message": "OK. The Metabolite Annotation File (MAF) is returned",
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
        # param validation
        if study_id is None:
            raise MetabolightsException(
                message="Please provide valid parameter for study identifier"
            )
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        study = StudyService.get_instance().get_study_by_req_or_mtbls_id(study_id)
        current_curation_request = CurationRequest(study.curation_request)
        current_status = StudyStatus(study.status)
        if current_status == StudyStatus.PRIVATE:
            raise MetabolightsException(message="Study status is 'Private'")
        if current_status not in (StudyStatus.PROVISIONAL, StudyStatus.PUBLIC):
            raise MetabolightsException(
                message="Only Provisional studies can be updated to 'Private'"
            )
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
        task_id = None
        task_status = None
        if current_status == StudyStatus.PUBLIC:
            StudyStatusHelper.update_status(
                study_id,
                StudyStatus.PRIVATE.name.lower(),
                first_public_date=study.first_public_date,
                first_private_date=study.first_private_date,
            )
        elif  current_status == StudyStatus.PROVISIONAL:
            task_id = str(uuid.uuid4())
            try:
                create_task(
                    study_id=study_id, task_name="UPDATE_STUDY_STATUS", message=task_id
                )
            except Exception as ex:
                raise MetabolightsException(
                    f"Failed to start status update task. {str(ex)}"
                )


            if current_status == StudyStatus.PROVISIONAL:
                StudyStatusHelper.update_status(
                    study_id,
                    StudyStatus.PRIVATE.name.lower(),
                    first_public_date=study.first_public_date,
                    first_private_date=study.first_private_date,
                )
            
            params = RevalidateStudyParameters(
                task_name=f"Make {study_id} private",
                study_id=study_id,
                obfuscation_code=study.obfuscationcode,
                current_status=study.status,
                target_status=StudyStatus.PRIVATE.value,
                api_token=user_token,
            )
            try:
                start_make_study_private_pipeline.apply_async(
                    kwargs={"params": params.model_dump()}, task_id=task_id
                )
            except Exception as ex:
                raise MetabolightsException(
                    f"Failed to start status update task. {str(ex)}"
                )
            
            task_status = StudyTaskStatus.EXECUTING
        
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
            "task_status": task_status,
            "async_task": True if task_id else False,
        }


class StudyStatusUpdateTask(Resource):
    @swagger.operation(
        summary="Check study status update task",
        nickname="Check study status update task",
        notes="""Check study status update task""",
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
                "message": "OK. The Metabolite Annotation File (MAF) is returned",
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
        # param validation
        if study_id is None:
            raise MetabolightsException(
                message="Please provide valid parameter for study identifier"
            )
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        study = StudyService.get_instance().get_study_by_req_or_mtbls_id(study_id)
        current_task: StudyTask = get_task(
            study_id=study.reserved_submission_id, task_name="UPDATE_STUDY_STATUS"
        )

        if not current_task and study.reserved_accession:
            current_task = get_task(
                study_id=study.reserved_accession, task_name="UPDATE_STUDY_STATUS"
            )

        current_status = StudyStatus(study.status)
        if current_task:
            return {
                "currentStudyId": study.acc,
                "currentStatus": current_status.to_camel_case_str(),
                "statusUpdateTaskId": current_task.last_execution_message,
                "statusUpdateTaskResult": current_task.last_execution_status
                if current_task
                else None,
            }

        else:
            return {
                "currentStudyId": study.acc,
                "currentStatus": current_status.to_camel_case_str(),
                "statusUpdateTaskId": None,
                "statusUpdateTaskResult": None,
            }

    @swagger.operation(
        summary="Delete study status update task",
        nickname="Delete study status update task",
        notes="""Delete study status update task""",
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
                "message": "OK. The Metabolite Annotation File (MAF) is returned",
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
    def delete(self, study_id: str):
        # param validation
        if study_id is None:
            raise MetabolightsException(
                message="Please provide valid parameter for study identifier"
            )
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_curator_role(user_token)
        study = StudyService.get_instance().get_study_by_req_or_mtbls_id(study_id)
        deleted_task = delete_task(
            study_id=study.reserved_accession, task_name="UPDATE_STUDY_STATUS"
        )
        deleted_task = delete_task(
            study_id=study.reserved_submission_id, task_name="UPDATE_STUDY_STATUS"
        )
        return {
            "study_id": study.acc,
            "task_id": deleted_task.id if deleted_task else None,
        }


class ProvisionalStudy(Resource):
    @swagger.operation(
        summary="Change study status from Private to Provisional",
        nickname="Change study status from Private to Provisional",
        notes="""Change study status from Private to Provisional""",
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
                "message": "OK. The Metabolite Annotation File (MAF) is returned",
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
        # param validation
        if study_id is None:
            raise MetabolightsException(
                message="Please provide valid parameter for study identifier"
            )
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        study = StudyService.get_instance().get_study_by_req_or_mtbls_id(study_id)
        study_status = StudyStatus(study.status)
        if study_status == StudyStatus.PROVISIONAL:
            raise MetabolightsException(message="Study status is 'Provisional'")

        if study_status not in (StudyStatus.PRIVATE, StudyStatus.DORMANT):
            raise MetabolightsException(
                message="Only private studies can be updated to 'Provisional'"
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

        status = StudyStatusHelper.update_status(
            study_id,
            StudyStatus.PROVISIONAL.name.lower(),
            first_public_date=study.first_public_date,
            first_private_date=study.first_private_date,
        )

        if not status:
            raise MetabolightsException(
                f"{study.acc} status not updated to private in database."
            )

        study = StudyService.get_instance().get_study_by_req_or_mtbls_id(study_id)
        # return {
        #     "task_name": "make study provisional",
        #     "study_id": study_id,
        #     "status": study_status.name.lower(),
        #     "message": f"{study_id} is provisional.",
        # }
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
            "task_id": None,
            "task_status": None,
            "async_task": False,
        }


# class DormantStudy(Resource):
#     @swagger.operation(
#         summary="Change study status from Private/Provisional to Dormant",
#         nickname="Change study status from Private/Provisional to Dormant",
#         notes="""Change study status from Private/Provisional to Dormant""",
#         parameters=[
#             {
#                 "name": "study_id",
#                 "description": "MTBLS Identifier",
#                 "required": True,
#                 "allowMultiple": False,
#                 "paramType": "path",
#                 "dataType": "string",
#             },
#             {
#                 "name": "user-token",
#                 "description": "User API token",
#                 "paramType": "header",
#                 "type": "string",
#                 "required": True,
#                 "allowMultiple": False,
#             },
#         ],
#         responseMessages=[
#             {
#                 "code": 200,
#                 "message": "OK. The Metabolite Annotation File (MAF) is returned",
#             },
#             {
#                 "code": 401,
#                 "message": "Unauthorized. Access to the resource requires user authentication.",
#             },
#             {
#                 "code": 403,
#                 "message": "Forbidden. Access to the study is not allowed for this user.",
#             },
#             {
#                 "code": 404,
#                 "message": "Not found. The requested identifier is not valid or does not exist.",
#             },
#         ],
#     )
#     @metabolights_exception_handler
#     def post(self, study_id: str):
#         # param validation
#         if study_id is None:
#             raise MetabolightsException(
#                 message="Please provide valid parameter for study identifier"
#             )
#         # User authentication
#         user_token = None
#         if "user_token" in request.headers:
#             user_token = request.headers["user_token"]
#         UserService.get_instance().validate_user_has_write_access(user_token, study_id)
#         study = StudyService.get_instance().get_study_by_req_or_mtbls_id(study_id)
#         study_status = StudyStatus(study.status)
#         if study_status == StudyStatus.DORMANT:
#             raise MetabolightsException(message="Study status is 'Dormant'")

#         if study_status not in (StudyStatus.PRIVATE, StudyStatus.PROVISIONAL):
#             raise MetabolightsException(
#                 message="Only provisional and privated studies can be updated to 'Dormant'"
#             )
#         study = StudyService.get_instance().get_study_by_req_or_mtbls_id(study_id)
#         current_task = get_task(
#             study_id=study.reserved_submission_id, task_name="UPDATE_STUDY_STATUS"
#         )
#         if current_task:
#             raise MetabolightsException(
#                 f"There is a study status update task running. {current_task.id}"
#             )
#         if study.reserved_accession:
#             current_task = get_task(
#                 study_id=study.reserved_accession, task_name="UPDATE_STUDY_STATUS"
#             )
#             if current_task:
#                 raise MetabolightsException(
#                     f"There is a study status update task running. {current_task.id}"
#                 )


#         status = StudyStatusHelper.update_status(
#             study_id,
#             StudyStatus.DORMANT.name.lower(),
#             first_public_date=study.first_public_date,
#             first_private_date=study.first_private_date,
#         )

#         if not status:
#             raise MetabolightsException(
#                 f"{study.acc} status not updated to private in database."
#             )

#         study = StudyService.get_instance().get_study_by_acc(study_id)
#         # return {
#         #     "task_name": "make study provisional",
#         #     "study_id": study_id,
#         #     "status": study_status.name.lower(),
#         #     "message": f"{study_id} is provisional.",
#         # }
#         current_curation_request = CurationRequest(study.curation_request)
#         current_status = StudyStatus(study.status)
#         ftp_private_relative_root_path = get_private_ftp_relative_root_path()
#         ftp_private_study_folder = study.acc.lower() + "-" + study.obfuscationcode
#         ftp_private_folder_path = os.path.join(
#             ftp_private_relative_root_path, ftp_private_study_folder
#         )

#         release_date = (
#             study.first_public_date.strftime("%Y-%m-%d")
#             if study.first_public_date
#             else study.releasedate.strftime("%Y-%m-%d")
#         )
#         return {
#             "release-date": release_date,
#             "curation_request": current_curation_request.to_camel_case_str(),
#             "assigned_study_id": study.acc,
#             "assigned_status": current_status.to_camel_case_str(),
#             "assigned_status_code": current_status.value,
#             "curation_request_code": current_curation_request,
#             "ftp_folder_path": ftp_private_folder_path,
#             "obfuscation_code": study.obfuscationcode,
#             "study_table_id": study.id,
#             "task_id": None,
#             "async_task": False,
#         }
