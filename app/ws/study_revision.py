import logging
from flask import request
from flask_restful_swagger import swagger
from flask_restful import Resource

from app.tasks.common_tasks.curation_tasks.study_revision import delete_study_revision, prepare_study_revision, sync_study_revision, sync_study_metadata_folder
from app.utils import metabolights_exception_handler
from app.ws.db.models import StudyRevisionModel
from app.ws.db.schemes import  User, Study
from app.ws.db.types import StudyRevisionStatus, StudyStatus, UserRole
from app.ws.study.study_revision_service import StudyRevisionService
from app.ws.study.study_service import StudyService
from app.ws.study.user_service import UserService

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
        
        user: User = UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        study: Study = StudyService.get_instance().get_study_by_acc(study_id)
        if study.revision_number > 0:
            revision = StudyRevisionService.get_study_revision(study_id, study.revision_number)
            if not revision:
                raise Exception(f"Study revision {study.revision_number} is not defined. Fix the revision.")
            if revision.status != StudyRevisionStatus.COMPLETED:
                raise Exception(f"Study revision {study.revision_number} is still in progress.")

        study_status = StudyStatus(study.status)
        user_role = UserRole(user.role)
        if user_role in {UserRole.ANONYMOUS, UserRole.REVIEWER}:
            raise Exception("User is not allowed to create a new revision")
        
        if user_role == UserRole.ROLE_SUBMITTER and study_status not in {StudyStatus.PRIVATE, StudyStatus.INREVIEW}:
            raise Exception("User is not allowed to create a new revision for this study.")
        
        if study_status not in {StudyStatus.PRIVATE, StudyStatus.INREVIEW, StudyStatus.PUBLIC}:
            raise Exception(f"User is not allowed to create a new revision for study {study.acc}.")
        
        revision: StudyRevisionModel = StudyRevisionService.increment_study_revision(study_id, revision_comment=revision_comment, created_by=user.username)
        prepare_study_revision.apply_async(kwargs={"study_id": study_id, "user_token": user_token})


        revision_model = StudyRevisionModel.model_validate(revision, from_attributes=True)
        return revision_model.model_dump()


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
    def put(self, study_id: str, revision_number: str):
        user_token = request.headers.get("user-token", None)
        status_str = request.headers.get("status", None)
        mapping = {"initiated": 0,  "in progress": 1, "failed": 2, "completed": 3}
        status_item = status_str.lower().replace("_", " ")
        if status_item not in mapping:
            raise Exception("Invalid status")
        status = StudyRevisionStatus(mapping[status_item]) if status_str else None
        task_completed_at = request.headers.get("task-completed-at", None)
        task_started_at = request.headers.get("task-started-at", None)
        task_message = request.headers.get("task-message", None)
        revision_comment = request.headers.get("revision-comment", None)
        
        revision = int(revision_number)
        
        
        UserService.get_instance().validate_user_has_curator_role(user_token)
        revision  = StudyRevisionService.update_study_revision_task_status(study_id=study_id, 
                                                                           revision_number=revision, 
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
        revision = StudyRevisionService.get_study_revision(study_id, int(revision_number))
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
        task = delete_study_revision.apply_async(kwargs={"study_id": study_id, "revision_number": revision_number})
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
        if study_status not in {StudyStatus.PRIVATE, StudyStatus.INREVIEW, StudyStatus.PUBLIC}:
            raise Exception(f"User is not allowed to sync FTP folder for study {study.acc}.")
        

        task = sync_study_metadata_folder.apply_async(kwargs={"study_id": study_id, "user_token": user_token})
        # task = sync_study_revision.apply_async(kwargs={"study_id": study_id, "user_token": user_token})

        return {"task": task.id, "message": "Task started."}