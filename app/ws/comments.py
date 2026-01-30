import logging
import os
from typing import Any

from flask import request
from flask_restful import Resource, abort
from flask_restful_swagger import swagger
from isatools import model as isa_model

from app.config import get_settings
from app.utils import (
    metabolights_exception_handler,
)
from app.ws.auth.permissions import (
    validate_submission_update,
)
from app.ws.isaApiClient import IsaApiClient

logger = logging.getLogger(__name__)


class StudyComments(Resource):
    @swagger.operation(
        summary="Update study comments",
        notes="""Example input json format.
        {
            "comments": [
                {
                    "name": "Funder",
                    "value": "Wellcome Trust"
                },
                {
                    "name": "Funder ROR ID",
                    "value": "https://ror.org/029chgv08"
                },
            ]
        }

        If there are multiple terms for comment value, use ; to separate them
        """,
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
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "comments",
                "description": "Comments in JSON format",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
        ],
    )
    @metabolights_exception_handler
    def put(self, study_id):
        result = validate_submission_update(request)
        study_id = result.context.study_id
        if request.data is None or not request.is_json:
            abort(400)
        comments = None
        try:
            data_dict = request.get_json()
            comments = data_dict["comments"]
        except Exception:
            abort(400, message="invalid body input")
        iac = IsaApiClient()
        root_path = get_settings().study.mounted_paths.study_metadata_files_root_path
        study_location = os.path.join(root_path, study_id)
        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, None, skip_load_tables=True, study_location=study_location
        )
        if isa_study.comments is None:
            isa_study.comments = []
        study_comments = {comment.name: comment for comment in isa_study.comments}
        for comment in comments:
            name = comment.get("name")
            value = comment.get("value")
            if not name:
                continue
            if name in study_comments:
                study_comments[name].value = value or ""
            else:
                isa_study.add_comment(name=name, value_=value or "")

        iac.write_isa_study(
            isa_inv,
            None,
            std_path,
            save_investigation_copy=False,
            save_assays_copy=False,
            save_samples_copy=False,
        )
        return True


class AssayComments(Resource):
    @swagger.operation(
        summary="Update study assay comments",
        notes="""Example input json format.
        {
            "comments": [
                {
                    "name": "Assay Type",
                    "value": "LC-MS"
                }
            ]
        }

        If there are multiple terms for comment value, use ; to separate them
        """,
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
                "name": "x-assay-file-name",
                "description": "Assay file name",
                "required": True,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "comments",
                "description": "Comments in JSON format",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
        ],
    )
    @metabolights_exception_handler
    def put(self, study_id):
        result = validate_submission_update(request)
        study_id = result.context.study_id
        if request.data is None or not request.is_json:
            abort(400)
        input_comments: None | list[dict[str, Any]] = None
        assay_filename = request.headers.get("x-assay-file-name")
        try:
            data_dict = request.get_json()
            input_comments = data_dict["comments"]
        except Exception:
            abort(400, message="invalid body input")
        iac = IsaApiClient()
        root_path = get_settings().study.mounted_paths.study_metadata_files_root_path
        study_location = os.path.join(root_path, study_id)
        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, None, skip_load_tables=True, study_location=study_location
        )
        isa_assay = None
        for item in isa_study.assays:
            assay: isa_model.Assay = item
            if assay.filename == assay_filename:
                isa_assay = assay
                break
        if isa_assay.comments is None:
            isa_assay.comments = []
        isa_comments = {comment.name: comment for comment in isa_assay.comments}
        for comment in input_comments:
            name = comment.get("name")
            value = comment.get("value")
            if not name:
                continue
            if name in isa_comments:
                isa_comments[name].value = value or ""
            else:
                isa_assay.add_comment(name=name, value_=value or "")

        iac.write_isa_study(
            isa_inv,
            None,
            std_path,
            save_investigation_copy=False,
            save_assays_copy=False,
            save_samples_copy=False,
        )
        return True


class StudyDesignDescriptorComments(Resource):
    @swagger.operation(
        summary="Update study design descriptor comments",
        notes="""Example input json format.
        {
            "comments": [
                {
                    "name": "Source",
                    "value": "submitter"
                },
            ]
        }

        If there are multiple terms for comment value, use ; to separate them
        """,
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
                "name": "x-term-name",
                "description": "Assay file name",
                "required": True,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "comments",
                "description": "Comments in JSON format",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
        ],
    )
    @metabolights_exception_handler
    def put(self, study_id):
        result = validate_submission_update(request)
        study_id = result.context.study_id
        if request.data is None or not request.is_json:
            abort(400)
        input_comments: None | list[dict[str, Any]] = None
        descriptor_name = request.headers.get("x-term-name")
        try:
            data_dict = request.get_json()
            input_comments = data_dict["comments"]
        except Exception:
            abort(400, message="invalid body input")
        iac = IsaApiClient()
        root_path = get_settings().study.mounted_paths.study_metadata_files_root_path
        study_location = os.path.join(root_path, study_id)
        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, None, skip_load_tables=True, study_location=study_location
        )
        design_descriptor: None | isa_model.OntologyAnnotation = None
        for item in isa_study.design_descriptors:
            ontology: isa_model.OntologyAnnotation = item
            if ontology.term == descriptor_name:
                design_descriptor = item
                break
        if design_descriptor.comments is None:
            design_descriptor.comments = []
        isa_comments = {
            comment.name.lower(): comment for comment in design_descriptor.comments
        }
        for comment in input_comments:
            name = comment.get("name", "")
            value = comment.get("value")
            if not name:
                continue
            if name.lower() in isa_comments:
                isa_comments[name.lower()].value = value or ""
            else:
                design_descriptor.add_comment(name=name, value_=value or "")

        iac.write_isa_study(
            isa_inv,
            None,
            std_path,
            save_investigation_copy=False,
            save_assays_copy=False,
            save_samples_copy=False,
        )
        return True
