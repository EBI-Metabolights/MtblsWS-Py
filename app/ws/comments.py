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
from app.ws.study_templates.models import PredefinedValueConfiguration
from app.ws.study_templates.utils import get_template_settings

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
    def patch(self, study_id):
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
        if not isa_study:
            abort(400, message="study is not found.")
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
        summary="Update study assay fields and comments",
        notes="""Example input json format.
        {
            "comments": [
                {
                    "name": "Assay Type",
                    "value": "LC-MS"
                }
            ],
            "fields": {
                "measurementType": "untargeted analysis"
            }
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
    def patch(self, study_id):
        result = validate_submission_update(request)
        study_id = result.context.study_id
        if request.data is None or not request.is_json:
            abort(400)
        input_comments: None | list[dict[str, Any]] = None
        assay_filename = request.headers.get("x-assay-file-name")
        if not assay_filename:
            abort(400, message="assay file name is not provided.")
        measurement_type = None
        try:
            data_dict = request.get_json()
            input_comments = data_dict["comments"]
            measurement_type = data_dict.get("fields", {}).get("measurementType")
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
        if not isa_assay:
            abort(
                400,
                message=f"assay {assay_filename} is not "
                "found in i_Investigation.txt file.",
            )
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
        if measurement_type:
            template_settings = get_template_settings()
            config: None | PredefinedValueConfiguration = (
                template_settings.measurement_types.get(measurement_type)
            )
            if not config or not config.ontology_term:
                valid_values = ", ".join(template_settings.measurement_types.keys())
                abort(
                    400,
                    message=f"measurement type  '{measurement_type}' is not valid. "
                    f"Expected values: {valid_values}",
                )

            isa_assay.measurement_type = isa_model.OntologyAnnotation(
                term=config.ontology_term.term or None,
                term_accession=config.ontology_term.term_accession_number or None,
                term_source=isa_model.OntologySource(
                    name=config.ontology_term.term_source_ref
                ),
            )

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
    def patch(self, study_id):
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
        if not design_descriptor:
            abort(400, message="design descriptor is not found.")
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


class StudyFactorComments(Resource):
    @swagger.operation(
        summary="Update study factor comments",
        notes="""Example input json format.
        {
            "comments": [
                {
                    "name": "Study Factor Value Format",
                    "value": "Ontology"
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
                "name": "x-factor-name",
                "description": "Factor name",
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
    def patch(self, study_id):
        result = validate_submission_update(request)
        study_id = result.context.study_id
        if request.data is None or not request.is_json:
            abort(400)
        input_comments: None | list[dict[str, Any]] = None
        factor_name = request.headers.get("x-factor-name")
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
        study_factor: None | isa_model.StudyFactor = None
        for item in isa_study.factors:
            factor: isa_model.StudyFactor = item
            if factor.name.lower() == factor_name.lower():
                study_factor = item
                break
        if not study_factor:
            abort(400, message="study factor is not found.")
        if study_factor.comments is None:
            study_factor.comments = []
        isa_comments = {
            comment.name.lower(): comment for comment in study_factor.comments
        }
        for comment in input_comments:
            name = comment.get("name", "")
            value = comment.get("value")
            if not name:
                continue
            if name.lower() in isa_comments:
                isa_comments[name.lower()].value = value or ""
            else:
                study_factor.add_comment(name=name, value_=value or "")

        iac.write_isa_study(
            isa_inv,
            None,
            std_path,
            save_investigation_copy=False,
            save_assays_copy=False,
            save_samples_copy=False,
        )
        return True
