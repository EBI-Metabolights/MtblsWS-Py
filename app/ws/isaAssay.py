#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-May-23
#  Modified by:   kenneth
#
#  Copyright 2019 EMBL - European Bioinformatics Institute
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import json
import logging
from typing import Dict, OrderedDict, Set, Tuple

from flask import request
from flask_restful import Resource, abort
from flask_restful_swagger import swagger
from isatools import model
from isatools.model import (
    Assay,
    OntologyAnnotation,
    Protocol,
    ProtocolParameter,
    Study,
)

from app.utils import MetabolightsException, metabolights_exception_handler
from app.ws.auth.permissions import validate_submission_update, validate_submission_view
from app.ws.db import schemes as db_model
from app.ws.db.types import UserRole
from app.ws.isa_table_templates import (
    add_new_assay_sheet,
    get_assay_type_from_file_name,
    get_protocol_descriptions,
)
from app.ws.isaApiClient import IsaApiClient
from app.ws.mm_models import AssaySchema
from app.ws.mtblsWSclient import WsClient
from app.ws.study.isa_table_models import NumericValue, OntologyValue
from app.ws.study.study_service import StudyService
from app.ws.study.utils import get_study_metadata_path
from app.ws.study_creation_model import AssayCreationRequest
from app.ws.study_templates.models import (
    OntologyTerm,
    PredefinedValueConfiguration,
    TemplateConfiguration,
    TemplateSettings,
)
from app.ws.study_templates.utils import get_template_settings
from app.ws.utils import (
    get_maf_name_from_assay_name,
    log_request,
    remove_file,
)

logger = logging.getLogger("wslog")
iac = IsaApiClient()
wsc = WsClient()


def extended_response(data=None, errs=None, warns=None):
    ext_resp = {
        "data": data if data else list(),
        "errors": errs if errs else list(),
        "warnings": warns if warns else list(),
    }
    return ext_resp


def get_assay(assay_list, filename):
    for index, assay in enumerate(assay_list):
        if assay.filename.lower() == filename:
            return assay


class StudyAssayDelete(Resource):
    @swagger.operation(
        summary="Delete an assay",
        notes='''Remove an assay from your study. Use the full assay file name,
        like this: "a_MTBLS123_LC-MS_positive_hilic_metabolite_profiling.txt"''',
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
            {
                "name": "assay_file_name",
                "description": "Assay definition",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
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
            {
                "code": 409,
                "message": "Conflict. The request could not be completed due to a conflict"
                " with the current state of study. This is usually issued to prevent duplications.",
            },
        ],
    )
    @metabolights_exception_handler
    def delete(self, study_id: str, assay_file_name: str):
        log_request(request)
        result = validate_submission_update(request)
        study_id = result.context.study_id
        is_curator = result.context.user_role in {
            UserRole.SYSTEM_ADMIN,
            UserRole.ROLE_SUPER_USER,
        }
        study_location = get_study_metadata_path(study_id)
        assay_file_name = assay_file_name or ""
        assay_file_name = assay_file_name.strip()
        if not assay_file_name:
            abort(404)

        # check if we should be keeping copies of the metadata
        save_audit_copy = True
        save_msg_str = "be"
        if "save_audit_copy" in request.headers:
            if (
                request.headers["save_audit_copy"]
                and request.headers["save_audit_copy"].lower() == "true"
            ):
                save_audit_copy = True
                save_msg_str = "be"
            else:
                save_audit_copy = False
                save_msg_str = "NOT be"

        study, isa_inv, std_path = iac.get_isa_study(
            study_id=study_id,
            skip_load_tables=True,
            study_location=study_location,
        )
        isa_study: Study = study
        input_assay = [x for x in isa_study.assays if x.filename == assay_file_name]
        if not input_assay:
            abort(404)
        selected_assay: Assay = input_assay[0]
        # unique_protocols = get_all_unique_protocols_from_study_assays(study_id, isa_study.assays)

        # Collect all protocols from templates
        assay_protocols_and_parameters: Dict[str, Dict[str, Set[str]]] = {}
        assay_type_protocols: Dict[str, Tuple[str, str, str]] = {}
        for item in isa_study.assays:
            assay: Assay = item
            a_file = assay.filename
            a_file = a_file.strip()
            assay_type = get_assay_type_from_file_name(assay.filename)

            if assay_type not in assay_type_protocols:
                study = StudyService.get_instance().get_study_by_req_or_mtbls_id(
                    study_id
                )
                template_version = study.template_version
                protocol_description = get_protocol_descriptions(
                    assay_type=assay_type, template_version=template_version
                )

                assay_type_protocols[assay_type] = protocol_description
            protocol_description = assay_type_protocols[assay_type]
            for protocol_name in protocol_description.get("protocols", []):
                # protocol_name = protocol.get("name")
                if protocol_name not in assay_protocols_and_parameters:
                    assay_protocols_and_parameters[protocol_name] = {}
                if a_file not in assay_protocols_and_parameters[protocol_name]:
                    assay_protocols_and_parameters[protocol_name][a_file] = set()
                parameters = protocol_description.get("protocolDefinitions", {}).get(
                    "parameters", []
                )
                assay_protocols_and_parameters[protocol_name][a_file].update(parameters)

        a_file = selected_assay.filename
        logger.info("Removing assay %s from study %s", assay_file_name, study_id)
        assay_type = get_assay_type_from_file_name(assay_file_name)
        # Get all unique protocols for the study, ie. any protocol that is only used once

        for protocol_name in assay_protocols_and_parameters:
            if protocol_name.lower() == "sample collection":
                continue
            assay_names = assay_protocols_and_parameters[protocol_name]
            if assay_file_name not in assay_names:
                continue
            if not assay_names or (
                assay_names and len(assay_names) == 1 and assay_file_name in assay_names
            ):
                obj = isa_study.get_prot(protocol_name)
                if obj:
                    isa_study.protocols.remove(obj)
            elif (
                assay_names and len(assay_names) > 1 and assay_file_name in assay_names
            ):
                assay_names = assay_protocols_and_parameters[protocol_name]
                other_assays = [x for x in assay_names if x != assay_file_name]
                assay_params = assay_names[assay_file_name]
                if assay_params:
                    new_params = set()
                    for other_assay in other_assays:
                        new_params = new_params.union(assay_names[other_assay])

                    if not assay_params.issubset(new_params):
                        new_params_list = []
                        protocol: Protocol = isa_study.get_prot(protocol_name)
                        for param in protocol.parameters:
                            protocol_param: ProtocolParameter = param
                            if protocol_param.parameter_name.term in new_params:
                                new_params_list.append(param)
                        protocol.parameters = new_params_list
        isa_study.assays.remove(selected_assay)
        maf_name = get_maf_name_from_assay_name(a_file)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(
            isa_inv,
            None,
            std_path,
            save_investigation_copy=save_audit_copy,
            save_assays_copy=save_audit_copy,
            save_samples_copy=save_audit_copy,
        )
        try:
            remove_file(
                study_location, a_file, always_remove=True, is_curator=is_curator
            )  # We have to remove active metadata files
            if maf_name is not None:
                remove_file(
                    study_location, maf_name, always_remove=True, is_curator=is_curator
                )
        except:
            logger.error(
                "Failed to remove assay file " + a_file + " from study " + study_id
            )

        return {"success": "The assay was removed from study " + study_id}


class AssayFile(Resource):
    @swagger.operation(
        summary="Add a new assay file",
        notes="Add a new assay to a study<pre><code>",
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
            {
                "name": "assay",
                "description": "Assay definition",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
        ],
    )
    @metabolights_exception_handler
    def post(self, study_id):
        log_request(request)
        result = validate_submission_update(request)
        study_id = result.context.study_id

        # settings = get_study_settings()
        # study_root_location = settings.mounted_paths.study_metadata_files_root_path
        # study_location = os.path.join(study_root_location, study_id)
        template_settings = get_template_settings()
        study: db_model.Study = StudyService.get_instance().get_study_by_acc(study_id)
        version_settings = template_settings.versions.get(study.template_version)

        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            new_assay_input = self.validate_assay_input(
                data_dict, template_settings, version_settings
            )
            selected_assay_type = new_assay_input.selected_assay_file_template
            selected_default_values = {
                "Parameter Value[Scan polarity]": None,
                "Parameter Value[Column type]": None,
            }
            for default_value in new_assay_input.assay_file_default_values:
                column_name = f"Parameter Value[{default_value.field_name}]"
                if column_name in selected_default_values:
                    selected_default_values[column_name] = (
                        default_value.default_value.annotation_value or None
                        if default_value.default_value
                        else None
                    )

            polarity = selected_default_values.get("Parameter Value[Scan polarity]", "")

            if polarity:
                if "pos" in polarity and "neg" not in polarity:
                    polarity = "positive"
                elif "neg" in polarity and "pos" not in polarity:
                    polarity = "negative"
                elif "alt" in polarity or ("neg" in polarity and "pos" in polarity):
                    polarity = "alternating"

            column_type = (
                selected_default_values.get("Parameter Value[Column type]", "") or ""
            )
            measurment_type_name = "untargeted metabolite profiling"
            if new_assay_input.selected_measurement_type == "targetted":
                measurment_type_name = "targeted metabolite profiling"

            column_default_values = {}

            default_value_comments: dict[str, dict[str, str]] = OrderedDict()
            default_values = OrderedDict(
                [
                    ("Assay Field Name", []),
                    ("Assay Field Default Value", []),
                    ("Assay Field Default Unit", []),
                    ("Assay Field Default Value Source REF", []),
                    ("Assay Field Default Value Accession Number", []),
                ]
            )

            for field in new_assay_input.assay_file_default_values:
                column_name = field.field_name
                default_value_comments[column_name] = {
                    "Assay Field Name": column_name,
                    "Assay Field Default Value": field.default_value.annotation_value,
                    "Assay Field Default Unit": "",
                    "Assay Field Default Value Source REF": "",
                    "Assay Field Default Value Accession Number": "",
                }
                if field.default_value and field.default_value.unit:
                    unit = field.default_value.unit
                    default_value_comments[column_name].update(
                        {
                            "Assay Field Default Unit": unit.annotation_value or "",
                            "Assay Field Default Value Source REF": unit.term_source.name
                            or ""
                            if unit.term_source
                            else "",
                            "Assay Field Default Value Accession Number": unit.term_accession
                            or "",
                        }
                    )
                elif field.default_value:
                    term = field.default_value
                    default_value_comments[column_name].update(
                        {
                            "Assay Field Default Value Source REF": term.term_source.name
                            or ""
                            if term.term_source
                            else "",
                            "Assay Field Default Value Accession Number": term.term_accession
                            or "",
                        }
                    )
                for comment_name, v in default_value_comments[column_name].items():
                    default_values.get(comment_name).append(v)

                if field.field_format and field.field_format.lower() == "numeric":
                    column_default_values[column_name] = NumericValue(
                        value=field.default_value.annotation_value
                        if field.default_value and field.default_value.annotation_value
                        else "",
                        unit=OntologyValue(
                            term=field.default_value.unit.annotation_value or "",
                            term_source_ref=field.default_value.unit.term_source.name
                            or "",
                            term_accession_number=field.default_value.unit.term_accession
                            or "",
                        )
                        if field.default_value and field.default_value.unit
                        else OntologyValue(
                            term="", term_source_ref="", term_accession_number=""
                        ),
                    )
                elif field.field_format and field.field_format.lower() == "text":
                    column_default_values[column_name] = (
                        field.default_value.annotation_value
                        if field.default_value and field.default_value.annotation_value
                        else ""
                    )
                else:
                    column_default_values[column_name] = OntologyValue(
                        term=field.default_value.annotation_value or ""
                        if field.default_value and field.default_value.annotation_value
                        else "",
                        term_source_ref=field.default_value.term_source.name or ""
                        if field.default_value and field.default_value.term_source
                        else "",
                        term_accession_number=field.default_value.term_accession or ""
                        if field.default_value and field.default_value.term_accession
                        else "",
                    )

            additional_assay_comments = self.create_assay_comments(
                new_assay_input,
                default_values,
                template_settings,
                version_settings,
            )
            column_type = column_type.lower().replace(" ", "-").strip()
            default_measurement_type: PredefinedValueConfiguration = (
                template_settings.measurement_types.get(
                    version_settings.default_measurement_type
                ).ontology_term
            )
            measurement_type: OntologyTerm = template_settings.omics_types.get(
                new_assay_input.selected_omics_type or "", default_measurement_type
            ).ontology_term

            success, assay_file_name, maf_filename = add_new_assay_sheet(
                study_id,
                selected_assay_type,
                polarity or "",
                column_type or "",
                measurement_type,
                column_default_values,
                template_version=study.template_version,
                measurment_type_name=measurment_type_name,
                additional_assay_comments=additional_assay_comments,
            )
            if success:
                return {
                    "assayFileName": assay_file_name,
                    "resultFileName": maf_filename,
                }
            else:
                abort(400, message="Process failed")
        except Exception as e:
            logger.error(f"Error while creating new study assay: {e}")
            abort(400, message="Incorrect JSON provided")

    def create_assay_comments(
        self,
        new_assay_input: AssayCreationRequest,
        default_values: dict[str, dict[str, str]],
        template_settings: TemplateSettings,
        version_settings: TemplateConfiguration,
    ):
        default_omics_type: PredefinedValueConfiguration = (
            template_settings.omics_types.get(
                version_settings.default_omics_type
            ).ontology_term
        )
        omics_type: OntologyTerm = template_settings.omics_types.get(
            new_assay_input.selected_omics_type or "", default_omics_type
        ).ontology_term
        assay_type_ontology = version_settings.active_assay_file_templates.get(
            new_assay_input.selected_assay_file_template
        ).ontology_term
        additional_assay_comments = [
            model.Comment(
                name="Assay Type Label",
                value=new_assay_input.selected_assay_file_template,
            ),
            model.Comment(name="Assay Type", value=assay_type_ontology.term or ""),
            model.Comment(
                name="Assay Type Term Source REF",
                value=assay_type_ontology.term_source_ref or "",
            ),
            model.Comment(
                name="Assay Type Term Accession Number",
                value=assay_type_ontology.term_accession_number or "",
            ),
            model.Comment(name="Omics Type", value=omics_type.term or ""),
            model.Comment(
                name="Omics Type Source REF",
                value=omics_type.term_source_ref or "",
            ),
            model.Comment(
                name="Omics Type Accession Number",
                value=omics_type.term_accession_number or "",
            ),
        ]

        for name, values in default_values.items():
            additional_assay_comments.append(
                model.Comment(name=name, value=";".join(values)),
            )
        desc_comments = OrderedDict(
            OrderedDict(
                [
                    ("Assay Descriptor", []),
                    ("Assay Descriptor Term Accession Number", []),
                    ("Assay Descriptor Term Source REF", []),
                    ("Assay Descriptor Category", []),
                    ("Assay Descriptor Source", []),
                ]
            )
        )
        default_source = (
            template_settings.descriptor_configuration.default_submitter_source
        )
        default_category = (
            template_settings.descriptor_configuration.default_descriptor_category
        )

        for desc in new_assay_input.design_descriptors:
            desc_comments["Assay Descriptor"].append(desc.annotation_value or "")
            desc_comments["Assay Descriptor Term Accession Number"].append(
                desc.term_accession or ""
            )
            desc_comments["Assay Descriptor Term Source REF"].append(
                desc.term_source.name or "" if desc.term_source else ""
            )
            source = ""
            category = ""
            for comment in desc.comments:
                if not source and comment.name == "Assay Descriptor Source":
                    source = comment.value or ""
                if not category and comment.name == "Assay Descriptor Category":
                    category = comment.value or ""

            source = source or default_source
            category = category or default_category
            desc_comments["Assay Descriptor Source"].append(source)
            desc_comments["Assay Descriptor Category"].append(category)

        for name, values in desc_comments.items():
            additional_assay_comments.append(
                model.Comment(name=name, value=";".join(values)),
            )

        return additional_assay_comments

    def validate_assay_input(
        self,
        data_dict,
        template_settings: TemplateSettings,
        version_settings: TemplateConfiguration,
    ) -> AssayCreationRequest:
        new_study_input = AssayCreationRequest.model_validate(data_dict)

        if not new_study_input.selected_assay_file_template:
            raise MetabolightsException(
                message="Assay file template is not selected.",
                http_code=400,
            )
        if (
            new_study_input.selected_assay_file_template
            not in version_settings.active_assay_file_templates
        ):
            raise MetabolightsException(
                message="Assay file template is not valid for this file template.",
                http_code=400,
            )
        if (
            new_study_input.selected_measurement_type
            not in version_settings.active_measurement_types
        ):
            raise MetabolightsException(
                message="Measurement type is not valid for this file template.",
                http_code=400,
            )
        if (
            new_study_input.selected_omics_type
            not in version_settings.active_omics_types
        ):
            raise MetabolightsException(
                message="Omics type is not valid for this file template.",
                http_code=400,
            )
        if (
            new_study_input.assay_result_file_type
            not in version_settings.active_result_file_formats
        ):
            raise MetabolightsException(
                message="Result file format is not valid for this file template.",
                http_code=400,
            )

        return new_study_input

class StudyAssay(Resource):
    @swagger.operation(
        summary="Get Study Assay",
        notes="""Get Study Assay.""",
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
                "name": "filename",
                "description": "Assay filename",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "list_only",
                "description": "List filenames only",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True,
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
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
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
    def get(self, study_id):
        result = validate_submission_view(request)
        study_id = result.context.study_id
        study_location = get_study_metadata_path(study_id)
        filename = None

        list_only = True
        if request.args:
            filename = (
                request.args.get("filename").lower()
                if request.args.get("filename")
                else None
            )
            list_only = (
                True if request.args.get("list_only").lower() == "true" else False
            )

        logger.info("Getting Assay %s for %s", filename, study_id)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, None, skip_load_tables=False, study_location=study_location
        )

        obj_list = isa_study.assays
        found = list()
        if not filename:
            found = obj_list
        else:
            assay = get_assay(obj_list, filename)
            if assay:
                found.append(assay)
        if not found:
            abort(404)
        logger.info("Found %d assays", len(found))

        sch = AssaySchema(many=True)
        if list_only:
            sch = AssaySchema(only=("filename",), many=True)
        return extended_response(data={"assays": sch.dump(found).data})

    @swagger.operation(
        summary="Add a new assay",
        notes="""Add a new assay to a study<pre><code>
{
 "assay": {
    "type": "LC-MS",
    "template_version": "1.0",
    "columns": [
            {
                "name"  : "polarity",
                "value" : "positive"
            },
            {
                "name"  : "column type",
                "value" : "hilic"
            },
            {
                "name"  : "Parameter Value[Instrument]",
                "value" : "My instrument make and model"
            }
        ]
    }
}</pre></code> </p>
Accepted values for:</br>
- <b>(mandatory)</b> "type" - "LC-MS", "LC-DAD", "GC-MS", "GCxGC-MS", "GC-FID", "DI-MS", "FIA-MS", "CE-MS", "MALDI-MS", "MSImaging", "NMR"</br>
- <b>(optional)</b> "polarity" - "positive", "negative" or "alternating"</br>
- <b>(optional)</b> "column type"  - "hilic", "reverse phase" or "direct infusion"</br>
</br>
<b>Acronyms:</b>  Diode array detection (LC-DAD), Tandem MS (GCxGC-MS), Flame ionisation detector (GC-FID),
Direct infusion (DI-MS), Flow injection analysis (FIA-MS), Capillary electrophoresis (CE-MS),
Matrix-assisted laser desorption-ionisation imaging mass spectrometry (MALDI-MS), Nuclear magnetic resonance (NMR),
Mass spec spectrometry (MSImaging)
</p>
Other columns, like "Parameter Value[Instrument]" must be matches exactly like the header in the assay file""",
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
            {
                "name": "assay",
                "description": "Assay definition",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
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
            {
                "code": 409,
                "message": "Conflict. The request could not be completed due to a conflict"
                " with the current state of study. This is usually issued to prevent duplications.",
            },
        ],
    )
    @metabolights_exception_handler
    def post(self, study_id):
        log_request(request)
        result = validate_submission_update(request)
        study_id = result.context.study_id
        study_location = get_study_metadata_path(study_id)

        # check if we should be keeping copies of the metadata
        save_audit_copy = False
        save_msg_str = "NOT be"
        if request.headers.get("save_audit_copy", "").lower() == "true":
            save_audit_copy = True
            save_msg_str = "be"

        # body content validation
        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            data = data_dict.get("assay", {})
            assay_type = data.get("type")
            # template_version = data.get("template_version", "")
            columns = data.get("columns", [])
            if assay_type is None:
                abort(412)

        except Exception:
            abort(400, message="Incorrect JSON provided")
        study = StudyService.get_instance().get_study_by_req_or_mtbls_id(study_id)
        polarity = ""
        column_type = ""
        column_default_values = {}
        for key_val in columns:
            name = key_val.get("name", "") or ""
            value = key_val.get("value", "") or ""

            if isinstance(value, dict):
                if hasattr(value, "unit"):
                    unit = value.get("unit", {})
                    default_value = NumericValue(
                        unit=OntologyValue(
                            term=unit.get("term") or "",
                            term_accession=unit.get("term_accession", "") or "",
                            term_source=unit.get("term_source", ""),
                        )
                    )
                else:
                    default_value = OntologyValue(
                        term=value.get("term", "") or "",
                        term_accession=value.get("term_accession", "") or "",
                        term_source=value.get("term_source", ""),
                    )
            else:
                default_value = str(value)
            if name.lower() == "polarity":
                term = (
                    default_value.term.lower()
                    if isinstance(default_value, OntologyAnnotation)
                    else default_value.lower()
                )
                if "pos" in term and "neg" not in term:
                    polarity = "positive"
                elif "neg" in term and "pos" not in term:
                    polarity = "negative"
                elif "alt" in term or ("neg" in term and "pos" in term):
                    polarity = "alternating"
                column_default_values["Parameter Value[Scan polarity]"] = default_value
            elif name.lower() == "column type":
                column_type = value.lower().replace(" ", "-").strip()
                column_default_values["Parameter Value[Column type]"] = default_value
            else:
                column_default_values[name] = default_value

        success, assay_file_name, maf_filename = add_new_assay_sheet(
            study_id,
            assay_type,
            polarity,
            column_type,
            column_default_values,
            template_version=study.template_version,
        )

        if success:
            return {
                "success": "The assay was added to study " + study_id,
                "protocols": "",
                "filename": assay_file_name,
                "maf": maf_filename,
                "assay": {},
            }
        else:
            return {
                "success": "failed ",
                "protocols": "",
                "filename": "",
                "maf": "",
                "assay": {},
            }
