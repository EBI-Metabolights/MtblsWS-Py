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
from typing import Dict, Set, Tuple

from flask import request
from flask_restful import Resource, abort
from flask_restful_swagger import swagger
from isatools.model import (
    Assay,
    OntologyAnnotation,
    Protocol,
    ProtocolParameter,
    Study,
)
from app.config import get_settings
from app.utils import metabolights_exception_handler
from app.ws.auth.permissions import validate_submission_update, validate_submission_view
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
from app.ws.study.user_service import UserService
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

        if assay_file_name is None:
            abort(404)
        assay_file_name = assay_file_name.strip()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            # user token is required
            abort(401)

        # query validation
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)
        study = StudyService.get_instance().get_study_by_req_or_mtbls_id(study_id)
        template_version = study.template_version
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
                # protocols = protocol_description.get("protocolDefinitions", [])

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
