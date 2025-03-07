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

import glob
import csv
import json
import logging
import os
import os.path
from typing import Dict, List, Set, Tuple
import numpy as np
import pandas as pd
from flask import request
from flask_restful import Resource, reqparse, abort
from flask_restful_swagger import swagger
from isatools.model import (
    Extract,
    Sample,
    OntologyAnnotation,
    Assay,
    Protocol,
    Study,
    ProtocolParameter,
    OntologySource
)
from app.utils import metabolights_exception_handler

from app.ws.isaApiClient import IsaApiClient
from app.ws.mm_models import (
    AssaySchema,
    ProcessSchema,
    OtherMaterialSchema,
    DataFileSchema,
    SampleSchema,
)
from app.ws.mtblsWSclient import WsClient
from app.ws.settings.utils import get_study_settings
from app.ws.utils import (
    get_assay_type_from_file_name,
    get_assay_headers_and_protocols,
    get_sample_headers_and_data,
    write_tsv,
    remove_file,
    get_maf_name_from_assay_name,
    add_new_protocols_from_assay,
    create_maf,
    add_ontology_to_investigation,
    read_tsv,
    log_request,
    copy_file,
    new_timestamped_folder,
    totuples,
)
from app.ws.db_connection import update_study_sample_type

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


def get_source(source_list, source_name):
    for source in source_list:
        if source.name.lower() == source_name.lower():
            return source
    return None


def get_sample(sample_list, sample_name):
    for sample in sample_list:
        if sample.name.lower() == sample_name.lower():
            return sample
    return None


def get_protocol(protocol_list, protocol_name):
    for protocol in protocol_list:
        if protocol.name.lower() == protocol_name.lower():
            return protocol
    return None


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
                "name": "save_audit_copy",
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
    def delete(self, study_id: str, assay_file_name: str):
        log_request(request)
        # param validation
        if study_id is None:
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
            api_key=user_token,
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
            assay_type = get_assay_type_from_file_name(study_id, assay.filename)

            if assay_type not in assay_type_protocols:
                (
                    tidy_header_row,
                    tidy_data_row,
                    protocols,
                    assay_desc,
                    assay_data_type,
                    assay_file_type,
                    assay_mandatory_type,
                ) = get_assay_headers_and_protocols(assay_type)
                assay_type_protocols[assay_type] = protocols
            protocols = assay_type_protocols[assay_type]
            for _, protocol_name, parameters in protocols:
                if protocol_name not in assay_protocols_and_parameters:
                    assay_protocols_and_parameters[protocol_name] = {}
                if a_file not in assay_protocols_and_parameters[protocol_name]:
                    assay_protocols_and_parameters[protocol_name][a_file] = set()

                if parameters:
                    for param in parameters.split(";"):
                        assay_protocols_and_parameters[protocol_name][a_file].add(param)

        a_file = selected_assay.filename
        logger.info("Removing assay " + assay_file_name + " from study " + study_id)
        assay_type = get_assay_type_from_file_name(study_id, assay_file_name)
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
                if not obj:
                    abort(404)
                # remove object
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
            user_token,
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
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        # query validation
        
        
        filename = None
        
        list_only = True
        if request.args:
            
            filename = request.args.get("filename").lower() if request.args.get("filename") else None
            list_only = True if request.args.get("list_only").lower() == "true" else False

        logger.info("Getting Assay %s for %s", filename, study_id)
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
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=False, study_location=study_location
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
                "name": "save_audit_copy",
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
    def post(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            # user token is required
            abort(401)

        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location_deprecated,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        # check if we should be keeping copies of the metadata
        save_audit_copy = False
        save_msg_str = "NOT be"
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"

        # body content validation
        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            data = data_dict["assay"]
            assay_type = data["type"]
            # platform = data['platform']
            try:
                columns = data["columns"]
            except:
                columns = []  # If not provided, ignore

            if assay_type is None:
                abort(412)

        except (Exception):
            abort(400, message="Incorrect JSON provided")
        study_metadata_location = os.path.join(
            get_study_settings().mounted_paths.study_metadata_files_root_path, study_id
        )
        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id=study_id,
            api_key=user_token,
            skip_load_tables=True,
            study_location=study_metadata_location,
        )

        # Also make sure the sample file is in the standard format of 's_MTBLSnnnn.txt'
        # isa_study, sample_file_name = update_correct_sample_file_name(isa_study, study_location, study_id)

        isa_inv, obi = add_ontology_to_investigation(
            isa_inv,
            "OBI",
            "29",
            "http://data.bioontology.org/ontologies/OBI",
            "Ontology for Biomedical Investigations",
        )

        # Add the new assay to the investigation file
        assay_file_name, assay, protocol_params, overall_technology = create_assay(
            assay_type, columns, study_id, obi
        )

        # add the assay to the study
        isa_study.assays.append(assay)

        maf_name = ""
        try:
            maf_name = get_maf_name_from_assay_name(assay_file_name)
            maf_df, annotation_file_name, new_column_counter = create_maf(
                overall_technology, study_metadata_location, assay_file_name, maf_name
            )
        except:
            logger.error(
                "Could not create MAF for study "
                + study_id
                + " under assay "
                + assay_file_name
            )

        message = update_assay_column_values(
            columns, assay_file_name, maf_file_name=maf_name
        )

        logger.info("A copy of the previous files will %s saved", save_msg_str)
        isa_study = add_new_protocols_from_assay(
            assay_type, protocol_params, assay_file_name, study_id, isa_study
        )
        iac.write_isa_study(
            isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy
        )

        protocol_names = ""
        for prot in protocol_params:
            protocol_names = protocol_names + prot[1] + ","

        json_assay = AssaySchema().dump(assay)

        return {
            "success": "The assay was added to study " + study_id,
            "protocols": protocol_names.rstrip(","),
            "filename": assay.filename,
            "maf": maf_name,
            "assay": json_assay[0],
        }


# def get_all_unique_protocols_from_study_assays(study_id, assays):
#     all_protocols = []
#     unique_protocols = []
#     all_names = []
#     short_list = []

#     try:
#         for assay in assays:
#             assay_type = get_assay_type_from_file_name(study_id, assay.filename)
#             tidy_header_row, tidy_data_row, protocols, assay_desc, assay_data_type, assay_file_type, \
#                 assay_mandatory_type = get_assay_headers_and_protocols(assay_type)
#             all_protocols = all_protocols + protocols
#     except:
#         return []

#     for protocol in all_protocols:
#         all_names.append(protocol[1])

#     for prot_name in all_names:
#         unique_protocols.append([prot_name, all_names.count(prot_name)])

#     unique_protocols = list(map(list, set(map(lambda i: tuple(i), unique_protocols))))

#     for i in unique_protocols:
#         if i[1] == 1:
#             short_list.append(i[0])

#     return short_list


def create_assay(assay_type, columns, study_id, ontology, output_folder=None):
    profiling = "metabolite_profiling"
    settings = get_study_settings()
    studies_path = (
        settings.mounted_paths.study_metadata_files_root_path
    )  # Root folder for all studies
    study_path = os.path.join(studies_path, study_id)  # This particular study
    polarity = ""
    column = ""
    for key_val in columns:
        if key_val["name"].lower() == "polarity":
            polarity = key_val["value"]

        if key_val["name"].lower() == "column type":
            column = key_val["value"]

    (
        tidy_header_row,
        tidy_data_row,
        protocols,
        assay_desc,
        assay_data_types,
        assay_file_type,
        assay_data_mandatory,
    ) = get_assay_headers_and_protocols(assay_type)

    assay_platform = assay_desc + " - " + polarity
    if column != "":
        assay_platform = assay_platform + " - " + column

    # this will be the final name for the copied assay template
    file_name = (
        "a_"
        + study_id.upper()
        + "_"
        + assay_type
        + "_"
        + polarity
        + "_"
        + column.replace(" ", "-").lower()
        + "_"
        + profiling
    )

    file_name = get_valid_assay_file_name(file_name, study_path)
    assay, overall_technology = get_new_assay(
        file_name, assay_platform, assay_type, ontology
    )

    if output_folder:
        study_path = output_folder
    file_name = os.path.join(study_path, file_name)

    try:
        file = open(file_name, "w", encoding="utf-8")
        writer = csv.writer(file, delimiter="\t", quotechar='"')
        writer.writerow(tidy_header_row)
        writer.writerow(tidy_data_row)
        file.close()
    except (FileNotFoundError, Exception):
        abort(500, message="Could not write the assay file")

    return file_name, assay, protocols, overall_technology


def get_valid_assay_file_name(file_name, study_path):
    # Has the filename has already been used in another assay?
    file_counter = 0
    assay_file = os.path.join(study_path, file_name + ".txt")
    file_exists = os.path.isfile(assay_file)
    while file_exists:
        file_counter += 1
        new_file = file_name + "-" + str(file_counter)
        if not os.path.isfile(os.path.join(study_path, new_file + ".txt")):
            file_name = new_file
            break

    return file_name + ".txt"


def get_new_assay(file_name, assay_platform, assay_type, ontology):
    assay = Assay(filename=file_name, technology_platform=assay_platform)
    # technologyType
    technology = OntologyAnnotation(
        term_accession="http://purl.obolibrary.org/obo/OBI_0000366",
        term="metabolite profiling",
    )
    # measurementType
    measurement = assay.measurement_type
    overall_technology = ""

    if assay_type in ["NMR", "MRImaging"]:
        technology.term = "NMR spectroscopy assay"
        technology.term_accession = "http://purl.obolibrary.org/obo/OBI_0000623"
        overall_technology = "NMR"
    else:
        technology.term = "mass spectrometry assay"
        technology.term_accession = "http://purl.obolibrary.org/obo/OBI_0000470"
        overall_technology = "MS"

    # Add the termSource to the technologyType
    technology.term_source = ontology
    # Add the ontology term to the assay.technologyType
    assay.technology_type = technology
    # Add the measurementType to the assay.measurementType
    measurement.term_source = ontology
    measurement.term = "metabolite profiling"
    measurement.term_accession = "http://purl.obolibrary.org/obo/OBI_0000366"
    assay.measurement_type = measurement

    try:
        result = AssaySchema().load(assay, partial=True)
    except Exception:
        abort(400)

    return assay, overall_technology


def update_assay_column_values(columns, assay_file_name, maf_file_name=None):
    # These are the real column headers from the assay file
    assay_col_type = "Parameter Value[Column type]"
    assay_scan_pol = "Parameter Value[Scan polarity]"
    assay_sample_name = "Sample Name"
    maf_column_name = "Metabolite Assignment File"

    try:
        table_df = read_tsv(assay_file_name)
    except FileNotFoundError:
        abort(400, message="The file " + assay_file_name + " was not found")

    for key_val in columns:  # These are the values from the JSON passed
        column_header = key_val["name"]
        cell_value = key_val["value"]

        try:
            if column_header.lower() == "polarity":
                column_index = table_df.columns.get_loc(assay_scan_pol)
            elif column_header.lower() == "column type":
                column_index = table_df.columns.get_loc(assay_col_type)
            else:
                column_index = table_df.columns.get_loc(column_header)

            update_cell(table_df, column_index, cell_value)
        except:
            logger.warning("Could not find %s in the assay file", column_header)

    # Also update the default sample name, this will trigger validation errors
    update_cell(table_df, table_df.columns.get_loc(assay_sample_name), "")

    # Replace the default MAF file_name with the correct one
    if maf_file_name is not None:
        update_cell(table_df, table_df.columns.get_loc(maf_column_name), maf_file_name)

    # Write the updated row back in the file
    message = write_tsv(table_df, assay_file_name)

    return message


def update_cell(table_df, column_index, cell_value):
    try:
        for row_val in range(table_df.shape[0]):
            table_df.iloc[int(0), int(column_index)] = cell_value
    except ValueError:
        abort(
            417,
            message="Unable to find the required 'value', 'row' and 'column' values",
        )


class AssayProcesses(Resource):
    @swagger.operation(
        summary="Get Assay Process Sequence",
        notes="""Get Assay Process Sequence.
                  <br>
                  Use assay filename, process or protocol name to filter results.""",
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
                "name": "assay_filename",
                "description": "Assay filename",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "process_name",
                "description": "Process name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "protocol_name",
                "description": "Protocol name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "list_only",
                "description": "List names only",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True,
            },
            {
                "name": "use_default_values",
                "description": "Provide default values when empty",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": False,
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
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        # query validation
        
        
        assay_filename = None
        
        process_name = None
        
        protocol_name = None
        
        list_only = True
        use_default_values = False
        if request.args:
            
            assay_filename = (
                request.args.get("assay_filename").lower() if request.args.get("assay_filename") else None
            )
            process_name = (
                request.args.get("process_name").lower() if request.args.get("process_name") else None
            )
            protocol_name = (
                request.args.get("protocol_name").lower() if request.args.get("protocol_name") else None
            )
            list_only = True if request.args.get("list_only").lower() == "true" else False
            use_default_values = (
                True if request.args.get("use_default_values").lower() == "true" else False
            )

        logger.info("Getting Processes for Assay %s in %s", assay_filename, study_id)
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
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=False, study_location=study_location
        )

        assay_list = list()
        warns = []
        if not assay_filename:
            assay_list = isa_study.assays
            warns.append(
                {
                    "message": "No Assay filename provided, so merging ProcessSequence for all assays."
                }
            )
        else:
            assay = get_assay(isa_study.assays, assay_filename)
            if assay:
                assay_list.append(assay)
        if not assay_list:
            abort(404)

        found = list()
        for assay in assay_list:
            process_list = assay.process_sequence
            if not process_name and not protocol_name:
                found = process_list
            else:
                for index, proto in enumerate(process_list):
                    if (
                        proto.name.lower() == process_name
                        or proto.executes_protocol.name.lower() == protocol_name
                    ):
                        found.append(proto)
            if not found:
                abort(404)
            logger.info("Found %d protocols", len(assay_list))

            # use default values
            if use_default_values:
                set_default_proc_name(process_list, warns)

                proc_list = get_first_process(process_list)
                set_default_output(assay, proc_list, warns)

        sch = ProcessSchema(many=True)
        if list_only:
            sch = ProcessSchema(
                only=(
                    "name",
                    "executes_protocol.name",
                    "prev_process.executes_protocol.name",
                    "next_process.executes_protocol.name",
                ),
                many=True,
            )
        return extended_response(
            data={"processSequence": sch.dump(found).data}, warns=warns
        )


def set_default_output(isa_assay, proc_list, warns):
    for i, proc in enumerate(proc_list):
        # check Extraction outputs
        if proc.executes_protocol.name == "Extraction":
            if not proc.outputs:
                # take inputs from next process
                if proc.next_process.inputs:
                    proc.outputs = proc.next_process.inputs
                    warns.append(
                        {
                            "message": "Using "
                            + (
                                proc.next_process.name
                                if proc.next_process.name
                                else proc.next_process.executes_protocol.name
                            )
                            + " inputs"
                            + " as outputs for "
                            + proc.name
                        }
                    )
                # create from self inputs
                elif proc.inputs:
                    # create output
                    for input in proc.inputs:
                        if isinstance(input, Sample):
                            extract = Extract(
                                name=input.name + "_" + "Extract",
                                comments=[
                                    {
                                        "name": "Inferred",
                                        "value": "Value was missing in ISA-Tab, "
                                        "so building from Sample name.",
                                    }
                                ],
                            )
                            proc.outputs.append(extract)
                            isa_assay.other_material.append(extract)
                            warns.append(
                                {"message": "Created new Extract " + extract.name}
                            )


def set_default_proc_name(obj_list, warns):
    for i, proc in enumerate(obj_list):
        if not proc.name:
            proc.name = "Process" + "_" + proc.executes_protocol.name
            warns.append({"message": "Added name to Process " + proc.name})


def get_first_process(proc_list):
    procs = list()
    for i, proc in enumerate(proc_list):
        if not proc.prev_process:
            procs.append(proc)
    return procs


class AssaySamples(Resource):
    @swagger.operation(
        summary="Get Assay Samples",
        notes="""Get Assay Samples.
                  <br>
                  Use assay filename or sample name to filter results.""",
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
                "name": "assay_filename",
                "description": "Assay filename",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "name",
                "description": "Assay Sample name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "list_only",
                "description": "List names only",
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
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        # query validation
        
        
        assay_filename = None
        
        sample_name = None
        
        list_only = True
        if request.args:
            
            assay_filename = (
                request.args.get("assay_filename").lower() if request.args.get("assay_filename") else None
            )
            sample_name = request.args.get("name").lower() if request.args.get("name") else None
            list_only = True if request.args.get("list_only").lower() == "true" else False

        logger.info("Getting Samples for Assay %s in %s", assay_filename, study_id)
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
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=False, study_location=study_location
        )

        assay_list = list()
        warns = []
        if not assay_filename:
            assay_list = isa_study.assays
            warns.append(
                {
                    "message": "No Assay filename provided, so merging Samples for all assays."
                }
            )
        else:
            assay = get_assay(isa_study.assays, assay_filename)
            if assay:
                assay_list.append(assay)
        if not assay_list:
            abort(404)

        found = list()
        for assay in assay_list:
            obj_list = assay.samples
            if not sample_name:
                found = obj_list
            else:
                for index, obj in enumerate(obj_list):
                    if obj.name.lower() == sample_name:
                        found.append(obj)
            if not found:
                abort(404)
            logger.info("Found %d Materials", len(assay_list))

        sch = SampleSchema(many=True)
        if list_only:
            sch = SampleSchema(only=("name",), many=True)
        return extended_response(data={"samples": sch.dump(found).data}, warns=warns)

    @swagger.operation(
        summary="Update Assay Samples",
        notes="""Update a list of Assay Samples. Only existing Samples will be updated, unknown will be ignored. 
        To change name, only one sample can be processed at a time.""",
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
                "name": "assay_filename",
                "description": "Assay filename",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "name",
                "description": "Assay Sample name. Leave empty if updating more than one sample.",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
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
                "name": "samples",
                "description": "Assay Sample list in ISA-JSON format.",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "list_only",
                "description": "List names only",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True,
            },
            {
                "name": "save_audit_copy",
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
        ],
    )
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        # query validation
        
        
        assay_filename = None
        
        sample_name = None
        
        list_only = True
        if request.args:
            
            assay_filename = (
                request.args.get("assay_filename").lower() if request.args.get("assay_filename") else None
            )
            sample_name = request.args.get("name").lower() if request.args.get("name") else None
            list_only = True if request.args.get("list_only").lower() == "true" else False
        if not assay_filename:
            logger.warning("Missing Assay filename.")
            abort(400)

        # header content validation
        save_audit_copy = False
        save_msg_str = "NOT be"
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"

        # body content validation
        sample_list = list()
        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            data = data_dict["samples"]
            # if partial=True missing fields will be ignored
            result = SampleSchema().load(data, many=True, partial=False)
            sample_list = result.data
            if len(sample_list) == 0:
                logger.warning("No valid data provided.")
                abort(400)
        except Exception as err:
            logger.warning("Bad format JSON request." + str(err))
            abort(400, message=str(err))

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

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=False, study_location=study_location
        )

        logger.info("Updating Samples for Assay %s in %s", assay_filename, study_id)
        assay = get_assay(isa_study.assays, assay_filename)
        if not assay:
            abort(404)

        logger.info(
            "Updating Study Samples details for %s in %s,", assay_filename, study_id
        )
        updated_samples = list()
        if sample_name:
            if len(sample_list) > 1:
                logger.warning("Requesting name update for more than one sample")
                abort(400, message="Requesting name update for more than one sample")
            sample = sample_list[0]
            if self.update_sample(isa_study, sample_name, sample):
                updated_samples.append(sample)
        else:
            for i, sample in enumerate(sample_list):
                if self.update_sample(isa_study, sample.name.lower(), sample):
                    updated_samples.append(sample)

        # check if all samples were updated
        warns = ""
        if len(updated_samples) != len(sample_list):
            warns = "Some of the samples were not updated. " "Updated " + str(
                len(updated_samples)
            ) + " out of " + str(len(sample_list))
            logger.warning(warns)

        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(
            isa_inv,
            user_token,
            std_path,
            save_investigation_copy=save_audit_copy,
            save_samples_copy=save_audit_copy,
            save_assays_copy=save_audit_copy,
        )

        sch = SampleSchema(many=True)
        if list_only:
            sch = SampleSchema(only=("name",), many=True)
        return extended_response(
            data={"samples": sch.dump(updated_samples).data}, warns=warns
        )

    def update_sample(self, isa_study, sample_name, new_sample):
        for i, sample in enumerate(isa_study.samples):
            if sample.name.lower() == sample_name:
                isa_study.samples[i].name = new_sample.name
                isa_study.samples[i].characteristics = new_sample.characteristics
                isa_study.samples[i].derives_from = new_sample.derives_from
                isa_study.samples[i].factor_values = new_sample.factor_values
                isa_study.samples[i].comments = new_sample.comments

        for i, process in enumerate(isa_study.process_sequence):
            for ii, sample in enumerate(process.outputs):
                if isinstance(sample, Sample) and sample.name.lower() == sample_name:
                    isa_study.process_sequence[i].outputs[ii].name = new_sample.name
                    isa_study.process_sequence[i].outputs[
                        ii
                    ].characteristics = new_sample.characteristics
                    isa_study.process_sequence[i].outputs[
                        ii
                    ].factor_values = new_sample.factor_values
                    isa_study.process_sequence[i].outputs[
                        ii
                    ].derives_from = new_sample.derives_from
                    isa_study.process_sequence[i].outputs[
                        ii
                    ].comments = new_sample.comments

        for isa_assay in isa_study.assays:
            for i, sample in enumerate(isa_assay.samples):
                if sample.name.lower() == sample_name:
                    isa_assay.samples[i].name = new_sample.name
                    isa_assay.samples[i].characteristics = new_sample.characteristics
                    isa_assay.samples[i].derives_from = new_sample.derives_from
                    isa_assay.samples[i].factor_values = new_sample.factor_values
                    isa_assay.samples[i].comments = new_sample.comments

        for i, process in enumerate(isa_assay.process_sequence):
            for ii, sample in enumerate(process.inputs):
                if isinstance(sample, Sample) and sample.name.lower() == sample_name:
                    isa_assay.process_sequence[i].inputs[ii].name = new_sample.name
                    isa_assay.process_sequence[i].inputs[
                        ii
                    ].characteristics = new_sample.characteristics
                    isa_assay.process_sequence[i].inputs[
                        ii
                    ].factor_values = new_sample.factor_values
                    isa_assay.process_sequence[i].inputs[
                        ii
                    ].derives_from = new_sample.derives_from
                    isa_assay.process_sequence[i].inputs[
                        ii
                    ].comments = new_sample.comments

                    logger.info("Updated sample: %s", new_sample.name)
                    return True
        return False


class AssayOtherMaterials(Resource):
    @swagger.operation(
        summary="Get Assay Other Materials",
        notes="""Get Assay Other Materials.
                  <br>
                  Use assay filename or material name to filter results.""",
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
                "name": "assay_filename",
                "description": "Assay filename",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "name",
                "description": "Assay Material name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "list_only",
                "description": "List names only",
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
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        # query validation
        
        
        assay_filename = None
        
        obj_name = None
        
        list_only = True
        if request.args:
            
            assay_filename = (
                request.args.get("assay_filename").lower() if request.args.get("assay_filename") else None
            )
            obj_name = request.args.get("name").lower() if request.args.get("name") else None
            list_only = True if request.args.get("list_only").lower() == "true" else False

        logger.info(
            "Getting Other Materials for Assay %s in %s", assay_filename, study_id
        )
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
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=False, study_location=study_location
        )

        assay_list = list()
        warns = []
        if not assay_filename:
            assay_list = isa_study.assays
            warns.append(
                {
                    "message": "No Assay filename provided, so merging Other Materials for all assays."
                }
            )
        else:
            assay = get_assay(isa_study.assays, assay_filename)
            if assay:
                assay_list.append(assay)
        if not assay_list:
            abort(404)

        found = list()
        for assay in assay_list:
            obj_list = assay.other_material
            if not obj_name:
                found = obj_list
            else:
                for index, obj in enumerate(obj_list):
                    if obj.name.lower() == obj_name:
                        found.append(obj)
            if not found:
                abort(404)
            logger.info("Found %d Materials", len(assay_list))

        sch = OtherMaterialSchema(many=True)
        if list_only:
            sch = OtherMaterialSchema(only=("name",), many=True)
        return extended_response(
            data={"otherMaterials": sch.dump(found).data}, warns=warns
        )


class AssayDataFiles(Resource):
    @swagger.operation(
        summary="Get Assay Data File",
        notes="""Get Assay Data File.
                  <br>
                  Use filename as query parameter for specific searching.""",
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
                "name": "assay_filename",
                "description": "Assay filename",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "data_filename",
                "description": "Data File name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "list_only",
                "description": "List names only",
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
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        # query validation
        
        
        assay_filename = None
        
        data_filename = None
        
        list_only = True
        if request.args:
            
            assay_filename = (
                request.args.get("assay_filename").lower() if request.args.get("assay_filename") else None
            )
            data_filename = (
                request.args.get("data_filename").lower() if request.args.get("data_filename") else None
            )
            list_only = True if request.args.get("list_only").lower() == "true" else False

        logger.info("Getting Data Files for Assay %s in %s", assay_filename, study_id)
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
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=False, study_location=study_location
        )

        assay_list = list()
        warns = []
        if not assay_filename:
            assay_list = isa_study.assays
            warns.append(
                {
                    "message": "No Assay filename provided, so merging Data files for all assays."
                }
            )
        else:
            assay = get_assay(isa_study.assays, assay_filename)
            if assay:
                assay_list.append(assay)
        if not assay_list:
            abort(404)

        found = list()
        for assay in assay_list:
            datafile_list = assay.data_files
            if not data_filename:
                found = datafile_list
            else:
                for index, obj in enumerate(datafile_list):
                    if obj.filename.lower() == data_filename:
                        found.append(obj)
            if not found:
                abort(404)
            logger.info("Found %d data files", len(assay_list))

        sch = DataFileSchema(many=True)
        if list_only:
            sch = DataFileSchema(only=("filename",), many=True)
        return extended_response(data={"dataFiles": sch.dump(found).data}, warns=warns)


class StudySampleTemplate(Resource):
    @swagger.operation(
        summary="Init Sample sheet",
        notes="""Initiate or Override sample sheet for given type from the template""",
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
                "name": "sample_type",
                "description": "Type of Sample",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": [
                    "minimum",
                    "clinical",
                    "in-vitro",
                    "only-in-vitro",
                    "non-clinical-tox",
                    "plant",
                    "isotopologue",
                    "metaspace-imaging",
                    "nmr-imaging",
                    "ms-imaging",
                    "minimum-bsd",
                ],
                "defaultValue": "minimum",
                "default": "minimum",
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
                "name": "force",
                "description": "Force overriding of sample sheet regardless of sample data if set True. Do nothing if data present and value set false",
                "required": False,
                "allowMultiple": False,
                "allowEmptyValue": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["True", "False"],
                "defaultValue": "False",
                "default": "False",
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
    def post(self, study_id):
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        # query validation        sampleType = None
        force_override = False
        if request.args:
            
            sampleType = (
                request.args.get("sample_type").lower() if request.args.get("sample_type") else "minimum"
            )

            if args and "force" in args and request.args.get("force"):
                force_override = True if request.args.get("force").lower() == "true" else False

        logger.info("Init Sample for %s; Type %s", study_id, sampleType)
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
        if not is_curator:
            abort(403)

        filename, protocols, update_status = create_sample_sheet(
            sample_type=sampleType, study_id=study_id, force_override=force_override
        )
        if update_status:
            return {
                "The sample sheet creation status": "Successful",
                "filename": filename,
            }
        else:
            return {"The sample sheet creation status": "Unsuccessful"}


def create_sample_sheet(sample_type, study_id, force_override):
    settings = get_study_settings()
    studies_path = (
        settings.mounted_paths.study_metadata_files_root_path
    )  # Root folder for all studies
    study_path = os.path.join(studies_path, study_id)  # This particular study

    (
        tidy_header_row,
        tidy_data_row,
        protocols,
        sample_desc,
        sample_data_types,
        sample_file_type,
        sample_data_mandatory,
    ) = get_sample_headers_and_data(sample_type)

    sample_file_name = "s_" + study_id.upper() + ".txt"
    sample_file_fullpath = os.path.join(study_path, sample_file_name)
    update_status = False
    try:
        if force_override:
            create_audit_copy_and_write_table(
                sample_file_name=sample_file_name,
                study_id=study_id,
                tidy_header_row=tidy_header_row,
                tidy_data_row=tidy_data_row,
            )
            update_status = update_study_sample_type(
                study_id=study_id, sample_type=sample_type
            )
        else:
            if os.path.exists(sample_file_fullpath):
                sample_df = pd.read_csv(
                    sample_file_fullpath, sep="\t", header=0, encoding="utf-8"
                )
                sample_df = sample_df.replace(np.nan, "", regex=True)  # Remove NaN
                df_data_dict = totuples(sample_df.reset_index(), "rows")
                row_count = len(df_data_dict["rows"])
                if row_count == 1:
                    create_audit_copy_and_write_table(
                        sample_file_name=sample_file_name,
                        study_id=study_id,
                        tidy_header_row=tidy_header_row,
                        tidy_data_row=tidy_data_row,
                    )
                    update_status = update_study_sample_type(
                        study_id=study_id, sample_type=sample_type
                    )
            else:
                create_audit_copy_and_write_table(
                    sample_file_name=sample_file_name,
                    study_id=study_id,
                    tidy_header_row=tidy_header_row,
                    tidy_data_row=tidy_data_row,
                )
                update_status = update_study_sample_type(
                    study_id=study_id, sample_type=sample_type
                )

    except Exception as ex:
        logger.error("Exception while overriding sample sheet - %s", ex)
        abort(500, message="Could not write the Sample file")

    return sample_file_name, protocols, update_status


def create_audit_copy_and_write_table(
    sample_file_name, study_id, tidy_header_row, tidy_data_row
):
    # Create audit copy
    settings = get_study_settings()
    studies_path = (
        settings.mounted_paths.study_metadata_files_root_path
    )  # Root folder for all studies
    study_path = os.path.join(studies_path, study_id)
    sample_file_fullpath = os.path.join(study_path, sample_file_name)
    if os.path.exists(sample_file_fullpath):
        update_path = os.path.join(
            settings.mounted_paths.study_audit_files_root_path,
            study_id,
            settings.audit_folder_name,
        )
        dest_path = new_timestamped_folder(update_path)
        src_file = sample_file_fullpath
        dest_file = os.path.join(dest_path, sample_file_name)
        logger.info("Copying %s to %s", src_file, dest_file)
        copy_file(src_file, dest_file)
    # Write Table data
    file = open(sample_file_fullpath, "w", encoding="utf-8")
    writer = csv.writer(file, delimiter="\t", quotechar='"')
    writer.writerow(tidy_header_row)
    writer.writerow(tidy_data_row)
    file.close()
