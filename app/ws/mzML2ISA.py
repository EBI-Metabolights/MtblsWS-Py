#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Apr-05
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
import logging
import os

from lxml import etree

from flask import request, jsonify
from flask_restful import Resource, abort
from flask_restful_swagger import swagger
from app.config import get_settings

from app.utils import metabolights_exception_handler
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.settings.utils import get_study_settings
from app.ws.study.user_service import UserService
from app.ws.utils import convert_to_isa, validate_mzml_files

logger = logging.getLogger("wslog")
wsc = WsClient()
iac = IsaApiClient()


class Convert2ISAtab(Resource):
    @swagger.operation(
        summary="Convert mzML files into ISA-Tab",
        notes="""</P><B>Be aware that any ISA-Tab files will be overwritten for this study</P>
        This process can run for a long while, please be patient</B>""",
        parameters=[
            {
                "name": "study_id",
                "description": "Existing Study Identifier for generating new ISA-Tab files",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user_token",
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
            {"code": 417, "message": "Unexpected result."},
        ],
    )
    def post(self, study_id):
        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None or study_id is None:
            abort(401)

        study_id = study_id.upper()

        # param validation
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

        status, message = convert_to_isa(study_location, study_id)

        if not status:
            abort(417, message=message)
        return message


class ValidateMzML(Resource):
    def __init__(self):
        self.xmlschema_map = {}

    @swagger.operation(
        summary="Validate mzML files",
        notes="""Validating mzML file structure. 
        This method will validate mzML files in both the study folder.
        Validated files in the study upload location will be moved to the study location""",
        parameters=[
            {
                "name": "study_id",
                "description": "Existing Study Identifier with mzML files to validate",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user_token",
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
    def post(self, study_id):
        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None or study_id is None:
            abort(401)

        study_id = study_id.upper()

        # param validation
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

        return self.validate_mzml_files(study_id)

    @swagger.operation(
        summary="Validate mzML files and report results",
        notes="""Searching and validating all mzML file recursively in the study folder.""",
        parameters=[
            {
                "name": "study_id",
                "description": "Existing Study Identifier with mzML files to validate",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user_token",
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
    def get(self, study_id):
        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None or study_id is None:
            abort(401)

        study_id = study_id.upper()

        UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        return self.validate_mzml_files(study_id)

    def validate_mzml_files(self, study_id):
        settings = get_settings()
        studies_folder = (
            settings.study.mounted_paths.study_readonly_files_actual_root_path
        )
        study_folder = os.path.join(studies_folder, study_id)
        xsd_path = settings.file_resources.mzml_xsd_schema_file_path
        xmlschema_doc = etree.parse(xsd_path)
        xmlschema = etree.XMLSchema(xmlschema_doc)

        files = glob.glob(os.path.join(study_folder, "*.mzML"))
        files_in_subfolders = glob.glob(
            os.path.join(study_folder, "**/*.mzML"), recursive=True
        )
        files.extend(files_in_subfolders)

        files.sort()
        error_list = []
        mzml_file_count = 0

        for file in files:
            mzml_file_count += 1
            relative_file_path = file.replace(study_folder + os.path.sep, "")

            is_valid, result, err = self.validate_xml(
                xml_file=file, xmlschema=xmlschema
            )

            if not is_valid:
                item = {
                    "file": relative_file_path,
                    "message": result,
                    "error": str(err),
                }
                error_list.append(item)

        return jsonify(
            {
                "total_mzml_file_count": mzml_file_count,
                "invalid_mzml_file_count": len(error_list),
                "errors": error_list,
            }
        )

    def validate_xml(self, xml_file=None, xmlschema=None):
        # parse xml
        try:
            doc = etree.parse(xml_file)
            if not xmlschema:
                # try to find schema
                root = doc.getroot()
                if root:
                    schema_key = (
                        "{http://www.w3.org/2001/XMLSchema-instance}schemaLocation"
                    )
                    if schema_key in root.attrib:
                        schema_value = root.attrib[schema_key]
                        parsed_schema_value = schema_value.split(" ")
                        location = parsed_schema_value[1]

                        if (
                            location in self.xmlschema_map
                            and self.xmlschema_map[location]
                        ):
                            xmlschema = self.xmlschema_map[location]
                        else:
                            xmlschema_doc = etree.parse(location)
                            xmlschema = etree.XMLSchema(xmlschema_doc)
                            self.xmlschema_map[location] = xmlschema

        except IOError as e:
            return False, {"Error": "Can not read the file "}, e
        except etree.XMLSyntaxError as e:
            return False, {"Invalid": "XML Syntax error "}, e
        except Exception as e:
            return False, {"Error": "Unexpected error"}, e
        try:
            if not xmlschema:
                return False, "Schema is not defined", ""
            xmlschema.assertValid(doc)
            return True, f" Valid XML file'", None
        except etree.DocumentInvalid as e:
            return False, "Schema validation is failed", e
