#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-May-08
#  Modified by:   Jiakang
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
import re
import types
import urllib.parse
from urllib.request import urlopen

import gspread
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from flask import jsonify, request
from flask_restful import Resource, abort
from flask_restful_swagger import swagger
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from owlready2 import get_ontology

from app.config import get_settings
from app.config.utils import get_host_internal_url
from app.study_folder_utils import convert_relative_to_real_path
from app.utils import current_time
from app.ws.auth.permissions import (
    public_endpoint,
    raise_deprecation_error,
    validate_user_has_curator_role,
)
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.ontology_info import (
    get_ontology_name,
    get_ontology_search_result,
    getOnto_info,
)
from app.ws.study_templates.models import ValidationConfiguration
from app.ws.study_templates.utils import get_validation_configuration
from app.ws.utils import log_request

logger = logging.getLogger("wslog")
iac = IsaApiClient()
wsc = WsClient()


def parse_input(value):
    if not value:
        return ""
    return urllib.parse.unquote(value.strip())


def parse_set_str(input_data: str, lowercase: bool = False):
    result = None
    if input_data:
        input_list = input_data.strip().strip("{").strip("}").split(",")
        if lowercase:
            result = [x.strip().lower() for x in input_list if x.strip()]
        else:
            result = [x.strip() for x in input_list if x.strip()]

    return result if result else None


class MtblsControlLists(Resource):
    @swagger.operation(
        summary="Get Metabolights default control lists for ISA metadata files.",
        notes="Get Metabolights default control lists for ISA metadata files.",
        parameters=[
            {
                "name": "name",
                "description": "Ontology query term",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            }
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
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def get(self):
        log_request(request)
        public_endpoint(request)

        configuration: ValidationConfiguration = get_validation_configuration()
        return jsonify({"controlLists": configuration.model_dump(by_alias=True)})


class Ontology(Resource):
    @swagger.operation(
        summary="Get ontology onto_information.",
        notes="Get ontology onto_information.",
        parameters=[
            {
                "name": "term",
                "description": "Ontology query term",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "branch",
                "description": "starting branch of ontology",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": [
                    "factor",
                    "role",
                    "taxonomy",
                    "organism part",
                    "characteristic",
                    "publication",
                    "design descriptor",
                    "unit",
                    "column type",
                    "instruments",
                    "confidence",
                    "sample type",
                ],
            },
            {
                "name": "mapping",
                "description": "taxonomy search approach",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["typo", "exact", "fuzzy"],
            },
            {
                "name": "queryFields",
                "description": "Specify the fields to search: {MTBLS, OLS, MTBLS_Zooma, Zooma, Bioportal}, "
                "if None (default), search will go one after another until get the result",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "ontology",
                "description": "Restrict a search to a set of ontologies",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
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
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def get(self):
        log_request(request)
        raise_deprecation_error(request)
        public_endpoint(request)
        term = parse_input(request.args.get("term"))
        branch = parse_input(request.args.get("branch"))
        mapping = parse_input(request.args.get("mapping"))
        queryFields = parse_set_str(parse_input(request.args.get("queryFields")))
        ontologies = parse_set_str(
            parse_input(request.args.get("ontology")), lowercase=False
        )
        if (
            not term.startswith("http://")
            and not term.startswith("https://")
            and ":" in term
        ):
            splitted_term = term.split(":")
            listed_ontologies = parse_set_str(splitted_term[0], lowercase=False)
            if not ontologies:
                ontologies = listed_ontologies
            else:
                ontologies.extend(listed_ontologies)

            term = splitted_term[1]
        if ontologies:
            ontologies = ",".join(set(ontologies))
        result = get_ontology_search_result(
            term, branch, ontologies, mapping, queryFields
        )
        return jsonify(result)

    @swagger.operation(
        summary="Add new entity to metabolights ontology",
        notes="""Add new entity to metabolights ontology.
              <br>
              <pre><code>
{
  "ontologyEntity": {
    "termName": "ABCC5",
    "definition": "The protein-coding gene ABCC5 located on the chromosome 3 mapped at 3q27.",
    "superclass": "design descriptor"
  }
}</code></pre>""",
        parameters=[
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "data",
                "description": "Ontology Entity in JSON format.",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
    def put(self):
        log_request(request)
        raise_deprecation_error(request)
        validate_user_has_curator_role(request)
        data_dict = None
        try:
            data_dict = json.loads(request.data.decode("utf-8"))["ontologyEntity"]
        except Exception as e:
            logger.info(e)
            abort(400)

        logger.info("Add %s to Metabolights ontology" % data_dict["termName"])
        # print('Add %s to Metabolights ontology' % data_dict['termName'])

        description = None
        if len(data_dict["definition"]) > 0:
            description = data_dict["definition"]
        try:
            addEntity(
                new_term=data_dict["termName"],
                supclass=data_dict["superclass"],
                definition=description,
            )
        except Exception as e:
            # print(e)
            logger.info(e)
            abort(400)


class Placeholder(Resource):
    @swagger.operation(
        summary="Get placeholder terms from study files",
        notes="Get placeholder terms",
        parameters=[
            {
                "name": "query",
                "description": "Data field to extract from study",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["factor", "design descriptor", "organism"],
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def get(self):
        log_request(request)
        raise_deprecation_error(request)
        public_endpoint(request)

        query = ""

        if request.args:
            query = request.args.get("query")
            if query is None:
                abort(400)
            if query:
                query = query.strip().lower()

        url = get_settings().google.sheets.zooma_sheet
        sheet_name = ""
        col = []

        if query == "factor":
            sheet_name = "factor"

            col = [
                "operation(Update/Add/Delete/Zooma/MTBLS)",
                "status (Done/Error)",
                "studyID",
                "old_name",
                "name",
                "annotationValue",
                "termAccession",
                "superclass",
                "definition",
            ]

        elif query == "design descriptor":
            sheet_name = "design descriptor"

            col = [
                "operation(Update/Add/Delete/Zooma/MTBLS)",
                "status (Done/Error)",
                "studyID",
                "old_name",
                "name",
                "matched_iri",
                "superclass",
                "definition",
            ]
        elif query == "organism":
            sheet_name = "organism"

            col = [
                "operation(Update/Add/Delete/Zooma/MTBLS)",
                "status (Done/Error)",
                "studyID",
                "old_organism",
                "organism",
                "organism_ref",
                "organism_url",
                "old_organismPart",
                "organismPart",
                "organismPart_ref",
                "organismPart_url",
                "superclass",
                "definition",
            ]
        else:
            abort(400)

        try:
            google_df = getGoogleSheet(url, sheet_name)

        except Exception as e:
            google_df = pd.DataFrame(columns=col)
            # print(e.args)
            logger.info("Fail to load spreadsheet from Google")
            logger.info(e.args)

        df = pd.DataFrame(get_metainfo(query))
        df_connect = pd.concat([google_df, df], ignore_index=True, sort=False)
        if query in ["factor", "design descriptor"]:
            df_connect = (
                df_connect.reindex(columns=col)
                .replace(np.nan, "", regex=True)
                .drop_duplicates(keep="first", subset=["studyID", "old_name"])
            )

        if query == "organism":
            df_connect = df_connect.replace("", np.nan, regex=True)
            df_connect = df_connect.dropna(
                subset=["old_organism", "old_organismPart"], thresh=1
            )
            df_connect = (
                df_connect.reindex(columns=col)
                .replace(np.nan, "", regex=True)
                .drop_duplicates(
                    keep="first", subset=["studyID", "old_organism", "old_organismPart"]
                )
            )

        adding_count = df_connect.shape[0] - google_df.shape[0]

        # Ranking the row according to studyIDs
        def extractNum(s):
            num = re.findall(r"\d+", s)[0]
            return int(num)

        df_connect["num"] = df_connect["studyID"].apply(extractNum)
        df_connect = df_connect.sort_values(by=["num"])
        df_connect = df_connect.drop("num", axis=1)

        replaceGoogleSheet(df_connect, url, sheet_name)
        return jsonify({"success": True, "add": adding_count})

    # ============================ Placeholder put ===============================
    @swagger.operation(
        summary="Make changes according to google old_term sheets",
        notes="Update/add/Delete placeholder terms",
        parameters=[
            {
                "name": "query",
                "description": "Data field to change",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["factor", "design descriptor", "organism"],
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def put(self):
        log_request(request)
        raise_deprecation_error(request)
        public_endpoint(request)
        query = ""

        if request.args:
            query = request.args.get("query")
            if query is None:
                abort(400)
            if query:
                query = query.strip().lower()

        google_url = get_settings().google.sheets.zooma_sheet
        sheet_name = ""
        col = []

        # get sheet_name
        if query == "factor":
            sheet_name = "factor"
            col = [
                "operation(Update/Add/Delete/Zooma/MTBLS)",
                "status (Done/Error)",
                "studyID",
                "old_name",
                "name",
                "annotationValue",
                "termAccession",
                "superclass",
                "definition",
            ]

        elif query == "design descriptor":
            sheet_name = "design descriptor"
            col = [
                "operation(Update/Add/Delete/Zooma/MTBLS)",
                "status (Done/Error)",
                "studyID",
                "old_name",
                "name",
                "matched_iri",
                "superclass",
                "definition",
            ]

        elif query == "organism":
            sheet_name = "organism"
            col = [
                "operation(Update/Add/Delete/Zooma/MTBLS)",
                "status (Done/Error)",
                "studyID",
                "old_organism",
                "organism",
                "organism_ref",
                "organism_url",
                "old_organismPart",
                "organismPart",
                "organismPart_ref",
                "organismPart_url",
                "superclass",
                "definition",
            ]

        else:
            abort(400)

        # Load google sheet
        google_df = getGoogleSheet(google_url, sheet_name)

        ch = google_df[(google_df[col[0]] != "") & (google_df[col[1]] == "")]

        for index, row in ch.iterrows():
            if query == "factor":
                (
                    operation,
                    studyID,
                    old_term,
                    term,
                    annotationValue,
                    termAccession,
                    superclass,
                    definition,
                ) = (
                    row["operation(Update/Add/Delete/Zooma/MTBLS)"],
                    row["studyID"],
                    row["old_name"],
                    row["name"],
                    row["annotationValue"],
                    row["termAccession"],
                    row["superclass"],
                    row["definition"],
                )
                context_path = get_settings().server.service.resources_path
                source = "{context_path}/studies/{study_id}/factors".format(
                    context_path=context_path, study_id=studyID
                )
                ws_url = (
                    get_settings().server.service.mtbls_ws_host
                    + ":"
                    + str(get_settings().server.service.rest_api_port)
                    + source
                )

                if operation.lower() in ["update", "u", "add", "A"]:
                    # ws_url = 'https://www.ebi.ac.uk/{context_path}/studies/{study_id}/factors'.format(context_path=context_path, study_id=studyID)
                    protocol = """
                                    {
                                        "factorName": "",
                                        "factorType": {
                                          "annotationValue": "",
                                          "termSource": {
                                            "name": "",
                                            "file": "",
                                            "version": "",
                                            "description": ""
                                          },
                                          "termAccession": ""
                                        }
                                    }
                                """
                    try:
                        onto_name = get_ontology_name(termAccession)[0]
                        onto_iri, onto_version, onto_description = getOnto_info(
                            onto_name
                        )

                        temp = json.loads(protocol)
                        temp["factorName"] = term
                        temp["factorType"]["annotationValue"] = annotationValue
                        temp["factorType"]["termSource"]["name"] = onto_name
                        temp["factorType"]["termSource"]["file"] = onto_iri
                        temp["factorType"]["termSource"]["version"] = onto_version
                        temp["factorType"]["termSource"]["description"] = (
                            onto_description
                        )
                        temp["factorType"]["termAccession"] = termAccession

                        data = json.dumps({"factor": temp})

                        if operation.lower() in ["update", "u"]:  # Update factor
                            response = requests.put(
                                ws_url,
                                params={"name": old_term},
                                headers={
                                    "user_token": get_settings().auth.service_account.api_token,
                                    "save_audit_copy": "true",
                                },
                                data=data,
                            )
                            logger.info(
                                "Made correction from {old_term} to {matchterm}({matchiri}) in {studyID}".format(
                                    old_term=old_term,
                                    matchterm=annotationValue,
                                    matchiri=termAccession,
                                    studyID=studyID,
                                )
                            )

                        else:  # Add factor
                            response = requests.post(
                                ws_url,
                                headers={
                                    "user_token": get_settings().auth.service_account.api_token,
                                    "save_audit_copy": "true",
                                },
                                data=data,
                            )

                            logger.info(
                                "Add {old_term} ({matchiri}) in {studyID}".format(
                                    old_term=old_term,
                                    matchiri=termAccession,
                                    studyID=studyID,
                                )
                            )
                        if response.status_code == 200:
                            google_df.loc[index, "status (Done/Error)"] = "Done"
                        else:
                            google_df.loc[index, "status (Done/Error)"] = "Error"

                        replaceGoogleSheet(google_df, google_url, sheet_name)
                    except Exception as e:
                        google_df.loc[index, "status (Done/Error)"] = "Error"
                        logger.info(e)

                # Delete factor
                elif operation.lower() in ["delete", "D"]:
                    try:
                        response = requests.delete(
                            ws_url,
                            params={"name": old_term},
                            headers={
                                "user_token": get_settings().auth.service_account.api_token,
                                "save_audit_copy": "true",
                            },
                        )

                        logger.info(
                            "delete {old_term} from {studyID}".format(
                                old_term=old_term, studyID=studyID
                            )
                        )

                        if response.status_code == 200:
                            google_df.loc[index, "status (Done/Error)"] = "Done"
                        else:
                            google_df.loc[index, "status (Done/Error)"] = "Error"

                        replaceGoogleSheet(google_df, google_url, sheet_name)

                    except Exception as e:
                        google_df.loc[index, "status (Done/Error)"] = "Error"
                        logger.info(e)

                # add factor term to MTBLS ontology
                elif operation.lower() == "mtbls":
                    try:
                        context_path = get_settings().server.service.resources_path
                        source = f"{context_path}/ebi-internal/ontology"
                        protocol = """{
                                        "termName": " ",
                                        "definition": " ",
                                        "superclass": " "
                                      }
                                   """

                        temp = json.loads(protocol)
                        temp["termName"] = annotationValue
                        temp["definition"] = definition
                        temp["superclass"] = superclass

                        data = json.dumps({"ontologyEntity": temp})
                        ws_url = (
                            get_settings().server.service.mtbls_ws_host
                            + ":"
                            + str(get_settings().server.service.rest_api_port)
                            + source
                        )

                        response = requests.put(
                            ws_url,
                            headers={
                                "user_token": get_settings().auth.service_account.api_token
                            },
                            data=data,
                        )
                        logger.info(
                            "add term {newterm} to {superclass} branch".format(
                                newterm=annotationValue, superclass=superclass
                            )
                        )
                        if response.status_code == 200:
                            google_df.loc[index, "status (Done/Error)"] = "Done"
                        else:
                            google_df.loc[index, "status (Done/Error)"] = "Error"

                        replaceGoogleSheet(google_df, google_url, sheet_name)

                    except Exception as e:
                        google_df.loc[index, "status (Done/Error)"] = "Error"
                        logger.info(e)

                # add factor term to zooma
                elif operation.lower() == "zooma":
                    try:
                        addZoomaTerm(
                            studyID,
                            Property_type="Factor",
                            Property_value=term,
                            url=termAccession,
                        )
                        result = "Done"
                    except Exception as e:
                        result = "Error"
                        google_df.loc[index, "status (Done/Error)"] = "Error"
                        logger.info(e)

                    google_df.loc[index, "status (Done/Error)"] = result
                    replaceGoogleSheet(google_df, google_url, sheet_name)

                else:
                    logger.info("Wrong operation tag in the spreadsheet")
                    abort(400)

            elif query == "design descriptor":
                (
                    operation,
                    studyID,
                    old_term,
                    term,
                    matched_iri,
                    superclass,
                    definition,
                ) = (
                    row["operation(Update/Add/Delete/Zooma/MTBLS)"],
                    row["studyID"],
                    row["old_name"],
                    row["name"],
                    row["matched_iri"],
                    row["superclass"],
                    row["definition"],
                )

                context_path = get_settings().server.service.resources_path
                source = "{context_path}/studies/{study_id}/descriptors".format(
                    context_path=context_path, study_id=studyID
                )
                ws_url = (
                    get_settings().server.service.mtbls_ws_host
                    + ":"
                    + str(get_settings().server.service.rest_api_port)
                    + source
                )

                # add / update descriptor
                if operation.lower() in ["update", "U", "add", "A"]:
                    protocol = """
                                    {
                                        "annotationValue": " ",
                                        "termSource": {
                                            "name": " ",
                                            "file": " ",
                                            "version": " ",
                                            "description": " "
                                        },
                                        "termAccession": " "
                                    }
                              """
                    try:
                        onto_name = get_ontology_name(matched_iri)[0]
                        onto_iri, onto_version, onto_description = getOnto_info(
                            onto_name
                        )

                        temp = json.loads(protocol)
                        temp["annotationValue"] = term
                        temp["termSource"]["name"] = onto_name
                        temp["termSource"]["file"] = onto_iri
                        temp["termSource"]["version"] = onto_version
                        temp["termSource"]["description"] = onto_description
                        temp["termAccession"] = matched_iri

                        data = json.dumps({"studyDesignDescriptor": temp})

                        if operation.lower() in ["update", "U"]:  # Update descriptor
                            response = requests.put(
                                ws_url,
                                params={"term": old_term},
                                headers={
                                    "user_token": get_settings().auth.service_account.api_token,
                                    "save_audit_copy": "true",
                                },
                                data=data,
                            )
                            logger.info(
                                "Made correction from {old_term} to {matchterm}({matchiri}) in {studyID}".format(
                                    old_term=old_term,
                                    matchterm=old_term,
                                    matchiri=matched_iri,
                                    studyID=studyID,
                                )
                            )
                        else:  # Add descriptor
                            response = requests.post(
                                ws_url,
                                headers={
                                    "user_token": get_settings().auth.service_account.api_token,
                                    "save_audit_copy": "true",
                                },
                                data=data,
                            )
                            logger.info(
                                "Add {old_term} to ({matchiri}) in {studyID}".format(
                                    old_term=old_term,
                                    matchiri=matched_iri,
                                    studyID=studyID,
                                )
                            )

                        if response.status_code == 200:
                            google_df.loc[index, "status (Done/Error)"] = "Done"
                        else:
                            google_df.loc[index, "status (Done/Error)"] = "Error"

                        replaceGoogleSheet(google_df, google_url, sheet_name)

                    except Exception as e:
                        google_df.loc[index, "status (Done/Error)"] = "Error"
                        logger.info(e)

                # Delete descriptor
                elif operation.lower() in ["delete", "D"]:
                    try:
                        response = requests.delete(
                            ws_url,
                            params={"term": old_term},
                            headers={
                                "user_token": get_settings().auth.service_account.api_token,
                                "save_audit_copy": "true",
                            },
                        )
                        logger.info(
                            "delete {old_term} from in {studyID}".format(
                                old_term=old_term, studyID=studyID
                            )
                        )

                        if response.status_code == 200:
                            google_df.loc[index, "status (Done/Error)"] = "Done"
                        else:
                            google_df.loc[index, "status (Done/Error)"] = "Error"

                        replaceGoogleSheet(google_df, google_url, sheet_name)

                    except Exception as e:
                        google_df.loc[index, "status (Done/Error)"] = "Error"
                        logger.info(e)

                # add descriptor to MTBLS ontology
                elif operation.lower() == "mtbls":
                    try:
                        context_path = get_settings().server.service.resources_path
                        source = f"{context_path}/ebi-internal/ontology"
                        protocol = """{
                                        "termName": " ",
                                        "definition": " ",
                                        "superclass": " "
                                      }
                                   """

                        temp = json.loads(protocol)
                        temp["termName"] = term
                        temp["definition"] = definition
                        temp["superclass"] = superclass

                        data = json.dumps({"ontologyEntity": temp})
                        ws_url = (
                            get_settings().server.service.mtbls_ws_host
                            + ":"
                            + str(get_settings().server.service.rest_api_port)
                            + source
                        )

                        response = requests.put(
                            ws_url,
                            headers={
                                "user_token": get_settings().auth.service_account.api_token
                            },
                            data=data,
                        )
                        logger.info(
                            "add term {newterm} to {superclass} branch".format(
                                newterm=term, superclass=superclass
                            )
                        )
                        if response.status_code == 200:
                            google_df.loc[index, "status (Done/Error)"] = "Done"
                        else:
                            google_df.loc[index, "status (Done/Error)"] = "Error"

                        replaceGoogleSheet(google_df, google_url, sheet_name)

                    except Exception as e:
                        google_df.loc[index, "status (Done/Error)"] = "Error"
                        logger.info(e)

                # add descriptor term to zooma
                elif operation.lower() == "zooma":
                    try:
                        addZoomaTerm(studyID, "Design Descriptor", term, matched_iri)
                        result = "Done"
                    except Exception as e:
                        result = "Error"
                        google_df.loc[index, "status (Done/Error)"] = "Error"
                        logger.info(e)

                    google_df.loc[index, "status (Done/Error)"] = result
                    replaceGoogleSheet(google_df, google_url, sheet_name)

                else:
                    logger.info("Wrong operation tag in the spreadsheet")
                    abort(400)

            elif query == "organism":
                operation, studyID = row[col[0]], row[col[2]]
                old_organism, organism, organism_ref, organism_url = (
                    row["old_organism"],
                    row["organism"],
                    row["organism_ref"],
                    row["organism_url"],
                )
                old_organismPart, organismPart, organismPart_ref, organismPart_url = (
                    row["old_organismPart"],
                    row["organismPart"],
                    row["organismPart_ref"],
                    row["organismPart_url"],
                )
                superclass, definition = row["superclass"], row["definition"]

                context_path = get_settings().server.service.resources_path
                source = "{context_path}/studies/{study_id}/organisms".format(
                    context_path=context_path, study_id=studyID
                )
                ws_url = (
                    get_settings().server.service.mtbls_ws_host
                    + ":"
                    + str(get_settings().server.service.rest_api_port)
                    + source
                )

                list_changes = []

                if organism not in ["", None]:
                    list_changes.append(
                        {
                            "old_term": old_organism,
                            "new_term": organism,
                            "onto_name": organism_ref,
                            "term_url": organism_url,
                            "superclass": superclass,
                            "definition": definition,
                            "characteristicsName": "Organism",
                        }
                    )
                if organismPart not in ["", None]:
                    list_changes.append(
                        {
                            "old_term": old_organismPart,
                            "new_term": organismPart,
                            "onto_name": organismPart_ref,
                            "term_url": organismPart_url,
                            "superclass": superclass,
                            "definition": definition,
                            "characteristicsName": "Organism part",
                        }
                    )
                # Update organism
                if operation.lower() in ["update", "U"]:
                    for change in list_changes:
                        protocol = """
                                    {
                                        "comments": [],
                                        "characteristicsName": "",
                                        "characteristicsType": {
                                            "comments": [],
                                            "annotationValue": " ",
                                            "termSource": {
                                                "comments": [],
                                                "name": " ",
                                                "file": " ",
                                                "version": " ",
                                                "description": " "
                                            },
                                            "termAccession": " "
                                        }
                                    }
                                """
                        try:
                            if change["onto_name"] in ["", None]:
                                onto_name = get_ontology_name(change["term_url"])[0]
                            else:
                                onto_name = change["onto_name"]

                            try:
                                onto_iri, onto_version, onto_description = getOnto_info(
                                    change["onto_name"]
                                )
                            except Exception as e:
                                logger.info(e)
                                logger.info(
                                    "Fail to load information about ontology {onto_name}".format(
                                        onto_name=change["onto_name"]
                                    )
                                )
                                onto_iri, onto_version, onto_description = "", "", ""

                            temp = json.loads(protocol)
                            temp["characteristicsName"] = change["characteristicsName"]
                            temp["characteristicsType"]["annotationValue"] = change[
                                "new_term"
                            ]
                            temp["characteristicsType"]["termSource"]["name"] = (
                                onto_name
                            )
                            temp["characteristicsType"]["termSource"]["file"] = onto_iri
                            temp["characteristicsType"]["termSource"]["version"] = (
                                onto_version
                            )
                            temp["characteristicsType"]["termSource"]["description"] = (
                                onto_description
                            )
                            temp["characteristicsType"]["termAccession"] = change[
                                "term_url"
                            ]

                            data = json.dumps({"characteristics": [temp]})

                            response = requests.post(
                                ws_url,
                                params={
                                    "existing_char_name": change["characteristicsName"],
                                    "existing_char_value": change["old_term"],
                                },
                                headers={
                                    "user_token": get_settings().auth.service_account.api_token,
                                    "save_audit_copy": "true",
                                },
                                data=data,
                            )
                            logger.info(
                                "Made correction from {old_term} to {matchterm}({matchiri}) in {studyID}".format(
                                    old_term=change["old_term"],
                                    matchterm=change["new_term"],
                                    matchiri=change["term_url"],
                                    studyID=studyID,
                                )
                            )

                            if response.status_code == 200:
                                google_df.loc[index, "status (Done/Error)"] = "Done"
                            else:
                                google_df.loc[index, "status (Done/Error)"] = "Error"

                            replaceGoogleSheet(google_df, google_url, sheet_name)

                        except Exception as e:
                            google_df.loc[index, "status (Done/Error)"] = "Error"
                            logger.info(e)

                # add organism to MTBLS ontology
                # add organism/organism separately
                elif operation.lower() == "mtbls":
                    for change in list_changes:
                        try:
                            context_path = get_settings().server.service.resources_path
                            source = f"{context_path}/ebi-internal/ontology"
                            protocol = """{
                                            "termName": " ",
                                            "definition": " ",
                                            "superclass": " "
                                          }
                                       """

                            temp = json.loads(protocol)
                            temp["termName"] = change["new_term"]
                            temp["definition"] = change["definition"]
                            temp["superclass"] = change["superclass"]

                            data = json.dumps({"ontologyEntity": temp})
                            ws_url = (
                                get_settings().server.service.mtbls_ws_host
                                + ":"
                                + str(get_settings().server.service.rest_api_port)
                                + source
                            )

                            response = requests.put(
                                ws_url,
                                headers={
                                    "user_token": get_settings().auth.service_account.api_token
                                },
                                data=data,
                            )
                            logger.info(
                                "add term {newterm} to {superclass} branch".format(
                                    newterm=change["new_term"],
                                    superclass=change["superclass"],
                                )
                            )
                            if response.status_code == 200:
                                google_df.loc[index, "status (Done/Error)"] = "Done"
                            else:
                                google_df.loc[index, "status (Done/Error)"] = "Error"

                            replaceGoogleSheet(google_df, google_url, sheet_name)

                        except Exception as e:
                            google_df.loc[index, "status (Done/Error)"] = "Error"
                            logger.info(e)

                # add organism term to zooma
                elif operation.lower() == "zooma":
                    for change in list_changes:
                        try:
                            property_type = change["characteristicsName"].replace(
                                " ", "_"
                            )
                            addZoomaTerm(
                                studyID,
                                property_type,
                                change["new_term"],
                                change["term_url"],
                            )
                            result = "Done"
                        except Exception as e:
                            result = "Error"
                            logger.info(e)

                        google_df.loc[index, "status (Done/Error)"] = result
                        replaceGoogleSheet(google_df, google_url, sheet_name)

                else:
                    logger.info("Wrong operation tag in the spreadsheet")
                    abort(400)

            else:
                logger.info("Wrong query field requested")
                abort(404)


class Cellosaurus(Resource):
    @swagger.operation(
        summary="[Deprecated] Get Cellosaurus entity and synonyms",
        notes="Get Cellosaurus terms",
        parameters=[
            {
                "name": "query",
                "description": "Query to search Cellosaurus",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def get(self):
        log_request(request)
        raise_deprecation_error(request)
        public_endpoint(request)

        query = ""
        if request.args:
            query = request.args.get("query")
            if query is None:
                abort(400)
            if query:
                query = query.strip()

        def getlink(query):
            try:
                keyword = query.replace(" ", "+")
                url = (
                    f"https://web.expasy.org/cgi-bin/cellosaurus/search?input={keyword}"
                )
                soup = BeautifulSoup(requests.get(url).text, "html.parser")
                links = []
                for rows in soup.find_all("tr"):
                    cells = rows.find_all("td")
                    cell_name = cells[0].get_text()
                    prefix = "https://web.expasy.org/cellosaurus/"
                    links.append(prefix + cell_name + ".txt")
                return links
            except Exception as e:
                # (e.args)
                logger.info(str(e))

        def getSynonyms(url):
            try:
                lines = urlopen(url).readlines()
                term = ""
                synonyms = []
                ID = ""
                for line in lines:
                    line = line.decode("utf-8")
                    if line.startswith("<pre>ID"):
                        line = " ".join(line.split())
                        term = line.partition("ID")[2].strip()

                    if line.startswith("AC"):
                        line = " ".join(line.split())
                        ID = line.partition("AC")[2].strip()

                    if line.startswith("SY"):
                        line = " ".join(line.split())
                        syn = line.partition("SY")[2].strip()
                        synonyms = syn.split(";")
                        synonyms = [x.strip() for x in synonyms]
                return ID, term, synonyms
            except Exception as e:
                # print(e.args)
                logger.info(e)

        links = getlink(query)
        result = []

        for link in links:
            ID, term, synoyms = getSynonyms(link)
            item = [term] + synoyms
            if query.lower() in [x.lower() for x in item]:
                result = item
                break

        if len(result) == 0:
            return []
        else:
            return jsonify({"CellosaurusTerm": [{"ID": ID, "synoyms": item}]})


def get_metainfo(query):
    """
    get placeholder/wrong-match terms from study investigation file
    :param query: factor / descriptor ...
    :return: list of dictionary results
    """
    res = []

    studyIDs = wsc.get_public_studies()["content"]
    logger.info("Getting {query} terms".format(query=query))

    for studyID in studyIDs:
        # print(f'get {query} from {studyID}.')
        if query.lower() == "factor":
            url = get_host_internal_url() + "/ws/studies/{study_id}/factors".format(
                study_id=studyID
            )

            try:
                resp = requests.get(
                    url,
                    headers={
                        "user_token": get_settings().auth.service_account.api_token
                    },
                )
                data = resp.json()

                for factor in data["factors"]:
                    temp_dict = {
                        "studyID": studyID,
                        "old_name": factor["factorName"],
                        "annotationValue": factor["factorType"]["annotationValue"],
                        "termAccession": factor["factorType"]["termAccession"],
                    }
                    res.append(temp_dict)
                    # if ('placeholder' in factor['factorType']['termAccession']) or (
                    #         len(factor['factorType']['termAccession']) == 0):
                    #     res.append(temp_dict)
                    # else:
                    #     abort(400)
            except:
                pass

        elif query.lower() == "design descriptor":
            url = get_host_internal_url() + "/ws/studies/{study_id}/descriptors".format(
                study_id=studyID
            )

            try:
                resp = requests.get(
                    url,
                    headers={
                        "user_token": get_settings().auth.service_account.api_token
                    },
                )
                data = resp.json()

                for descriptor in data["studyDesignDescriptors"]:
                    temp_dict = {
                        "studyID": studyID,
                        "old_name": descriptor["annotationValue"],
                        "matched_iri": descriptor["termAccession"],
                    }
                    res.append(temp_dict)
                    # if ('placeholder' in temp_dict['matched_iri']) or (len(temp_dict['matched_iri']) == 0):
                    #     res.append(temp_dict)
                    # else:
                    #     abort(400)
            except:
                pass

        elif query.lower() == "organism":
            url = get_host_internal_url() + "/ws/studies/{study_id}/organisms".format(
                study_id=studyID
            )

            try:
                resp = requests.get(
                    url,
                    headers={
                        "user_token": get_settings().auth.service_account.api_token
                    },
                )
                data = resp.json()
                for organism in data["organisms"]:
                    temp_dict = {
                        "studyID": studyID,
                        "old_organism": organism["Characteristics[Organism]"],
                        "organism_ref": organism["Term Source REF"],
                        "organism_url": organism["Term Accession Number"],
                        "old_organismPart": organism["Characteristics[Organism part]"],
                        "organismPart_ref": organism["Term Source REF.1"],
                        "organismPart_url": organism["Term Accession Number.1"],
                    }

                    res.append(temp_dict)

                    # if ('placeholder' in temp_dict['organism_url']) or ('placeholder' in temp_dict['organismPart_url']) \
                    #         or (len(temp_dict['organism_url']) == 0) or (len(temp_dict['organismPart_url']) == 0):
                    #     res.append(temp_dict)
                    #
                    # else:
                    #     abort(400)
            except:
                pass
        else:
            abort(400)
    return res


def insertGoogleSheet(data, url, worksheetName):
    """
    :param data: list of data
    :param url: url of google sheet
    :param worksheetName: worksheet name
    :return: Nan
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        get_settings().google.connection.google_sheet_api, scope
    )
    gc = gspread.authorize(credentials)
    try:
        wks = gc.open_by_url(url).worksheet(worksheetName)
        wks.append_row(data, value_input_option="RAW")
    except Exception as e:
        # print(e.args)
        logger.info(e.args)


def setGoogleSheet(df, url, worksheetName):
    """
    set whole dataframe to google sheet, if sheet existed create a new one
    :param df: dataframe want to save to google sheet
    :param url: url of google sheet
    :param worksheetName: worksheet name
    :return: Nan
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        get_settings().google.connection.google_sheet_api, scope
    )
    gc = gspread.authorize(credentials)
    try:
        wks = gc.open_by_url(url).worksheet(worksheetName)
        # print(worksheetName + ' existed... create a new one')
        wks = gc.open_by_url(url).add_worksheet(
            title=worksheetName + "_1", rows=df.shape[0], cols=df.shape[1]
        )
    except Exception as e:
        wks = gc.open_by_url(url).add_worksheet(
            title=worksheetName, rows=df.shape[0], cols=df.shape[1]
        )
        logger.info(e.args)
    set_with_dataframe(wks, df)


def getGoogleSheet(url, worksheetName):
    """
    get google sheet
    :param url: url of google sheet
    :param worksheetName: work sheet name
    :return: data frame
    """
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            get_settings().google.connection.google_sheet_api, scope
        )
        gc = gspread.authorize(credentials)
        wks = gc.open_by_url(url).worksheet(worksheetName)
        content = wks.get_all_records()
        df = pd.DataFrame(content)
        return df
    except Exception as e:
        # print(e.args)
        logger.info(e.args)


def replaceGoogleSheet(df, url, worksheetName):
    """
    replace the old google sheet with new data frame, old sheet will be clear
    :param df: dataframe
    :param url: url of google sheet
    :param worksheetName: work sheet name
    :return: Nan
    """
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            get_settings().google.connection.google_sheet_api, scope
        )
        gc = gspread.authorize(credentials)
        wks = gc.open_by_url(url).worksheet(worksheetName)
        wks.clear()
        set_with_dataframe(wks, df)
    except Exception as e:
        logger.info(e.args)


def addZoomaTerm(studyID, Property_type, Property_value, url):
    """
    :param studyID: studyID
    :param Property_type: Term to be annotated
    :param Property_value: annotation value
    :param url: annotation url
    :return: Nan
    """
    zooma_path = get_settings().file_resources.mtbls_zooma_file
    zooma_df = pd.read_csv(zooma_path, sep="\t")
    lastID = int(zooma_df.iloc[-1]["BIOENTITY"].split("_")[1])
    bioentity = "metabo_" + str(lastID + 1)
    t = current_time().strftime("%d/%m/%Y %H:%M")
    temp = {
        "STUDY": studyID,
        "BIOENTITY": bioentity,
        "PROPERTY_TYPE": Property_type,
        "PROPERTY_VALUE": Property_value,
        "SEMANTIC_TAG": url,
        "ANNOTATOR": "Jiakang Chang",
        "ANNOTATION_DATE": t,
    }
    zooma_df = pd.concat([zooma_df, temp], ignore_index=True)
    zooma_df.to_csv(zooma_path, sep="\t", index=False)


def addEntity(new_term, supclass, definition=None):
    """
    add new term to the ontology and save it

    :param new_term: New term
    :param supclass:  superclass/branch name or iri of new term
    :param definition (optional): definition of the new term
    """

    def getid(onto):
        """
        this method usd for get the last un-take continuously term ID
        :param onto: ontology
        :return: the last id for the new term
        """

        temp = []
        for c in onto.classes():
            if str(c).lower().startswith("metabolights") or str(c).lower().startswith(
                "ontology"
            ):
                temp.append(str(c))

        last = max(temp)
        temp = str(int(last[-6:]) + 1).zfill(6)
        id = "MTBLS_" + temp
        return id

    file = convert_relative_to_real_path(
        get_settings().file_resources.mtbls_ontology_file
    )
    try:
        onto = get_ontology(file).load()

    except Exception as e:
        logger.info(e.args)
        abort(400)
        return []

    all_class = []
    for cls in onto.classes():
        logger.info(cls)
        all_class += cls.label

    if new_term.lower() in [x.lower() for x in all_class]:
        logger.info("Operation rejected, term exciting")
        # print('Operation rejected, term exciting')
        abort(400)
        return []

    id = getid(onto)
    namespace = onto.get_namespace("http://www.ebi.ac.uk/metabolights/ontology/")

    with namespace:
        try:
            cls = onto.search_one(label=supclass)
            if cls is None:
                cls = onto.search_one(iri=supclass)
            if cls is None:
                logger.info(f"Can't find superclass named {supclass}")
                # print(f"Can't find superclass named {supclass}")
                abort(400)
                return []

            # check duplicate
            subs = cls.descendants()
            for sub in subs:
                if sub.label[0].lower() == new_term.lower():
                    # print('adding rejected, duplicated term: ' + new_term)
                    logger.info("adding rejected, duplicated term: " + new_term)
                    return []

            newEntity = types.new_class(id, (cls,))
            newEntity.label = new_term
            if definition:
                newEntity.isDefinedBy = definition
            else:
                pass
        except Exception as e:
            # print(e)
            logger.info(e.args)
            abort(400)
            return []
        file = convert_relative_to_real_path(
            get_settings().file_resources.mtbls_ontology_file
        )

        onto.save(file=file, format="rdfxml")
