#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Jan-08
#  Modified by:   kenneth
#
#  Copyright 2020 EMBL - European Bioinformatics Institute
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
import os

import pandas as pd
from flask import request
from flask_restful import Resource, abort
from flask_restful_swagger import swagger

from app.utils import metabolights_exception_handler
from app.ws.auth.permissions import (
    public_endpoint,
    raise_deprecation_error,
    validate_submission_update,
    validate_user_has_curator_role,
)
from app.ws.isa_table_templates import create_maf_sheet
from app.ws.mtblsWSclient import WsClient
from app.ws.report_builders.combined_maf_builder import CombinedMafBuilder
from app.ws.settings.utils import get_study_settings
from app.ws.study.study_service import StudyService
from app.ws.utils import read_tsv

logger = logging.getLogger("wslog")
# MetaboLights (Java-Based) WebService client
wsc = WsClient()


# Convert panda DataFrame to json tuples object
def totuples(df, text) -> dict:
    d = [
        dict([(colname, row[i]) for i, colname in enumerate(df.columns)])
        for row in df.values
    ]
    return {text: d}


def insert_row(idx, df, df_insert):
    return pd.concat(
        [df.iloc[:idx,], df_insert, df.iloc[idx:,]], ignore_index=True
    ).reset_index(drop=True)


class MtblsMAFSearch(Resource):
    """Get MAF from studies (assays)"""

    @swagger.operation(
        summary="Search for metabolite onto_information to use in the Metabolite Annotation file",
        nickname="MAF search",
        notes="Get a given MAF associated with assay {assay_id} for a MTBLS Study with {study_id} in JSON format",
        parameters=[
            {
                "name": "query_type",
                "description": "The type of data to search for",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
                "enum": ["name", "databaseid", "inchi", "smiles"],
            },
            {
                "name": "search_value",
                "description": "The search string",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK. The metabolite search result is returned"},
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
    def get(self, query_type):
        public_endpoint(request)
        # param validation
        if query_type is None:
            abort(404)

        search_value = None
        if request.args:
            search_value = request.args.get("search_value")

        if search_value is None:
            abort(404)

        search_res = wsc.get_maf_search(query_type, search_value)
        return search_res


class MetaboliteAnnotationFile(Resource):
    @swagger.operation(
        summary="Read, and add missing samples for a MAF",
        nickname="Get MAF for a given MTBLS Assay",
        notes="""Create or update a Metabolite Annotation File for an assay.
<pre><code>
{
  "data": [
    { "assay_file_name": "a_some_assay_file.txt" },
    { "assay_file_name": "a_some_assay_file-1.txt" }
  ]
}
</code></pre>""",
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
                "name": "data",
                "description": "Assay File names",
                "required": True,
                "allowMultiple": False,
                "paramType": "body",
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
            {"code": 417, "message": "Incorrect parameters provided"},
        ],
    )
    @metabolights_exception_handler
    def post(self, study_id):
        result = validate_submission_update(request)
        study_id = result.context.study_id

        data_dict = json.loads(request.data.decode("utf-8"))
        assay_files = data_dict.get("data", [])

        # param validation
        if not assay_files:
            abort(
                417,
                message='Please ensure the JSON has at least one "assay_file_name" element',
            )
        study_location = os.path.join(
            get_study_settings().mounted_paths.study_metadata_files_root_path, study_id
        )
        assay_file_names: list[str] = []
        invalid_assay_files: list[str] = []
        for k in assay_files:
            if isinstance(k, dict) and k.get("assay_file_name"):
                file_name = k.get("assay_file_name")
                file_path = os.path.join(study_location, file_name)
                if not os.path.exists(file_path):
                    invalid_assay_files.append(file_name)
                else:
                    assay_file_names.append(file_name)
        if invalid_assay_files:
            abort(417, message="invalid assay files: " + ", ".join(invalid_assay_files))

        study = StudyService.get_instance().get_study_by_req_or_mtbls_id(study_id)
        logger.info("MAF: Getting ISA-JSON Study %s", study_id)

        invalid_maf_files = []
        maf_files = {}
        assay_file_samples = {}
        for assay_file in assay_file_names:
            annotation_file_name = None
            assay_file_path = os.path.join(study_location, assay_file)
            assay_df = read_tsv(assay_file_path)
            if "Metabolite Assignment File" not in assay_df.columns:
                continue

            annotation_file_names = set(assay_df["Metabolite Assignment File"].unique())
            samples = []
            if "Sample Name" not in assay_df.columns:
                for cell in assay_df["Sample Name"]:
                    if cell and cell.strip():
                        samples.append(cell)
            assay_file_samples[assay_file] = samples

            for file in annotation_file_names:
                if file and file.strip():
                    if not file.endswith("_v2_maf.tsv"):
                        invalid_maf_files.append(file)
                        continue
                    if file not in maf_files:
                        maf_files[file] = []
                    maf_files[file].append(assay_file)
        updated_maf_files = []
        new_maf_files = []
        failed_maf_files = []
        for maf_file, assay_files in maf_files.items():
            sample_names = []
            samples_set = set()
            for file in assay_files:
                for sample in assay_file_samples[assay_file]:
                    if sample not in samples_set:
                        samples_set.add(sample)
                        sample_names.append(sample)

            main_technology_type = "NMR" if "nmr" in assay_file else "MS"
            maf_file_path = os.path.join(study_location, maf_file)
            valid = False
            if os.path.exists(maf_file_path):
                maf_df = read_tsv(assay_file_path)
                if not maf_df.empty and len(maf_df) > 1:
                    valid = True
                    for sample in sample_names:
                        if sample not in maf_df.columns:
                            maf_df[sample] = ""
                    updated_maf_files.append(maf_file)
            if os.path.exists(maf_file_path) and not valid:
                success = create_maf_sheet(
                    study_path=study_location,
                    maf_file_name=maf_file,
                    main_technology_type=main_technology_type,
                    template_version=study.template_version,
                    sample_names=sample_names,
                )
                if success:
                    new_maf_files.append(maf_file)
                else:
                    failed_maf_files.append(maf_file)

        return {
            "update_maf_files": updated_maf_files,
            "new_maf_files": new_maf_files,
            "failed_maf_files": failed_maf_files,
            "invalid_maf_files": invalid_maf_files,
        }


class CombineMetaboliteAnnotationFiles(Resource):
    @swagger.operation(
        summary="[Deprecated] Combine MAFs for a list of studies",
        nickname="Combine MAF files",
        notes="""Combine MAF files for a given list of studies, by analytical method. If a study contains assays with
        more than one analytical method, it will endeavour to try and select only the MAFs that correspond to the chosen
        analytical method.
    <pre><code>
    {
      "data": [
        "MTBLS1",
        "MTBLS2",
        "MTBLS3"
      ],
      "method": "NMR"
    }
    </code></pre>""",
        parameters=[
            {
                "name": "data",
                "description": "Studies to combine MAFs for, and the analytical method",
                "required": True,
                "allowMultiple": False,
                "paramType": "body",
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
            {"code": 417, "message": "Incorrect parameters provided"},
        ],
    )
    def post(self):
        raise_deprecation_error(request)
        validate_user_has_curator_role(request)
        data_dict = json.loads(request.data.decode("utf-8"))
        studies_to_combine = data_dict["data"]
        method = data_dict["method"]

        if studies_to_combine is None:
            abort(417)

        combiBuilder = CombinedMafBuilder(
            studies_to_combine=studies_to_combine, method=method
        )

        combiBuilder.build()
        r_d = {
            "status": f"Combined Maf File Built, with {len(combiBuilder.unopenable_maf_register)} MAFs unable to be "
            f"opened: {str(combiBuilder.unopenable_maf_register)}. There were "
            f"{len(combiBuilder.missing_maf_register)} studies that had no MAF files: "
            f"{str(combiBuilder.missing_maf_register)} .There were "
            f"{len(combiBuilder.no_relevant_maf_register)} studies that had MAF files, but no "
            f"MAF file matching the given analytical method {method}: "
            f"{str(combiBuilder.no_relevant_maf_register)}"
        }
        # logging it out in the event of the swagger UI crashing as these are useful numbers
        logger.info(r_d["status"])
        return r_d
