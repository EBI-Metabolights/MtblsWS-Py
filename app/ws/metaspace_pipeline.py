#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Feb-28
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

from flask import request
from flask_restful import Resource, abort
from flask_restful_swagger import swagger
from metaspace.sm_annotation_utils import SMInstance

from app.ws.auth.permissions import raise_deprecation_error, validate_submission_update
from app.ws.metaspace_utils import annotate_metaspace, import_metaspace
from app.ws.study.utils import get_study_metadata_path
from app.ws.utils import log_request

logger = logging.getLogger("wslog")


class MetaspacePipeLine(Resource):
    @swagger.operation(
        summary="[Deprecated] Import files files and metadata from METASPACE to a MTBLS study",
        nickname="Import data from METASPACE",
        notes="""Import files files and metadata from METASPACE to a MetaboLights study.
        </p>Please note that METASPACE API keys will take priority over username/password.
        </p>METASPACE Users can generate an API key in the "API access" section of https://metaspace2020.eu/user/me.
        </br>
        If you are the dataset owner in METASPACE, you automatically get a link from METASPACE to your MetaboLights study
        </p>
        Data-sets must belong to the relevant project if you supply both values. Only supply one project if a dataset is linked
            </p><pre><code>{
    "project": {
        "metaspace-api-key": "12489afjhadkjfhajfh",
        "metaspace-password": "asdfjsahdf",
        "metaspace-email": "someone@here.com",
        "metaspace-projects": "project_id1,project_id2",
        "metaspace-datasets": "ds_id1,ds_id2"
    }
} </code></pre>""",
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
                "name": "project",
                "description": "METASPACE project info",
                "paramType": "body",
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
            {"code": 200, "message": "OK. Files/Folders were copied across."},
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
    def post(self, study_id):
        raise_deprecation_error(request)
        log_request(request)
        # param validation
        result = validate_submission_update(request)
        study_id = result.context.study_id
        obfuscation_code = result.context.obfuscation_code
        user_token = result.context.user_api_token
        study_location = get_study_metadata_path(study_id)

        investigation = None
        metaspace_projects = None
        metaspace_api_key = None
        metaspace_password = None
        metaspace_email = None
        metaspace_datasets = None

        # body content validation
        if request.data:
            try:
                data_dict = json.loads(request.data.decode("utf-8"))
                project = data_dict["project"]
                if project:
                    if "metaspace-api-key" in project:
                        metaspace_api_key = project["metaspace-api-key"]
                    if "metaspace-password" in project:
                        metaspace_password = project["metaspace-password"]
                    if "metaspace-email" in project:
                        metaspace_email = project["metaspace-email"]
                    if "metaspace-datasets" in project:
                        metaspace_datasets = project["metaspace-datasets"]
                        logger.info(
                            "Requesting METASPACE datasets " + metaspace_datasets
                        )
                    if "metaspace-projects" in project:
                        metaspace_projects = project["metaspace-projects"]
                        logger.info(
                            "Requesting METASPACE projects " + metaspace_projects
                        )

                    # study_location = os.path.join(study_location, 'METASPACE')

                    sm = SMInstance()
                    if metaspace_api_key:
                        """
                        Log in with API key
                        Users can generate an API key in the "API access" section of https://metaspace2020.eu/user/me
                        If you're connecting to our GraphQL API directly, API key authentication requires an HTTP
                        header "Authorization: Api-Key " followed by the key. """
                        sm.login(email=None, password=None, api_key=metaspace_api_key)
                        # logged_id = sm.logged_in
                    elif metaspace_password and metaspace_email:
                        sm.login(
                            email=metaspace_email,
                            password=metaspace_password,
                            api_key=None,
                        )
                    else:
                        abort(
                            406,
                            message="No METASPACE API key or username/password provided.",
                        )

                    if not os.path.isdir(study_location):
                        os.makedirs(study_location, exist_ok=True)

                    # Annotate the METASPACE project and return all relevant dataset and project ids
                    metaspace_project_ids, metaspace_dataset_ids = annotate_metaspace(
                        study_id=study_id,
                        sm=sm,
                        metaspace_projects=metaspace_projects,
                        metaspace_datasets=metaspace_datasets,
                    )

                    investigation = import_metaspace(
                        study_id=study_id,
                        dataset_ids=metaspace_dataset_ids,
                        study_location=study_location,
                        user_token=user_token,
                        obfuscation_code=obfuscation_code,
                        sm_instance=sm,
                    )
            except KeyError:
                abort(406, message="No 'project' parameter was provided.")
            except AttributeError as e:
                abort(417, message="Missing attribute/element in JSON string" + str(e))
            except Exception as e:
                abort(417, message=str(e))

        if investigation:
            return {"Success": "METASPACE data imported successfully"}
        else:
            return {
                "Warning": "Please check if METASPACE data was successfully imported"
            }
