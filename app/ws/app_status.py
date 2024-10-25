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

import logging
from flask_restful import Resource
from flask_restful_swagger import swagger
from flask import request
from app.config import get_settings
from app.tasks.system_monitor_tasks.integration_check import check_integrations
from app.utils import current_time, metabolights_exception_handler
from app.ws.study.user_service import UserService


"""
MtblsWS-Py App Status

Basic description of the Web Service
"""

logger = logging.getLogger('wslog')


class MaintenanceStatus(Resource):
    """Status of the Web Service"""
    @swagger.operation(
        summary="Checks if it is under maintenance",
        nickname="maintenance",
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            }
        ]
    )
    @metabolights_exception_handler
    def get(self):

        maintenance = False
        try: 
            maintenance = get_settings().server.service.maintenance_mode
            
            return {"message": None, "content": maintenance, "error": None }
            
        except Exception as ex:
            # no cache or invalid cache
            return {"message": None, "content": maintenance, "error": None }

class IntegrationCheck(Resource):
    """Status of the Web Service and dependencies"""
    @swagger.operation(
        summary="Checks internal and external integrations (curator only)",
        nickname="integration check",
        parameters=[
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            }
        ]
    )
    @metabolights_exception_handler
    def get(self):
        user_token = None

        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        UserService.get_instance().validate_user_has_curator_role(user_token)
        integration_check_result = None
        try: 
            integration_check_result = check_integrations()
            if integration_check_result:
                return integration_check_result
            
        except Exception as ex:
            return {"message": None, "content": "", "error": str(ex) }

        return {"status": "ERROR", "executed_on":  "", "time": int(current_time().timestamp()) , "test_results": ""}