#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-May-09
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

import logging

from flask_restful import abort

from app.utils import MetabolightsException
from app.ws.chebi.search.chebi_search_manager import ChebiSearchManager
from app.ws.db_connection import create_empty_study, \
    get_release_date_of_study
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from app.ws.email.email_service import EmailService
from app.ws.study import commons
from app.ws.study.user_service import UserService

"""
MetaboLights WS client

Updated from the Java-based REST resources
"""

logger = logging.getLogger('wslog')


class WsClient:
    search_manager: ChebiSearchManager = None
    email_service: EmailService = None
    elasticsearch_service: ElasticsearchService = None

    def __init__(self, search_manager: ChebiSearchManager = None, email_service: EmailService = None,
                 elasticsearch_service: ElasticsearchService = None):
        WsClient.email_service = email_service
        WsClient.search_manager = search_manager
        WsClient.elasticsearch_service = elasticsearch_service

    def get_study_location(self, study_id, user_token):
        return commons.get_study_location(study_id, user_token)

    def get_maf_search(self, search_type, search_value):
        # Updated to remove Java WS /genericcompoundsearch/{search_type}/{search_value} dependency
        result = None
        try:
            result = self.search_manager.search_by_type(search_type, search_value)
        except Exception as e:
            abort(500, message=f"MAF search failed {e.args}")

        if not result or result.err:
            abort(400, result.err)

        json_result = result.dict()
        return json_result

    @staticmethod
    def get_public_studies():
        return commons.get_public_studies_list()

    @staticmethod
    def get_private_studies():
        return commons.get_private_studies_list()

    @staticmethod
    def get_non_public_studies():
        return commons.get_non_public_studies_list()

    @staticmethod
    def get_study_by_type(stype, publicS=True):
        return commons.get_study_by_status(stype, publicS)

    @staticmethod
    def get_all_studies_for_user(user_token):
        return commons.get_all_studies_for_user(user_token)

    @staticmethod
    def get_permissions(study_id, user_token, obfuscation_code=None):
        return commons.get_permissions(study_id, user_token, obfuscation_code)

    @staticmethod
    def get_user_email(user_token):
        return commons.get_user_email(user_token)

    @staticmethod
    def create_upload_folder(study_id, obfuscation_code, user_token, send_email=True):
        # Updated to remove Java WS /study/requestFtpFolderOnApiKey dependency

        UserService.get_instance().validate_user_has_write_access(user_token, study_id)

        return commons.create_ftp_folder(study_id, obfuscation_code, user_token,
                                         email_service=WsClient.email_service, send_email=send_email)

    def add_empty_study(self, user_token):
        # Updated to remove Java WS /study/createEmptyStudy dependency

        user = UserService.get_instance().validate_user_has_submitter_or_super_user_role(user_token)

        study_id = create_empty_study(user_token)
        if not study_id:
            raise MetabolightsException("Error while creating new study in db")
        user_email = user.username
        submitters_email_list = [user_email]
        release_date = get_release_date_of_study(study_id)
        self.email_service.send_email_for_queued_study_submitted(study_id, release_date,
                                                                 user_email, submitters_email_list)
        return study_id

    def reindex_study(self, study_id, user_token, include_validation_results: bool = False, sync: bool = False):
        # Updated to remove Java WS /study/reindexStudyOnToken dependency

        UserService.get_instance().validate_user_has_submitter_or_super_user_role(user_token)
        try:
            self.elasticsearch_service.reindex_study(study_id, user_token, include_validation_results, sync=sync)
            return True, f" {study_id} is successfully indexed"
        except MetabolightsException as e:
            logger.error(f'{str(e)}')
            raise e
