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
from typing import Union

from app.config import get_settings
from app.utils import MetabolightsException
from app.ws.chebi.search.chebi_search_manager import ChebiSearchManager
from app.ws.chebi.search.curated_metabolite_table import CuratedMetaboliteTable
from app.ws.chebi.search.models import CompoundSearchResponseModel
from app.ws.chebi.wsproxy import get_chebi_ws_proxy
from app.ws.db.dbmanager import DBManager
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from app.ws.email.email_service import EmailService
from app.ws.study import commons

"""
MetaboLights WS client

Updated from the Java-based REST resources
"""

logger = logging.getLogger("wslog")


class WsClient:
    default_search_manager: Union[None, ChebiSearchManager] = None
    email_service: Union[None, EmailService] = None
    elasticsearch_service: Union[None, ElasticsearchService] = None

    def __init__(
        self,
        search_manager: Union[None, ChebiSearchManager] = None,
        email_service: Union[None, EmailService] = None,
        elasticsearch_service: Union[None, ElasticsearchService] = None,
    ):
        self.search_manager = (
            search_manager if search_manager else WsClient.default_search_manager
        )
        if not self.search_manager:
            chebi_proxy = get_chebi_ws_proxy()
            curation_table_file_path = (
                get_settings().chebi.pipeline.curated_metabolite_list_file_location
            )
            curation_table = CuratedMetaboliteTable.get_instance(
                curation_table_file_path
            )
            chebi_search_manager = ChebiSearchManager(
                ws_proxy=chebi_proxy, curated_metabolite_table=curation_table
            )
            WsClient.default_search_manager = chebi_search_manager
            self.search_manager = chebi_search_manager

        self.email_service = email_service if email_service else WsClient.email_service
        self.elasticsearch_service = (
            elasticsearch_service
            if elasticsearch_service
            else WsClient.elasticsearch_service
        )
        if not self.elasticsearch_service:
            db_manager = DBManager.get_instance()
            study_settings = get_settings().study
            elasticsearch_settings = get_settings().elasticsearch
            self.elasticsearch_service = ElasticsearchService(
                settings=elasticsearch_settings,
                db_manager=db_manager,
                study_settings=study_settings,
            )
            WsClient.elasticsearch_service = self.elasticsearch_service

    def get_maf_search(
        self, search_type, search_value, json_object: bool = True
    ) -> Union[CompoundSearchResponseModel, dict]:
        # Updated to remove Java WS /genericcompoundsearch/{search_type}/{search_value} dependency
        result = None
        try:
            result = self.search_manager.search_by_type(search_type, search_value)
        except Exception as e:
            message = f"MAF search failed search type: {search_type} value: {search_value}, {e.args}"
            logger.error(message)
            print(message)
            return None

        if not result or result.err:
            if result.err:
                logger.warning(f"Result is not valid {str(result.err)}")
            return None
        if json_object:
            return result.model_dump()
        return result

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
        return commons.create_ftp_folder(
            study_id,
            obfuscation_code,
            user_token,
            email_service=WsClient.email_service,
            send_email=send_email,
        )

    def reindex_study(
        self,
        study_id,
        user_token=None,
        include_validation_results: bool = False,
        sync: bool = False,
    ):
        try:
            self.elasticsearch_service.reindex_study(
                study_id, user_token, include_validation_results, sync=sync
            )
            return True, f" {study_id} is successfully indexed"
        except MetabolightsException as e:
            logger.error("%s", e)
            raise e
