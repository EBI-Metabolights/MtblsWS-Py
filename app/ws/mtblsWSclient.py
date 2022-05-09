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

import json
import logging
import os

import requests
from flask import current_app as app, abort

from app.ws.chebi.search.chebi_search_manager import ChebiSearchManager
from app.ws.db_connection import check_access_rights, get_public_studies, get_private_studies, get_study_by_type, \
    get_email, query_study_submitters, create_empty_study, \
    get_release_date_of_study, get_submitted_study_ids_for_user
from app.ws.email.email_service import EmailService, get_email_service

"""
MetaboLights WS client

Use the Java-based REST resources provided from MTBLS
"""

logger = logging.getLogger('wslog')


class WsClient:

    def __init__(self, search_manager: ChebiSearchManager = None, email_service: EmailService = None):
        self.search_manager = search_manager
        self.email_service = email_service
        if not self.email_service:
            self.email_service = get_email_service(app)
        if not self.search_manager:
            self.search_manager = ChebiSearchManager()

    def get_study_location(self, study_id, user_token):
        """
        Get the actual location of the study files in the File System

        :param study_id: Identifier of the study in MetaboLights
        :param user_token: User API token. Used to check for permissions
        """
        logger.info('Getting actual location for Study %s on the filesystem', study_id)

        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            self.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        location = os.path.join(app.config.get('STUDY_PATH'), study_id.upper())
        location = os.path.join(app.config.get('DEBUG_STUDIES_PATH'), location.strip('/'))
        if not os.path.isdir(location):
            abort(404, 'There is no path for %s' %(study_id,))
        logger.info('... found study folder %s', location)
        return location

    def get_maf_search(self, search_type, search_value):
        # Updated to remove Java WS /genericcompoundsearch/{search_type}/{search_value} dependency
        result = None
        try:
            result = self.search_manager.search_by_type(search_type, search_value)
            return result.json()
        except Exception as e:
            abort(500, "MAF search failed")

        if not result or result.err:
            abort(400, result.err)

    @staticmethod
    def get_public_studies():
        logger.info('Getting all public studies')
        studies = []
        study_list = get_public_studies()
        for acc in study_list:
            studies.append(acc[0])

        logger.info('... found %d public studies', len(studies))
        return {"studies": len(studies), "content": studies}

    @staticmethod
    def get_private_studies():
        logger.info('Getting all private studies')
        studies = []
        study_list = get_private_studies()
        for acc in study_list:
            studies.append(acc[0])

        logger.info('... found %d private studies', len(studies))
        return {"studies": len(studies), "content": studies}

    @staticmethod
    def get_study_by_type(stype, publicS=True):
        logger.info('Getting all public studies')
        studyID, studytype = get_study_by_type(stype, publicS)
        res = json.dumps(dict(zip(studyID, studytype)))
        return res

    @staticmethod
    def get_all_studies_for_user(user_token):
        # remove this method if CloneAccession is not used
        logger.info('Getting submitted studies using user_token')
        study_id_list = get_submitted_study_ids_for_user(user_token)
        text_resp = json.dumps(study_id_list)
        logger.info('Found the following studies %s', text_resp)
        return text_resp

    # used to index the tuple response
    CAN_READ = 0
    CAN_WRITE = 1

    @staticmethod
    def get_permissions(study_id, user_token, obfuscation_code=None):
        """
        Check MTBLS-WS for permissions on this Study for this user

        Study       User    Submitter   Curator     Reviewer/Read-only
        SUBMITTED   ----    Read+Write  Read+Write  Read
        INCURATION  ----    Read        Read+Write  Read
        INREVIEW    ----    Read        Read+Write  Read
        PUBLIC      Read    Read        Read+Write  Read

        :param obfuscation_code:
        :param study_id:
        :param user_token:
        :return: study details and permission levels

        """

        if not user_token:
            user_token = "public_access_only"

        # Reviewer access will pass the study obfuscation code instead of api_key
        if study_id and not obfuscation_code and user_token.startswith("ocode:"):
            logger.info("Study obfuscation code passed instead of user API_CODE")
            obfuscation_code = user_token.replace("ocode:", "")

        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        updated_date, study_status = check_access_rights(user_token, study_id.upper(),
                                                         study_obfuscation_code=obfuscation_code)

        logger.info("Read access: " + str(read_access) + ". Write access: " + str(write_access))

        return is_curator, read_access, write_access, obfuscation_code, study_location, release_date, \
               submission_date, study_status

    @staticmethod
    def get_user_email(user_token):

        user_email= get_email(user_token)
        logger.info(" User Email: " + str(user_email))
        return user_email

    @staticmethod
    def get_queue_folder():
        # Updated to remove Java WS /study/getQueueFolder dependency
        queue_folder = app.config.get('STUDY_QUEUE_FOLDER')
        logger.info('Found queue upload folder for this server as:' + queue_folder)

        return queue_folder

    def create_upload_folder(self, study_id, obfuscation_code, user_token):
        # Updated to remove Java WS /study/requestFtpFolderOnApiKey dependency
        is_curator, read_access, write_access, study_obfuscation_code, study_location, release_date, submission_date, study_status = \
            self.get_permissions("MTBLS1", user_token)
        if not write_access:
            abort(401, "No permission")

        new_folder_name = study_id.lower() + '-' + obfuscation_code
        ftp_folder = os.path.join(app.config.get('MTBLS_FTP_ROOT'), new_folder_name)
        os_upload = ftp_folder
        if not os.path.exists(ftp_folder):
            logger.info('Creating a new study upload folder for Study %s', study_id)
            ftp_private_folder_root = app.config.get('MTBLS_PRIVATE_FTP_ROOT')
            ftp_path = os.path.join(ftp_private_folder_root, new_folder_name)
            raw_files_path = os.path.join(ftp_path, "RAW_FILES")
            derived_files_path = os.path.join(ftp_path, "DERIVED_FILES")
            logger.info(f"Creating folder {ftp_path}")
            os.makedirs(ftp_path, mode=0o770, exist_ok=True)
            os.makedirs(raw_files_path, mode=0o770, exist_ok=True)
            os.makedirs(derived_files_path, mode=0o770, exist_ok=True)
            os_upload = ftp_path
            user_email = get_email(user_token)
            submitter_emails = query_study_submitters(study_id)
            submitters_email_list = [ submitter[0] for submitter in submitter_emails]
            self.email_service.send_email_for_requested_ftp_folder_created(study_id,
                                                                           ftp_path, user_email, submitters_email_list)
        upload_loc = ftp_folder
        private_ftp_user = app.config.get("PRIVATE_FTP_SERVER_USER")
        if private_ftp_user in ftp_folder:
            upload_location = ftp_folder.split('/' + private_ftp_user)  # FTP/Aspera root starts here
            upload_location = [x for x in upload_location if x]
            if len(upload_location) > 0:
                upload_loc = upload_location[1]
            else:
                upload_loc = upload_location[0]

        return {'os_upload_path': os_upload, 'upload_location': upload_loc}

    def add_empty_study(self, user_token):
        # Updated to remove Java WS /study/createEmptyStudy dependency
        study_id = create_empty_study(user_token)
        user_email = get_email(user_token)
        submitter_emails = query_study_submitters(study_id)
        submitters_email_list = [submitter[0] for submitter in submitter_emails]
        release_date = get_release_date_of_study(study_id)
        self.email_service.send_email_for_queued_study_submitted(study_id, release_date,
                                                                 user_email, submitters_email_list)
        return study_id


    @staticmethod
    def reindex_study(study_id, user_token):
        # TODO Updated to remove Java WS /study/reindexStudyOnToken dependency
        resource = app.config.get('MTBLS_WS_RESOURCES_PATH') + "/study/reindexStudyOnToken"
        url = app.config.get('MTBLS_WS_HOST') + app.config.get('MTBLS_WS_PORT') + resource
        logger.info('Reindex study ' + study_id)
        resp = requests.post(
            url,
            headers={"content-type": "application/x-www-form-urlencoded", "cache-control": "no-cache"},
            data={"token": user_token, "study_id": study_id}
        )

        if resp.status_code != 200:
            abort(resp.status_code)

        message = resp.text
        return True, message
