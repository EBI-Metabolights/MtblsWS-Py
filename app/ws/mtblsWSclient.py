import os
import logging
import requests
from datetime import datetime
from flask_restful import abort
from flask import current_app as app

"""
MetaboLights WS client

Use the Java-based REST resources provided from MTBLS
"""

logger = logging.getLogger('wslog')


class WsClient:

    def get_study_location(self, study_id, user_token):
        """
        Get the actual location of the study files in the File System

        :param study_id: Identifier of the study in MetaboLights
        :param user_token: User API token. Used to check for permissions
        """
        logger.info('Getting actual location for Study %s on the filesystem, using API-Key %s', study_id, user_token)
        study = self.get_study(study_id, user_token)
        location = study["content"]["studyLocation"]
        logger.info('... found study folder %s', location)
        location = os.path.join(app.config.get('DEBUG_STUDIES_PATH'), location.strip('/'))
        return location

    def get_study_updates_location(self, study_id, user_token):
        """
        Get location for output updates in a MetaboLights study.
        This is where afected files are copied before applying changes, for audit purposes.
        :param study_id:
        :param user_token:
        :return:
        """
        logger.info('Getting location for output updates for Study %s on the filesystem, using API-Key %s',
                    study_id, user_token)

        study = self.get_study(study_id, user_token)
        std_folder = study["content"]["studyLocation"]

        update_folder = std_folder + app.config.get('UPDATE_PATH_SUFFIX')
        update_folder = os.path.join(app.config.get('DEBUG_STUDIES_PATH'), update_folder.strip('/'))
        logger.info('... found updates folder %s', update_folder)
        return update_folder

    def get_study_status_and_release_date(self, study_id, user_token):
        study_json = self.get_study(study_id, user_token)
        std_status = study_json["content"]["studyStatus"]
        release_date = study_json["content"]["studyPublicReleaseDate"]
        # 2012-02-14 00:00:00.0
        readable =  datetime.fromtimestamp(release_date/1000).strftime('%Y-%m-%d %H:%M:%S.%f')

        return [std_status, readable]

    def get_sample_names(self, study_id, user_token):
        sample_json = self.get


    def get_study(self, study_id, user_token):
        """
        Get the JSON object for a MTBLS study
        by calling current Java-based WS
            {{server}}{{port}}/metabolights/webservice/study/MTBLS_ID

        :param study_id: Identifier of the study in MetaboLights
        :param user_token: User API token. Used to check for permissions
        """
        logger.info('Getting JSON object for Study %s, using API-Key %s', study_id, user_token)
        resource = app.config.get('MTBLS_WS_RESOURCES_PATH') + "/study/" + study_id
        url = app.config.get('MTBLS_WS_HOST') + app.config.get('MTBLS_WS_PORT') + resource
        resp = requests.get(url, headers={"user_token": user_token})
        if resp.status_code != 200:
            abort(resp.status_code)

        json_resp = resp.json()

        # double check for errors
        if json_resp["message"] is not None:
            if json_resp["message"] == 'Study not found':
                abort(404)
        if json_resp["err"] is not None:
            if user_token is None:
                abort(401)
            else:
                abort(403)

        logger.info('... found Study  %s', json_resp['content']['title'])
        return json_resp

    def get_study_maf(self, study_id, assay_id, user_token):
        """
        Get the JSON object for a given MAF for a MTBLS study
        by calling current Java-based WS
            {{server}}{{port}}/metabolights/webservice/study/MTBLS_ID/assay/ASSAY_ID/jsonmaf

        :param study_id: Identifier of the study in MetaboLights
        :param assay_id: The number of the assay for the given study_id
        :param user_token: User API token. Used to check for permissions
        """
        logger.info('Getting JSON object for MAF for Study %s (Assay %s), using API-Key %s', study_id, assay_id, user_token)
        resource = app.config.get('MTBLS_WS_RESOURCES_PATH') + "/study/" + study_id + "/assay/" + assay_id + "/jsonmaf"
        url = app.config.get('MTBLS_WS_HOST') + app.config.get('MTBLS_WS_PORT') + resource
        resp = requests.get(url, headers={"user_token": user_token})
        if resp.status_code != 200:
            abort(resp.status_code)

        json_resp = resp.json()
        return json_resp

    def get_maf_search(self, search_type, search_value):
        """
        Get the JSON object for a given MAF for a MTBLS study
        by calling current Java-based WS
            {{server}}{{port}}/metabolights/webservice/genericcompoundsearch/{search_type}/{search_value}

        :param search_type: The type of search to preform. 'name','databaseid','smiles','inchi'
        :param search_value: The actual value to search for
        """
        resource = app.config.get('MTBLS_WS_RESOURCES_PATH') + "/genericcompoundsearch/" + search_type
        url = app.config.get('MTBLS_WS_HOST') + app.config.get('MTBLS_WS_PORT') + resource
        if search_type == 'name' or search_type == 'databaseid':
            resp = requests.get(url + "/" + search_value, headers={"body": search_value})

        if search_type == 'inchi' or search_type == 'smiles':
            bytes_search = search_value.encode()
            resp = requests.post(url, data={search_type: bytes_search})

        if resp.status_code != 200:
            abort(resp.status_code)

        json_resp = resp.json()
        return json_resp

    def get_study_status(self, study_id, user_token):
        """
        Get the status of the Study: PUBLIC, INCURATION, ...
        :param study_id:
        :param user_token:
        :return:
        """
        logger.info('Getting the status of the Study %s, using API-Key %s', study_id, user_token)
        study = self.get_study(study_id, user_token)
        std_status = study["content"]["studyStatus"]
        logger.info('... found Study is %s', std_status)
        return std_status

    def is_study_public(self, study_id, user_token):
        """
        Check if the Study is public
        :param study_id:
        :param user_token:
        :return:
        """
        logger.info('Checking if Study %s is public, using API-Key %s', study_id, user_token)
        study = self.get_study(study_id, user_token)
        # Check for
        #   "publicStudy": true
        # and
        #   "studyStatus": "PUBLIC"
        std_status = study["content"]["studyStatus"]
        std_public = study["content"]["publicStudy"]
        is_public = std_public and std_status == "PUBLIC"
        logger.info('... found Study is %s', std_status)
        return is_public

    def get_public_studies(self):
        logger.info('Getting all public studies')
        resource = app.config.get('MTBLS_WS_RESOURCES_PATH') + "/study/list"
        url = app.config.get('MTBLS_WS_HOST') + app.config.get('MTBLS_WS_PORT') + resource
        resp = requests.get(url)
        if resp.status_code != 200:
            abort(resp.status_code)

        json_resp = resp.json()
        logger.info('... found %d public studies', len(json_resp['content']))
        return json_resp

    def get_all_studies_for_user(self, user_token):
        resource = app.config.get('MTBLS_WS_RESOURCES_PATH') + "/study/studyListOnUserToken?userToken="+user_token
        url = app.config.get('MTBLS_WS_HOST') + app.config.get('MTBLS_WS_PORT') + resource
        logger.info('Getting all studies for user_token %s using url %s', user_token, url)
        resp = requests.get(url)
        if resp.status_code != 200:
            abort(resp.status_code)

        text_resp = resp.text
        return text_resp

    def is_user_token_valid(self, user_token):
        logger.info('Checking for user credentials in MTBLS-Labs')
        resource = app.config.get('MTBLS_WS_RESOURCES_PATH') + "/labs/" + "authenticateToken"
        url = app.config.get('MTBLS_WS_HOST') + app.config.get('MTBLS_WS_PORT') + resource
        resp = requests.post(url, data='{"token":"' + user_token + '"}')
        if resp.status_code != 200:
            abort(resp.status_code)

        user = resp.headers.get('user')
        jwt = resp.headers.get('jwt')
        if user is None or jwt is None:
            abort(403)
        logger.info('... found user %s with jwt key: %s', user, jwt)
        return True

    # used to index the tuple response
    CAN_READ = 0
    CAN_WRITE = 1

    def get_permisions(self, study_id, user_token):
        """
        Check MTBLS-WS for permissions on this Study for this user

        Study       User    Submitter   Curator
        SUBMITTED   ----    Read+Write  Read+Write
        INCURATION  ----    Read        Read+Write
        INREVIEW    ----    Read        Read+Write
        PUBLIC      Read    Read        Read+Write

        :param study_id:
        :param user_token:
        :return:
        """
        logger.info('Checking for user permisions in MTBLS WS for Study %s, using API-Key %s', study_id, user_token)
        resource = app.config.get('MTBLS_WS_RESOURCES_PATH') + "/study/" + study_id + "/getPermissions"
        url = app.config.get('MTBLS_WS_HOST') + app.config.get('MTBLS_WS_PORT') + resource
        resp = requests.post(url,
                             headers={"content-type": "application/x-www-form-urlencoded",
                                      "cache-control": "no-cache"},
                             data="token=" + (user_token or ''))
        if resp.status_code != 200:
            abort(resp.status_code)

        json_resp = resp.json()
        import json
        cont = json.loads(json_resp['content'])
        read_access = cont['read']
        write_access = cont['write']
        logger.info('... found permissions for reading: %s and writing: %s', read_access, write_access)
        return read_access, write_access

    def get_queue_folder(self):
        logger.info('Getting get queue upload folder for this server')
        resource = app.config.get('MTBLS_WS_RESOURCES_PATH') + "/study/getQueueFolder"
        url = app.config.get('MTBLS_WS_HOST') + app.config.get('MTBLS_WS_PORT') + resource
        resp = requests.get(url)
        if resp.status_code != 200:
            abort(resp.status_code)

        text_resp = resp.content
        return text_resp

    def create_upload_folder(self, study_id):
        logger.info('Creating a new study upload folder for Study %s', study_id)
        resource = app.config.get('WS_APP_BASE_LINK') + "/" + study_id + "/files/requestFtpFolder"
        # https://www.ebi.ac.uk/metabolights/<STUDY>/files/requestFtpFolder
        url = app.config.get('MTBLS_WS_HOST') + app.config.get('MTBLS_WS_PORT') + resource
        resp = requests.get(url)
        if resp.status_code != 200:
            abort(resp.status_code)

        return resp.status_code

