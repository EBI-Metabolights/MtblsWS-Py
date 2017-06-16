import logging
import config
import requests
from flask_restful import abort

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

        update_folder = std_folder + config.UPDATE_PATH_SUFFIX
        logger.info('... found updates folder %s', update_folder)
        return update_folder

    def get_study(self, study_id, user_token):
        """
        Get the JSON object for a MTBLS study
        by calling current Java-based WS
            {{server}}{{port}}/metabolights/webservice/study/MTBLS_ID

        :param study_id: Identifier of the study in MetaboLights
        :param user_token: User API token. Used to check for permissions
        """
        logger.info('Getting JSON object for Study %s, using API-Key %s', study_id, user_token)
        resource = config.MTBLS_WS_RESOURCES_PATH + "/study/" + study_id
        url = config.MTBLS_WS_HOST + config.MTBLS_WS_PORT + resource
        resp = requests.get(url, headers={"user_token": user_token})
        if resp.status_code != 200:
            if resp.status_code == 401:
                abort(401)
            if resp.status_code == 403:
                abort(403)
            if resp.status_code == 404:
                abort(404)
            if resp.status_code == 500:
                abort(500)

        json_resp = resp.json()

        # double check for errors
        if json_resp["err"] is not None:
            if user_token is None:
                abort(401)
            else:
                abort(403)

        logger.info('... found Study  %s', json_resp['content']['title'])
        return json_resp

    def get_study_status(self, study_id, user_token):
        """
        Get the status of the Study: PUBLIC, INCURATION, ...
        :param study_id:
        :param user_token:
        :return:
        """
        logger.info('Getting the status of the Study, using API-Key %s', study_id, user_token)
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
        resource = config.MTBLS_WS_RESOURCES_PATH + "/study/list"
        url = config.MTBLS_WS_HOST + config.MTBLS_WS_PORT + resource
        resp = requests.get(url)

        if resp.status_code != 200:
            if resp.status_code == 401:
                abort(401)
            if resp.status_code == 403:
                abort(403)
            if resp.status_code == 404:
                abort(404)
            if resp.status_code == 500:
                abort(500)

        json_resp = resp.json()
        logger.info('... found %d public studies', len(json_resp['content']))
        return json_resp

    def is_user_token_valid(self, user_token):
        logger.info('Checking for user credentials in MTBLS-Labs')
        resource = config.MTBLS_WS_RESOURCES_PATH + "/labs/" + "authenticateToken"
        url = config.MTBLS_WS_HOST + config.MTBLS_WS_PORT + resource
        resp = requests.post(url, data='{"token":"' + user_token + '"}')
        if resp.status_code != 200:
            if resp.status_code == 401:
                abort(401)
            if resp.status_code == 403:
                abort(403)
            if resp.status_code == 404:
                abort(404)
            if resp.status_code == 500:
                abort(500)

        user = resp.headers.get('user')
        jwt = resp.headers.get('jwt')
        if user is None or jwt is None:
            abort(403)
        logger.info('... found user %s with jwt key: %s', user, jwt)
        return True
