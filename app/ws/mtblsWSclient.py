import logging
import config
import requests
from flask import jsonify
from flask_restful import abort

"""
MetaboLights WS client

Use the Java-based REST resources provided from MTBLS

author: jrmacias@ebi.ac.uk
date: 20160520
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
        resource = config.MTBLS_WS_RESOURCES_PATH + "/study/" + study_id
        url = config.MTBLS_WS_HOST + config.MTBLS_WS_PORT + resource
        resp = requests.get(url, headers={"user_token": user_token}).json()

        # check response is OK
        if resp['err'] is not None:
            logger.error("Authentication error on Mtbls-WS")
            abort(403, message=resp['err']['message'])

        if resp['content'] is None:
            if resp['message'] == 'Study not found':
                logger.error("Failed to find the MTBLS Study")
                abort(404, message=resp['message'])
            else:
                logger.error("Internal error on Mtbls-WS")
                abort(500, message=resp['err']['message'])

        location = resp["content"]["studyLocation"]
        logger.info('Got %s', location)
        return location

    def get_study_updates_location(self, study_id, user_token):
        """
        Get location for output updates in a MetaboLights study.
        This is where afected files are copied before applying changes, for audit purposes.
        :param study_id:
        :param user_token:
        :return:
        """
        logger.info('Getting location for output update for Study %s on the filesystem, using API-Key %s', study_id, user_token)
        resource = config.MTBLS_WS_RESOURCES_PATH + "/study/" + study_id
        url = config.MTBLS_WS_HOST + config.MTBLS_WS_PORT + resource
        resp = requests.get(url, headers={"user_token": user_token}).json()
        std_folder = resp["content"]["studyLocation"]
        update_folder = std_folder + config.UPDATE_PATH_SUFFIX
        logger.info('Got %s', update_folder)
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
        resp = requests.get(url, headers={"user_token": user_token}).json()
        if resp["err"] is not None:
            response = jsonify({
                "message": resp["message"],
                "cause": resp["err"]["localizedMessage"]
            })
            response.status_code = 403
            return response
        else:
            return resp
