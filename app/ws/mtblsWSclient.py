import os
import config
import requests
from flask import jsonify, Response

"""
MetaboLights WS client

Use the Java-based REST resources provided from MTBLS

author: jrmacias@ebi.ac.uk
date: 20160520
"""


class WsClient:

    def get_study_location(self, study_id, user_token):
        """
        Get the actual location of the study files in the File System

        :param study_id: Identifier of the study in MetaboLights
        :param user_token: User API token. Used to check for permissions
        """
        if config.DEBUG:
            return os.path.normpath(os.path.join(config.STUDIES_INPUT_PATH, study_id))
        else:
            resource = config.MTBLS_WS_RESOURCES_PATH + "/study/" + study_id
            url = config.MTBLS_WS_HOST + config.MTBLS_WS_PORT + resource
            resp = requests.get(url, headers={"user_token": user_token}).json()
            return resp["content"]["studyLocation"]

    def get_study_updates_location(self, study_id, user_token):
        """
        Get location for output updates in a MetaboLights study (possibly a user MTBLS-Labs folder)

        :param study_id:
        :param user_token:
        :return:
        """
        if config.DEBUG:
            return os.path.normpath(os.path.join(config.STUDIES_OUTPUT_PATH, study_id))
        else:
            resource = config.MTBLS_WS_RESOURCES_PATH + "/study" + study_id + "/updates"
            url = config.MTBLS_WS_HOST + config.MTBLS_WS_PORT + resource
            resp = requests.get(url, headers={"user_token": user_token}).json()
            return resp["content"]["studyLocation"]

    def get_study(self, study_id, user_token):
        """
        Get the JSON object for a MTBLS study
        by calling current Java-based WS
            {{server}}{{port}}/metabolights/webservice/study/MTBLS_ID

        :param study_id: Identifier of the study in MetaboLights
        :param user_token: User API token. Used to check for permissions
        """
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
