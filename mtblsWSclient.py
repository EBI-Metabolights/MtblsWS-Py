import os
import config
import requests
from flask import jsonify

"""
MetaboLights WS client

Use the Java-based REST resources provided from MTBLS

author: jrmacias
date: 20160520
"""


class WsClient:

    def get_study_location(self, study_id, user_token):
        """
        Get the actual location of the study files in the File System

        :param study_id: Identifier of the study IN MetaboLights
        :param user_token: User API token. Used to check for permissions
        """
        if config.DEBUG:
            return jsonify({'Study-Location': os.path.normpath(os.path.join(config.PROJECT_PATH, config.TEST_DATA_PATH))})
        else:
            resource = config.MTBLS_WS_RESOURCES_PATH + '/study/' + study_id
            url = config.MTBLS_WS_HOST + config.MTBLS_WS_PORT + resource
            resp = requests.get(url, headers={"user_token": user_token}).json()
            return jsonify({'Study-Location': resp['content']['studyLocation']})
