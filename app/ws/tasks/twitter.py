import json
import logging
import os
import time
from datetime import timedelta, datetime
from functools import lru_cache
from typing import Dict

import tweepy
from flask import request, current_app as app
from flask_restful import Resource, abort
from flask_restful_swagger import swagger
from pydantic import BaseSettings

from app.utils import metabolights_exception_handler, MetabolightsException
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study
from app.ws.db.settings import get_directory_settings
from app.ws.db.types import StudyStatus
from app.ws.db.wrappers import update_study_model_from_directory, create_study_model_from_db_study
from app.ws.study.user_service import UserService
from app.ws.utils import log_request

logger = logging.getLogger('wslog')
app_secrets_dir = app.config.get('SECRETS_DIR')


class TwitterSettings(BaseSettings):
    twitter_credentials: Dict = None

    class Config:
        # read and set secrets from this secret directory
        secrets_dir = app_secrets_dir


@lru_cache(1)
def get_twitter_credentials():
    return TwitterSettings()


class PublicStudyTweet(Resource):
    @swagger.operation(
        summary="Query studies and tweet new public ones",
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
            },
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax."
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication. "
                           "Please provide a study id and a valid user token"
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token"
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    @metabolights_exception_handler
    def post(self):
        log_request(request)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            abort(401)
        UserService.get_instance(app).validate_user_has_curator_role(user_token)

        with DBManager.get_instance(app).session_maker() as db_session:
            end = datetime.today()
            start = end - timedelta(days=1)

            query = db_session.query(Study)
            query = query.filter(Study.status == StudyStatus.PUBLIC.value,
                                 Study.releasedate >= start, Study.releasedate < end)
            new_public_studies = query.all()
            api = self.configure_twitter_api()
            directory_settings = get_directory_settings(app)
            study_folders = directory_settings

            for study in new_public_studies:
                m_study = create_study_model_from_db_study(study)
                update_study_model_from_directory(m_study, study_folders, title_and_description_only=True)
                if not study.title:
                    raise MetabolightsException(message=f"Title is not valid for {study.acc}", http_code=501)

                short_title = study.title if len(study.title) <= 60 else study.title[:60]
                url = app.conf.get("WS_APP_BASE_LINK")
                message = f"{study.acc}: {short_title} {url}/{study.acc}"
                api.update_status(status=message)

    @staticmethod
    def configure_twitter_api(twitter_credentials=None):
        if not twitter_credentials:
            twitter_credentials = get_twitter_credentials()

        consumer_key = twitter_credentials["consumer_key"]
        consumer_secret = twitter_credentials["consumer_secret"]
        access_token = twitter_credentials["token"]
        access_token_secret = twitter_credentials["token_secret"]
        # not used now
        bearer = twitter_credentials["bearer"]
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth)
        return api
