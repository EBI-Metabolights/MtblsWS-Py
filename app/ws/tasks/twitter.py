import logging
from datetime import timedelta, datetime

import tweepy
from flask import request, current_app as app
from flask_restful import Resource, abort, reqparse
from flask_restful_swagger import swagger
from app.config import get_settings

from app.utils import metabolights_exception_handler
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study
from app.ws.db.types import StudyStatus
from app.ws.db.wrappers import update_study_model_from_directory, create_study_model_from_db_study
from app.ws.settings.utils import get_study_settings
from app.ws.study.user_service import UserService
from app.ws.utils import log_request

logger = logging.getLogger('wslog')



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
            },
            {
                "name": "release_date",
                "description": "Study release date in DD/MM/YYYY format. Default value is yesterday",
                "paramType": "query",
                "type": "string",
                "required": False,
                "allowMultiple": False
            },
            {
                "name": "dry_run",
                "description": "Query only public studies published on releasedate without posting tweet.",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": 'true'
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

        parser = reqparse.RequestParser()
        parser.add_argument('dry_run')
        parser.add_argument('release_date')
        date_format = "%d/%m/%Y"
        dry_run = False
        release_date = None
        if request.args:
            args = parser.parse_args(req=request)
            dry_run = True if args['dry_run'].lower() == 'true' else False
            release_date_str = args['release_date']
            if release_date_str:
                try:
                    release_date = datetime.strptime(release_date_str, date_format)
                except Exception as e:
                    error = f'Release date parse error {str(e)}'
                    logger.error(error)
                    abort(401)

        if not release_date:
            today = datetime.today()
            today = today.replace(hour=0, minute=0, second=0, microsecond=0)
            release_date = today - timedelta(days=1)

        UserService.get_instance().validate_user_has_curator_role(user_token)

        with DBManager.get_instance().session_maker() as db_session:
            end = release_date + timedelta(days=1)
            start = release_date

            query = db_session.query(Study)
            query = query.filter(Study.status == StudyStatus.PUBLIC.value,
                                 Study.releasedate >= start, Study.releasedate < end)
            new_public_studies = query.all()
            settings = get_study_settings()
            study_folders = settings.mounted_paths.study_metadata_files_root_path
            url = get_settings().server.service.ws_app_base_link

            public_study_messages = []
            public_study_ids = []
            for study in new_public_studies:
                try:
                    m_study = create_study_model_from_db_study(study)
                    update_study_model_from_directory(m_study, study_folders, title_and_description_only=True)
                except Exception as e:
                    logger.error(f"Error while reading title of study for {study.acc}")
                    raise e
                if not m_study.title:
                    logger.error(f"Title is not read or valid for {study.acc}")
                    continue

                short_title = m_study.title if len(m_study.title) <= 60 else m_study.title[:60]
                twitter_message = f"{study.acc}: {short_title} {url}/{study.acc}"
                public_study_messages.append(twitter_message)
                public_study_ids.append(study.acc)

            if public_study_ids:
                public_studies = ', '.join(public_study_ids)
                logger.info(f'New public studies posted on twitter: {public_studies}')
            else:
                logger.info(f'There is no public studies posted on twitter')

            if dry_run:
                tweets = public_study_messages
                return {"message": 'dry_run', 'release_date': str(release_date.strftime(date_format)),
                        'public_studies': public_study_ids, 'twitter_messages': tweets}

            try:
                client = self.twitter_client()
            except Exception as e:
                error = f'Twitter api configuration error {str(e)}'
                logger.error(error)
                return {"message": error, 'release_date': str(release_date.strftime(date_format)),
                        'public_studies': public_study_ids, 'twitter_messages': None}

            twitter_messages = []
            for twitter_message in public_study_messages:
                try:
                    response = client.create_tweet(text=twitter_message)
                    twitter_messages.append(response)
                    result = {"message": 'successful', 'release_date': str(release_date.strftime(date_format)),
                      'public_studies': public_study_ids, 'twitter_messages': twitter_messages}
                except Exception as e:
                    logger.warning(f'Error while sending twitter message {str(e)}')
                    result = {"status": 'failure', 'release_date': str(release_date.strftime(date_format)),
                      'error': str(e), }
            
            return result

    @staticmethod
    def configure_twitter_api(twitter_credentials=None):
        if not twitter_credentials:
            twitter_credentials = get_settings().twitter.connection

        consumer_key = twitter_credentials.consumer_key
        consumer_secret = twitter_credentials.consumer_secret
        access_token = twitter_credentials.token
        access_token_secret = twitter_credentials.token_secret
        # not used now
        bearer = twitter_credentials.bearer
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth)
        return api
    
    @staticmethod
    def twitter_client(twitter_credentials=None):
        if not twitter_credentials:
            twitter_credentials = app.config.get('TWITTER_CREDENTIALS')

        consumer_key = twitter_credentials["consumer_key"]
        consumer_secret = twitter_credentials["consumer_secret"]
        access_token = twitter_credentials["token"]
        access_token_secret = twitter_credentials["token_secret"]
        client = tweepy.Client(consumer_key=consumer_key,
                       consumer_secret=consumer_secret,
                       access_token=access_token,
                       access_token_secret=access_token_secret)
        return client