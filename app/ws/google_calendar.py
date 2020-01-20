#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Jan-17
#  Modified by:   kenneth
#
#  Copyright 2020 EMBL - European Bioinformatics Institute
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import logging
import os.path
import pickle

from flask import request, abort, current_app as app
from flask_restful import Resource
from flask_restful_swagger import swagger
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from app.ws.db_connection import get_all_studies
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import safe_str

logger = logging.getLogger('wslog')
wsc = WsClient()


def get_google_calendar_events():
    resource_folder = os.path.join('.', 'resources')
    pickle_file = os.path.join(resource_folder, 'token.pickle')
    # https://developers.google.com/calendar/quickstart/python
    SCOPES = ['https://www.googleapis.com/auth/calendar']
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is created automatically
    # when the authorization flow completes for the first time.
    if os.path.exists(pickle_file):
        with open(pickle_file, 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    # Run this locally to get a new pickle file
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(app.config.get('GOOGLE_CALENDAR_TOKEN'), SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(pickle_file, 'wb') as token:
            pickle.dump(creds, token)
    service = build('calendar', 'v3', credentials=creds)

    calendar_id = app.config.get('GOOGLE_CALENDAR_ID')
    events = service.events().list(calendarId=calendar_id, maxResults=2500).execute()

    return service, events


def add_calendar_event(events, service, study_id=None, study_status=None, due_date=None):
    if study_status.lower() == 'in curation':
        add_google_calendar_event(events, service, event_text=study_id, event_date=due_date, delete_only=False)
    else:
        add_google_calendar_event(events, service, event_text=study_id, event_date=due_date, delete_only=True)


def add_google_calendar_event(events, service,  event_text=None, event_date=None, delete_only=False):
    # Refer to the Python quickstart on how to setup the environment:
    # https://developers.google.com/calendar/quickstart/python
    # Change the scope to 'https://www.googleapis.com/auth/calendar' and delete any
    # stored credentials.

    _event = {
        'summary': event_text,
        'start': {
            'date': event_date
        },
        'end': {
            'date': event_date
        },
        'reminders': {
            'useDefault': False,
            'overrides': [
                {'method': 'email', 'minutes': 24 * 60},
                {'method': 'popup', 'minutes': 10},
            ],
        },
    }

    # events = None
    calendar_id = app.config.get('GOOGLE_CALENDAR_ID')
    for existing_event in events['items']:
        existing_id = existing_event['summary']
        existing_date = existing_event['start']['date']
        if event_text == existing_id:  # and event_date != existing_date:
            msg = 'Event already exists, deleting: ' + \
                  event_text + ' ' + event_date + ' ' + existing_event.get('id')
            logger.info(msg)
            print(msg)
            service.events().delete(calendarId=calendar_id, eventId=existing_event['id']).execute()

    # Add new event
    if not delete_only:
        try:
            new_event = service.events().insert(calendarId=calendar_id, body=_event).execute()
            created_text = 'Event created: ' + event_text + ' ' + event_date
            logger.info(created_text)
            print(created_text)
        except Exception as e:
            error_text = 'Event ' + event_text + ' could not be created on the ' + event_date + '. Error: ' + str(e)
            logger.error(error_text)
            print('Error: ' + error_text)


class GoogleCalendar(Resource):
    @swagger.operation(
        summary="Create (or update) Jira tickets for MetaboLights study curation (curator only)",
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
    def post(self):
        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None:
            abort(401)

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions('MTBLS4', user_token)
        if not is_curator:
            abort(403)

        status, message = update_or_create_calendar_entries(user_token=user_token)

        if status:
            return {'Success': message}
        else:
            return {'Error': message}


def update_or_create_calendar_entries(user_token=None):
    try:

        studies = get_all_studies(user_token)
        service, events = get_google_calendar_events()

        for study in studies:
            study_id = safe_str(study[0])
            user_name = safe_str(study[1])
            release_date = safe_str(study[2])
            update_date = safe_str(study[3])
            study_status = safe_str(study[4])
            curator = safe_str(study[5])
            status_change = safe_str(study[6])
            curation_due_date = safe_str(study[7])
            issue = []
            summary = None

            # date is 'YYYY-MM-DD HH24:MI'
            due_date = status_change[:10]

            logger.info('Updating Google Calendar for ' + study_id + '. Values: ' +
                        user_name + '|' + release_date + '|' + update_date + '|' + study_status + '|' +
                        curator + '|' + status_change + '|' + due_date)

            add_calendar_event(events, service, study_id=study_id, study_status=study_status, due_date=due_date)

            logger.info('Updated Google Calendar for study ' + study_id)
            print('Updated Google Calendar for study ' + study_id)
    except Exception as e:
        logger.error("Google Calendar update failed for " + study_id + ". " + str(e))
        return False, 'Google Calendar update failed: ' + str(e)
    return True, 'Google Calendar entries updated successfully'
