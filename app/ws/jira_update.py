from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from flask import request, abort, current_app as app
from app.ws.mtblsWSclient import WsClient
from jira import JIRA
from app.ws.db_connection import get_all_studies
import logging

# https://jira.readthedocs.io
options = {
    'server': 'https://www.ebi.ac.uk/panda/jira/'}
logger = logging.getLogger('wslog')
wsc = WsClient()
project = 12732  # The id for the 'METLIGHT' project
curation_epic = 'METLIGHT-1' # id 10236 'METLIGHT-1 Epic'
curation_lable = 'curation'


class Jira(Resource):
    @swagger.operation(
        summary="Create (or update) Jira tickets for MetaboLights study curation.",
        notes="If no study id (accession number) is given, all tickets will be updated.",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier to update Jira ticket for, leave empty for all",
                "required": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
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
    def put(self):

        user_token = None
        study_id = None
        passed_id = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None:
            abort(401)

        parser = reqparse.RequestParser()
        parser.add_argument('study_id', help="Study Identifier to update Jira ticket for, leave empty for all")
        if request.args:
            args = parser.parse_args(req=request)
            study_id = args['study_id']
            passed_id = study_id

        if study_id is None:
            study_id = 'MTBLS121'  # Template LC-MS study. If no study id has been passed, assume curator
            passed_id = None

        study_id = study_id.upper()

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        logger.info('Creating a new study audit folder for study %s', study_id)
        status, message, updated_studies_list = update_or_create_jira_issue(passed_id, user_token, is_curator)

        if status:
            return {'Success': message}
        else:
            return {'Error': message}


def update_or_create_jira_issue(study_id, user_token, is_curator):
    try:
        params = app.config.get('JIRA_PARAMS')
        user_name = params['username']
        password = params['password']

        updated_studies = []
        try:
            jira = JIRA(options=options, basic_auth=(user_name, password))
        except:
            return False, 'Could not connect to JIRA server, incorrect username or password?'

        # Get the MetaboLights project
        mtbls_project = jira.project(project)

        studies = [study_id]
        if not study_id and is_curator:
            studies = get_all_studies(user_token)

        for study in studies:
            study_id = study[0]
            user_name = study[1]
            release_date = study[2]
            update_date = study[3]
            study_status = study[4]
            issue = []
            summary = None

            # Get an issue based on a study accession search pattern
            search_param = "project='" + mtbls_project.key + "' AND summary  ~ '" + study_id + " \\\-\\\ 20*'"
            issues = jira.search_issues(search_param)  # project = MetaboLights AND summary ~ 'MTBLS121 '
            new_summary = study_id + ' - ' + release_date.replace('-', '') + ' - ' + \
                          study_status + ' (' + user_name + ')'
            try:
                if issues:
                    issue = issues[0]
                else:
                    if study_status == 'Submitted':
                        logger.info("Could not find Jira issue for " + search_param)
                        print("Creating new Jira issue for " + search_param)
                        issue = jira.create_issue(project=mtbls_project.key, summary='MTBLS study - To be updated',
                                                  description='Created by API', issuetype={'name': 'Story'})
                    else:
                        continue  # Only create new cases if the study is in status Submitted
            except Exception:  # We could not find or create a Jira issue
                    continue

            summary = issue.fields.summary  # Follow pattern 'MTBLS123 - YYYYMMDD - Status'
            if summary.startswith('MTBLS') and summary != new_summary:  # Release date or status has changed

                # Add "Curation" Epic
                issues_to_add = [issue.key]
                jira.add_issues_to_epic(curation_epic, issues_to_add)  # Add the Curation Epic
                labels = maintain_jira_labels(issue, study_status, user_name)

                # Add a comment to the issue.
                comment_text = 'Status ' + study_status + '. Database update date ' + update_date
                jira.add_comment(issue, comment_text)

                # Change the issue's summary, comments and description.
                issue.update(summary=new_summary, fields={"labels": labels}, notify=False)
                updated_studies.append(study_id)
                logger.info('Updated Jira case for study ' + study_id)
                print('Updated Jira case for study ' + study_id)
    except Exception:
        return False, 'Update failed', updated_studies
    return True, 'Ticket(s) updated successfully', updated_studies


def maintain_jira_labels(issue, study_status, user_name):
    # Add the 'curation' label if needed
    labels = issue.fields.labels

    submitted_flag = False
    curation_flag = False
    in_curation_flag = False
    inreview_flag = False
    metabolon_flag = False
    placeholder_flag = False
    metaspace_flag = False
    submitted_label = 'submitted'
    curation_label = 'curation'
    in_curation_label = 'in_curation'
    inreview_label = 'in_review'
    metabolon_label = 'metabolon'
    placeholder_label = 'placeholder'
    metaspace_label = 'metaspace'

    for i in labels:
        if i == submitted_label:
            submitted_flag = True
        elif i == curation_label:
            curation_flag = True
        elif i == in_curation_label:
            in_curation_flag = True
        elif i == inreview_label:
            inreview_flag = True
        elif i == metabolon_label:
            metabolon_flag = True
        elif i == placeholder_label:
            placeholder_flag = True
        elif i == metaspace_label:
            metaspace_flag = True

    if not curation_flag:  # Do not confuse with the In Curation status, this is a "curator tasks" flag
        labels.append(curation_label)

    if study_status == 'Submitted' and not submitted_flag:  # The "Submitted" label is not present
        labels.append(submitted_label)

    if study_status == 'In Curation':
        if not in_curation_flag:  # The "in_curation" label is not present
            labels.append(in_curation_label)
        if submitted_flag:
            labels.remove(submitted_label)

    if study_status == 'In Review':
        if not inreview_flag:  # The "in_review" label is not present
            labels.append(inreview_label)
        if in_curation_flag:
            labels.remove(in_curation_label)

    if study_status == 'Public':  # The "in_review" label should now be replaced
        if inreview_flag:
            labels.remove(inreview_label)
        if in_curation_flag:
            labels.remove(in_curation_label)
        labels.append('public')

    if "Placeholder" in user_name and not placeholder_flag:
        labels.append(placeholder_label)

    if "Metabolon" in user_name and not metabolon_flag:
        labels.append(metabolon_label)

    if "Metaspace" in user_name and not metaspace_flag:
        labels.append(metaspace_label)

    return labels
