import logging

from app.tasks.worker import (MetabolightsTask, celery, send_email)
from app.ws.db_connection import get_all_studies
from app.config import get_settings
from jira import JIRA
from app.ws.utils import safe_str

logger = logging.getLogger(__name__)
options = {
    'server': 'https://www.ebi.ac.uk/panda/jira/'}
project = 12732  # The id for the 'METLIGHT' project
curation_epic = 'METLIGHT-1'  # id 10236 'METLIGHT-1 Epic'
curation_lable = 'curation'

@celery.task(base=MetabolightsTask, name="app.tasks.common_tasks.admin_tasks.create_jira_tickets.update_or_create_jira_issue_task")
def update_or_create_jira_issue_task(user_token: str):
    email = "metabolights-dev@ebi.ac.uk"
    try:
        params = get_settings().jira.connection
        user_name = params.username
        password = params.password
        default_curator = 'metabolights-api'

        updated_studies = []
        try:
            jira = JIRA(options=options, basic_auth=(user_name, password))
        except:
            return False, 'Could not connect to JIRA server, incorrect username or password?', updated_studies

        # Get the MetaboLights project
        mtbls_project = jira.project(project)

        studies = get_all_studies(user_token)
        for study in studies:
            study_id = None
            user_name = None
            release_date = None
            update_date = None
            study_status = None
            curator = None
            status_change = None
            curation_due_date = None

            try:
                study_id = safe_str(study[0])
                user_name = safe_str(study[1])
                release_date = safe_str(study[2])
                update_date = safe_str(study[3])
                study_status = safe_str(study[4])
                curator = safe_str(study[5])
                status_change = safe_str(study[6])
                curation_due_date = safe_str(study[7])
            except Exception as e:
                logger.error(str(e))
            issue = []
            summary = None

            # date is 'YYYY-MM-DD HH24:MI'
            due_date = status_change[:10]

            logger.info('Checking Jira ticket for ' + study_id + '. Values: ' +
                        user_name + '|' + release_date + '|' + update_date + '|' + study_status + '|' +
                        curator + '|' + status_change + '|' + due_date)

            # Get an issue based on a study accession search pattern
            search_param = "project='" + mtbls_project.key + "' AND summary  ~ '" + study_id + r" \\\-\\\ 20*'"
            issues = jira.search_issues(search_param)  # project = MetaboLights AND summary ~ 'MTBLS121 '
            new_summary = study_id + ' - ' + release_date.replace('-', '') + ' - ' + \
                          study_status + ' (' + user_name + ')'
            try:
                if issues:
                    issue = issues[0]
                else:
                    if study_status == 'Provisional' or study_status == 'Private':
                        logger.info("Could not find Jira issue for " + search_param)
                        print("Creating new Jira issue for " + search_param)
                        issue = jira.create_issue(project=mtbls_project.key, summary='MTBLS study - To be updated',
                                                  description='Created by API', issuetype={'name': 'Story'})
                    else:
                        continue  # Only create new cases if the study is in status Provisional/Private
            except:  # We could not find or create a Jira issue.
                continue

            summary = issue.fields.summary  # Follow pattern 'MTBLS123 - YYYYMMDD - Status'

            if not summary.startswith('MTBLS'):
                continue  # Skip all cases that are not related the study accession numbers

            try:
                assignee = issue.fields.assignee.name
            except:
                assignee = ""

            assignee_changed = False
            valid_curator = False
            jira_curator = ""
            if curator:
                if curator.lower() == 'mark':
                    jira_curator = 'mwilliam'
                    valid_curator = True
                elif curator.lower() == 'pamela':
                    jira_curator = 'ppruski'
                    valid_curator = False
                elif curator.lower() == 'xuefei' or curator.lower() == 'reza' or curator.lower() == 'keeva':
                    jira_curator = default_curator  # We do not have a current curation listed in the log
                    valid_curator = True

                assignee_changed = True if assignee != jira_curator else False
            else:
                jira_curator = ""

            if not status_change:
                status_change = "No status changed date reported"
            # Release date or status has changed, or the assignee (curator) has changed

            summary_changed = True if summary != new_summary else False
            curator_update = True if assignee != default_curator and jira_curator != default_curator else False
            if assignee_changed or summary_changed:

                # Add "Curation" Epic
                issues_to_add = [issue.key]
                jira.add_issues_to_epic(curation_epic, issues_to_add)  # Add the Curation Epic
                labels = maintain_jira_labels(issue, study_status, user_name)

                # Add a comment to the issue.
                comment_text = 'Current status ' + study_status + '. Status last changed date ' + status_change + \
                               '. Curation due date ' + due_date + '. Database update date ' + update_date
                if jira_curator == default_curator:
                    comment_text = comment_text + '. Default curator has been changed from "' \
                                   + curator + '" to "' + default_curator + '"'
                if assignee_changed:
                    comment_text = comment_text + '. Curator in Jira changed from "' + assignee + '" to "' + jira_curator + '"'

                if summary_changed:
                    comment_text = comment_text + '. Summary in Jira changed from "' + summary + '" to "' + new_summary + '"'

                jira.add_comment(issue, comment_text)

                # Change the issue's summary, comments and description.
                issue.update(summary=new_summary, fields={"labels": labels}, notify=False)

                # if valid_curator:  # ToDo, what if the curation log is not up to date?
                issue.update(assignee={'name': jira_curator}, notify=False)

                updated_studies.append(study_id)
                logger.info('Updated Jira case for study ' + study_id)
                print('Updated Jira case for study ' + study_id)
    except Exception as e:
        logger.error("Jira updated failed !" + str(e))
        send_email("Jira Tickets creation Task", "Ticket(s) update failed", None, email, None)
        return False, 'Update failed: ' + str(e), str(study_id)
    send_email("Jira Tickets creation Task", "Ticket(s) updated successfully", None, email, None)
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
    submitted_label = 'provisional'
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

    if study_status == 'Provisional' and not submitted_flag:  # The "Submitted" label is not present
        labels.append(submitted_label)

    if study_status == 'Private':
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