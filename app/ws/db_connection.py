import psycopg2
import traceback
import os
import logging
import re
from flask import current_app as app
from app.ws.utils import get_single_file_information

logger = logging.getLogger('wslog')

query_all_studies = """
     select s.acc, 
     to_char(s.releasedate, 'YYYYMMDD'), 
     to_char(s.updatedate, 'YYYYMMDD'), 
     case when s.status = 0 then 'Submitted' 
                  when s.status = 1 then 'In Curation'
                  when s.status = 2 then 'In Review'
                  when s.status = 3 then 'Public'
                  else 'Dormant' end as status
    from studies s
    where exists (select 1 from users where apitoken = (%s) and role = 1);"""

query_studies_user = """
    SELECT distinct s.acc, 
    to_char(s.releasedate,'YYYY-MM-DD'), 
      case when s.status = 0 then 'Submitted' 
           when s.status = 1 then 'In Curation'
           when s.status = 2 then 'In Review' 
           when s.status = 3 then 'Public' 
           else 'Dormant' end 
    from studies s, users u, study_user su 
    where s.id = su.studyid and su.userid = u.id and u.apitoken = (%s);
    """

query_user_access_rights = """
    select distinct role, read, write, obfuscationcode, releasedate, submissiondate, 
    case when status = 0 then 'Submitted' when status = 1 then 'In Curation' when status = 2 then 'In Review' 
         when status = 3 then 'Public' else 'Dormant' end as status, 
    acc from
    ( 
        select 'curator' as role, 'True' as read, 'True' as write, s.obfuscationcode, s.releasedate, s.submissiondate, s.status, s.acc from studies s
        where exists (select 1 from users where apitoken = '#user_token#' and role = 1)
        and acc = '#study_id#' -- CURATOR
        union select * from (
            select 'user' as role, 'True' as read, 'True' as write, s.obfuscationcode, s.releasedate, s.submissiondate, s.status, s.acc 
            from studies s, study_user su, users u
            where s.acc = '#study_id#' and s.status = 0 and s.id = su.studyid and su.userid = u.id and 
            u.apitoken = '#user_token#' -- USER own data, submitted
            union
            select 'user' as role, 'True' as read, 'False' as write, s.obfuscationcode, s.releasedate, s.submissiondate, s.status, s.acc 
            from studies s, study_user su, users u
            where s.acc = '#study_id#' and s.status in (1, 2, 4) and s.id = su.studyid and su.userid = u.id and 
            u.apitoken = '#user_token#' -- USER own data, in curation, review or dormant
            union 
            select 'user' as role, 'True' as read, 'False' as write, s.obfuscationcode, s.releasedate, s.submissiondate, s.status, s.acc 
            from studies s where acc = '#study_id#' and status = 3) user_data 
        where not exists(select 1 from users where apitoken = '#user_token#' and role = 1)
    ) study_user_data;
"""


def get_all_studies_for_user(user_token):

    study_list = execute_query(query_studies_user, user_token)
    study_location = app.config.get('STUDY_PATH')
    file_name = 'i_Investigation.txt'
    isa_title = 'Study Title'
    isa_descr = 'Study Description'

    complete_list = []
    for i, row in enumerate(study_list):
        title = 'N/A'
        description = 'N/A'

        study_id = row[0]
        release_date = row[1]
        status = row[2]

        complete_study_location = os.path.join(study_location, study_id)
        complete_file_name = os.path.join(complete_study_location, file_name)

        logger.info('Trying to load the investigation file (%s) for Study %s', file_name, study_id)
        # Get the Assay table or create a new one if it does not already exist
        try:
            with open(complete_file_name, encoding='utf-8') as f:
                for line in f:
                    line = re.sub('\s+', ' ', line)
                    if line.startswith(isa_title):
                        title = line.replace(isa_title, '').replace(' "', '').replace('" ', '')
                    if line.startswith(isa_descr):
                        description = line.replace(isa_descr, '').replace(' "', '').replace('" ', '')
        except FileNotFoundError:
            logger.error("The file %s was not found", complete_file_name)

        complete_list.append({'accession': study_id,
                              'updated': get_single_file_information(complete_file_name),
                              'releaseDate': release_date,
                              'status': status,
                              'title': title,
                              'description': description})

    return complete_list


def get_all_studies(user_token):
    data = execute_query(query_all_studies, user_token)
    return data


def check_access_rights(user_token, study_id):

    study_list = execute_query(query_user_access_rights, user_token, study_id)
    study_location = app.config.get('STUDY_PATH')
    complete_study_location = os.path.join(study_location, study_id)
    complete_file_name = os.path.join(complete_study_location, 'i_Investigation.txt')
    isa_date_format = "%Y-%m-%d"
    is_curator = False
    read_access = False
    write_access = False
    obfuscation_code = ""
    release_date = None
    submission_date = None
    updated_date = None
    study_status = ""

    for i, row in enumerate(study_list):
        role = row[0]
        read_access = row[1]
        if read_access == 'True':
            read_access = True
        else:
            read_access = False

        write_access = row[2]
        if write_access == 'True':
            write_access = True
        else:
            write_access = False

        obfuscation_code = row[3]
        release_date = row[4]
        # release_date = release_date.strftime("%c")
        release_date = release_date.strftime(isa_date_format)

        submission_date = row[5]
        submission_date = submission_date.strftime(isa_date_format)
        study_status = row[6]
        acc = row[7]

        updated_date = get_single_file_information(complete_file_name)

        if role == 'curator':
            is_curator = True
            break  # The api-code gives you 100% access rights, so no need to check any further

    return is_curator, read_access, write_access, obfuscation_code, complete_study_location, release_date, submission_date, updated_date, study_status


def execute_query(query, user_token, study_id=None):
    try:
        params = app.config.get('DB_PARAMS')
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = query.replace('\\', '')
        if study_id is None:
            cursor.execute(query, [user_token])
        else:
            query2 = query_user_access_rights.replace("#user_token#", user_token)
            query2 = query2.replace("#study_id#", study_id)
            cursor.execute(query2)
        data = cursor.fetchall()
        conn.close()

        return data

    except psycopg2.Error as e:
        print("Unable to connect to the database")
        print(e.pgcode)
        print(e.pgerror)
        print(traceback.format_exc())
