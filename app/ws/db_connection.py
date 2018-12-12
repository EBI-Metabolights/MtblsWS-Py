import psycopg2
import traceback
import os
import logging
import re
from flask import current_app as app
from app.ws.utils import get_single_file_information

logger = logging.getLogger('wslog')


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
select distinct role, rw, obfuscationcode, releasedate, submissiondate, 
case when status = 0 then 'Submitted' 
              when status = 1 then 'In Curation'
              when status = 2 then 'In Review'
              when status = 3 then 'Public'
              else 'Dormant' end as status, 
acc 
from
(  select 'curator' as role, 'rw' as rw, s.obfuscationcode, s.releasedate, s.submissiondate, s.status, s.acc 
   from studies s
   where exists (select 1 from users where apitoken = '#user_token#' and role = 1) --user_token
   and acc = '#study_id#' -- CURATOR --study_id
 union
   select 'user' as role, 'rw' as rw, s.obfuscationcode, s.releasedate, s.submissiondate, s.status, s.acc 
   from studies s, study_user su, users u
   where s.acc = '#study_id#' and s.status = 1 and s.id = su.studyid and su.userid = u.id and  --study_id
   u.apitoken = '#user_token#' -- USER own data, submitted  --user_token
 union
   select 'user' as role, 'r' as rw, s.obfuscationcode, s.releasedate, s.submissiondate, s.status, s.acc 
   from studies s, study_user su, users u
   where s.acc = '#study_id#' and s.status != 3 and s.id = su.studyid and su.userid = u.id and  --study_id
   u.apitoken = '#user_token#' -- USER own data, not submitted  --user_token
union 
   select 'user' as role, 'r' as rw, s.obfuscationcode, s.releasedate, s.submissiondate, s.status, s.acc 
   from studies s where acc = '#study_id#' and status = 3 
   and not exists(select 1 from users where apitoken = '#user_token#' and role = 1)  --user_token
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


def check_access_rights(user_token, study_id):

    study_list = execute_query(query_user_access_rights, user_token, study_id)
    study_location = app.config.get('STUDY_PATH')

    complete_list = []
    for i, row in enumerate(study_list):
        role = row[0]
        rw = row[1]
        obfuscationcode = row[2]
        submissiondate = row[3]
        releasedate = row[4]
        submissiondate = row[5]
        status = row[5]
        acc = row[6]
        complete_list.append({'user_role': role, 'read_write': rw, 'api_code': api_code,
                              'obfuscationcode': obfuscationcode, 'releasedate': releasedate,
                              'submissiondate': submissiondate, 'status': status, 'study_location': study_location})

    return complete_list


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
