import psycopg2
import traceback
import os
import logging
import re
from flask import current_app as app

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

        complete_list.append({'accession': study_id, 'release-date': release_date, 'status': status,
                              'title': title, 'description': description})

    return complete_list


def execute_query(query, user_token):
    try:
        params = app.config.get('DB_PARAMS')
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = query.replace('\\', '')
        cursor.execute(query, [user_token])
        data = cursor.fetchall()
        conn.close()

        return data

    except psycopg2.Error as e:
        print("Unable to connect to the database")
        print(e.pgcode)
        print(e.pgerror)
        print(traceback.format_exc())
