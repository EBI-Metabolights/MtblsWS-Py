#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-May-16
#  Modified by:   kenneth
#
#  Copyright 2019 EMBL - European Bioinformatics Institute
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import psycopg2
import traceback
import os
import logging
import re
from flask import current_app as app
from app.ws.utils import get_single_file_information, check_user_token

logger = logging.getLogger('wslog')

query_curation_log = "select * from curation_log_temp order by acc_short asc;"

query_all_studies = """
    select * from (
        select s.acc, 
          string_agg(u.firstname || ' ' || u.lastname, ', ') as username, 
          to_char(s.releasedate, 'YYYYMMDD') as release_date,
          to_char(s.updatedate, 'YYYYMMDD') as update_date, 
          case when s.status = 0 then 'Submitted' 
               when s.status = 1 then 'In Curation'
               when s.status = 2 then 'In Review'
               when s.status = 3 then 'Public'
               else 'Dormant' end as status,
          curator
        from 
          studies s,
          study_user su,
          users u
        where
           date_trunc('day',s.updatedate)>=date_trunc('day',current_date-90) and
           s.id = su.studyid and
           su.userid = u.id
    group by 1,3,4,5,6) status
    where exists (select 1 from users where apitoken = (%s) and role = 1);"""

query_studies_user = """
    SELECT distinct s.acc, 
    to_char(s.releasedate,'YYYY-MM-DD'), 
    to_char(s.submissiondate,'YYYY-MM-DD'), 
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


def create_user(first_name, last_name, email, affiliation, affiliation_url, address, orcid, api_token,
                password_encoded, metaspace_api_key):

    insert_user_query = \
        "INSERT INTO users(address, affiliation, affiliationurl, apitoken, email, firstname, " \
        "joindate, lastname, password, role, status, username, orcid, metaspace_api_key) " \
        "VALUES ('address_value', 'affiliation_value', 'affiliationurl_value', 'apitoken_value', 'email_value', " \
        "'firstname_value', current_timestamp, 'lastname_value', 'password_value', 2, 0, 'email_value', " \
        "'orcid_value', 'metaspace_api_key_value' );"

    subs = {"address_value": address, "affiliation_value": affiliation,
            "affiliationurl_value": affiliation_url, "apitoken_value": api_token,
            "email_value": email, "firstname_value": first_name, "lastname_value": last_name,
            "password_value": password_encoded, "orcid_value": orcid, "metaspace_api_key_value": metaspace_api_key}

    for key, value in subs.items():
        insert_user_query = insert_user_query.replace(str(key), str(value))

    query = insert_user_query

    try:
        params = app.config.get('DB_PARAMS')
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        conn.close()
        return True, "User account '" + email + "' created successfully"

    except Exception as e:
        return False, str(e)


def update_user(first_name, last_name, email, affiliation, affiliation_url, address, orcid, api_token,
                password_encoded, existing_user_name, is_curator, metaspace_api_key):

    update_user_query = \
        "update users set address = 'address_value', affiliation = 'affiliation_value', " \
        "affiliationurl = 'affiliationurl_value', email = 'email_value', " \
        "firstname = 'firstname_value', lastname = 'lastname_value', username = 'email_value', " \
        "orcid = 'orcid_value', metaspace_api_key = 'metaspace_api_key_value' " \
        "where username = 'existing_user_name_value'"

    if not is_curator:
        update_user_query = update_user_query + " and apitoken = 'apitoken_value'"

    update_user_query = update_user_query + ";"

    subs = {"address_value": address, "affiliation_value": affiliation,
            "affiliationurl_value": affiliation_url, "apitoken_value": api_token,
            "email_value": email, "firstname_value": first_name, "lastname_value": last_name,
            "password_value": password_encoded, "orcid_value": orcid, "username_value": email,
            "existing_user_name_value": existing_user_name, "metaspace_api_key_value": metaspace_api_key}

    for key, value in subs.items():
        update_user_query = update_user_query.replace(str(key), str(value))

    query = update_user_query

    try:
        params = app.config.get('DB_PARAMS')
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(query)
        number_of_users = cursor.rowcount
        conn.commit()
        conn.close()

        if number_of_users == 1:
            return True, "User account '" + existing_user_name + "' updated successfully"
        else:
            return False, "User account '" + existing_user_name + "' could not be updated"

    except Exception as e:
        return False, str(e)


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
        submission_date = row[2]
        status = row[3]

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
                              'createdDate': submission_date,
                              'status': status.strip(),
                              'title': title.strip(),
                              'description': description.strip()})

    return complete_list


def get_all_studies(user_token):
    data = execute_query(query_all_studies, user_token)
    return data


def get_curation_log(user_token):
    data = execute_query(query_curation_log, user_token)
    return data


def biostudies_acc_to_mtbls(biostudies_id):

    if not biostudies_id:
        return None

    # Default query to get the mtbls accession
    query = "SELECT acc from studies where biostudies_acc = '#biostudies_id#';"
    query = query.replace("#biostudies_id#", biostudies_id)
    query = query.replace('\\', '')

    try:
        params = app.config.get('DB_PARAMS')
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        conn.close()
        return data[0]

    except Exception as e:
        return False, "MTBLS accession was not found for BioStudies accession " + biostudies_id


def biostudies_accession(study_id, biostudies_id, method):

    if not study_id:
        return None

    # Default query to get the biosd accession
    s_query = "SELECT biostudies_acc from studies where acc = '#study_id#';"

    if method == 'add':
        query = "update studies set biostudies_acc = '#biostudies_acc#' where acc = '#study_id#';"
    elif method == 'query':
        query = s_query
    elif method == 'delete':
        query = "update studies set biostudies_acc = '' where acc = '#study_id#';"

    query = query.replace("#study_id#", study_id)
    s_query = s_query.replace("#study_id#", study_id)
    if biostudies_id:
        query = query.replace("#biostudies_acc#", biostudies_id)
    query = query.replace('\\', '')

    try:
        params = app.config.get('DB_PARAMS')
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(query)

        if method == 'add' or method == 'delete':
            conn.commit()
            cursor.execute(s_query)

        data = cursor.fetchall()
        conn.close()
        return True, data[0]

    except Exception as e:
        return False, "BioStudies accession was not added to the study"


def check_access_rights(user_token, study_id):

    study_list = execute_query(query_user_access_rights, user_token, study_id)
    if study_list is None or not check_user_token(user_token):
        return False, False, False, 'ERROR', 'ERROR', 'ERROR', 'ERROR', 'ERROR', 'ERROR'

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

    return is_curator, read_access, write_access, obfuscation_code, complete_study_location, release_date, \
           submission_date, updated_date, study_status


def study_submitters(study_id, user_email, method):

    if not study_id or len(user_email) < 5:
        return None

    if method == 'add':
        query = 'insert into study_user(userid,studyid) ' \
                'select u.id, s.id from users u, studies s where u.email = %s and acc=%s;'
    elif method == 'delete':
        query = 'delete from study_user su where exists(' \
                'select u.id, s.id from users u, studies s ' \
                'where su.userid = u.id and su.studyid = s.id and u.email = %s and acc=%s);'

    try:
        params = app.config.get('DB_PARAMS')
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(query, (user_email, study_id))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        return False


def override_validations(study_id, method, override=""):

    if not study_id:
        return None

    if method == 'query':
        query = "select override from studies where acc = '#study_id#';"
    elif method == 'update':
        query = "update studies set override = '#override#' where acc = '#study_id#';"

    try:
        params = app.config.get('DB_PARAMS')
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()

        if method == 'query':
            query = query.replace("#study_id#", study_id.upper())
            query = query.replace('\\', '')
            cursor.execute(query)
            data = cursor.fetchall()
            conn.close()
            return data[0]
        elif method == 'update' and override:
            query = query.replace("#study_id#", study_id.upper())
            query = query.replace("#override#", override)
            query = query.replace('\\', '')
            cursor.execute(query)
            conn.commit()
            conn.close()
    except Exception as e:
        return False


def update_study_status(study_id, study_status):
    status = '0'
    study_status = study_status.lower()
    if study_status == 'submitted':
        status = '0'
    elif study_status == 'in curation':
        status = '1'
    elif study_status == 'in review':
        status = '2'
    elif study_status == 'public':
        status = '3'
    elif study_status == 'dormant':
        status = '4'

    query = "update studies set status = '" + status + "' where acc = '" + study_id + "';"

    try:
        params = app.config.get('DB_PARAMS')
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error('Database update of study status failed with error ' + str(e))
        return False


def execute_query(query, user_token, study_id=None):

    if not user_token:
        return None

    data = []

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
