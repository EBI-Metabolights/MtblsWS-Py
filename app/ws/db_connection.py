#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Jan-13
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
import datetime
import logging
import os
import re
import traceback
import uuid

import psycopg2
import psycopg2.extras
from flask import current_app as app, abort
from psycopg2 import pool

from app.ws.utils import get_single_file_information, check_user_token, val_email

logger = logging.getLogger('wslog')

stop_words = "insert", "select", "drop", "delete", "from", "into", "studies", "users", "stableid", "study_user", \
             "curation_log_temp", "ref_", "ebi_reporting", "exists"

query_curation_log = "select * from curation_log_temp order by acc_short asc;"

query_all_studies = """
    select * from (
        select 
          s.acc, 
          string_agg(u.firstname || ' ' || u.lastname, ', ' order by u.lastname) as username, 
          to_char(s.releasedate, 'YYYYMMDD') as release_date,
          to_char(s.updatedate, 'YYYYMMDD') as update_date, 
          case when s.status = 0 then 'Submitted' 
               when s.status = 1 then 'In Curation'
               when s.status = 2 then 'In Review'
               when s.status = 3 then 'Public'
               else 'Dormant' end as status,
          curator,
          to_char(s.status_date, 'YYYY-MM-DD HH24:MI') as status_date,
          to_char(s.status_date + INTERVAL '28 day', 'YYYY-MM-DD') as due_date
        from 
          studies s,
          study_user su,
          users u
        where
           s.id = su.studyid and
           su.userid = u.id
    group by 1,3,4,5,6,7,8) status
    where exists (select 1 from users where apitoken = %(apitoken)s and role = 1);
    """

query_study_info = """
        select * from (
             select s.acc                                                                as studyid,
                    string_agg(u.firstname || ' ' || u.lastname, ', ' order by u.lastname) as username,
                    case
                        when s.status = 0 then 'Submitted'
                        when s.status = 1 then 'In Curation'
                        when s.status = 2 then 'In Review'
                        when s.status = 3 then 'Public'
                        else 'Dormant' end                                               as status,
                    case
                        when s.placeholder = '1' then 'Yes'
                        else ''
                        end                                                              as placeholder
    
    
             from studies s,
                  study_user su,
                  users u
             where s.id = su.studyid
               and su.userid = u.id
        group by 1, 3, 4) status
        where exists(select 1 from users where apitoken = %(apitoken)s and role = 1);
"""

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
    where s.id = su.studyid and su.userid = u.id and u.apitoken = %(apitoken)s;
    """

query_submitted_study_ids_for_user = """
    SELECT distinct s.acc 
    from studies s, users u, study_user su 
    where s.id = su.studyid and su.userid = u.id and u.apitoken = %(user_token)s and s.status=0;
    """

get_next_mtbls_id = """
select (seq + 1) as next_acc from stableid where prefix = %(stable_id_prefix)s;
"""

insert_empty_study = """   
    insert into studies (id, acc, obfuscationcode, releasedate, status, studysize, submissiondate, 
    updatedate, validations, validation_status) 
    values ( 
        (select nextval('hibernate_sequence')),
        %(acc)s, 
        %(obfuscationcode)s,
        %(releasedate)s,
        0, 0, current_timestamp, 
        current_timestamp, '{"entries":[],"status":"RED","passedMinimumRequirement":false,"overriden":false}', 'error');
"""

update_metaboligts_id_sequence = """
    update stableid set seq = (select (seq + 1) as next_acc from stableid where prefix = %(stable_id_prefix)s) 
    where prefix = %(stable_id_prefix)s;
"""
link_study_with_user = """
insert into study_user(userid, studyid) select u.id, s.id from users u, studies s where lower(u.email) = %(email)s and acc=%(acc)s;
"""

insert_empty_study_with_id = """   
    insert into studies (id, acc, obfuscationcode, releasedate, status, studysize, submissiondate, 
    updatedate, validations, validation_status) 
    values ( 
        (select nextval('hibernate_sequence')),
        %(acc)s, 
        %(obfuscationcode)s,
        %(releasedate)s,
        0, 0, current_timestamp, 
        current_timestamp, '{"entries":[],"status":"RED","passedMinimumRequirement":false,"overriden":false}', 'error');
"""

query_user_access_rights = """
    SELECT DISTINCT role, read, write, obfuscationcode, releasedate, submissiondate, 
        CASE    WHEN status = 0 THEN 'Submitted'
                WHEN status = 1 THEN 'In Curation' 
                WHEN status = 2 THEN 'In Review' 
                WHEN status = 3 THEN 'Public' 
                ELSE 'Dormant' end 
        AS status, 
        acc from
        ( 
            SELECT 'curator' as role, 'True' as read, 'True' as write, s.obfuscationcode, 
                    s.releasedate, s.submissiondate, s.status, s.acc from studies s
            WHERE   exists (select 1 from users where apitoken = %(apitoken)s and role = 1)
                    AND acc = %(study_id)s -- CURATOR
            UNION 
            SELECT * from (
                SELECT 'user' as role, 'True' as read, 'True' as write, s.obfuscationcode, s.releasedate, s.submissiondate, s.status, s.acc 
                from studies s, study_user su, users u
                where s.acc = %(study_id)s and s.status = 0 and s.id = su.studyid and su.userid = u.id and 
                u.apitoken = %(apitoken)s -- USER own data, submitted
                UNION
                SELECT 'user' as role, 'True' as read, 'False' as write, s.obfuscationcode, s.releasedate, s.submissiondate, s.status, s.acc 
                from studies s, study_user su, users u
                where s.acc = %(study_id)s and s.status in (1, 2, 4) and s.id = su.studyid and su.userid = u.id and 
                u.apitoken = %(apitoken)s -- USER own data, in curation, review or dormant
                UNION 
                SELECT 'user' as role, 'True' as read, 'False' as write, s.obfuscationcode, s.releasedate, s.submissiondate, s.status, s.acc 
                from studies s where acc = %(study_id)s and status = 3) user_data 
            WHERE NOT EXISTS (SELECT 1 from users where apitoken = %(apitoken)s and role = 1)
        ) study_user_data;
"""

study_by_obfuscation_code_query = """
    select distinct 'user', 'True', 'False', obfuscationcode, releasedate, submissiondate,
    case when status = 0 then 'Submitted'
         when status = 1 then 'In Curation'
         when status = 2 then 'In Review'
         when status = 3 then 'Public'
         else 'Dormant' end as status,
    acc from studies
    where obfuscationcode = %(study_obfuscation_code)s and acc= %(study_id)s;
"""

def create_user(first_name, last_name, email, affiliation, affiliation_url, address, orcid, api_token,
                password_encoded, metaspace_api_key):
    val_email(email)

    insert_user_query = """
        INSERT INTO users (
            address, affiliation, affiliationurl,
            apitoken, email, firstname, 
            joindate,
            lastname, password, 
            role, status,
            username, orcid, metaspace_api_key
            ) 
        VALUES 
        (
            %(address_value)s, %(affiliation_value)s, %(affiliationurl_value)s,
            %(apitoken_value)s, %(email_value)s, %(firstname_value)s,
            current_timestamp, 
            %(lastname_value)s, %(password_value)s, 
            2, 0,
            %(email_value)s, %(orcid_value)s, %(metaspace_api_key_value)s
        );
    """

    input_values = {"address_value": address, "affiliation_value": affiliation,
            "affiliationurl_value": affiliation_url, "apitoken_value": api_token,
            "email_value": email, "firstname_value": first_name, "lastname_value": last_name,
            "password_value": password_encoded, "orcid_value": orcid, "metaspace_api_key_value": metaspace_api_key}

    query = insert_user_query

    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query, input_values)
        conn.commit()
        release_connection(postgresql_pool, conn)
        return True, "User account '" + email + "' created successfully"

    except Exception as e:
        return False, str(e)


def update_user(first_name, last_name, email, affiliation, affiliation_url, address, orcid, api_token,
                password_encoded, existing_user_name, is_curator, metaspace_api_key):
    val_email(existing_user_name)
    val_email(email)

    update_user_query = """
        update users set address = %(address_value)s, affiliation = %(affiliation_value)s,
        affiliationurl = %(affiliationurl_value)s, email = %(email_value)s,
        firstname = %(firstname_value)s, lastname = %(lastname_value)s, username = %(email_value)s,
        orcid = %(orcid_value)s, metaspace_api_key = %(metaspace_api_key_value)s
        where username = %(existing_user_name_value)s
    """


    if not is_curator:
        update_user_query = update_user_query + " and apitoken = %s(apitoken_value)s"

    update_user_query = update_user_query + ";"

    input_values = {"address_value": address,
            "affiliation_value": affiliation,
            "affiliationurl_value": affiliation_url,
            "apitoken_value": api_token,
            "email_value": email,
            "firstname_value": first_name,
            "lastname_value": last_name,
            "password_value": password_encoded,
            "orcid_value": orcid,
            "username_value": email,
            "existing_user_name_value": existing_user_name,
            "metaspace_api_key_value": metaspace_api_key}

    query = update_user_query

    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query, input_values)
        number_of_users = cursor.rowcount
        conn.commit()
        release_connection(postgresql_pool, conn)

        if number_of_users == 1:
            return True, "User account '" + existing_user_name + "' updated successfully"
        else:
            return False, "User account '" + existing_user_name + "' could not be updated"

    except Exception as e:
        return False, str(e)


def get_user(username):
    """
    Get a single user from the database, searching by attribute username.
    First validate the username and then concatenate it into the query.
    :return: a user object as a dict.
    """
    val_query_params(username)
    get_user_query = """
        select firstname, lastname, email, affiliation, affiliationurl, address, orcid, metaspace_api_key 
        from users
        where username = %(username)s;
    """
    postgresql_pool = None
    conn = None
    data = None
    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(get_user_query, {'username': username})
        data = [dict((cursor.description[i][0], value) for i, value in enumerate(row)) for row in cursor.fetchall()]
    except Exception as e:
        logger.error('An error occurred while retrieving user {0}: {1}'.format(username, e))
        abort(500)
    finally:
        release_connection(postgresql_pool, conn)

    if data:
        return {'user': data[0]}
    else:
        # no user found by that username, abort with 404
        abort(404, 'User with username {0} not found.'.format(username))


def get_all_private_studies_for_user(user_token):
    val_query_params(user_token)

    study_list = execute_select_query(query=query_studies_user, user_token=user_token)
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

        if status.strip() == "Submitted":
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


def get_all_studies_for_user(user_token):
    val_query_params(user_token)

    study_list = execute_select_query(query=query_studies_user, user_token=user_token)
    if not study_list:
        return []
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
    data = execute_select_query(query=query_all_studies, user_token=user_token)
    return data


def get_study_info(user_token):
    data = execute_select_query(query=query_study_info, user_token=user_token)
    return data


def get_public_studies_with_methods():
    query = "select acc, studytype from studies where status = 3;"
    postgresql_pool, conn, cursor = get_connection()
    cursor.execute(query)
    data = cursor.fetchall()
    release_connection(postgresql_pool, conn)
    return data


def get_public_studies():
    query = "select acc from studies where status = 3;"
    postgresql_pool, conn, cursor = get_connection()
    cursor.execute(query)
    data = cursor.fetchall()
    release_connection(postgresql_pool, conn)
    return data


def get_private_studies():
    query = "select acc from studies where status = 0;"
    postgresql_pool, conn, cursor = get_connection()
    cursor.execute(query)
    data = cursor.fetchall()
    release_connection(postgresql_pool, conn)
    return data


def get_study_by_type(sType, publicStudy=True):
    q2 = ' '
    if publicStudy:
        q2 = ' status in (2, 3) and '
    input_data = {}
    if type(sType) == str:
        q3 = "studytype = %(study_type)s"
        input_data['study_type'] =  sType
    # fuzzy search
    elif type(sType) == list:
        db_query = []
        counter = 0
        for type_item in sType:
            counter = counter + 1
            input = "study_type_" + str(counter)
            query = "studytype like %("+ input +")s"
            input_data[input] = "%" + type_item + "%"
            db_query.append(query)
        q3 = ' and '.join(db_query)

    else:
        return None

    query = "SELECT acc, studytype FROM studies WHERE {q2} = {q3};".format(q2=q2, q3=q3)
    postgresql_pool, conn, cursor = get_connection()
    cursor.execute(query, input_data)
    data = cursor.fetchall()
    studyID = [r[0] for r in data]
    studytype = [r[1] for r in data]
    return studyID, studytype


def update_release_date(study_id, release_date):
    val_acc(study_id)
    query_update_release_date = "update studies set releasedate = %(releasedate)s where acc = %(study_id)s;"
    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query_update_release_date, {'releasedate': release_date, 'study_id': study_id})
        conn.commit()
        release_connection(postgresql_pool, conn)
        return True, "Date updated for study " + study_id

    except Exception as e:
        return False, str(e)


def add_placeholder_flag(study_id):
    val_acc(study_id)
    query_update = "update studies set placeholder = 1, status = 0 where acc = %(study_id)s;"
    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query_update, {'study_id': study_id})
        conn.commit()
        release_connection(postgresql_pool, conn)
        return True, "Placeholder flag updated for study %s" % (study_id, )

    except Exception as e:
        return False, str(e)

def get_obfuscation_code(study_id):
    val_acc(study_id)
    query = "select obfuscationcode from studies where acc = %(study_id)s;"
    postgresql_pool, conn, cursor = get_connection()
    cursor.execute(query, {'study_id': study_id})
    data = cursor.fetchall()
    release_connection(postgresql_pool, conn)
    return data


def get_study(study_id):
    val_acc(study_id)
    query = '''
    select 
       case
           when s.placeholder = '1' then 'Placeholder'
           when s.status = 0 then 'Submitted'
           when s.status = 1 then 'In Curation'
           when s.status = 2 then 'In Review'
           when s.status = 3 then 'Public'
           else 'Dormant' end                  as status,
       s.studytype,
       su.uname                                as submitter,
       su.country                              as country,
       s.curator,
       s.species,
       s.sample_rows,
       s.assay_rows,
       s.maf_rows,
       s.placeholder,
       s.validation_status,
       s.number_of_files,
       s.studysize ::text   as studySize,
       to_char(s.releasedate, 'YYYY-MM-DD')    as releasedate,
       to_char(s.submissiondate, 'YYYY-MM-DD') as submissiondate,
       to_char(s.updatedate, 'YYYY-MM-DD')     as updatedate

    from studies s
             left join (
        select su.studyid,
               string_agg(u.firstname || ' ' || u.lastname, ', ') as uname,
               string_agg(u.address, ', ')                        as country
        from users u
                 join study_user su on u.id = su.userid
        group by su.studyid
    ) as su on s.id = su.studyid
    where s.acc = %(study_id)s;
'''



    postgresql_pool, conn, cursor = get_connection2()
    cursor.execute(query, {'study_id': study_id})
    data = cursor.fetchall()
    result = []
    for row in data:
        result.append(dict(row))

    release_connection(postgresql_pool, conn)
    return result[0]


def biostudies_acc_to_mtbls(biostudies_id):
    if not biostudies_id:
        return None

    val_query_params(biostudies_id)
    # Default query to get the mtbls accession
    query = "SELECT acc from studies where biostudies_acc = %(biostudies_id)s;"

    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query, {'biostudies_id': biostudies_id})
        data = cursor.fetchall()
        # conn.close()
        release_connection(postgresql_pool, conn)
        return data[0]

    except Exception as e:
        return False, "MTBLS accession was not found for BioStudies accession " + biostudies_id


def biostudies_accession(study_id, biostudies_id, method):
    if not study_id:
        return None

    val_acc(study_id)

    # Default query to get the biosd accession
    s_query = "SELECT biostudies_acc from studies where acc = %(study_id)s;"

    if method == 'add':
        query = "update studies set biostudies_acc = %(biostudies_id)s where acc = %(study_id)s;"
    elif method == 'query':
        query = s_query
    elif method == 'delete':
        query = "update studies set biostudies_acc = '' where acc = %(study_id)s;"

    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query, {'study_id': study_id, 'biostudies_acc': biostudies_id})

        if method == 'add' or method == 'delete':
            conn.commit()
            cursor.execute(s_query, {'study_id': study_id})

        data = cursor.fetchall()
        # conn.close()
        release_connection(postgresql_pool, conn)
        return True, data[0]

    except Exception as e:
        return False, "BioStudies accession was not added to the study"


def mtblc_on_chebi_accession(chebi_id):
    if not chebi_id:
        return None

    if not chebi_id.startswith('CHEBI'):
        logger.error("Incorrect ChEBI accession number string pattern")
        abort(406, "%s incorrect ChEBI accession number string pattern" % chebi_id)

    # Default query to get the biosd accession
    query = "select acc from ref_metabolite where temp_id = %(chebi_id)s;"
    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query, {'chebi_id': chebi_id})
        data = cursor.fetchall()
        release_connection(postgresql_pool, conn)
        return True, data[0]

    except IndexError:
        return False, "No metabolite was found for this ChEBI id"


def check_access_rights(user_token, study_id, study_obfuscation_code=None):
    val_acc(study_id)
    val_query_params(user_token)
    val_query_params(study_obfuscation_code)

    study_list = None
    try:
        study_list = execute_query(query=query_user_access_rights, user_token=user_token, study_id=study_id,
                                   study_obfuscation_code=study_obfuscation_code)
    except Exception as e:
        logger.error("Could not query the database " + str(e))

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


def get_email(user_token):
    val_query_params(user_token)
    user_email = None
    try:
        user_email = get_user_email(user_token)
    except Exception as e:
        logger.error("Could not query the database " + str(e))
    return user_email


def study_submitters(study_id, user_email, method):
    if not study_id or len(user_email) < 5:
        return None

    val_acc(study_id)
    if user_email:
        val_email(user_email)
    query = None
    if method == 'add':
        query = """
            insert into study_user(userid, studyid)
            select u.id, s.id from users u, studies s where lower(u.email) = %(email)s and acc=%(study_id)s;    
        """
    elif method == 'delete':
        query = """
            delete from study_user su where exists(
            select u.id, s.id from users u, studies s
            where su.userid = u.id and su.studyid = s.id and lower(u.email) = %(email)s and acc=%(study_id)s); 
        """


    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query, {'email': user_email.lower(), 'study_id': study_id})
        conn.commit()
        release_connection(postgresql_pool, conn)
        return True
    except Exception as e:
        return False


def get_all_study_acc():
    # Select all study accessions which are not in Dormant status or currently only a placeholder
    query = "select acc from studies where placeholder != '1' and status != 4;"
    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query)
        data = cursor.fetchall()
        release_connection(postgresql_pool, conn)
        return data
    except Exception as e:
        return False


def get_user_email(user_token):

    input = "select email from users where apitoken = %(apitoken)s;"
    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(input, {'apitoken': user_token})
        data = cursor.fetchone()[0]
        release_connection(postgresql_pool, conn)
        return data
    except Exception as e:
        return False


def get_submitted_study_ids_for_user(user_token):
    val_query_params(user_token)

    study_id_list = execute_select_with_params(query_submitted_study_ids_for_user, {"user_token": user_token})
    complete_list = []
    for i, row in enumerate(study_id_list):
        study_id = row[0]
        complete_list.append(study_id)
    return complete_list


def create_empty_study(user_token, study_id=None, obfuscationcode=None):
    email = get_email(user_token)
    if not email:
        return None

    conn = None
    postgresql_pool = None
    try:
        postgresql_pool, conn, cursor = get_connection()
        acc = study_id
        stable_id_input = {"stable_id_prefix": app.config.get("MTBLS_STABLE_ID_PREFIX")}
        if not study_id:
            cursor.execute(get_next_mtbls_id, stable_id_input)
            result = cursor.fetchone()
            if not result:
                logger.error("There is not data prefix with MTBLS in stableid table")
                raise ValueError()
            data = result[0]
            acc = f"{app.config.get('MTBLS_STABLE_ID_PREFIX')}{data}"
        if not obfuscationcode:
            obfuscationcode = str(uuid.uuid4())
        releasedate = (datetime.datetime.today() + datetime.timedelta(days=365))
        content = {"acc": acc,
                   "obfuscationcode": obfuscationcode,
                   "releasedate": releasedate,
                   "email": email}
        cursor.execute(insert_empty_study, content)
        cursor.execute(update_metaboligts_id_sequence, stable_id_input)
        cursor.execute(link_study_with_user, content)
        conn.commit()
        return acc
    except Exception as e:
        if conn:
            conn.rollback()
        return None
    finally:
        if postgresql_pool and conn:
            release_connection(postgresql_pool, conn)


def execute_select_with_params(query, params):
    conn = None
    postgresql_pool = None
    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query, params)

        data = cursor.fetchall()
        return data
    except Exception as e:
        if conn:
            conn.rollback()
        return None
    finally:
        if postgresql_pool and conn:
            release_connection(postgresql_pool, conn)


def execute_query_with_parameter(query, parameters):
    conn = None
    postgresql_pool = None
    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query, parameters)
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        return None
    finally:
        if postgresql_pool and conn:
            release_connection(postgresql_pool, conn)


def get_release_date_of_study(study_id):
    query = f"select acc, to_char(releasedate, 'DD/MM/YYYY') as release_date from studies where acc={study_id};"
    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query)
        data = cursor.fetchone()[0]
        release_connection(postgresql_pool, conn)
        return data
    except Exception as e:
        return None


def query_study_submitters(study_id):
    val_acc(study_id)

    if not study_id:
        return None

    query = """
        select u.email from users u, studies s, study_user su where
        su.userid = u.id and su.studyid = s.id and acc = %(study_id)s; 
    """
    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query, {'study_id': study_id})
        data = cursor.fetchall()
        release_connection(postgresql_pool, conn)
        return data
    except Exception as e:
        return False


def get_username_by_token(token):
    query = "select concat(firstname,' ',lastname) from users where apitoken = %(apitoken)s;"
    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query, {'apitoken': token})
        data = cursor.fetchone()[0]
        release_connection(postgresql_pool, conn)
        return data
    except Exception as e:
        return False


def override_validations(study_id, method, override=""):
    val_acc(study_id)

    if not study_id:
        return None

    if method == 'query':
        query = "select override from studies where acc = '#study_id#';"
    elif method == 'update':
        query = "update studies set override = '#override#' where acc = '#study_id#';"

    try:
        postgresql_pool, conn, cursor = get_connection()

        if method == 'query':
            query = query.replace("#study_id#", study_id.upper())
            query = query.replace('\\', '')
            cursor.execute(query)
            data = cursor.fetchall()
            release_connection(postgresql_pool, conn)
            return data[0]
        elif method == 'update' and override:
            query = query.replace("#study_id#", study_id.upper())
            query = query.replace("#override#", override)
            query = query.replace('\\', '')
            cursor.execute(query)
            conn.commit()
            # conn.close()
            release_connection(postgresql_pool, conn)
    except Exception as e:
        return False


def update_validation_status(study_id, validation_status):
    val_acc(study_id)

    if study_id and validation_status:
        logger.info('Updating database validation status to ' + validation_status + ' for study ' + study_id)
        query = "update studies set validation_status = '" + validation_status + "' where acc = '" + study_id + "';"
        try:
            postgresql_pool, conn, cursor = get_connection()
            cursor.execute(query)
            conn.commit()
            release_connection(postgresql_pool, conn)
            return True
        except Exception as e:
            logger.error('Database update of validation status failed with error ' + str(e))
            return False
    else:
        return False


def update_study_status_change_date(study_id):
    val_acc(study_id)

    query = "update studies set status_date = current_timestamp where acc = %(study_id)s;"
    status, msg = insert_update_data(query, {'study_id': study_id})
    if not status:
        logger.error('Database update of study status date failed with error ' + msg)
        return False
    return True


def insert_update_data(query, inputs=None):
    try:
        postgresql_pool, conn, cursor = get_connection()
        if inputs:
            cursor.execute(query, inputs)
        else:
            cursor.execute(query)
        conn.commit()
        release_connection(postgresql_pool, conn)
        return True, "Database command success " + query
    except Exception as e:
        msg = 'Database command ' + query + 'failed with error ' + str(e)
        logger.error(msg)
        return False, msg


def update_study_status(study_id, study_status, is_curator=False):
    val_acc(study_id)

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

    query = "UPDATE studies SET status = %(status)s"
    if not is_curator:  # Add 28 days to the database release date when a submitter change the status
        query = query + ", updatedate = CURRENT_DATE, releasedate = CURRENT_DATE + integer '28'"
    if study_status == 'public' and is_curator:
        query = query + ", updatedate = CURRENT_DATE, releasedate = CURRENT_DATE"

    query = query + " WHERE acc = %(study_id)s;"

    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query, {'study_id': study_id, 'status': status})
        conn.commit()
        release_connection(postgresql_pool, conn)
        return True
    except Exception as e:
        logger.error('Database update of study status failed with error ' + str(e))
        return False


def execute_select_query(query, user_token):
    if not user_token:
        return None
    val_query_params(user_token)

    input_data = {'apitoken': user_token}

    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query, input_data)
        data = cursor.fetchall()
        release_connection(postgresql_pool, conn)
        return data
    except psycopg2.Error as e:
        print("Unable to connect to the database")
        print(e.pgcode)
        print(e.pgerror)
        print(traceback.format_exc())
    except Exception as e:
        print("Error: " + str(e))
        logger.error("Error: " + str(e))
    return None


def execute_query(query=None, user_token=None, study_id=None, study_obfuscation_code=None):
    if not user_token and study_obfuscation_code:
        return None

    input_data = {'apitoken': user_token,
                  'study_id': study_id,
                  'study_obfuscation_code': study_obfuscation_code
     }

    if not study_obfuscation_code:
        obfuscation_code = ""
    else:
        obfuscation_code = study_obfuscation_code

    data = []

    if study_id:
        val_acc(study_id)

    # Check that study_id, study_obfuscation_code does not contain any sql statements etc
    val_query_params(user_token)
    val_query_params(obfuscation_code)

    try:
        postgresql_pool, conn, cursor = get_connection()
        if study_id is None and study_obfuscation_code is None:
            cursor.execute(query, input_data)
        elif study_id and user_token and not study_obfuscation_code:
            cursor.execute(query_user_access_rights, input_data)
        elif study_id and study_obfuscation_code:
            cursor.execute(study_by_obfuscation_code_query, input_data)
        data = cursor.fetchall()
        release_connection(postgresql_pool, conn)

        return data

    except psycopg2.Error as e:
        print("Unable to connect to the database")
        print(e.pgcode)
        print(e.pgerror)
        print(traceback.format_exc())
    except Exception as e:
        print("Error: " + str(e))
        logger.error("Error: " + str(e))
    return data

def get_connection():
    postgresql_pool = None
    conn = None
    cursor = None
    params = app.config.get('DB_PARAMS')
    conn_pool_min = app.config.get('CONN_POOL_MIN')
    conn_pool_max = app.config.get('CONN_POOL_MAX')
    try:
        postgresql_pool = psycopg2.pool.SimpleConnectionPool(conn_pool_min, conn_pool_max, **params)
        conn = postgresql_pool.getconn()
        cursor = conn.cursor()
    except Exception as e:
        logger.error("Could not query the database " + str(e))
        if postgresql_pool:
            postgresql_pool.closeall()
    return postgresql_pool, conn, cursor


def get_connection2():
    postgresql_pool = None
    conn = None
    cursor = None
    try:
        params = app.config.get('DB_PARAMS')
        conn_pool_min = app.config.get('CONN_POOL_MIN')
        conn_pool_max = app.config.get('CONN_POOL_MAX')
        postgresql_pool = psycopg2.pool.SimpleConnectionPool(conn_pool_min, conn_pool_max, **params)
        conn = postgresql_pool.getconn()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except Exception as e:
        logger.error("Could not query the database " + str(e))
        if postgresql_pool:
            postgresql_pool.closeall()
    return postgresql_pool, conn, cursor


def release_connection(postgresql_pool, ps_connection):
    try:
        postgresql_pool.putconn(ps_connection)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL", error)
        logger.error("Error while releasing PostgreSQL connection. " + str(error))


def database_maf_info_table_actions(study_id=None):
    if study_id:

        val_acc(study_id)

        status, msg = insert_update_data("delete from maf_info where acc = %(study_id)s;", {'study_id': study_id})
    else:
        try:
            sql_trunc = "truncate table maf_info;"
            sql_drop = "drop table maf_info;"
            sql_create = """
                CREATE table maf_info(  
                                        acc VARCHAR, 
                                        database_identifier VARCHAR, 
                                        metabolite_identification VARCHAR, 
                                        database_found VARCHAR,
                                        metabolite_found VARCHAR);
            """
            status, msg = insert_update_data(sql_trunc)
            status, msg = insert_update_data(sql_drop)
            status, msg = insert_update_data(sql_create)
        except Exception as e:
            logger.warning("Database table maf_info error " + str(e))


def add_maf_info_data(acc, database_identifier, metabolite_identification, database_found, metabolite_found):
    val_acc(acc)
    status = False
    msg = None
    sql = """
        insert into maf_info values(
                                %(acc)s, 
                                %(database_identifier)s, 
                                %(metabolite_identification)s,
                                %(database_found)s, 
                                %(metabolite_found)s
                                );
    """
    input_data = {'acc': acc,
                  'database_identifier': database_identifier,
                  "metabolite_identification": metabolite_identification,
                  "database_found": database_found,
                  'metabolite_found': metabolite_found}
    try:
        status, msg = insert_update_data(sql, input_data)
    except Exception as e:
        return False, str(e)
    return status, msg


def val_acc(study_id=None):
    if study_id:
        if not study_id.startswith(app.config.get("MTBLS_STABLE_ID_PREFIX")) or study_id.lower() in stop_words:
            logger.error("Incorrect accession number string pattern")
            abort(406, "'%s' incorrect accession number string pattern" % study_id)


def val_query_params(text_to_val):
    if text_to_val:
        for word in str(text_to_val).split():
            if word.lower() in stop_words:
                abort(406, "'" + text_to_val + "' not allowed.")
