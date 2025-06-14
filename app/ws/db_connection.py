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
from typing import Union

import psycopg2
import psycopg2.extras
from psycopg2 import pool
from app.config import get_settings
from app.utils import (
    MetabolightsDBException,
    current_time,
    current_utc_time_without_timezone,
)
from app.ws.db.models import StudyRevisionModel
from app.ws.db.types import CurationRequest
from app.ws.settings.utils import get_study_settings
from app.ws.study import identifier_service
from app.ws.utils import (
    get_single_file_information,
    check_user_token,
    val_email,
    fixUserDictKeys,
)

logger = logging.getLogger("wslog")

stop_words = (
    "insert",
    "select",
    "drop",
    "delete",
    "from",
    "into",
    "studies",
    "users",
    "stableid",
    "study_user",
    "curation_log_temp",
    "ref_",
    "ebi_reporting",
    "exists",
)

query_curation_log = "select * from curation_log_temp order by acc_short asc;"

query_all_studies = """
    select * from (
        select 
          s.acc, 
          string_agg(u.firstname || ' ' || u.lastname, ', ' order by u.lastname) as username, 
          to_char(s.releasedate, 'YYYYMMDD') as release_date,
          to_char(s.updatedate, 'YYYYMMDD') as update_date, 
          case when s.status = 0 then 'Provisional' 
               when s.status = 1 then 'Private'
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
                        when s.status = 0 then 'Provisional'
                        when s.status = 1 then 'Private'
                        when s.status = 2 then 'In Review'
                        when s.status = 3 then 'Public'
                        else 'Dormant' end                                               as status,
                    case
                        when s.placeholder = '1' then 'Yes'
                        else ''
                        end                                                              as placeholder
                    s.revision_number,
                    s.revision_datetime,
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
      case when s.status = 0 then 'Provisional' 
           when s.status = 1 then 'Private'
           when s.status = 2 then 'In Review' 
           when s.status = 3 then 'Public' 
           else 'Dormant' end,
      case when s.curation_request = 0 then 'MANUAL_CURATION' 
           when s.curation_request = 1 then 'NO_CURATION'
           when s.curation_request = 2 then 'SEMI_AUTOMATED_CURATION' 
           else 'MANUAL_CURATION' end,
    s.id,
    s.revision_number,
    s.revision_datetime
    from studies s, users u, study_user su 
    where s.id = su.studyid and su.userid = u.id and u.apitoken = %(apitoken)s;
    """

query_provisional_study_ids_for_user = """
    SELECT distinct s.acc 
    from studies s, users u, study_user su 
    where s.id = su.studyid and su.userid = u.id and u.apitoken = %(user_token)s and s.status=0;
    """

insert_study_with_provisional_id = """   
    insert into studies (id, obfuscationcode, releasedate, status, studysize, submissiondate, 
    updatedate, validations, validation_status, reserved_submission_id, acc) 
    values ( 
        %(new_unique_id)s,
        %(obfuscationcode)s,
        %(releasedate)s,
        0, 0, %(current_time)s, 
        %(current_time)s, '{"entries":[],"status":"RED","passedMinimumRequirement":false,"overriden":false}', 'error',
        %(req_id)s,
        %(req_id)s
        );
    insert into study_user(userid, studyid) values (%(userid)s, %(new_unique_id)s);
"""

reserve_mtbls_accession_sql = """  
    LOCK TABLE stableid IN ACCESS EXCLUSIVE MODE;
    update stableid set seq = (select (seq + 1) as next_acc from stableid where prefix = %(stable_id_prefix)s)  
    where prefix = %(stable_id_prefix)s; 
    update studies set reserved_accession = 'MTBLS' || (select seq as current_acc from stableid where prefix = %(stable_id_prefix)s) 
    where id = %(table_id)s;
"""
get_study_id_sql = """
select acc from studies where id = %(unique_id)s;
"""

get_user_id_sql = """
select id from users where lower(username)=%(username)s;
"""

query_user_access_rights = """
    SELECT DISTINCT role, read, write, obfuscationcode, releasedate, submissiondate, 
        CASE    WHEN status = 0 THEN 'Provisional'
                WHEN status = 1 THEN 'Private' 
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
                u.apitoken = %(apitoken)s -- USER own data, provisional
                UNION
                SELECT 'user' as role, 'True' as read, 'False' as write, s.obfuscationcode, s.releasedate, s.submissiondate, s.status, s.acc 
                from studies s, study_user su, users u
                where s.acc = %(study_id)s and s.status in (1, 2, 4) and s.id = su.studyid and su.userid = u.id and 
                u.apitoken = %(apitoken)s -- USER own data, private, review or dormant
                UNION 
                SELECT 'user' as role, 'True' as read, 'False' as write, s.obfuscationcode, s.releasedate, s.submissiondate, s.status, s.acc 
                from studies s where acc = %(study_id)s and status = 3) user_data 
            WHERE NOT EXISTS (SELECT 1 from users where apitoken = %(apitoken)s and role = 1)
        ) study_user_data;
"""

study_by_obfuscation_code_query = """
    select distinct 'user', 'True', 'False', obfuscationcode, releasedate, submissiondate,
    case when status = 0 then 'Provisional'
         when status = 1 then 'Private'
         when status = 2 then 'In Review'
         when status = 3 then 'Public'
         else 'Dormant' end as status,
    acc from studies
    where obfuscationcode = %(study_obfuscation_code)s and acc= %(study_id)s;
"""


def create_user(
    first_name,
    last_name,
    email,
    affiliation,
    affiliation_url,
    address,
    orcid,
    api_token,
    password_encoded,
    metaspace_api_key,
):
    email = email.lower()
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
            %(current_time)s, 
            %(lastname_value)s, %(password_value)s, 
            2, 0,
            %(email_value)s, %(orcid_value)s, %(metaspace_api_key_value)s
        );
    """

    input_values = {
        "address_value": address,
        "affiliation_value": affiliation,
        "affiliationurl_value": affiliation_url,
        "apitoken_value": api_token,
        "email_value": email,
        "firstname_value": first_name,
        "lastname_value": last_name,
        "password_value": password_encoded,
        "orcid_value": orcid,
        "metaspace_api_key_value": metaspace_api_key,
        "current_time": current_utc_time_without_timezone(),
    }

    query = insert_user_query

    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query, input_values)
        conn.commit()
        release_connection(postgresql_pool, conn)
        return True, "User account '" + email + "' created successfully"

    except Exception as e:
        return False, str(e)


def update_user(
    first_name,
    last_name,
    email,
    affiliation,
    affiliation_url,
    address,
    orcid,
    api_token,
    password_encoded,
    existing_user_name,
    is_curator,
    metaspace_api_key,
):
    val_email(existing_user_name)
    val_email(email)

    update_user_query = (
        "update users set address = 'address_value', affiliation = 'affiliation_value', "
        "affiliationurl = 'affiliationurl_value', email = 'email_value', "
        "firstname = 'firstname_value', lastname = 'lastname_value', username = 'email_value', "
        "orcid = 'orcid_value', metaspace_api_key = 'metaspace_api_key_value' "
        "where username = 'existing_user_name_value'"
    )

    if not is_curator:
        update_user_query = update_user_query + " and apitoken = 'apitoken_value'"

    update_user_query = update_user_query + ";"

    subs = {
        "address_value": address,
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
        "metaspace_api_key_value": metaspace_api_key,
    }

    for key, value in subs.items():
        val_query_params(str(value))
        update_user_query = update_user_query.replace(str(key), str(value))

    query = update_user_query

    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query)
        number_of_users = cursor.rowcount
        conn.commit()
        release_connection(postgresql_pool, conn)

        if number_of_users == 1:
            return (
                True,
                "User account '" + existing_user_name + "' updated successfully",
            )
        else:
            return (
                False,
                "User account '" + existing_user_name + "' could not be updated",
            )

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
        select firstname, lastname, lower(email), affiliation, affiliationurl, address, orcid, metaspace_api_key 
        from users
        where username = %(username)s;
    """
    postgresql_pool = None
    conn = None
    data = None
    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(get_user_query, {"username": username})
        data = [
            dict((cursor.description[i][0], value) for i, value in enumerate(row))
            for row in cursor.fetchall()
        ]
    except Exception as e:
        logger.error(
            "An error occurred while retrieving user {0}: {1}".format(username, e)
        )
        raise MetabolightsDBException(
            exception=e,
            http_code=500,
            message=f"An error occurred while retrieving user {username}",
        )
    finally:
        release_connection(postgresql_pool, conn)

    if data:
        return {"user": fixUserDictKeys(data[0])}
    else:
        # no user found by that username, fail with 404
        raise MetabolightsDBException(
            http_code=404, message=f"username {username} not found."
        )


def get_all_private_studies_for_user(user_token):
    val_query_params(user_token)

    study_list = execute_select_query(query=query_studies_user, user_token=user_token)
    settings = get_study_settings()
    study_location = settings.mounted_paths.study_metadata_files_root_path
    file_name = settings.investigation_file_name
    isa_title = "Study Title"
    isa_descr = "Study Description"

    complete_list = []
    for i, row in enumerate(study_list):
        title = "N/A"
        description = "N/A"

        study_id = row[0]
        release_date = row[1]
        submission_date = row[2]
        status = row[3]
        curation_request = row[4]

        if status.strip() == "Provisional":
            complete_study_location = os.path.join(study_location, study_id)
            complete_file_name = os.path.join(complete_study_location, file_name)

            logger.info(
                "Trying to load the investigation file (%s) for Study %s",
                file_name,
                study_id,
            )
            # Get the Assay table or create a new one if it does not already exist
            try:
                with open(complete_file_name, encoding="utf-8") as f:
                    for line in f:
                        line = re.sub(r"\s+", " ", line)
                        if line.startswith(isa_title):
                            title = (
                                line.replace(isa_title, "")
                                .replace(' "', "")
                                .replace('" ', "")
                            )
                        if line.startswith(isa_descr):
                            description = (
                                line.replace(isa_descr, "")
                                .replace(' "', "")
                                .replace('" ', "")
                            )
            except FileNotFoundError:
                logger.error("The file %s was not found", complete_file_name)
            revision_status = None
            revision_task_message = None
            revision_comment = None
            if row[6] is not None:
                revision_success, revision = get_study_revision(
                    study_id=study_id, revision_number=row[6]
                )
                if revision_success:
                    revision_comment = revision[3] or ""
                    revision_status = revision[4]
                    revision_task_message = revision[5] or ""

            complete_list.append(
                {
                    "accession": study_id,
                    "updated": get_single_file_information(complete_file_name),
                    "releaseDate": release_date,
                    "createdDate": submission_date,
                    "status": status,
                    "title": title.strip(),
                    "description": description.strip(),
                    "curationRequest": curation_request.strip(),
                    "revisionNumber": row[6],
                    "revisionDatetime": row[7],
                    "revisionStatus": revision_status,
                    "revisionComment": revision_comment,
                    "revisionTaskMessage": revision_task_message,
                }
            )

    return complete_list


def get_all_studies_for_user(user_token):
    val_query_params(user_token)

    study_list = execute_select_query(query=query_studies_user, user_token=user_token)
    if not study_list:
        return []
    study_location = get_settings().study.mounted_paths.study_metadata_files_root_path
    file_name = "i_Investigation.txt"
    isa_title = "Study Title"
    isa_descr = "Study Description"

    complete_list = []
    for i, row in enumerate(study_list):
        title = "N/A"
        description = "N/A"

        study_id = row[0]
        release_date = row[1]
        submission_date = row[2]
        status = row[3]
        curation_request = row[4]
        table_id = row[5]

        if not study_id:
            logger.error(f"Study ID is empty for id {table_id}")
            continue
        complete_study_location = os.path.join(study_location, study_id)
        complete_file_name = os.path.join(complete_study_location, file_name)

        logger.info(
            "Trying to load the investigation file (%s) for Study %s",
            file_name,
            study_id,
        )
        # Get the Assay table or create a new one if it does not already exist
        try:
            with open(complete_file_name, encoding="utf-8") as f:
                for line in f:
                    line = re.sub(r"\s+", " ", line)
                    if line.startswith(isa_title):
                        title = (
                            line.replace(isa_title, "")
                            .replace(' "', "")
                            .replace('" ', "")
                        )
                    if line.startswith(isa_descr):
                        description = (
                            line.replace(isa_descr, "")
                            .replace(' "', "")
                            .replace('" ', "")
                        )
        except FileNotFoundError:
            logger.error("The file %s was not found", complete_file_name)
        except:
            logger.error("An exception occurred while parsing ISA")
            logger.info("Retrying with another encoding ")
            with open(complete_file_name, encoding="latin-1") as f:
                for line in f:
                    line = re.sub(r"\s+", " ", line)
                    if line.startswith(isa_title):
                        title = (
                            line.replace(isa_title, "")
                            .replace(' "', "")
                            .replace('" ', "")
                        )
                    if line.startswith(isa_descr):
                        description = (
                            line.replace(isa_descr, "")
                            .replace(' "', "")
                            .replace('" ', "")
                        )
        http_url = None
        ftp_url = None
        globus_url = None
        aspera_path = None
        if status == "Public":
            configuration = get_settings().ftp_server.public.configuration
            http_url = os.path.join(
                configuration.public_studies_http_base_url, study_id
            )
            ftp_url = os.path.join(configuration.public_studies_ftp_base_url, study_id)
            globus_url = os.path.join(
                configuration.public_studies_globus_base_url, study_id
            )
            aspera_path = os.path.join(
                configuration.public_studies_aspera_base_path, study_id
            )

        revision_number = row[6]
        revision_status = None
        revision_task_message = None
        revision_comment = None
        if revision_number > 0:
            revision_success, revision = get_study_revision(
                study_id=study_id, revision_number=row[6]
            )
            if revision_success:
                revision_comment = revision[3] or ""
                revision_status = revision[4]
                revision_task_message = revision[5] or ""

        revision_datetime = row[7].isoformat() if row[7] else None
        complete_list.append(
            {
                "accession": study_id,
                "updated": get_single_file_information(complete_file_name),
                "releaseDate": release_date,
                "createdDate": submission_date,
                "status": status.strip(),
                "title": title.strip(),
                "description": description.strip(),
                "curationRequest": curation_request.strip(),
                "revisionNumber": row[6],
                "revisionDatetime": revision_datetime,
                "revisionStatus": revision_status,
                "revisionComment": revision_comment,
                "revisionTaskMessage": revision_task_message,
                "studyHttpUrl": http_url,
                "studyFtpUrl": ftp_url,
                "studyGlobusUrl": globus_url,
                "studyAsperaPath": aspera_path,
            }
        )

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


def get_all_non_public_studies():
    query = "select acc from studies where status = 0 OR status = 1 OR status = 2;"
    postgresql_pool, conn, cursor = get_connection()
    cursor.execute(query)
    data = cursor.fetchall()
    release_connection(postgresql_pool, conn)
    return data


def get_study_by_type(sType, publicStudy=True):
    q2 = " "
    if publicStudy:
        q2 = " status in (2, 3) and "
    input_data = {}
    if type(sType) is str:
        q3 = "studytype = %(study_type)s"
        input_data["study_type"] = sType
    # fuzzy search
    elif type(sType) is list:
        db_query = []
        counter = 0
        for type_item in sType:
            counter = counter + 1
            input = "study_type_" + str(counter)
            query = "studytype like %(" + input + ")s"
            input_data[input] = "%" + type_item + "%"
            db_query.append(query)
        q3 = " and ".join(db_query)

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
    query_update_release_date = (
        "update studies set releasedate = %(releasedate)s where acc = %(study_id)s;"
    )
    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(
            query_update_release_date,
            {"releasedate": release_date, "study_id": study_id},
        )
        conn.commit()
        release_connection(postgresql_pool, conn)
        return True, "Date updated for study " + study_id

    except Exception as e:
        return False, str(e)


def add_placeholder_flag(study_id):
    val_acc(study_id)
    query_update = (
        "update studies set placeholder = 1, status = 0 where acc = %(study_id)s;"
    )
    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query_update, {"study_id": study_id})
        conn.commit()
        release_connection(postgresql_pool, conn)
        return True, "Placeholder flag updated for study %s" % (study_id,)

    except Exception as e:
        return False, str(e)


def get_obfuscation_code(study_id):
    val_acc(study_id)
    query = "select obfuscationcode from studies where acc = %(study_id)s;"
    postgresql_pool, conn, cursor = get_connection()
    cursor.execute(query, {"study_id": study_id})
    data = cursor.fetchall()
    release_connection(postgresql_pool, conn)
    return data


def get_id_list_by_req_id(req_id: Union[None, str]):
    identifier = identifier_service.default_provisional_identifier
    parts = identifier.get_id_parts(req_id)
    if not parts or len(parts) < 2:
        raise ValueError("Invalid provisional ID")

    query = "select id from studies where reserved_submission_id = %(val)s or id = %(unique_id)s;"
    postgresql_pool, conn, cursor = get_connection()
    cursor.execute(query, {"val": req_id, "unique_id": parts[1]})
    data = cursor.fetchall()
    release_connection(postgresql_pool, conn)
    data = [x for x in data if x[0] == int(parts[1])]

    return data


def reserve_mtbls_accession(study_id):
    val_acc(study_id)
    query = "select id from studies where acc = %(study_id)s;"
    postgresql_pool, conn, cursor = get_connection()
    try:
        cursor.execute(query, {"study_id": study_id})
        data = cursor.fetchall()
        if data:
            cursor.execute(
                reserve_mtbls_accession_sql,
                {"stable_id_prefix": "MTBLS", "table_id": data[0][0]},
            )
            conn.commit()
            get_reserved_acc_query = (
                "select id, reserved_accession from studies where id = %(table_id)s;"
            )
            cursor.execute(get_reserved_acc_query, {"table_id": data[0][0]})
            data = cursor.fetchall()
            if data:
                return data[0][1]
    finally:
        release_connection(postgresql_pool, conn)
    return None


def update_study_id_from_mtbls_accession(study_id):
    val_acc(study_id)
    query = "select id, reserved_accession from studies where acc = %(study_id)s;"
    postgresql_pool, conn, cursor = get_connection()
    try:
        cursor.execute(query, {"study_id": study_id})
        data = cursor.fetchall()
        if data:
            set_reserved_acc_query = "update studies set acc = %(reserved_accession)s where id = %(table_id)s;"
            cursor.execute(
                set_reserved_acc_query,
                {"table_id": data[0][0], "reserved_accession": data[0][1]},
            )
            conn.commit()
            get_reserved_acc_query = (
                "select id, acc from studies where id = %(table_id)s;"
            )
            cursor.execute(get_reserved_acc_query, {"table_id": data[0][0]})
            data = cursor.fetchall()
            if data:
                return data[0][1]
    except Exception as e:
        conn.rollback()
        logger.error(str(e))
    finally:
        release_connection(postgresql_pool, conn)
    return None


def update_study_id_from_provisional_id(study_id):
    val_acc(study_id)
    query = "select id, reserved_submission_id from studies where acc = %(study_id)s;"
    postgresql_pool, conn, cursor = get_connection()
    try:
        cursor.execute(query, {"study_id": study_id})
        data = cursor.fetchall()
        if data:
            set_reserved_acc_query = (
                "update studies set acc = %(provisional_id)s where id = %(table_id)s;"
            )
            cursor.execute(
                set_reserved_acc_query,
                {"table_id": data[0][0], "provisional_id": data[0][1]},
            )
            conn.commit()
            get_reserved_acc_query = (
                "select id, acc from studies where id = %(table_id)s;"
            )
            cursor.execute(get_reserved_acc_query, {"table_id": data[0][0]})
            data = cursor.fetchall()
            if data:
                return data[0][1]

    except Exception as e:
        conn.rollback()
        logger.error(str(e))
    finally:
        release_connection(postgresql_pool, conn)
    return None


def get_study(study_id):
    val_acc(study_id)
    query = """
    select 
       case
           when s.placeholder = '1' then 'Placeholder'
           when s.status = 0 then 'Provisional'
           when s.status = 1 then 'Private'
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
"""

    postgresql_pool, conn, cursor = get_connection2()
    cursor.execute(query, {"study_id": study_id})
    data = cursor.fetchall()
    result = [dict(row) for row in data]

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
        cursor.execute(query, {"biostudies_id": biostudies_id})
        data = cursor.fetchall()
        # conn.close()
        release_connection(postgresql_pool, conn)
        return data[0] if data else None
    except Exception as e:
        logger.error(
            f"Error while searching def biostudies_acc_to_mtbls(biostudies_id): {str(e)}"
        )
        return None


def biostudies_accession(study_id, biostudies_id, method):
    if not study_id:
        return None

    val_acc(study_id)

    # Default query to get the biosd accession
    s_query = "SELECT biostudies_acc from studies where acc = %(study_id)s;"
    query = None
    if method == "add":
        query = "update studies set biostudies_acc = %(biostudies_id)s where acc = %(study_id)s;"
    elif method == "query":
        query = s_query
    elif method == "delete":
        query = "update studies set biostudies_acc = '' where acc = %(study_id)s;"
    if not query:
        return False, "Not a valid method for adding the biostudies accession"
    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query, {"study_id": study_id, "biostudies_id": biostudies_id})

        if method == "add" or method == "delete":
            conn.commit()
            cursor.execute(s_query, {"study_id": study_id})

        data = cursor.fetchall()
        # conn.close()
        release_connection(postgresql_pool, conn)
        return True, data[0]

    except Exception as e:
        return False, "BioStudies accession was not added to the study"


def get_study_revision(study_id, revision_number):
    query = "select accession_number, revision_datetime, revision_number, revision_comment, status, task_message from study_revisions where accession_number=%(study_id)s and revision_number=%(revision_number)s;"
    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(
            query, {"study_id": study_id, "revision_number": revision_number}
        )
        data = cursor.fetchall()
        release_connection(postgresql_pool, conn)
        if data:
            return True, data[0]
        return False, None

    except IndexError:
        return False, "No metabolite was found for this ChEBI id"


def mtblc_on_chebi_accession(chebi_id):
    if not chebi_id:
        return None

    if not chebi_id.startswith("CHEBI"):
        logger.error("Incorrect ChEBI accession number string pattern")
        raise MetabolightsDBException(
            message=f"{chebi_id} is incorrect ChEBI accession number string pattern",
            http_code=406,
        )

    # Default query to get the biosd accession
    query = "select acc from ref_metabolite where temp_id = %(chebi_id)s;"
    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query, {"chebi_id": chebi_id})
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
        study_list = execute_query(
            query=query_user_access_rights,
            user_token=user_token,
            study_id=study_id,
            study_obfuscation_code=study_obfuscation_code,
        )
    except Exception as e:
        logger.error("Could not query the database " + str(e))

    if study_list is None or not check_user_token(user_token):
        return False, False, False, "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR"
    settings = get_study_settings()
    study_location = settings.mounted_paths.study_metadata_files_root_path
    investigation_file_name = settings.investigation_file_name
    complete_study_location = os.path.join(study_location, study_id)
    complete_file_name = os.path.join(complete_study_location, investigation_file_name)
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
        if read_access == "True":
            read_access = True
        else:
            read_access = False

        write_access = row[2]
        if write_access == "True":
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

        if role == "curator":
            is_curator = True
            break  # The api-code gives you 100% access rights, so no need to check any further

    return (
        is_curator,
        read_access,
        write_access,
        obfuscation_code,
        complete_study_location,
        release_date,
        submission_date,
        updated_date,
        study_status,
    )


def get_email(user_token) -> Union[None, str]:
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
    val_email(user_email)
    user_email = user_email.lower()
    query = None
    if method == "add":
        query = """
            insert into study_user(userid, studyid)
            select u.id, s.id from users u, studies s where lower(u.email) = %(email)s and acc=%(study_id)s;    
        """
    elif method == "delete":
        query = """
            delete from study_user su where exists(
            select u.id, s.id from users u, studies s
            where su.userid = u.id and su.studyid = s.id and lower(u.email) = %(email)s and acc=%(study_id)s); 
        """

    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query, {"email": user_email, "study_id": study_id})
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
    input = "select lower(email) from users where apitoken = %(apitoken)s;"
    try:
        postgresql_pool, conn, cursor = get_connection()
        if cursor:
            cursor.execute(input, {"apitoken": user_token})
            data = cursor.fetchone()[0]
            release_connection(postgresql_pool, conn)
            return data
        return False
    except Exception as e:
        logger.warning(f"User is not fetched for token {user_token}")
        return False


def get_provisional_study_ids_for_user(user_token):
    val_query_params(user_token)

    study_id_list = execute_select_with_params(
        query_provisional_study_ids_for_user, {"user_token": user_token}
    )
    complete_list = [row[0] for row in study_id_list]

    return complete_list


def create_empty_study(user_token, study_id=None, obfuscationcode=None):
    email = get_email(user_token)
    # val_email(email)
    email = email.lower()
    conn = None
    postgresql_pool = None
    req_id = study_id
    current_time = current_utc_time_without_timezone()
    releasedate = current_time + datetime.timedelta(days=365)
    if not obfuscationcode:
        obfuscationcode = str(uuid.uuid4())
    try:
        postgresql_pool, conn, cursor = get_connection()
        if not cursor:
            raise MetabolightsDBException(
                http_code=503, message="There is no database connection"
            )

        user_id = None
        cursor.execute(get_user_id_sql, {"username": email})
        result = cursor.fetchone()
        if result:
            user_id = result[0] if result else None

        if not user_id:
            message = f"User detail for {email} is not fetched."
            logger.error(message)
            raise MetabolightsDBException(http_code=501, message=message)

        cursor.execute(f"SELECT nextval('hibernate_sequence')")
        new_unique_id = cursor.fetchone()[0]
        conn.commit()
        if not req_id:
            req_id = identifier_service.default_provisional_identifier.get_id(
                new_unique_id, current_time
            )

        content = {
            "req_id": req_id,
            "obfuscationcode": obfuscationcode,
            "releasedate": releasedate,
            "email": email,
            "new_unique_id": new_unique_id,
            "userid": user_id,
            "current_time": current_time,
        }
        cursor.execute(insert_study_with_provisional_id, content)
        conn.commit()
        cursor.execute(get_study_id_sql, {"unique_id": new_unique_id})
        fetched_study = cursor.fetchone()
        return fetched_study[0]

    except Exception as ex:
        if conn:
            conn.rollback()
        if isinstance(ex, MetabolightsDBException):
            raise ex
        raise MetabolightsDBException(
            http_code=501,
            message="Error while creating study. Try later.",
            exception=ex,
        )
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
        cursor.execute(query, {"study_id": study_id})
        data = cursor.fetchall()
        release_connection(postgresql_pool, conn)
        return data
    except Exception as e:
        return False


def get_username_by_token(token):
    query = "select concat(firstname,' ',lastname) from users where apitoken = %(apitoken)s;"
    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query, {"apitoken": token})
        data = cursor.fetchone()[0]
        release_connection(postgresql_pool, conn)
        return data
    except Exception as e:
        return False


def override_validations(study_id, method, override=""):
    val_acc(study_id)

    if not study_id:
        return None

    if method == "query":
        query = "select override from studies where acc = '#study_id#';"
    elif method == "update":
        query = "update studies set override = '#override#' where acc = '#study_id#';"

    try:
        postgresql_pool, conn, cursor = get_connection()

        if method == "query":
            query = query.replace("#study_id#", study_id.upper())
            query = query.replace("\\", "")
            cursor.execute(query)
            data = cursor.fetchall()
            release_connection(postgresql_pool, conn)
            return data[0]
        elif method == "update" and override:
            query = query.replace("#study_id#", study_id.upper())
            query = query.replace("#override#", override)
            query = query.replace("\\", "")
            cursor.execute(query)
            conn.commit()
            # conn.close()
            release_connection(postgresql_pool, conn)
    except Exception as e:
        return False


def query_comments(study_id):
    """
    Get any comments associated with a study.

    :param study_id: The accession number of the study we want to retrieve comments for.
    :return: The comments as a string (can be null if none are found)
    """
    val_acc(study_id)

    if not study_id:
        return None

    query = "select comment from studies where acc = '#study_id#';"

    postgresql_pool, conn, cursor = get_connection()
    query = query.replace("#study_id#", study_id.upper())
    query = query.replace("\\", "")
    cursor.execute(query)
    data = cursor.fetchall()
    release_connection(postgresql_pool, conn)
    return data[0]


def update_comments(study_id, comments=None):
    """
    Update the comments string for the given study row in the studies table.

    :param study_id: The accession number of the study we want to update comments for
    :param comments: The new comments string.
    """
    val_acc(study_id)
    if comments is None:
        comments = ""
    if not study_id:
        return None
    query = "update studies set comment = '#comments#' where acc = '#study_id#';"

    postgresql_pool, conn, cursor = get_connection()
    query = query.replace("#study_id#", study_id.upper())
    query = query.replace("#comments#", comments)
    query = query.replace("\\", "")
    cursor.execute(query)
    conn.commit()
    release_connection(postgresql_pool, conn)
    return True


def update_validation_status(study_id, validation_status):
    val_acc(study_id)

    if study_id and validation_status:
        logger.info(
            "Updating database validation status to "
            + validation_status
            + " for study "
            + study_id
        )
        query = (
            "update studies set validation_status = '"
            + validation_status
            + "' where acc = '"
            + study_id
            + "';"
        )
        try:
            postgresql_pool, conn, cursor = get_connection()
            cursor.execute(query)
            conn.commit()
            release_connection(postgresql_pool, conn)
            return True
        except Exception as e:
            logger.error(
                "Database update of validation status failed with error " + str(e)
            )
            return False
    else:
        return False


def update_study_status_change_date(
    study_id, change_time: Union[None, datetime.datetime] = None
):
    val_acc(study_id)
    if not change_time:
        change_time = current_time()
    query = (
        "update studies set status_date = %(current_time)s where acc = %(study_id)s;"
    )
    status, msg = insert_update_data(
        query, {"study_id": study_id, "current_time": change_time}
    )
    if not status:
        logger.error("Database update of study status date failed with error " + msg)
        return False
    return True


def update_study_sample_type(study_id, sample_type):
    val_acc(study_id)
    query = "update studies set sample_type = %(sample_type)s where acc = %(study_id)s;"
    status, msg = insert_update_data(
        query, {"study_id": study_id, "sample_type": sample_type}
    )
    if not status:
        logger.error("Database update of study sample type failed with error " + msg)
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
        msg = "Database command " + query + "failed with error " + str(e)
        logger.error(msg)
        return False, msg


def update_study_status(
    study_id,
    study_status,
    first_public_date=None,
    first_private_date=None,
):
    val_acc(study_id)

    status = "0"
    study_status = study_status.lower()
    if study_status == "provisional":
        status = "0"
    elif study_status == "private":
        status = "1"
    elif study_status == "in review":
        status = "2"
    elif study_status == "public":
        status = "3"
    elif study_status == "dormant":
        status = "4"

    query = "UPDATE studies SET status = %(status)s, status_date = CURRENT_DATE, updatedate = CURRENT_DATE"

    if study_status == "public" and first_public_date is None:
        query = query + ", first_public_date = CURRENT_DATE"
    if study_status in {"private", "in review"} and first_private_date is None:
        query = query + ", first_private_date = CURRENT_DATE"

    query = query + " WHERE acc = %(study_id)s;"

    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query, {"study_id": study_id, "status": status})
        conn.commit()
        release_connection(postgresql_pool, conn)
        return True
    except Exception as e:
        logger.error("Database update of study status failed with error " + str(e))
        return False


def update_curation_request(
    study_id, curation_request: Union[CurationRequest, None] = None
):
    val_acc(study_id)
    if curation_request is None:
        return False
    current = current_time()
    query = "UPDATE studies SET curation_request = %(curation_request)s"
    query += ", updatedate = %(current)s, status_date = %(current)s"
    query += " WHERE acc = %(study_id)s;"

    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(
            query,
            {
                "study_id": study_id,
                "curation_request": curation_request.value,
                "current": current,
            },
        )
        conn.commit()
        release_connection(postgresql_pool, conn)
        return True
    except Exception as e:
        logger.error(
            "Database update of study curation_request failed with error " + str(e)
        )
        return False


def update_modification_time(
    study_id, update_time: Union[datetime.datetime, None] = None
):
    val_acc(study_id)
    if not update_time:
        update_time = "CURRENT_TIME"
    query = "UPDATE studies SET updatedate = %(date_time)s"
    query += " WHERE acc = %(study_id)s;"

    try:
        postgresql_pool, conn, cursor = get_connection()
        cursor.execute(query, {"study_id": study_id, "date_time": update_time})
        conn.commit()
        release_connection(postgresql_pool, conn)
        return True
    except Exception as e:
        logger.error(
            "Database update of study modification time failed with error " + str(e)
        )
        return False


def execute_select_query(query, user_token):
    if not user_token:
        return None
    val_query_params(user_token)

    input_data = {"apitoken": user_token}

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


def execute_query(
    query=None, user_token=None, study_id=None, study_obfuscation_code=None
):
    if not user_token and study_obfuscation_code:
        return None

    input_data = {
        "apitoken": user_token,
        "study_id": study_id,
        "study_obfuscation_code": study_obfuscation_code,
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
    settings = get_settings()
    params = settings.database.connection.model_dump()

    conn_pool_min = settings.database.configuration.conn_pool_min
    conn_pool_max = settings.database.configuration.conn_pool_max
    try:
        postgresql_pool = psycopg2.pool.SimpleConnectionPool(
            conn_pool_min, conn_pool_max, **params
        )
        conn = postgresql_pool.getconn()
        cursor = conn.cursor()
    # TODO: Actual exception handling, this is crap
    except Exception as e:
        logger.error("Could not query the database " + str(e))
        if postgresql_pool:
            postgresql_pool.closeall()
            postgresql_pool = None
            conn = None
            cursor = None
    return postgresql_pool, conn, cursor


def get_connection2():
    postgresql_pool = None
    conn = None
    cursor = None
    try:
        settings = get_settings()
        params = settings.database.connection.model_dump()
        conn_pool_min = settings.database.configuration.conn_pool_min
        conn_pool_max = settings.database.configuration.conn_pool_max
        postgresql_pool = psycopg2.pool.SimpleConnectionPool(
            conn_pool_min, conn_pool_max, **params
        )
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

        status, msg = insert_update_data(
            "delete from maf_info where acc = %(study_id)s;", {"study_id": study_id}
        )
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


def add_maf_info_data(
    acc,
    database_identifier,
    metabolite_identification,
    database_found,
    metabolite_found,
):
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
    input_data = {
        "acc": acc,
        "database_identifier": database_identifier,
        "metabolite_identification": metabolite_identification,
        "database_found": database_found,
        "metabolite_found": metabolite_found,
    }
    try:
        status, msg = insert_update_data(sql, input_data)
    except Exception as e:
        return False, str(e)
    return status, msg


def add_metabolights_data(content_name, data_format, content):
    status = False
    msg = None
    sql = """
        insert into metabolights_data_reuse(content_name, data_format, content) values(
                                %(content_name)s, 
                                %(data_format)s, 
                                %(content)s
                                );
    """
    input_data = {
        "content_name": content_name,
        "data_format": data_format,
        "content": content,
    }
    try:
        status, msg = insert_update_data(sql, input_data)
    except Exception as e:
        return False, str(e)
    return status, msg


def val_acc(study_id=None):
    if study_id:
        if (
            not (study_id.startswith("MTBLS") or study_id.startswith("REQ"))
            or study_id.lower() in stop_words
        ):
            logger.error("Incorrect accession number string pattern")
            raise MetabolightsDBException(
                message=f"{study_id} is incorrect accession number string pattern",
                http_code=406,
            )


def val_query_params(text_to_val):
    if text_to_val:
        for word in str(text_to_val).split():
            if word.lower() in stop_words:
                raise MetabolightsDBException(
                    message=f"{text_to_val} not allowed.", http_code=406
                )
