import os
from typing import List, Tuple, Union

import psycopg

from app.ws.db.dbmanager import DBManager
from app.ws.db.types import StudyStatus


def sort_by_study_id(value: Tuple[str, str, int]):
    if value and value[0]:
        val = os.path.basename(value[0]).upper().replace("MTBLS", "")
        if val.isnumeric():
            return int(val)
    return -1


def get_studies(status_code: Union[None, StudyStatus] = None, reverse: bool = True):
    connection = None
    try:
        connection = psycopg.connect(DBManager.get_instance().get_db_url())

        # Create a cursor object
        cursor = connection.cursor()
        # Your SQL query
        if status_code:
            sql_query = f"SELECT acc, obfuscationcode, status, submissiondate, releasedate  FROM studies where status = {status_code.value}"
        else:
            sql_query = "SELECT acc, obfuscationcode, status, submissiondate, releasedate FROM studies"

        # Execute the query
        cursor.execute(sql_query)

        # Fetch all the results
        results = cursor.fetchall()
        studies: List = [row for row in results]
        # Print the results or process them as needed
        studies.sort(key=sort_by_study_id, reverse=reverse)
        return studies
    except (Exception, psycopg.Error) as error:
        print("Error connecting to PostgreSQL:", error)
    finally:
        # Close the cursor and connection
        if connection:
            cursor.close()
            connection.close()
