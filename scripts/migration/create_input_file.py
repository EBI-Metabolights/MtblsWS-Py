import os
from typing import List
import psycopg2

from app.config import get_settings
from app.ws.db.types import StudyStatus

def sort_by_study_id(key: str):
        if key:
            val = os.path.basename(key).upper().replace("MTBLS", "")
            if val.isnumeric():
                return int(val)
        return -1
    
def get_study_id_list(status_code: StudyStatus = None):
    settings = get_settings().database.connection
    try:
        connection = psycopg2.connect(
            host=settings.host,
            database=settings.database,
            user=settings.user,
            password=settings.password
        )

        # Create a cursor object
        cursor = connection.cursor()
        # Your SQL query
        if status_code:
            sql_query = f"SELECT acc FROM studies where status = {status_code.value}"
        else:
            sql_query = f"SELECT acc FROM studies"
            
        # Execute the query
        cursor.execute(sql_query)

        # Fetch all the results
        results = cursor.fetchall()
        study_ids = [row[0] for row in results]
        # Print the results or process them as needed
        study_ids.sort(key=sort_by_study_id, reverse=True)
        return study_ids
    except (Exception, psycopg2.Error) as error:
        print("Error connecting to PostgreSQL:", error)
    finally:
        # Close the cursor and connection
        if connection:
            cursor.close()
            connection.close()


def create_input_file():
    for status_code in StudyStatus:
        study_ids: List[str] = get_study_id_list(status_code = status_code)
        with open(f"target_studies_{status_code.name}.txt", "w") as f:
            for study_id in study_ids:
                f.write(f"{study_id}\n")
                
    study_ids: List[str] = get_study_id_list(status_code=None)
    with open(f"target_studies_ALL.txt", "w") as f:
        for study_id in study_ids:
            f.write(f"{study_id}\n")   
    
if __name__ == "__main__":
    create_input_file()