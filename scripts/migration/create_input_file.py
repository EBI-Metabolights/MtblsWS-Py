import os
from typing import List, Tuple
import psycopg2

from app.config import get_settings
from app.ws.db.types import StudyStatus
from scripts.migration.utils import get_studies


def create_input_file():
    for status_code in StudyStatus:
        studies: List[str] = get_studies(status_code = status_code)
        with open(f"target_studies_{status_code.name}.txt", "w") as f:
            for study in studies:
                f.write(f"{study[0]}\n")
                
    studies: List[str] = get_studies(status_code=None)
    with open(f"target_studies_ALL.txt", "w") as f:
        for study in studies:
            f.write(f"{study[0]}\n")
    
if __name__ == "__main__":
    create_input_file()