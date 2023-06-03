import datetime
import json
import logging
import os

import pandas as pd

from app.tasks.worker import (MetabolightsTask, celery, get_flask_app,
                              send_email)
from app.utils import MetabolightsDBException
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study, User
from app.ws.db.types import StudyStatus
from app.ws.folder_maintenance import StudyFolderMaintenanceTask
from app.ws.settings.utils import get_study_settings
from app.ws.study.user_service import UserService

logger = logging.getLogger(__name__)
from app.tasks.worker import get_flask_app


def sort_by_study_id(key: str):
    if key:
        val = os.path.basename(key).upper().replace("MTBLS", "")
        if val.isnumeric():
            return int(val)
    return -1

@celery.task(base=MetabolightsTask, name="app.tasks.common_tasks.admin_tasks.maintain_workers")
def maintain_workers():
    
    try:
        stats = celery.control.inspect().stats()
        print(stats)    
    except Exception as ex:
        # inputs = {"subject": "Study id creation on DB was failed.",
        #             "body":f"Study id on db creation was failed: folder: {folder_name}, user: {user.username} <p> {str(exc)}"}
        # send_technical_issue_email.apply_async(kwargs=inputs)
        raise ex                    
