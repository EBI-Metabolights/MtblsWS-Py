import logging
import os
import celery
from app.tasks.worker import MetabolightsTask, celery
from app.tasks.worker import MetabolightsTask
from app.ws.db.schemes import Study
from app.ws.google_sheet_utils import curation_log_database_query

from app.ws.settings.utils import get_study_settings
from app.ws.study.study_service import StudyService
from app.ws.study.validation.commons import update_validation_schema_files

import datetime
import json
import logging
import os
import re
import urllib
from datetime import datetime

import gspread
import numpy as np
import pandas as pd
import psycopg2
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from owlready2 import urllib
from app.config import get_settings

from app.services.storage_service.acl import Acl
from app.services.storage_service.storage_service import StorageService
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study
from app.ws.db.types import StudyStatus
from app.ws.db_connection import get_study_info, get_study_by_type, get_public_studies
from app.ws.misc_utilities.dataframe_utils import DataFrameUtils
from app.ws.mtblsWSclient import WsClient
from app.ws.study.commons import create_ftp_folder
from app.ws.study.user_service import UserService
from app.ws.utils import log_request, writeDataToFile

logger = logging.getLogger("wslog")


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=10,
    max_retries=3,
    soft_time_limit=60 * 15,
    name="app.tasks.common_tasks.curation_tasks.curation_log_sheet_tasks.curation_log_query",
)
def curation_log_query(self):
    # settings = get_study_settings()
    # file_path = os.path.join(settings.mounted_paths.study_internal_files_root_path, study_id)
    # validation_report_file_name = settings.validation_report_file_name
    # validation_file = os.path.join(file_path, validation_report_file_name)
    # study: Study = StudyService.get_instance().get_study_by_acc(study_id)
    # self.update_state(state='STARTED', meta={'study_id': study_id})
    # return update_validation_schema_files(validation_file, study_id, user_token, study.obfuscationcode)

    try:
        logger.info("Updating curation log-Database Query")
        curation_log_database_query()
        return {"curation log update": True}
    except Exception as e:
        logger.info(e)
        print(e)
        raise e
