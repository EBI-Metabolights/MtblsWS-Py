import datetime
import json
import logging
import os
from typing import Any, Dict, List
import celery
from app.config import get_settings
from app.tasks.worker import (MetabolightsTask, celery, send_email)
from app.tasks.worker import MetabolightsTask
from app.ws.chebi.search.chebi_search_manager import ChebiSearchManager
from app.ws.chebi.search.curated_metabolite_table import CuratedMetaboliteTable
from app.ws.chebi.wsproxy import get_chebi_ws_proxy
from app.ws.chebi_pipeline_utils import run_chebi_pipeline
from app.ws.db.schemes import Study
from app.ws.mtblsWSclient import WsClient

from app.ws.settings.utils import get_study_settings
from app.ws.study.study_service import StudyService
from app.ws.study.validation.commons import update_validation_schema_files
from celery.result import AsyncResult
from app.tasks.worker import celery
from app.ws.redis.redis import RedisStorage, get_redis_server

logger = logging.getLogger('wslog')




def init_chebi_search_manager():
    settings = get_settings()
    # These code completes WsClient initialization using flask app context
    if not WsClient.search_manager:
        chebi_proxy = get_chebi_ws_proxy()
        curation_table_file_path = settings.chebi.pipeline.curated_metabolite_list_file_location
        curation_table = CuratedMetaboliteTable.get_instance(curation_table_file_path)
        chebi_search_manager = ChebiSearchManager(ws_proxy=chebi_proxy, curated_metabolite_table=curation_table)
        WsClient.search_manager = chebi_search_manager

        

@celery.task(bind=True, base=MetabolightsTask, max_retries=1, soft_time_limit=60*60*24, name="app.tasks.common_tasks.curation_tasks.validation.run_chebi_pipeline_task")
def run_chebi_pipeline_task(self, study_id: str, user_token: str, annotation_file_name: str, email: str, classyfire_search: bool = True, update_study_maf: bool = False):
    output: Dict[str, Any] = {}   
    start = datetime.datetime.now()
    status = "initiated"
    output["study_id"] = study_id
    output["input_maf_file"] = annotation_file_name
    output["start_time"] = start.strftime("%Y-%m-%d %H:%M:%S")
    output["executed_on"] = os.uname().nodename
    output["task_id"] = str(self.request.id)
    output["status"] = status
    task_name = f"CHEBI pipeline task for {study_id}: {self.request.id}"
    try:
        key = f"chebi_pipeline:{study_id}"
        get_redis_server().set_value(key, self.request.id)
        body_intro = f"CHEBI pipeline task is started for {study_id} {annotation_file_name} file. <p>"
        body = body_intro + json.dumps(output, indent=4)
        output["start_time"] = start.strftime("%Y-%m-%d %H:%M:%S"),
        output["executed_on"] = os.uname().nodename,
        send_email(f"CHEBI pipeline task is started for study {study_id}.", body, None, email, None)
        init_chebi_search_manager()
        output["result"] = run_chebi_pipeline(study_id, user_token, annotation_file_name, run_silently=False, classyfire_search=classyfire_search, update_study_maf=update_study_maf, run_on_cluster=False, email=email, task_name=task_name)
        status = "success"
    except Exception as ex:
        status = "failed"
        output["Failure reason"] = f"{str(ex)}"
        raise ex
    finally:
        end = datetime.datetime.now()
        time_difference = end - start
        hours, remainder = divmod(time_difference.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        time_format = "{:02}:{:02}:{:02} [HH:MM:ss]".format(int(hours), int(minutes), int(seconds))
        output["elapsed_time"] = time_format
        output["end_time"] = end.strftime("%Y-%m-%d %H:%M:%S"),
        output["status"] = status

        body_intro = f"You can see the result of your CHEBI pipeline task {str(self.request.id)}.<p>" 
        result_str = body_intro + json.dumps(output, indent=4)
        result_str = result_str.replace("\n", "<p>")
        
        get_redis_server().delete_value(key=key)

        send_email(f"Result of the CHEBI pipeline task - {study_id}", result_str, None, email, None)
    return output
    