import datetime
import json
import logging
import os
from pathlib import Path
import shutil
from typing import Any, Dict, List, Union

from pydantic import BaseModel
from app.tasks.common_tasks.curation_tasks.validation import update_validation_files
from app.tasks.worker import celery
from celery.result import AsyncResult
from app.utils import current_time
from app.ws.redis.redis import RedisStorage, get_redis_server
from app.ws.settings.utils import get_study_settings
from app.ws.study.validation.model import ValidationReportFile, ValidationTaskDescription, validation_message_sorter

logger = logging.getLogger("ws_log")
UTC_SIMPLE_DATE_FORMAT='%Y-%m-%d %H:%M:%S'

def get_validation_report(study_id: str, level: str="all") -> ValidationReportFile:
        
    study_id = study_id.upper()

    settings = get_study_settings()
        
    internal_files_folder = os.path.join(settings.mounted_paths.study_internal_files_root_path, study_id)

    validation_file = Path(os.path.join(internal_files_folder, settings.validation_report_file_name))
    max_response_in_section =  settings.max_validation_messages_count_in_response
    internal_folder_path = Path(internal_files_folder)
    if not internal_folder_path.exists():
        internal_folder_path.mkdir(exist_ok=True)
        
    report = None
    try:
        validation_schema = json.loads(validation_file.read_text(encoding="utf-8"))
        report = ValidationReportFile.parse_obj(validation_schema)
        if report and report.validation:
            if not report.validation.last_update_time:
                report.validation.last_update_timestamp = validation_file.stat().st_mtime
                update_time = datetime.datetime.fromtimestamp(report.validation.last_update_timestamp)
                report.validation.last_update_time = update_time.strftime('%Y-%m-%d-%H:%M')
            if report.validation.validations:
                sections = report.validation.validations
                for section in sections:
                    current_details = section.details
                    if level != 'all':
                        current_details = [ x for x in section.details if x.status == level ]
                    if level == 'all':
                        if current_details and len(current_details) > max_response_in_section:
                            current_details.sort(key=validation_message_sorter)
                            section.details = current_details[:max_response_in_section]
                            section.has_more_items = True
                    else:
                        if current_details and len(current_details) > max_response_in_section:
                            section.details = current_details[:max_response_in_section]
                            section.has_more_items = True
                        
        # if "validation" in validation_schema and validation_schema["validation"]:
        #     if "last_update_time" not in validation_schema["validation"]:
        #         last_update_timestamp = validation_file.stat().st_mtime
        #         update_time = datetime.datetime.fromtimestamp(last_update_timestamp)
        #         last_update_time_str = update_time.strftime('%Y-%m-%d-%H:%M')
        #         validation_schema["validation"]["last_update_time"] = last_update_time_str
        #         validation_schema["validation"]["last_update_timestamp"] = update_time.timestamp()
        #     if "validations" in validation_schema["validation"] and validation_schema["validation"]["validations"]:
        #         sections = validation_schema["validation"]["validations"]
        #         for section in sections:
        #             if "details" in section and len(section["details"]) > max_response_in_section:
        #                 section["details"].sort(key=validation_message_sorter)
        #                 section["details"] = section["details"][:max_response_in_section]
                
            return report
    except Exception as exc:
        if validation_file.exists():
            if  validation_file.is_symlink():
                validation_file.unlink()
            elif validation_file.is_file():
                os.remove(validation_file)
            elif validation_file.is_dir():
                shutil.rmtree(str(validation_file))
                
        logger.error(f"{str(exc)}")
    if not report:
        report = ValidationReportFile()
        # validation_file.write_text(report.dict())
    return report

def get_validation_report_content(study_id: str, level: str="all"):
    
    return get_validation_report(study_id, level).dict()
   
    

def update_validation_files_task(study_id, user_token, force_to_start=True):
    key = f"validation_files:update:{study_id}"
    desc = get_validation_task_description(key)
    
    start_new_task = False
    result = None
    message = ""
    if not desc or not desc.task_id:
        start_new_task = True
        message = ""
    else:
        result: AsyncResult = celery.AsyncResult(desc.task_id)
        if result and result.state != "PENDING":
            now = current_time()
            last_update_time_str = now.strftime(UTC_SIMPLE_DATE_FORMAT)
            done_time = result.date_done.timestamp() if result.date_done else 0
            task_done_time_str = result.date_done.strftime(UTC_SIMPLE_DATE_FORMAT) if result.date_done else ""
            desc.last_update_time = now.timestamp()
            desc.last_update_time_str = last_update_time_str
            desc.task_done_time = done_time
            desc.task_done_time_str = task_done_time_str
            desc.last_status = result.status
                
        if not result or result.state == "PENDING" or result.state == "REVOKED":
            start_new_task = True
            message = "Validation task is not active."
            report: ValidationReportFile = get_validation_report(study_id)
            desc.task_done_time = report.validation.last_update_timestamp
            desc.task_done_time_str = report.validation.last_update_time
        else:
            if result.ready():
                if result.successful():
                    message = "Validation task was completed."
                else:
                    message = "Validation task was failed."
                start_new_task = True
                # get_redis_server().delete_value(key)
            else:
                message = "There is a running / waiting task. Waiting its result."
    if start_new_task and force_to_start:
        inputs = {"study_id": study_id, "user_token": user_token}
        task = update_validation_files.apply_async(kwargs=inputs, expires=60*5)
        result: AsyncResult = celery.AsyncResult(task.id)
        
        message = f"New task is started."
        now = current_time()
        last_update_time_str = now.strftime(UTC_SIMPLE_DATE_FORMAT)
        done_time = result.date_done.timestamp() if result.date_done else 0
        task_done_time_str = result.date_done.strftime(UTC_SIMPLE_DATE_FORMAT) if result.date_done else ""
        desc = ValidationTaskDescription(task_id=task.id, last_status=result.status, task_done_time=done_time, last_update_time=now.timestamp(), last_update_time_str=last_update_time_str, task_done_time_str=task_done_time_str)
        save_validation_task(key, desc, ex=60*60)
    if not force_to_start:
        start_new_task = False
    if not desc:
        desc = ValidationTaskDescription()
    return {"new_task": start_new_task, "message": message, "task": desc.dict()}

def get_validation_task_description(key: str) -> ValidationTaskDescription:
    try:
        redis: RedisStorage = get_redis_server()
    except Exception:
        # no cache or invalid cache
        print("Redis server is not available")
        return None
    value = None
    try:
        value = redis.get_value(key).decode()
        return parse_validation_task_value(value)
    except Exception as exc:
        logger.error("Error parsing redis value")
        return None

def save_validation_task(key, desc: ValidationTaskDescription, ex=None):
    value = f"{desc.task_id}|{desc.last_status}|{str(desc.last_update_time)}|{desc.task_done_time}"
    redis: RedisStorage = get_redis_server()
    redis.set_value(key, value, ex=ex)

def parse_validation_task_value(value: str):
    if not value:
        return None
    try:
        parts = value.split("|")

        desc = ValidationTaskDescription()
        desc.task_id = parts[0]
        desc.last_status = parts[1]
        desc.last_update_time = float(parts[2])
        if len(parts) >= 4:
            desc.task_done_time = float(parts[3])
        last_update_time = datetime.datetime.fromtimestamp(desc.last_update_time)
        desc.last_update_time_str = last_update_time.strftime('%Y-%m-%d-%H:%M') if desc.last_update_time > 0 else ""
        date_done = datetime.datetime.fromtimestamp(desc.task_done_time)
        desc.task_done_time_str = date_done.strftime('%Y-%m-%d-%H:%M') if desc.task_done_time > 0 else ""
        
        return desc
    except Exception as exc:
        raise exc