import datetime
import logging
from typing import Union

from pydantic import BaseModel
from app.tasks.common_tasks.curation_tasks.validation import update_validation_files
from app.tasks.worker import celery
from celery.result import AsyncResult
from app.ws.redis.redis import RedisStorage, get_redis_server

logger = logging.getLogger("ws_log")
class ValidationTaskDescription(BaseModel):
    task_id: str = ""
    last_update_time: Union[int, float] = 0
    last_status: str = ""
    task_done_time: Union[int, float] = 0
    

def update_validation_files_task(study_id, user_token):
    key = f"validation_files:update:{study_id}"
    desc = get_validation_task_description(key)
    
    start_new_task = False
    result = None
    message = ""
    if not desc or not desc.task_id:
        start_new_task = True
        message = "There is no previous validation task. New task is started."
    else:
        result: AsyncResult = celery.AsyncResult(desc.task_id)
        if not result or result.state == "PENDING" or result.state == "REVOKED":
            start_new_task = True
            message = "Previous validation task is not active. New task is started"
        else:
            if result.ready():
                if result.successful():
                    message = "Previous validation task was completed. New validation task is re-started."
                else:
                    message = "Previous validation task was failed. New validation task is re-started."
                start_new_task = True
                get_redis_server().delete_value(key)
            else:
                message = "There is a running / waiting task. Waiting its result."
    if start_new_task:
        inputs = {"study_id": study_id, "user_token": user_token}
        task = update_validation_files.apply_async(kwargs=inputs, expires=60*5)
        result: AsyncResult = celery.AsyncResult(task.id)
        now = datetime.datetime.now()
        last_update_time_str = now.strftime('%Y-%m-%d-%H:%M')
        done_time = result.date_done.timestamp() if result.date_done else 0
        desc = ValidationTaskDescription(task_id=task.id, last_status=result.status, task_done_time=done_time, last_update_time=now.timestamp())
        save_validation_task(key, desc, ex=60*60)
        
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
        return desc
    except Exception as exc:
        raise exc