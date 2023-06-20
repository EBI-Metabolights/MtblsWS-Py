import datetime
import logging
import os
from typing import Union
from pydantic import BaseModel
from pyparsing import Any
from app.tasks.bash_client import BashExecutionResult, CapturedBashExecutionResult, LoggedBashExecutionResult
from app.tasks.datamover_tasks.basic_tasks.execute_commands import execute_bash_command
from app.tasks.worker import celery
from celery.result import AsyncResult
from app.utils import MetabolightsException


from app.ws.redis.redis import RedisStorage, get_redis_server

logger = logging.getLogger("ws_log")

class TaskDescription(BaseModel):
    task_id: str = ""
    last_update_time: Union[int, float] = 0
    last_status: str = ""
    task_done_time: Union[int, float] = 0
    stdout_log_filename: str = ""
    stderr_log_filename: str = ""


class BashExecutionTaskStatus(BaseModel):
    description: TaskDescription = None
    running: bool = False
    wait_in_seconds: float = 0
    result_ready: bool = False
    result: BashExecutionResult = None


WAIT_STATES = {"STARTED", "INITIATED", "RECEIVED", "STARTED", "RETRY", "PROGRESS"}


class HpcWorkerBashRunner:
    def __init__(
        self,
        task_name: str,
        study_id: str,
        command: str = None,
        expires=60 * 5,
        result_expires=30,
        min_rerun_time_in_seconds=5,
        stdout_log_file_path=None,
        stderr_log_file_path=None,
    ) -> None:
        self.redis: RedisStorage = get_redis_server()
        self.study_id = study_id
        self.task_name = task_name
        self.key = f"{task_name}:{study_id}"
        self.expires = expires
        self.command = command
        self.result_expires = result_expires
        self.min_rerun_period_in_seconds = min_rerun_time_in_seconds
        self.stderr_log_file_path = stderr_log_file_path
        self.stdout_log_file_path = stdout_log_file_path

    def get_bash_execution_status(self, result_only: bool = False):
        task_status: BashExecutionTaskStatus = self.get_status()
        task_description = task_status.description
        if not result_only:
            if not task_description or (not task_status.running and task_description.last_status != "SUCCESS"):
                if not self.command:
                    raise MetabolightsException(message="Bash command is not set.")
                task_status = self.execute(
                    self.command,
                    stdout_log_file_path=self.stdout_log_file_path,
                    stderr_log_file_path=self.stderr_log_file_path,
                )

        if task_status.result_ready and task_status.result:
            self.redis.delete_value(self.key)
        return task_status

    def evaluate_current_status(self) -> BashExecutionTaskStatus:
        task_status = BashExecutionTaskStatus()
        desc: TaskDescription = self.get_task_description()
        if desc and desc.task_id:
            result: AsyncResult = celery.AsyncResult(desc.task_id)
            if result and result.state != "PENDING" and result.state != "REVOKED":
                if result.state in WAIT_STATES:
                    task_status.running = True
                else:
                    task_status.result_ready = result.ready()
                    if task_status.result_ready:
                        task_status.wait_in_seconds = self.get_wait_time(result.date_done.timestamp())
                        if result.successful() and isinstance(result.result, dict):
                            if "stdout" in result.result:
                                task_status.result = CapturedBashExecutionResult.parse_obj(result.result)
                            elif "stdout_log_file_path" in result.result:
                                task_status.result = LoggedBashExecutionResult.parse_obj(result.result)
                            else:
                                raise MetabolightsException(message="unexpected bash result")

                        desc.task_done_time = result.date_done.isoformat()
                task_status.description = desc
                desc.task_id = result.task_id
                desc.last_update_time = datetime.datetime.now().timestamp()
                desc.last_status = result.state
                desc.stderr_log_filename = (
                    os.path.basename(self.stderr_log_file_path) if self.stderr_log_file_path else ""
                )
                desc.stdout_log_filename = (
                    os.path.basename(self.stdout_log_file_path) if self.stdout_log_file_path else ""
                )

                self.save_task_description(desc)
            else:
                if desc.last_status == "SUCCESS" or desc.last_status == "FAILURE":
                    task_status.wait_in_seconds = datetime.datetime.fromtimestamp(desc.task_done_time)

        return task_status

    def get_status(self) -> BashExecutionTaskStatus:
        return self.evaluate_current_status()

    def execute(self, command, stdout_log_file_path=None, stderr_log_file_path=None) -> BashExecutionTaskStatus:
        inputs = {
            "command": command,
            "stdout_log_file_path": stdout_log_file_path,
            "stderr_log_file_path": stderr_log_file_path,
        }
        task = execute_bash_command.apply_async(kwargs=inputs, expires=self.expires)
        task_id = task.id
        if task_id:
            result: AsyncResult = celery.AsyncResult(task_id)
            now = datetime.datetime.now().timestamp()
            state = result.state
            stderr_log_filename = os.path.basename(self.stderr_log_file_path) if stderr_log_file_path else ""
            stdout_log_filename = os.path.basename(self.stdout_log_file_path) if stdout_log_file_path else ""
            desc = TaskDescription(
                task_id=result.id,
                last_status=state,
                last_update_time=now,
                stdout_log_filename=stdout_log_filename,
                stderr_log_file_path=stderr_log_filename,
            )
            self.save_task_description(desc)
            task_status = BashExecutionTaskStatus()
            task_status.description = desc
            task_status.running = True
            return task_status

        raise MetabolightsException(http_code=501, message="Task can not be started")

    def get_wait_time(self, last_update_time: float):
        now = datetime.datetime.now().timestamp()
        elapsed_time = now - last_update_time
        if elapsed_time < 0:
            elapsed_time = 0

        if elapsed_time >= self.min_rerun_period_in_seconds:
            return 0
        else:
            return self.min_rerun_period_in_seconds - elapsed_time

    def get_task_description(self) -> TaskDescription:
        try:
            if not self.redis:
                self.redis: RedisStorage = get_redis_server()
        except Exception:
            # no cache or invalid cache
            print("Redis server is not available")
            return None
        value = None
        try:
            value = self.redis.get_value(self.key).decode()
            return self.parse_redis_value(value)
        except Exception as exc:
            logger.error("Error parsing redis value")
            return None

    def save_task_description(self, desc: TaskDescription):
        value = f"{desc.task_id}|{desc.last_status}|{str(desc.last_update_time)}|{desc.task_done_time}|{desc.stdout_log_filename}|{desc.stderr_log_filename}"
        self.redis.set_value(self.key, value)

    def parse_redis_value(self, value: str):
        if not value:
            return None
        try:
            parts = value.split("|")

            desc = TaskDescription()
            desc.task_id = parts[0]
            desc.last_status = parts[1]
            desc.last_update_time = float(parts[2])
            if len(parts) >= 4:
                desc.task_done_time = float(parts[3])
            if len(parts) >= 6:
                desc.stdout_log_filename = parts[4]
                desc.stderr_log_filename = parts[5]
            return desc
        except Exception as exc:
            raise exc
