import datetime
import logging

from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import StudyTask
from app.ws.db.types import StudyTaskStatus
from app.ws.study.study_service import StudyService

logger = logging.getLogger(__name__)


def complete_task(study_id, task_name: str, status: str, message: str) -> StudyTask:
    tasks = StudyService.get_instance().get_study_tasks(
        study_id=study_id, task_name=task_name
    )
    if tasks:
        with DBManager.get_instance().session_maker() as db_session:
            task: StudyTask = tasks[0]
            task.last_execution_message = message
            task.last_execution_status = status
            task.last_request_executed = task.last_request_time
            db_session.add(task)
            db_session.commit()
    else:
        logger.error("%s %s There is no task", study_id, task_name)
        # raise Exception(f"{study_id} {task_name}. There is no task")


def get_task(study_id, task_name: str) -> StudyTask:
    tasks = StudyService.get_instance().get_study_tasks(
        study_id=study_id, task_name=task_name
    )
    if tasks:
        task: StudyTask = tasks[0]
        return task
    else:
        return None


def delete_task(study_id, task_name: str) -> StudyTask:
    if not study_id:
        return
    tasks = StudyService.get_instance().get_study_tasks(
        study_id=study_id, task_name=task_name
    )
    task = None
    if tasks:
        with DBManager.get_instance().session_maker() as db_session:
            task: StudyTask = tasks[0]
            db_session.delete(task)
            db_session.commit()
    else:
        logger.error("%s %s There is no task", study_id, task_name)
    return task


def complete_task_with_failure(task_id, task_name: str) -> StudyTask:
    if not task_id:
        return None
    with DBManager.get_instance().session_maker() as db_session:
        query = db_session.query(StudyTask)
        filtered = query.filter(
            StudyTask.last_execution_message == task_id,
            StudyTask.task_name == task_name,
        )
        result = filtered.all()
        task: StudyTask = result[0] if result else None
        if task:
            task.last_execution_status = StudyTaskStatus.EXECUTION_FAILED
            db_session.add(task)
            db_session.commit()
    return task


def create_task(study_id, task_name: str, message: str = "") -> StudyTask:
    tasks = StudyService.get_instance().get_study_tasks(
        study_id=study_id, task_name=task_name
    )
    with DBManager.get_instance().session_maker() as db_session:
        if tasks:
            task: StudyTask = tasks[0]
        else:
            now = datetime.datetime.now(datetime.UTC)
            task = StudyTask()
            task.study_acc = study_id
            task.task_name = task_name
            task.last_request_time = now
            task.last_execution_time = now
            task.last_request_executed = now
            task.last_execution_status = StudyTaskStatus.NOT_EXECUTED
            task.last_execution_message = "Task is initiated."
        execution_message = message or "Task is initiated."
        if task.last_execution_status in {
            StudyTaskStatus.NOT_EXECUTED,
            StudyTaskStatus.EXECUTION_FAILED,
        }:
            task.last_execution_status = StudyTaskStatus.EXECUTING
            task.last_execution_time = task.last_request_time
            task.last_request_executed = task.last_execution_time
            task.last_execution_message = execution_message
            db_session.add(task)
            db_session.commit()
            return task
        else:
            raise Exception(
                f"{study_id} task {task_name} failed. "
                f"Task status is {task.last_execution_status}."
            )
