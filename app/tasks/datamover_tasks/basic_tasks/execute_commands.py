import logging

from app.services.storage_service.acl import Acl
from app.tasks.bash_client import BashClient
from app.tasks.worker import MetabolightsTask, celery

logger = logging.getLogger("datamover_worker")


@celery.task(
    bind=True, base=MetabolightsTask, name="app.tasks.datamover_tasks.basic_tasks.execute_commands"
)
def execute_bash_command(self, command: str, stdout_log_file_path: Union[None, str] = None, stderr_log_file_path: Union[None, str] = None, email=None, task_name=None):
    self.update_state(state='STARTED', meta={'command': command})

    result = BashClient.execute_command(command=command, stdout_log_file_path=stdout_log_file_path, stderr_log_file_path=stderr_log_file_path, email=email, task_name=task_name)
    return result.model_dump()