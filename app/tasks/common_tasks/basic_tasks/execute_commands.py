from typing import Union

from app.tasks.bash_client import BashClient
from app.tasks.worker import MetabolightsTask, celery


@celery.task(
    bind=True,
    base=MetabolightsTask,
    name="app.tasks.common_tasks.basic_tasks.execute_commands",
)
def execute_bash_command(
    self,
    command: str,
    stdout_log_file_path: Union[None, str] = None,
    stderr_log_file_path: Union[None, str] = None,
):
    self.update_state(state="STARTED", meta={"command": command})
    result = BashClient.execute_command(
        command=command,
        stdout_log_file_path=stdout_log_file_path,
        stderr_log_file_path=stderr_log_file_path,
    )
    return result.model_dump()
