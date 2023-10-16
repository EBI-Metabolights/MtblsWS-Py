import logging
import os
import subprocess
from typing import List, Union
import uuid

from pydantic import BaseModel
from app.config import get_settings

from jinja2 import Environment, PackageLoader, select_autoescape

logger = logging.getLogger("wslog")
env = Environment(
    loader=PackageLoader("resources.templates", "scripts"),
    autoescape=select_autoescape(["html", "xml"]),
)


class BashExecutionResult(BaseModel):
    command: str = ""
    returncode: int = -1


class LoggedBashExecutionResult(BashExecutionResult):
    stdout_log_file_path: Union[str, None] = None
    stderr_log_file_path: Union[str, None] = None


class CapturedBashExecutionResult(BashExecutionResult):
    stdout: List[str] = []
    stderr: List[str] = []


class BashClient(object):
    @staticmethod
    def execute_command(
        command: str,
        stdout_log_file_path: str = None,
        stderr_log_file_path: str = None,
        timeout: Union[None, float] = None,
    ) -> Union[LoggedBashExecutionResult, CapturedBashExecutionResult]:
        logger.info(f" A command is being executed : '{command}'")
        print(f" A command is being executed  : '{command}'")
        logger.info(f"stdout_log_file_path: {stdout_log_file_path}")
        logger.info(f"stderr_log_file_path: {stderr_log_file_path}")
        stdout_dir_path = os.path.dirname(stdout_log_file_path)
        stderr_dir_path = os.path.dirname(stdout_log_file_path)
        os.makedirs(stdout_dir_path, exist_ok=True)
        os.makedirs(stderr_dir_path, exist_ok=True)
        stdout_log_file = None
        stderr_log_file = None
        try:
            if stdout_log_file_path:
                stdout_log_file = open(stdout_log_file_path, "w")
            if stderr_log_file_path:
                stderr_log_file = open(stderr_log_file_path, "w")

            if stderr_log_file or stdout_log_file:
                result = subprocess.run(
                    command,
                    shell=True,
                    stderr=stderr_log_file,
                    stdout=stdout_log_file,
                    check=False,
                    timeout=timeout,
                )
                execution_result = LoggedBashExecutionResult(
                    returncode=result.returncode,
                    command=result.args,
                    stdout_log_file_path=stdout_log_file_path,
                    stderr_log_file_path=stderr_log_file_path,
                )
                logger.info(str(execution_result.dict()))
            else:
                result = subprocess.run(
                    command,
                    shell=True,
                    capture_output=True,
                    check=False,
                    timeout=timeout,
                )
                execution_result = CapturedBashExecutionResult(
                    returncode=result.returncode,
                    command=result.args,
                    stderr=result.stderr.decode().split("\n"),
                    stdout=result.stdout.decode().split("\n"),
                )
                logger.info(str(execution_result.dict()))
        except Exception as ex:
            if stderr_log_file:
                stderr_log_file.write(str(ex))
            else:
                logger.error(f"Error: {str(ex)}")
        finally:
            if stderr_log_file:
                try:
                    stderr_log_file.close()
                except Exception:
                    pass
            if stdout_log_file:
                try:
                    stdout_log_file.close()
                except Exception:
                    pass
        return execution_result

    @staticmethod
    def execute_command_and_nowait(command: str) -> None:
        try:
            logger.info(f" A command is being executed  : '{command}'")
            print(f" A command is being executed  : '{command}'")
            subprocess.run(command, shell=True, check=False)
        except Exception as e:
            logger.error(f"Could not execute command '{command}'. {str(e)}")
            raise e

    @staticmethod
    def build_ssh_command(hostname: str, username: str = None):
        command = []
        command.append("ssh")
        command.append("-o")
        command.append("StrictHostKeyChecking=no")
        command.append("-o")
        command.append("LogLevel=quiet")
        command.append("-o")
        command.append("UserKnownHostsFile=/dev/null")
        if not username:
            command.append(f"{hostname}")
        else:
            command.append(f"{username}@{hostname}")
        return " ".join(command)

    @staticmethod
    def prepare_script_from_template(script_template_name: str, **kwargs):
        template = env.get_template(script_template_name)
        content = template.render(kwargs)
        basename = (
            os.path.basename(script_template_name).replace(".j2", "").replace(".", "_")
        )

        temp_file_name = f"{basename}_{str(uuid.uuid4())}.sh"
        file_input_path = os.path.join(
            get_settings().server.temp_directory_path, temp_file_name
        )

        with open(file_input_path, "w") as f:
            f.writelines(content)
        os.chmod(file_input_path, mode=0o770)
        return file_input_path
