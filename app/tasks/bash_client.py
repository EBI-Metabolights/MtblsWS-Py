import json
import logging
import os
import subprocess
import uuid
from typing import List, Literal, Union

from jinja2 import Environment, PackageLoader, select_autoescape
from pydantic import BaseModel

from app.config import get_settings
from app.tasks.worker import send_email

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

    def __str__(self, **kwargs):
        results = [f"Command: {self.command}\n"]
        results.append(f"Return code: {self.returncode}\n")
        stderr = "\n".join(self.stderr)
        stdout = "\n".join(self.stdout)
        results.append(f"Stderr: {stderr}\n")
        results.append(f"Stdout: {stdout}\n")

        return "".join(results)


class BashClient(object):
    @staticmethod
    def execute_command(
        command: str,
        stdout_log_file_path: Union[None, str] = None,
        stderr_log_file_path: Union[None, str] = None,
        timeout: Union[None, float] = None,
        email: Union[None, str] = None,
        task_name: Union[None, str] = None,
    ) -> Union[LoggedBashExecutionResult, CapturedBashExecutionResult]:
        logger.info(f" A command is being executed : '{command}'")
        print(f" A command is being executed  : '{command}'")
        execution_result = None
        if stdout_log_file_path:
            logger.info(f"stdout_log_file_path: {stdout_log_file_path}")
        if stderr_log_file_path:
            logger.info(f"stderr_log_file_path: {stderr_log_file_path}")
        if stdout_log_file_path:
            stdout_dir_path = os.path.dirname(stdout_log_file_path)
            os.makedirs(stdout_dir_path, exist_ok=True)

        if stderr_log_file_path:
            stderr_dir_path = os.path.dirname(stderr_log_file_path)
            os.makedirs(stderr_dir_path, exist_ok=True)
        stdout_log_file = None
        stderr_log_file = None
        try:
            if stdout_log_file_path:
                stdout_log_file = open(stdout_log_file_path, "w")
            if stderr_log_file_path:
                stderr_log_file = open(stderr_log_file_path, "w")
            if stderr_log_file or stdout_log_file:
                execution_result = LoggedBashExecutionResult()
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
                logger.info(str(execution_result.model_dump()))
            else:
                execution_result = CapturedBashExecutionResult()
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
                logger.info(str(execution_result.model_dump()))
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
            if email and task_name:
                result_str = json.dumps(execution_result.model_dump(), indent=4)
                result_str = result_str.replace("\n", "<p>")
                send_email(
                    f"Result of the task: {task_name}", result_str, None, email, None
                )

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
    def build_ssh_command_base(
        command_name: Literal["ssh", "scp"],
        identity_file: Union[None, str] = None,
        options: Union[None, List[str]] = None,
        tunnel_username: Union[None, str] = None,
        tunnel_hostname: Union[None, str] = None,
    ) -> str:
        command = []
        command.append(command_name)
        if identity_file:
            command.append("-i")
            command.append(identity_file)
        command.append("-o")
        command.append("StrictHostKeyChecking=no")
        command.append("-o")
        command.append("LogLevel=quiet")
        command.append("-o")
        command.append("UserKnownHostsFile=/dev/null")
        if options:
            command.extend(options)
        if tunnel_username and tunnel_hostname:
            command.append("-o")
            subcommand = ["ssh"]
            if identity_file:
                subcommand.append("-i")
                subcommand.append(identity_file)
            subcommand.append("-o")
            subcommand.append("StrictHostKeyChecking=no")
            subcommand.append("-o")
            subcommand.append("LogLevel=quiet")
            subcommand.append("-o")
            subcommand.append("UserKnownHostsFile=/dev/null")
            subcommand.append("-W")
            subcommand.append("%h:%p")
            subcommand.append(f"{tunnel_username}@{tunnel_hostname}")
            command.append(f"ProxyCommand='{' '.join(subcommand)}'")

        return " ".join(command)

    @staticmethod
    def build_ssh_command(
        hostname: str,
        username: Union[None, str] = None,
        identity_file: Union[None, str] = None,
        options: Union[None, List[str]] = None,
        tunnel_username: Union[None, str] = None,
        tunnel_hostname: Union[None, str] = None,
    ) -> str:
        command = [
            BashClient.build_ssh_command_base(
                "ssh", identity_file, options, tunnel_username, tunnel_hostname
            )
        ]
        if not username:
            command.append(f"{hostname}")
        else:
            command.append(f"{username}@{hostname}")
        return " ".join(command)

    @staticmethod
    def build_scp_command(
        hostname: str,
        source_path: str,
        target_path: str,
        username: Union[None, str] = None,
        identity_file: Union[None, str] = None,
        create_target_path: bool = False,
        options: Union[None, List[str]] = None,
        tunnel_username: Union[None, str] = None,
        tunnel_hostname: Union[None, str] = None,
    ) -> str:
        if create_target_path:
            if options:
                options.append("-r")
            else:
                options = ["-r"]
        command = [
            BashClient.build_ssh_command_base(
                "scp", identity_file, options, tunnel_username, tunnel_hostname
            )
        ]
        command.append(source_path)
        if not username:
            command.append(f"{hostname}")
        else:
            command.append(f"{username}@{hostname}:{target_path}")
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
