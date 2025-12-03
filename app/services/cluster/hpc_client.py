import logging
import os
import shutil
from abc import ABC, abstractmethod
from typing import List, Union

from pydantic import BaseModel

from app.config.model.hpc_cluster import HpcClusterDefaultSettings
from app.config.model.worker import SingularityImageConfiguration
from app.tasks.bash_client import BashClient
from app.utils import MetabolightsException, current_time

logger = logging.getLogger("wslog")


class HpcJob(BaseModel):
    job_id: str = ""
    status: str = ""
    name: str = ""
    submit_time: int = 0
    queue: str = ""


class SubmittedJobResult(BaseModel):
    return_code: int = 0
    job_ids: List[int] = []
    stdout: List[str] = []
    stderr: List[str] = []


class HpcClient(ABC):
    def __init__(self, settings: HpcClusterDefaultSettings) -> None:
        self.settings = settings

    @abstractmethod
    def get_job_name_env_variable(self):
        pass

    @abstractmethod
    def convert_to_runtime_limit(self, time_in_seconds: int):
        pass

    @abstractmethod
    def submit_hpc_job(
        self,
        script_path: str,
        job_name: str,
        output_file=None,
        error_file=None,
        account=None,
        queue: Union[None, str] = None,
        timeout: Union[None, float] = 30.0,
        runtime_limit: Union[None, str] = None,
        cpu: int = 1,
        mem: str = "",
    ) -> SubmittedJobResult:
        pass

    @abstractmethod
    def kill_jobs(
        self,
        job_id_list: List[str],
        failing_gracefully=False,
        timeout: Union[None, float] = 30.0,
    ) -> SubmittedJobResult:
        pass

    @abstractmethod
    def get_job_status(
        self,
        job_names: Union[None, str, List[str]] = None,
        timeout: Union[None, float] = 30.0,
    ) -> List[HpcJob]:
        pass

    def _get_hpc_ssh_command(self, submission_command: str) -> str:
        commands = []
        connection = self.settings.connection
        tunnel = self.settings.ssh_tunnel
        hpc_ssh_command = BashClient.build_ssh_command(
            hostname=connection.host,
            username=connection.username,
            identity_file=connection.identity_file,
            tunnel_username=tunnel.username if self.settings.use_ssh_tunnel else None,
            tunnel_hostname=tunnel.host if self.settings.use_ssh_tunnel else None,
        )
        commands.append(hpc_ssh_command)
        commands.append(submission_command)
        return " ".join(commands)

    def run_singularity(
        self,
        task_name: str,
        command: str,
        command_arguments: str,
        singularity_image_configuration: SingularityImageConfiguration,
        unique_task_name: bool = True,
        hpc_queue_name: Union[None, str] = None,
        additional_mounted_paths: Union[None, List[str]] = None,
        sif_image_file_url: Union[None, str] = None,
        source_config_file_path: Union[None, str] = None,
        source_secrets_folder_path: Union[None, str] = None,
        maximum_uptime_in_seconds: int = 0,
        temp_directory_path: str = "/tmp",
        mem_in_mb: int = 0,
        cpu: int = 1,
    ):
        if not sif_image_file_url:
            logger.error("SINGULARITY_IMAGE_FILE_URL is not defined.")
            raise MetabolightsException("SINGULARITY_IMAGE_FILE_URL is not defined.")
        if not source_config_file_path:
            logger.error("singularity image source config file is not defined.")
            raise MetabolightsException(
                "singularity image source config file is not defined."
            )
        if not source_secrets_folder_path:
            logger.error("singularity image secrets folder is not defined.")
            raise MetabolightsException(
                "singularity image secrets folder is not defined."
            )

        messages: List[str] = []
        additional_mounted_paths = (
            additional_mounted_paths if additional_mounted_paths else []
        )
        if unique_task_name:
            logger.info(f"unique task name is enambled. {task_name} will be searched.")
            try:
                jobs = self.get_job_status()
            except Exception as exc:
                logger.info("There is no datamover job.")
                jobs = []
            current_tasks = []
            for job in jobs:
                if job.name.startswith(task_name):
                    current_tasks.append(job)
            if current_tasks:
                message = f"{task_name} is already running."
                logger.info(message)
                messages.append(message)
                return None, messages
            message = f"{task_name} is not running. New job will be submitted."
            logger.info(message)

        worker_config = singularity_image_configuration
        script_template = worker_config.run_singularity_script_template_name
        temp_value = str(int(current_time().timestamp() * 1000))
        worker_name = f"worker_{task_name}"
        sif_file_name = os.path.basename(sif_image_file_url)
        parent_shared_path = os.path.join(temp_directory_path, "run_singularity")
        worker_shared_path = os.path.join(parent_shared_path, worker_name)
        inputs = {
            "DOCKER_BOOTSTRAP_COMMAND": command,
            "JOB_NAME_ENV_VAR_NAME": self.get_job_name_env_variable(),
            "DOCKER_APP_ROOT": worker_config.docker_deployment_path,
            "DOCKER_BOOTSTRAP_COMMAND_ARGUMENTS": command_arguments,
            "REMOTE_SERVER_BASE_PATH": worker_config.worker_deployment_root_path,
            "GITLAB_API_TOKEN": worker_config.gitlab_api_token,
            "SINGULARITY_IMAGE_URL": sif_image_file_url,
            "SINGULARITY_IMAGE_FILENAME": sif_file_name,
            "WORKER_NAME": worker_name,
            "LOGS_PATH": worker_config.logs_path,
            "HOME_DIR": worker_config.user_home_binding_source_path,
            "HOME_DIR_MOUNT_PATH": worker_config.user_home_binding_target_path,
            "WORKER_SHARED_PATH": worker_shared_path,
            "SHARED_PATHS": [],
        }
        if not hpc_queue_name:
            hpc_queue_name = self.settings.default_queue
        inputs["SHARED_PATHS"] = worker_config.shared_paths
        if additional_mounted_paths:
            inputs.update(additional_mounted_paths)
        script_path = BashClient.prepare_script_from_template(script_template, **inputs)
        out_log_path = os.path.join(
            self.settings.job_track_log_location, f"{task_name}_out.log"
        )
        err_log_path = os.path.join(
            self.settings.job_track_log_location, f"{task_name}_err.log"
        )
        logger.debug("Script file: %s", script_path)
        logger.debug("Job err log files: %s", err_log_path)
        logger.debug("Job out log files: %s", out_log_path)

        temp_path = None
        local_tmp_folder_path = worker_shared_path
        try:
            script_file_name = "run_singularity.sh"
            # tmp_folder =  f"run_singularity_{temp_value}"
            # local_tmp_folder_path = os.path.join(temp_directory_path, "run_singularity", tmp_folder)
            if os.path.exists(local_tmp_folder_path):
                shutil.rmtree(local_tmp_folder_path, ignore_errors=True)
            os.makedirs(local_tmp_folder_path, exist_ok=True)
            config_file_path = source_config_file_path
            target_config_file_path = os.path.join(local_tmp_folder_path, "config.yaml")

            secrets_folder_path = source_secrets_folder_path
            target_render_folder_path = os.path.join(local_tmp_folder_path, ".secrets")
            target_script_path = os.path.join(local_tmp_folder_path, script_file_name)

            shutil.copy2(config_file_path, target_config_file_path)
            shutil.copytree(
                secrets_folder_path, target_render_folder_path, dirs_exist_ok=True
            )
            shutil.copy2(script_path, target_script_path)
            logger.debug("Files are copied to %s", local_tmp_folder_path)
            if os.path.exists(target_script_path):
                logger.debug("Script file is copied to %s", target_script_path)
            else:
                logger.error("Script file is not copied to %s", target_script_path)
                raise MetabolightsException(
                    "Script file is not copied to %s", target_script_path
                )
            shutil.make_archive(
                f"{parent_shared_path}/{worker_name}",
                "tar",
                parent_shared_path,
                worker_name,
            )
            tar_filename = f"{worker_name}.tar"
            transfer_package_path = os.path.join(parent_shared_path, tar_filename)
            copy_transfer_package_command = None

            deployment_path = worker_config.worker_deployment_root_path
            root_path = os.path.join(deployment_path, worker_name)
            target_tar_file_path = os.path.join(deployment_path, tar_filename)
            source_path = transfer_package_path
            ssh_tunnel_host = (
                self.settings.ssh_tunnel.host if self.settings.use_ssh_tunnel else None
            )
            ssh_tunnel_username = (
                self.settings.ssh_tunnel.username
                if self.settings.use_ssh_tunnel
                else None
            )
            # ssh_tunnel_identity_file = self.settings.ssh_tunnel.identity_file if self.settings.use_ssh_tunnel else None

            messages.append(f"Copy file to {root_path}")
            hostname = self.settings.connection.host
            host_username = self.settings.connection.username
            identity_file = self.settings.connection.identity_file

            copy_transfer_package_command = BashClient.build_scp_command(
                hostname=hostname,
                source_path=source_path,
                target_path=deployment_path,
                username=host_username,
                identity_file=identity_file,
                create_target_path=True,
                tunnel_hostname=ssh_tunnel_host,
                tunnel_username=ssh_tunnel_username,
            )
            extract_subcommands = [
                BashClient.build_ssh_command(
                    hostname=hostname,
                    username=host_username,
                    identity_file=identity_file,
                    tunnel_hostname=ssh_tunnel_host,
                    tunnel_username=ssh_tunnel_username,
                )
            ]
            extract_subcommands.append(
                f"tar -xvf {target_tar_file_path} -C {worker_config.worker_deployment_root_path}"
            )
            extract_command = " ".join(extract_subcommands)

            logger.debug("Copy file command: %s", copy_transfer_package_command)
            result = BashClient.execute_command(copy_transfer_package_command)
            logger.debug("Copy file command result: %s", result)

            if result.returncode != 0:
                logger.error(
                    f"File copy task failed to {root_path}\n. {str(result.stderr)}, {str(result.stdout)}"
                )
                return None, result.stderr
            else:
                logger.info(
                    f"File copy task completed. Target path: {root_path}\n.{str(result.stderr)}, {str(result.stdout)}"
                )

            logger.debug("File package extract command: %s", extract_command)
            result = BashClient.execute_command(extract_command)
            logger.debug("File package extract command result: %s", result)

            if result.returncode != 0:
                logger.error(
                    f"File package extract task failed to {root_path}\n. {str(result.stderr)}, {str(result.stdout)}"
                )
                return None, result.stderr
            else:
                logger.info(
                    f"File package extract task completed. Target path: {root_path}\n.{str(result.stderr)}, {str(result.stdout)}"
                )

                # commands.append(copy_script_command)
                # copy_singularity_run_script = " ".join(commands)
                # logger.debug("Copy script command: %s", copy_singularity_run_script)
                # result: CapturedBashExecutionResult = BashClient.execute_command(copy_singularity_run_script)
                # logger.debug("Copy script result: %s", result)

                # shutil.rmtree(temp_path, ignore_errors=True)

            # delete_commands = [BashClient.build_ssh_command(ssh_tunnel_host, username=ssh_tunnel_username, identity_file=ssh_tunnel_identity_file)]
            # delete_commands.append(f"rm -rf {temp_path}")
            # delete_temp_command = " ".join(delete_commands)
            # logger.debug("Temp file delete command:\n%s", delete_temp_command)
            # result: CapturedBashExecutionResult = BashClient.execute_command(delete_temp_command)
            # logger.debug("Temp file on ssh tunnel deleted: %s", result)
            max_uptime = maximum_uptime_in_seconds
            runtime_limit = None
            if max_uptime > 0:
                runtime_limit = self.convert_to_runtime_limit(max_uptime)

            mem = f"{mem_in_mb}MB" if mem_in_mb > 0 else 0
            cpu = cpu if cpu > 1 else 1

            submission_result = self.submit_hpc_job(
                script_path,
                task_name,
                output_file=out_log_path,
                error_file=err_log_path,
                queue=hpc_queue_name,
                runtime_limit=runtime_limit,
                cpu=cpu,
                mem=mem,
            )
            job_id = (
                submission_result.job_ids[0]
                if submission_result and submission_result.job_ids
                else None
            )

            logger.info(f"Job submission job: {submission_result}")

            messages.append(
                f"New job was submitted with job id {job_id} for {task_name}"
            )
        except Exception as exc:
            message = f"Exception after kill jobs command. {str(exc)}"
            logger.warning(message)
            messages.append(message)
            return None, messages
        finally:
            if script_path and os.path.exists(script_path):
                os.remove(script_path)
            if temp_path and os.path.exists(temp_path):
                if os.path.isdir(temp_path):
                    shutil.rmtree(temp_path)
                else:
                    os.remove(temp_path)

            if local_tmp_folder_path and os.path.exists(local_tmp_folder_path):
                shutil.rmtree(local_tmp_folder_path)
        return job_id, messages
