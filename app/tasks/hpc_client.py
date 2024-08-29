


import logging
import os
import re
import shutil
import uuid
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import List, Union

from pydantic import BaseModel

from app.config import get_settings
from app.config.model.hpc_cluster import HpcClusterConfiguration
from app.tasks.bash_client import BashClient, CapturedBashExecutionResult
from app.tasks.hpc_rsync_worker import HpcRsyncWorker
from app.utils import MetabolightsException, current_time
from app.ws.settings.utils import get_cluster_settings

logger = logging.getLogger('wslog')


class HpcJob(BaseModel):
    job_id: str = ""
    status: str = ""
    name: str = ""
    submit_time: int = 0
    
class HpcClient(ABC):
    def __init__(self, cluster_settings: Union[None, HpcClusterConfiguration] = None, submit_with_ssh: bool=True, datetime_format=None) -> None:
        self.cluster_settings = cluster_settings
        if not cluster_settings:
            self.cluster_settings = get_cluster_settings()
        self.settings = get_settings()
        self.submit_with_ssh = submit_with_ssh
        self.datetime_format = datetime_format
        if not datetime_format:
            self.datetime_format = "%Y-%m-%dT%H:%M:%S"
    
    @abstractmethod
    def convert_to_runtime_limit(self, time_in_seconds: int):
        pass

    @abstractmethod
    def submit_hpc_job(self, script_path: str, job_name: str, output_file=None, error_file=None, account=None, queue: Union[None, str] = None, timeout: Union[None, float]=30.0, runtime_limit: Union[None, str] = None) -> int:
        pass
    
    @abstractmethod
    def kill_jobs(self, job_id_list: List[str], failing_gracefully=False, timeout: Union[None, float]=15.0):
        pass

    @abstractmethod
    def get_job_status(self, job_names: Union[None, str, List[str]]=None, timeout: Union[None, float]=15.0) -> List[HpcJob]:
        pass
    

    def _get_hpc_ssh_command(self, submission_command: str) -> int:
        if self.submit_with_ssh:
            settings = get_settings()
            dmc = self.settings.hpc_cluster.datamover.connection
            datamover_ssh_command: str = BashClient.build_ssh_command(hostname=dmc.host, username=dmc.username, identity_file=dmc.identity_file)
            if settings.hpc_cluster.datamover.run_ssh_on_hpc_compute:
                cc = self.settings.hpc_cluster.compute.connection
                compute_ssh_command: str = BashClient.build_ssh_command(hostname=cc.host, username=cc.username,  identity_file=cc.identity_file)
                return f"{compute_ssh_command} {datamover_ssh_command} {submission_command}" 
            else:
                return f"{datamover_ssh_command} {submission_command}"
        return f"{submission_command}"
    

    def run_singularity(self, 
                        task_name: str, 
                        command: str, 
                        command_arguments: str, 
                        unique_task_name: bool = True, 
                        hpc_queue_name: Union[None, str] = None, 
                        additional_mounted_paths:  Union[None, List[str]] = None, 
                        sif_image_file_url: Union[None, str]=None):
        messages: List[str] = []
        additional_mounted_paths = additional_mounted_paths if additional_mounted_paths else []
        if unique_task_name:
            try:
                jobs = self.get_job_status()
            except Exception as exc:
                logger.info("There is no datamover job")
                jobs = []
            current_tasks = []
            for job in jobs:
                if job.name.startswith(task_name):
                    current_tasks.append(job)
            if current_tasks:
                message = f"{task_name} is already running."
                logger.warning(message)
                messages.append(message)
                return None, messages
        
        settings = get_settings()
        worker_config = settings.workers.datamover_workers.singularity_image_configuration
        script_template = worker_config.run_singularity_script_template_name
        if not sif_image_file_url:
            raise MetabolightsException("SINGULARITY_IMAGE_FILE_URL is not defined.")
        temp_value = str(int(current_time().timestamp()*1000))
        worker_name = f"worker_{task_name}"
        sif_file_name = os.path.basename(sif_image_file_url)
        inputs = {
                    "DOCKER_BOOTSTRAP_COMMAND": command,
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
                    "SHARED_PATHS": []
                }
        if not hpc_queue_name:
            hpc_queue_name = get_settings().hpc_cluster.datamover.queue_name
        if hpc_queue_name == settings.hpc_cluster.datamover.queue_name:
            inputs["SHARED_PATHS"] = worker_config.shared_paths
        if additional_mounted_paths:
            inputs.update(additional_mounted_paths)
        script_path = BashClient.prepare_script_from_template(script_template, **inputs)
                
        out_log_path = os.path.join(settings.hpc_cluster.configuration.job_track_log_location, f"{task_name}_out.log")
        err_log_path = os.path.join(settings.hpc_cluster.configuration.job_track_log_location, f"{task_name}_err.log")
        temp_path = None
        local_tmp_folder_path = None
        try:
            script_file_name = f"run_singularity.sh"
            tmp_folder =  f"run_singularity_{temp_value}"
            local_tmp_folder_path = os.path.join(self.settings.server.temp_directory_path, tmp_folder)
            os.makedirs(local_tmp_folder_path, exist_ok=True)
            
            config_file_path = os.environ.get("DATAMOVER_CONFIG_FILE_PATH", default="")
            if not config_file_path:
                config_file_path = os.path.realpath(worker_config.config_file_path)
                if not config_file_path:
                    config_file_path = os.path.realpath("datamover-config.yaml")
            target_config_file_path = os.path.join(local_tmp_folder_path, "config.yaml")
            
            secrets_folder_path = os.environ.get("DATAMOVER_SECRETS_PATH", default="")
            if not secrets_folder_path:
                secrets_folder_path = os.path.realpath(worker_config.secrets_path)
                if not secrets_folder_path:
                    secrets_folder_path = os.path.realpath(".datamover-secrets")
            target_secrets_folder_path = os.path.join(local_tmp_folder_path, ".secrets")
            target_script_path = os.path.join(local_tmp_folder_path, script_file_name)
            
            shutil.copy2(config_file_path, target_config_file_path)
            shutil.copytree(secrets_folder_path, target_secrets_folder_path, dirs_exist_ok=True)
            shutil.copy2(script_path, target_script_path)
            hostname = self.settings.hpc_cluster.compute.connection.host
            host_username = self.settings.hpc_cluster.compute.connection.username
            identity_file = self.settings.hpc_cluster.compute.connection.identity_file
            root_path = os.path.join(worker_config.worker_deployment_root_path, worker_name)
            if not self.settings.hpc_cluster.datamover.run_ssh_on_hpc_compute:
                source_path=f"{local_tmp_folder_path}/"
                target_path=f"{host_username}@{hostname}:{root_path}/"
                commands = [HpcRsyncWorker.build_rsync_command(source_path=source_path, target_path=target_path, rsync_arguments="-av", identity_file=identity_file)]
                copy_singularity_run_script = " ".join(commands)

                BashClient.execute_command(copy_singularity_run_script)
            else:
                deleted_files = self.settings.hpc_cluster.datamover.mounted_paths.cluster_rw_storage_recycle_bin_root_path
                os.makedirs(os.path.join(deleted_files, tmp_folder), exist_ok=True)
                temp_path = os.path.join(deleted_files, tmp_folder)
                shutil.copytree(local_tmp_folder_path, temp_path, dirs_exist_ok=True)
                
                commands = [BashClient.build_ssh_command(hostname, username=host_username, identity_file=identity_file)]
                datamover = self.settings.hpc_cluster.datamover.connection.host
                datamover_username = self.settings.hpc_cluster.datamover.connection.username
                datamover_identity_file = self.settings.hpc_cluster.datamover.connection.identity_file
                include_list=["config.yaml", ".secrets", ".secrets/*", script_file_name]
                exclude_list=["*"]
                source_path=f"{temp_path}/"
                target_path=f"{datamover_username}@{datamover}:{root_path}/"
                commands.append(HpcRsyncWorker.build_rsync_command(source_path=source_path, target_path=target_path, rsync_arguments="-av", 
                                                                   identity_file=datamover_identity_file, include_list=include_list, 
                                                                   exclude_list=exclude_list))
                copy_singularity_run_script = " ".join(commands)
                result: CapturedBashExecutionResult = BashClient.execute_command(copy_singularity_run_script)
                if result.returncode != 0:
                    return None, str(result.stderr)
                shutil.rmtree(temp_path, ignore_errors=True)
            max_uptime = self.settings.workers.datamover_workers.maximum_uptime_in_seconds
            runtime_limit=None
            if max_uptime > 0:
                runtime_limit = self.convert_to_runtime_limit(max_uptime)
            job_id, _, _ = self.submit_hpc_job(
                        script_path, task_name, output_file=out_log_path, error_file=err_log_path, queue=hpc_queue_name, runtime_limit=runtime_limit
                    )
            if logger.level >= logging.INFO:
                file = Path(script_path)
                logger.info(file.read_text())
                
            messages.append(f"New job was submitted with job id {job_id} for {task_name}")
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