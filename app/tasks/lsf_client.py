


import datetime
from enum import Enum
import logging
import os
import re
from typing import List, Union
import uuid
from app.config import get_settings
from app.config.model.hpc_cluster import HpcClusterConfiguration
from app.tasks.bash_client import BashClient, CapturedBashExecutionResult
from app.utils import MetabolightsException

from app.ws.settings.utils import get_cluster_settings

logger = logging.getLogger('wslog')

from pydantic import BaseModel

class JobState(str, Enum):
    PEND = "PEND"
    RUN = "RUN"
    DONE = "DONE"
    UNKOWN = "UNKOWN"

class HpcJob(BaseModel):
    job_id: str = ""
    status: str = ""
    name: str = ""
    submit_time: int = 0
    

    
class LsfClient(object):
    
    def __init__(self, cluster_settings: HpcClusterConfiguration=None, submit_with_ssh: bool=True, datetime_format=None) -> None:
        self.cluster_settings = cluster_settings
        if not cluster_settings:
            self.cluster_settings = get_cluster_settings()
        self.settings = get_settings()
        self.submit_with_ssh = submit_with_ssh
        self.datetime_format = datetime_format
        if not datetime_format:
            self.datetime_format = "%b %d %H:%M"

        
    def submit_hpc_job(self, script_path: str, job_name: str, output_file=None, error_file=None, account=None, queue: str=None, timeout: Union[None, float]=30.0) -> int:
        if not queue:
            queue = get_settings().hpc_cluster.datamover.queue_name
        bsub_command = self._get_submit_command(script_path, queue, job_name, output_file, error_file, account)
        result: CapturedBashExecutionResult = BashClient.execute_command(bsub_command, timeout=timeout)
        stdout = result.stdout
        status_line = result.stdout[0] if result.stdout else ""
        job_id = ""
        pattern = re.compile('Job <(.+)> is submitted to queue <(.+)>.*', re.IGNORECASE)
        match =  pattern.match(status_line)
        if match:
            match_result = match.groups()
            if not match_result[0] or not match_result[0].isnumeric():
                raise MetabolightsException(message=f"No job id is defined.")
            else:
                job_id = int(match_result[0])
            if match_result[1] != queue:
                raise MetabolightsException(message=f"Unexpected queue, job submitted to {match_result[1]}.")
            message = f"Script was sumbitted: Job name: {job_name}, queue: {match_result[1]}, job id: {match_result[0]}"
            logger.info(message) 
            print(message)
        else:
            raise MetabolightsException(message="output does not have a job id.")
            
        return job_id, stdout, result.stderr
            
    def kill_jobs(self, job_id_list: List[str], failing_gracefully=False, timeout: Union[None, float]=15.0):
        kill_command = f"{self.cluster_settings.job_kill_command} {' '.join(job_id_list)}"
        ssh_command = BashClient.build_ssh_command(hostname=self.settings.hpc_cluster.datamover.connection.host, username=self.settings.hpc_cluster.datamover.connection.username)       
        command = f"{ssh_command} {kill_command}"  
        result: CapturedBashExecutionResult = BashClient.execute_command(command, timeout=timeout)
        pattern = re.compile('Job <(.+)>.*', re.IGNORECASE)
        lines = result.stdout
        killed_job_id_list = []
        for line in lines:
            if line.strip():
                match =  pattern.match(line)
                if match:
                    match_result = match.groups()
                    if not match_result[0] or not match_result[0].isnumeric() and not failing_gracefully:
                        raise MetabolightsException(message=f"No job id is defined.")
                           
                    killed_job_id_list.append(match_result[0])
        return killed_job_id_list, result.stdout, result.stderr       
          
    def get_job_status(self, job_names: Union[None, str, List[str]]=None, timeout: Union[None, float]=15.0) -> List[HpcJob]:
        if not job_names:
            job_names = []
        elif isinstance(job_names, str):
            job_names = [job_names]
        
        command = self._get_job_status_command()

        result: CapturedBashExecutionResult = BashClient.execute_command(command, timeout=timeout)  
        results = [] 
        if result.stdout:
            lines = result.stdout
            for line in lines:
                if line.strip():
                    columns = line.split()
                    if len(columns) < 7:
                        raise MetabolightsException(message=f"Return format is not valid.")
                    status = columns[2]
                    job_id = columns[0]
                    submit_time_str = " ".join(columns[7:]) if columns[7] else ""
                    submit_time = 0
                    try:
                        submit_datetime = datetime.datetime.strptime(submit_time_str, self.datetime_format)
                        submit_datetime = submit_datetime.replace(year=datetime.datetime.now().year)
                        submit_time = submit_datetime.timestamp()

                    except Exception as ex:
                        pass
                    item = HpcJob(job_id=job_id, status=status, name=columns[6].strip(), submit_time=submit_time)
                    if  job_names:
                        if columns[6].strip() in job_names:
                            results.append(item)
                    else:
                        results.append(item)
            return results
        else:
            raise MetabolightsException(message=f"No result returned.")
        
    def _get_job_status_command(self):
        command = f"{self.cluster_settings.job_running_command} -noheader -w -P {self.cluster_settings.job_project_name}"
        ssh_command = BashClient.build_ssh_command(hostname=self.settings.hpc_cluster.datamover.connection.host, username=self.settings.hpc_cluster.datamover.connection.username)
        
        return f"{ssh_command} {command}"        
        
    def _get_submit_command(self, script: str, job_name: str, queue=None, output_file=None, error_file=None, account=None) -> int:
        script_file_path = self._prepare_script_to_submit_on_lsf(script, queue, job_name, output_file, error_file, account)
        submission_command = f"bsub < {script_file_path}"
        
        if self.submit_with_ssh:
            ssh_command = BashClient.build_ssh_command(hostname=self.settings.hpc_cluster.datamover.connection.host, username=self.settings.hpc_cluster.datamover.connection.username)
            return f"{ssh_command} {submission_command}"
        return f"{submission_command}"
    
    def _build_sub_command(self, command: str, job_name: str, queue=None, output_file=None, error_file=None, account=None):
        bsub_command = [self.settings.hpc_cluster.configuration.job_submit_command]
        
        bsub_command.append("-q")
        if queue:
            bsub_command.append(queue)
        else:
            bsub_command.append(get_settings().hpc_cluster.compute.default_queue)

        bsub_command.append("-P")
        bsub_command.append(self.cluster_settings.job_project_name)
        
        bsub_command.append("-J")
        bsub_command.append(job_name)
        if output_file:
            bsub_command.append("-o")
            bsub_command.append(output_file)
        if error_file:
            bsub_command.append("-e")
            bsub_command.append(error_file)
        if account:
            bsub_command.append("-u")
            bsub_command.append(account)
        
        return " ".join(bsub_command) + f" {command}"     
        
    def _prepare_script_to_submit_on_lsf(self, script_path: str, job_name: str, queue=None, output_file=None, error_file=None, account=None, runtime_limit:str=None, cpu:int = 1, rusage: str = "rusage[mem=2048]"):
        if not os.path.exists(script_path):
            raise MetabolightsException(message=f"Script path {script_path} does not exist.")
        lines = []
        with open(script_path, "r") as f:
            lines = f.readlines()
        
        if not lines:
            raise MetabolightsException(message=f"Script {script_path} is empty.")
        
        if lines[0].strip() == "#!/bin/bash":
            lines[0] = "\n"
        
        inputs = [x.strip() for x in lines]
        
        bsub_comments = []
        bsub_comments.append(f"-P {self.cluster_settings.job_project_name}")
        
        bsub_comments.append(f"-J {job_name}")
        if queue:
            bsub_comments.append(f"-q {queue}")
        if account:
            bsub_comments.append(f"-u {account}")
        if output_file:
            bsub_comments.append(f"-o {output_file}")
        if error_file:
            bsub_comments.append(f"-e {error_file}")
        if runtime_limit:
            bsub_comments.append(f"-W {runtime_limit}")
        if rusage:
            bsub_comments.append(f"-R {rusage}")
        bsub_comments.append(f"-n {str(cpu)}")
        
        bsub_comments = [f"#BSUB {x}" for x in bsub_comments]
        content = ["#!/bin/bash"]
        content.extend(bsub_comments)
        content.append("")
        content.extend(inputs)
        basename = os.path.basename(script_path).replace(".", "_")
        
        content = [f"{x}\n" for x in content]
        temp_file_name =  f"{basename}_bsub_script_{str(uuid.uuid4())}.sh"
        file_input_path = os.path.join(get_settings().server.temp_directory_path, temp_file_name)
        
        with open(file_input_path, "w") as f:
            f.writelines(content)
        return file_input_path
            
    def run_singularity(self, task_name: str, command: str, command_arguments: str, unique_task_name: bool = True, hpc_queue_name: str=None, additional_mounted_paths: List[str] = None):
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
                
        
        inputs = {
                    "DOCKER_BOOTSTRAP_COMMAND": command,
                    "DOCKER_APP_ROOT": worker_config.docker_deployment_path,
                    "DOCKER_BOOTSTRAP_COMMAND_ARGUMENTS": command_arguments,
                    "REMOTE_SERVER_BASE_PATH": worker_config.worker_deployment_root_path,
                    "SINGULARITY_IMAGE_DESCRIPTOR": worker_config.current_singularity_file_descriptor,
                    "CONFIG_FILE_PATH": worker_config.config_file_path,
                    "SECRETS_PATH": worker_config.secrets_path,
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
        try:
            job_id, _, _ = self.submit_hpc_job(
                        script_path, task_name, output_file=out_log_path, error_file=err_log_path, queue=hpc_queue_name
                    )
            hostname=self.settings.hpc_cluster.datamover.connection.host
            target_path = os.path.join(worker_config.worker_deployment_root_path, f"run_singularity_{task_name}.sh")
            copy_singularity_run_script = f"scp {script_path} {hostname}:{target_path}"
            BashClient.execute_command(copy_singularity_run_script)
            
            messages.append(f"New job was submitted with job id {job_id} for {task_name}")
        except Exception as exc:
            message = f"Exception after kill jobs command. {str(exc)}"
            logger.warning(message)
            messages.append(message)
            return None, messages
        finally:
            if script_path and os.path.exists(script_path):
                os.remove(script_path)
        return job_id, messages