


import datetime
import logging
import os
import re
import shutil
import uuid
from enum import Enum
from pathlib import Path
from typing import List, Union

from app.config import get_settings
from app.config.model.hpc_cluster import HpcClusterConfiguration
from app.tasks.bash_client import BashClient, CapturedBashExecutionResult
from app.tasks.hpc_client import HpcClient, HpcJob
from app.tasks.hpc_rsync_worker import HpcRsyncWorker
from app.utils import MetabolightsException, current_time
from app.ws.settings.utils import get_cluster_settings

logger = logging.getLogger('wslog')

from pydantic import BaseModel


class JobState(str, Enum):
    PEND = "PEND"
    RUN = "RUN"
    DONE = "DONE"
    UNKOWN = "UNKOWN"

class SlurmClient(HpcClient):
    
    def __init__(self, cluster_settings: Union[None, HpcClusterConfiguration] = None, submit_with_ssh: bool=True) -> None:
        super(SlurmClient, self).__init__(cluster_settings, submit_with_ssh, datetime_format="%Y-%m-%dT%H:%M:%S")

    def convert_to_runtime_limit(self, time_in_seconds: int):
        hours = time_in_seconds // 3600
        minutes = (time_in_seconds % 3600) // 60
        secs = time_in_seconds % 60
        return f"{int(hours):02}:{int(minutes):02}:{int(secs):02}"
        
    def submit_hpc_job(self, script_path: str, job_name: str, output_file=None, error_file=None, account=None, queue: Union[None, str] = None, timeout: Union[None, float]=30.0, runtime_limit: Union[None, str] = None) -> int:
        if not queue:
            queue = get_settings().hpc_cluster.datamover.queue_name
        hpc_command = self._get_submit_command(script_path, queue, job_name, output_file, error_file, account, runtime_limit=runtime_limit)
        result: CapturedBashExecutionResult = BashClient.execute_command(hpc_command, timeout=timeout)
        stdout = result.stdout
        status_line = result.stdout[0] if result.stdout else ""
        job_id = ""
        pattern = re.compile('Submitted batch job (.+).*', re.IGNORECASE)
        match =  pattern.match(status_line)
        if match:
            match_result = match.groups()
            if not match_result[0] or not match_result[0].isnumeric():
                raise MetabolightsException(message=f"No job id is defined.")
            else:
                job_id = int(match_result[0])

            message = f"Script was sumbitted: Job name: {job_name}, queue: {queue}, job id: {match_result[0]}"
            logger.info(message) 
            print(message)
        else:
            raise MetabolightsException(message="output does not have a job id.")
            
        return job_id, stdout, result.stderr
            
    def kill_jobs(self, job_id_list: List[str], failing_gracefully=False, timeout: Union[None, float]=15.0):
        kill_command = f"scancel {' '.join(job_id_list)}"
        command = self._get_hpc_ssh_command(kill_command)
        result: CapturedBashExecutionResult = BashClient.execute_command(command, timeout=timeout)
        if result.returncode == 0:
            return job_id_list, result.stdout, result.stderr
        if not failing_gracefully:
            raise MetabolightsException(message=f"No job id is defined.")
        return [], result.stdout, result.stderr
        

    def get_job_status(self, job_names: Union[None, str, List[str]]=None, timeout: Union[None, float]=15.0) -> List[HpcJob]:
        if not job_names:
            job_names = []
        elif isinstance(job_names, str):
            job_names = [job_names]
        
        command = self._get_job_status_command()

        result: CapturedBashExecutionResult = BashClient.execute_command(command, timeout=timeout)  
        results = [] 
        if result and result.stdout:
            lines = result.stdout
            for line in lines:
                if line.strip():
                    
                    columns = line.split("::")
                    if len(columns) < 7:
                        raise MetabolightsException(message=f"Return format is not valid.")
                    job_name = ""
                    if not columns[6].strip().startswith(f"{self.cluster_settings.job_project_name}---"):
                        continue
                    else:
                        job_name = columns[6].strip().replace(f"{self.cluster_settings.job_project_name}---", "")
                    status = columns[2]
                    job_id = columns[0]
                    submit_time_str = " ".join(columns[7:]) if columns[7] else ""
                    submit_time = 0
                    try:
                        submit_datetime = datetime.datetime.strptime(submit_time_str, self.datetime_format)
                        submit_time = submit_datetime.timestamp()

                    except Exception as ex:
                        pass
                    item = HpcJob(job_id=job_id, status=status, name=columns[6].strip(), submit_time=submit_time)
                    if  job_names:
                        if job_name in job_names:
                            results.append(item)
                    else:
                        results.append(item)
            return results
        else:
            raise MetabolightsException(message=f"No result returned for the command:\n'{command}'")
        
    def _get_job_status_command(self):
        command = f'squeue -h --format=%i::%P::%T::%u::%l::%A::%j::%V'
        return self._get_hpc_ssh_command(command)
    
    
    def _get_submit_command(self, script: str, job_name: str, queue=None, output_file=None, error_file=None, account=None, runtime_limit: Union[None, str] = None) -> int:
        script_file_path = self._prepare_script_to_submit_on_hpc(script, queue, job_name, output_file, error_file, account, runtime_limit=runtime_limit)
        submission_command = f"sbatch < {script_file_path}"
        return self._get_hpc_ssh_command(submission_command)
  
    def _prepare_script_to_submit_on_hpc(self, script_path: str, job_name: str, queue=None, output_file=None, error_file=None, account=None, runtime_limit: Union[None, str] = None, cpu:int = 1, rusage: str = "4G"):
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
        
        hpc_comments = []
        # hpc_comments.append(f"-P {self.cluster_settings.job_project_name}")
        
        hpc_comments.append(f"-J {self.cluster_settings.job_project_name}---{job_name}")
        if queue:
            hpc_comments.append(f"-p {queue}")
        if account:
            hpc_comments.append(f"-A {account}")
        if output_file:
            hpc_comments.append(f"-o {output_file}")
        if error_file:
            hpc_comments.append(f"-e {error_file}")
        if runtime_limit:
            hpc_comments.append(f"--time={runtime_limit}")
        else:
            hpc_comments.append(f"--time=12:00:00")
        if rusage:
            hpc_comments.append(f"--mem={rusage}")
        hpc_comments.append(f"-n {str(cpu)}")
        
        hpc_comments = [f"#SBATCH {x}" for x in hpc_comments]
        content = ["#!/bin/bash"]
        content.extend(hpc_comments)
        content.append("")
        content.extend(inputs)
        basename = os.path.basename(script_path).replace(".", "_")
        
        content = [f"{x}\n" for x in content]
        temp_file_name =  f"{basename}_slurm_script_{str(uuid.uuid4())}.sh"
        file_input_path = os.path.join(get_settings().server.temp_directory_path, temp_file_name)
        
        with open(file_input_path, "w") as f:
            f.writelines(content)
        return file_input_path
