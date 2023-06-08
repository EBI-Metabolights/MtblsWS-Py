


from enum import Enum
import logging
import os
import re
import subprocess
from typing import List, Union
import uuid
from app.tasks.bash_client import BashClient
from app.utils import MetabolightsException
from app.ws.settings.hpc_cluster import HpcClusterSettings
from app.ws.settings.utils import get_cluster_settings

logger = logging.getLogger('wslog')

class JobState(str, Enum):
    PEND = "PEND"
    RUN = "RUN"
    DONE = "DONE"
    UNKOWN = "UNKOWN"
    
class LsfClient(object):
    
    def __init__(self, cluster_settings: HpcClusterSettings=None) -> None:
        self.cluster_settings = cluster_settings
        if not cluster_settings:
            self.cluster_settings = get_cluster_settings()
        
    def submit_datamover_job(self, script_path: str, job_name: str, output_file=None, error_file=None, account=None) -> int:
        queue = self.cluster_settings.cluster_lsf_bsub_datamover_queue
        bsub_command = self._get_submit_command(script_path, queue, job_name, output_file, error_file, account)
        stdout, stderr = BashClient.execute_command(bsub_command)
        job_id = ""
        pattern = re.compile('Job <(.+)> is submitted to queue <(.+)>.*', re.IGNORECASE)
        match =  pattern.match(stdout)
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
            
        return job_id, stdout, stderr
            
    def kill_jobs(self, job_id_list: List[str], failing_gracefully=False):
        kill_command = f"{self.cluster_settings.job_kill_command} {' '.join(job_id_list)}"
        ssh_command = BashClient.build_ssh_command(hostname=self.cluster_settings.cluster_lsf_host, username=self.cluster_settings.cluster_lsf_datamover_user)       
        command = f"{ssh_command} {kill_command}"  
        stdout, stderr = BashClient.execute_command(command)
        
        pattern = re.compile('Job <(.+)>.*', re.IGNORECASE)
        lines = stdout.split("\n")
        killed_job_id_list = []
        for line in lines:
            if line.strip():
                match =  pattern.match(line)
                if match:
                    match_result = match.groups()
                    if not match_result[0] or not match_result[0].isnumeric() and not failing_gracefully:
                        raise MetabolightsException(message=f"No job id is defined.")
                           
                    killed_job_id_list.append(match_result[0])
        return killed_job_id_list, stdout, stderr        
          
    def get_job_status(self, job_names: Union[None, str, List[str]]=None):
        if not job_names:
            job_names = []
        elif isinstance(job_names, str):
            job_names = [job_names]
        
        command = self._get_job_status_command()

        stdout, stderr = BashClient.execute_command(command)  
        results = [] 
        if stdout:
            lines = stdout.split("\n")
            for line in lines:
                if line.strip():
                    columns = line.split()
                    if len(columns) < 7:
                        raise MetabolightsException(message=f"Return format is not valid.")
                    status = columns[2]
                    job_id = columns[0]
                    if  job_names:
                        if columns[6].strip() in job_names:
                            results.append((job_id, columns[6].strip(), status))
                    else:
                        results.append((job_id, columns[6].strip(), status))
            return results
        else:
            raise MetabolightsException(message=f"No result returned.")
        
    def _get_job_status_command(self):
        command = f"{self.cluster_settings.job_running_command} -noheader -w -P {self.cluster_settings.job_project_name}"
        ssh_command = BashClient.build_ssh_command(hostname=self.cluster_settings.cluster_lsf_host, username=self.cluster_settings.cluster_lsf_datamover_user)
        
        return f"{ssh_command} {command}"        
        
    def _get_submit_command(self, script: str, job_name: str, queue=None, output_file=None, error_file=None, account=None) -> int:
        ssh_command = BashClient.build_ssh_command(hostname=self.cluster_settings.cluster_lsf_host, username=self.cluster_settings.cluster_lsf_datamover_user)     
        script_file_path = self._prepare_script_to_submit_on_lsf(script, queue, job_name, output_file, error_file, account)
        submission_command = f"bsub < {script_file_path}"
        
        return f"{ssh_command} {submission_command}"
    
    def _build_sub_command(self, command: str, job_name: str, queue=None, output_file=None, error_file=None, account=None):
        bsub_command = [self.cluster_settings.job_submit_command]
        
        bsub_command.append("-q")
        if queue:
            bsub_command.append(queue)
        else:
            bsub_command.append(self.cluster_settings.cluster_lsf_bsub_default_queue)

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
        file_input_path = os.path.join("/tmp", temp_file_name)
        
        with open(file_input_path, "w") as f:
            f.writelines(content)
        return file_input_path