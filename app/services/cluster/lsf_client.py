


import datetime
import logging
import os
import re
import uuid
from enum import Enum
from typing import List, Union

from app.config import get_settings
from app.config.model.hpc_cluster import HpcClusterDefaultSettings
from app.tasks.bash_client import BashClient, CapturedBashExecutionResult
from app.services.cluster.hpc_client import HpcClient, HpcJob, SubmittedJobResult
from app.utils import MetabolightsException, current_time

logger = logging.getLogger('wslog')

from pydantic import BaseModel

    
class LsfClient(HpcClient):
    
    def __init__(self, settings: HpcClusterDefaultSettings) -> None:
        if not settings or not settings.workload_manager or not isinstance(settings, HpcClusterDefaultSettings):
            raise MetabolightsException("Invalid settings for LSF client")
        if settings.workload_manager.lower() != "lsf":
                raise MetabolightsException("Invalid workload manager for LSF client")
        super(LsfClient, self).__init__(settings)

    def convert_to_runtime_limit(self, time_in_seconds: int):
        hours = time_in_seconds // 3600
        minutes = (time_in_seconds % 3600) // 60
        return f"{int(hours):02}:{int(minutes):02}"
    
    def get_job_name_env_variable(self):
        return "LSB_JOBNAME"
    
    def submit_hpc_job(self, 
                       script_path: str, 
                       job_name: str, 
                       output_file=None, 
                       error_file=None, 
                       account=None, 
                       queue: Union[None, str] = None, 
                       timeout: Union[None, float]=30.0, 
                       runtime_limit: Union[None, str] = None, 
                       cpu: int=1,
                       mem: str="",
                       mail_type: str = ""
                       ) -> SubmittedJobResult:
        if not queue:
            queue = self.settings.default_queue
        bsub_command = self._get_submit_command(script_path, queue, job_name, output_file, error_file, account, runtime_limit=runtime_limit, mem=mem, cpu=cpu)
        logger.debug(f"Submitting job {job_name} with command\n{bsub_command}")
        result: CapturedBashExecutionResult = BashClient.execute_command(bsub_command, timeout=timeout)
        logger.debug(f"submit_hpc_job result: {result}")
        stdout = result.stdout
        status_line = result.stdout[0] if result.stdout else ""
        job_id = ""
        pattern = re.compile('Job <(.+)> is submitted to queue <(.+)>.*', re.IGNORECASE)
        match =  pattern.match(status_line)
        if match:
            match_result = match.groups()
            if not match_result[0] or not match_result[0].isnumeric():
                logger.error("No job id is defined.")
                raise MetabolightsException(message=f"No job id is defined.")
            else:
                job_id = int(match_result[0])
            if match_result[1] != queue:
                message=f"Unexpected queue, job submitted to {match_result[1]}."
                logger.error(message)
                raise MetabolightsException(message=message)
            message = f"Script was sumbitted: Job name: {job_name}, queue: {match_result[1]}, job id: {match_result[0]}"
            logger.info(message) 
        else:
            logger.error("Output does not have a job id.")
            raise MetabolightsException(message=f"output does not have a job id. {str(result.stdout)}\n {str(result.stderr)}")
            
        return SubmittedJobResult(return_code=result.returncode, job_ids=[job_id], stdout=stdout if stdout else [], stderr=result.stderr if result.stderr else [])


    def kill_jobs(self, job_id_list: List[str], failing_gracefully=False, timeout: Union[None, float]=30.0) -> SubmittedJobResult:
        kill_command = f"bkill {' '.join(job_id_list)}"
        command = self._get_hpc_ssh_command(kill_command)
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
        return SubmittedJobResult(return_code=result.returncode, job_ids=killed_job_id_list, stdout=result.stdout if result.stdout else [], stderr=result.stderr if result.stderr else [])


    def get_job_status(self, job_names: Union[None, str, List[str]]=None, timeout: Union[None, float]=30.0) -> List[HpcJob]:
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
                    queue = columns[1]
                    submit_time_str = " ".join(columns[7:]) if columns[7] else ""
                    submit_time = 0
                    try:
                        submit_datetime = datetime.datetime.strptime(submit_time_str, self.settings.stdout_datetime_format)
                        submit_datetime = submit_datetime.replace(year=current_time().year)
                        submit_time = submit_datetime.timestamp()

                    except Exception as ex:
                        pass
                    item = HpcJob(job_id=job_id, status=status, name=columns[6].strip(), submit_time=submit_time, queue=queue)
                    if  job_names:
                        if columns[6].strip() in job_names:
                            results.append(item)
                    else:
                        results.append(item)
            return results
        else:
            raise MetabolightsException(message=f"No result returned.")
        
    def _get_job_status_command(self):
        command = f"bjobs -noheader -w -P {self.settings.job_prefix}"
        return self._get_hpc_ssh_command(command)
    
    
    def _get_submit_command(self, script_path: str, job_name: str, queue=None, output_file=None, error_file=None, account=None, runtime_limit: Union[None, str] = None, cpu: int=1, mem: str="") -> str:
        script_file_path = self._prepare_script_to_submit_on_hpc(
                                                script_path=script_path, 
                                                queue=queue, 
                                                job_name=job_name, 
                                                output_file=output_file, 
                                                error_file=error_file, 
                                                account=account, 
                                                runtime_limit=runtime_limit,
                                                cpu=cpu,
                                                mem=mem)
        submission_command = f"bsub < {script_file_path}"
        return self._get_hpc_ssh_command(submission_command)
    
    def _prepare_script_to_submit_on_hpc(self, script_path: str, job_name: str, queue=None, output_file=None, error_file=None, account=None, runtime_limit: Union[None, str] = None, cpu:int = 1, mem: str = ""):
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
        bsub_comments.append(f"-P {self.settings.job_prefix}")
        
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
        else:
            bsub_comments.append(f"-W {self.convert_to_runtime_limit(self.settings.job_default_runtime_limit_in_secs)}")
        if mem:
            bsub_comments.append(f"-R {mem}")
        else:
            bsub_comments.append(f"-R rusage[mem={self.settings.job_default_memory_in_mb}MB]")
        if cpu > 1:
            bsub_comments.append(f"-n {str(cpu)}")
        else:
            bsub_comments.append(f"-n {str(self.settings.job_default_cpu)}")
        
        bsub_comments = [f"#BSUB {x}" for x in bsub_comments]
        content = ["#!/bin/bash"]
        content.extend(bsub_comments)
        content.append("")
        content.extend(inputs)
        basename = os.path.basename(script_path).replace(".", "_")
        
        content = [f"{x}\n" for x in content]
        temp_file_name =  f"{basename}_bsub_script_{str(uuid.uuid4())}.sh"
        file_input_path = os.path.join(get_settings().server.temp_directory_path, temp_file_name)
        logger.debug(f"Writing bsub script to {file_input_path}")
        logger.debug("".join(content))
        
        with open(file_input_path, "w") as f:
            f.writelines(content)
        return file_input_path

