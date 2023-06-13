
import logging
import os
import time
from typing import Union, List
from app.config import get_settings
from app.file_utils import make_dir_with_chmod

from app.services.storage_service.acl import Acl
from app.services.storage_service.file_manager import FileManager
from app.services.storage_service.models import CommandOutput
from app.services.storage_service.unmounted.data_mover_client import DataMoverAvailableStorage
from app.utils import MetabolightsException
from app.ws.cluster_jobs import submit_job
from app.ws.settings.utils import get_cluster_settings, get_study_settings


logger = logging.getLogger('wslog_datamover')

class ReadOnlyFileVolumeManager(object):
    
    def __init__(self):
        self.study_settings = get_study_settings()
        self.cluster_settings = get_cluster_settings()
    
    
    def create_folder(self, study_id: str, folder_paths: Union[str, List[str]], acl: Acl = Acl.AUTHORIZED_READ_WRITE, exist_ok: bool = True) -> bool:
        if not folder_paths:
            return False
        paths = []
        if isinstance(folder_paths, str):
            paths.append(folder_paths)
        else:
            paths = folder_paths

        try:
            result = self.create_folder_on_remote_volume(study_id, "readonly_volume_create_folder", paths, acl.value, exist_ok)
        except (OSError, Exception):
            return False

        return result

    def create_folder_on_remote_volume(self, study_id: str, requestor: str, folder_paths: Union[str, List[str]], chmod: int = 0o770, exist_ok: bool = True) -> bool:
        """
        Create folders on remote
        """
        paths = []
        if isinstance(folder_paths, str):
            paths.append(folder_paths)
        else:
            paths = folder_paths

        if paths:
            joined_paths = " ".join(paths)
            chmod_string = '2' + str(oct(chmod & 0o777)).replace('0o', '')
            command = "mkdir"
            exist_ok_param = "-p" if exist_ok else ''
            params = f"{exist_ok_param} -m {chmod_string} {joined_paths}"

            output: CommandOutput = self._execute_and_get_result(study_id, requestor, command, params)
            return output.execution_status
        else:
            return False

    def _execute_and_get_result(self, study_id: str, requestor: str, command: str, params: str) -> CommandOutput
        mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths
        study_internal_files_folder = os.path.join( get_settings().study.mounted_paths.study_internal_files_root_path, study_id)
        study_log_folder = os.path.join(study_internal_files_folder, self.study_settings.internal_logs_folder_name)
        cluster_study_internal_files_folder = os.path.join(mounted_paths.cluster_study_internal_files_root_path, study_id)
        cluster_study_log_folder = os.path.join(cluster_study_internal_files_folder, self.study_settings.internal_logs_folder_name)
        logger.info("Sending cluster job : " + command + " " + params + " ;For Study :- " + study_id)
        
        status, message, job_out, job_err, log_file = submit_job(False, None, queue=get_settings().hpc_cluster.datamover.queue_name,
                                                                 job_cmd=command, job_params=params,
                                                                 submitter=requestor, log=True,
                                                                 log_path=cluster_study_log_folder)
        log_file_name = os.path.basename(log_file)
        log_file_study_path = os.path.join(study_log_folder, log_file_name)
        status1 = self.check_if_job_successful(status=status, job_out=job_out, log_file_study_path=log_file_study_path)
        self._log_job_output(status=status, job_out=job_out, job_err=job_err, log_file_study_path=log_file_study_path)

        if command == 'stat':
            output = self.read_first_line(log_file_study_path)
        else:
            output = "None"

        return CommandOutput(execution_status=status1, execution_output=output)

    def check_if_job_successful(self, status, job_out, log_file_study_path):
        if status:
            if "is submitted to queue" in job_out:
                for x in range(0, self.cluster_settings.job_status_read_timeout):
                    if self.str_in_file(file_path=log_file_study_path, word='Successfully completed'):
                        return True
                    if self.str_in_file(file_path=log_file_study_path, word='Exited with exit code'):
                        return False
                    time.sleep(1)
                logger.error(f'Failed to read the file content in {self.cluster_settings.job_status_read_timeout} seconds')
                return False
            else:
                logger.error('Job was not submitted to queue')
                return False
        else:
            logger.error('Job submission failed!')
            return False

    def _log_job_output(self, status, job_out, job_err, log_file_study_path):
        logger.info("----------------------- ")
        logger.info("Requestor " + self.requestor)
        logger.info("Job execution status -  " + str(status))
        logger.info("Job output -  " + job_out)
        logger.info("Job error -  " + job_err)
        logger.info("Log file  -  " + log_file_study_path)
        logger.info("----------------------- ")


    def str_in_file(self, file_path, word):
        try:
            with open(file_path, 'r') as file:
                # read all content of a file
                content = file.read()
                # check if string present in a file
                if word in content:
                    return True
                else:
                    return False
        except OSError:
            logger.error('Failed to read file')
            return False

    def read_first_line(self, file_path):
        try:
            fline = open(file_path).readline().rstrip()
            return fline
        except OSError:
            logger.error('Failed to read file')
            return None
        
    def read_second_line(self, file_path):
        try:
            f = open(file_path)
            lines = f.readlines()
            return lines[1].rstrip()
        except OSError:
            logger.error('Failed to read file')
            return None

    def read_lines(self, file_path):
        try:
            output = ""
            f = open(file_path)
            lines = f.readlines()
            if len(lines) > 1:
                output = output + lines[1].rstrip()
            if len(lines) > 2:
                if 'sent' not in lines[2]:
                    output = output + "," + lines[2].rstrip()
            if len(lines) > 3:
                if 'sent' not in lines[3]:
                    output = output + "," + lines[3].rstrip()
            return output
        except OSError:
            logger.error('Failed to read file')
            return None