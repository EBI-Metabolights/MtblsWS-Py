import os
import time
from datetime import datetime
from typing import List, Union

from app.file_utils import make_dir_with_chmod
from app.services.storage_service.models import SyncCalculationTaskResult, SyncTaskResult, CommandOutput, \
    SyncTaskStatus, SyncCalculationStatus
from app.ws.cluster_jobs import submit_job, list_jobs
import logging

logger = logging.getLogger('wslog_datamover')


class DataMoverAvailableStorage(object):

    def __init__(self, requestor, study_id, app):
        self.app = app
        self.requestor = requestor
        self.studyId = study_id
        self.source_study_path = app.config.get('STUDY_PATH') + study_id
        self.ftp_user_home_path = app.config.get('LSF_DATAMOVER_FTP_PRIVATE_HOME')
        self.studies_root_path_datamover = app.config.get('LSF_DATAMOVER_STUDY_PATH')
        self.datamover_absolute_studies_path = os.path.join(self.ftp_user_home_path, self.studyId)

    def sync_from_studies_folder(self, target_ftp_folder: str, ignore_list: List[str] = None,
                                 **kwargs):
        result: SyncTaskResult = self.check_folder_sync_status()
        if result.status == SyncTaskStatus.RUNNING or result.status == SyncTaskStatus.PENDING:
            return False

        target_study_ftp_folder_path = self._get_absolute_ftp_private_path(target_ftp_folder)

        make_dir_with_chmod(self._get_study_log_folder(), 0o777)
        command = "rsync"
        rsync_exclude_list = self.app.config.get('RSYNC_EXCLUDE_LIST')
        exclude = ''
        for file in rsync_exclude_list:
            exclude = f'{exclude} --exclude {file}'

        if ignore_list:
            for ignore_file in ignore_list:
                exclude = f'{exclude} --exclude {ignore_file}'
        data_mover_study_path = self._get_absolute_study_datamover_path(self.studyId)
        params = f"-auv {exclude} {data_mover_study_path}/* {target_study_ftp_folder_path}/."
        submitter = f"{self.studyId}_do"
        study_log_file = os.path.join(self._get_study_log_folder(), f"{submitter}_{command}.log")
        self.create_empty_file(file_path=study_log_file)
        logger.info("Sending cluster job : " + command + " " + params + " ;For Study :- " + self.studyId)

        status, message, job_out, job_err, log_file = submit_job(False, None, queue=self.app.config.get('LSF_DATAMOVER_Q'),
                                                                 job_cmd=command, job_params=params,
                                                                 submitter=submitter, log=True,
                                                                 log_path=self._get_study_log_datamover_path())
        logger.info("Job output -  " + job_out)
        logger.info("Job error -  " + job_err)
        logger.info("Log file  -  " + study_log_file)
        return status

    def sync_from_ftp_folder(self, source_ftp_folder: str, ignore_list: List[str] = None, **kwargs) -> bool:

        result: SyncTaskResult = self.check_folder_sync_status()
        if result.status == SyncTaskStatus.RUNNING or result.status == SyncTaskStatus.PENDING:
            return False

        source_study_ftp_folder_path = self._get_absolute_ftp_private_path(source_ftp_folder)
        target_study_folder = self._get_absolute_study_datamover_path(self.studyId)
        make_dir_with_chmod(self._get_study_log_folder(), 0o777)
        command = "rsync"
        if ignore_list:
            exclude = ''
            for ignore_file in ignore_list:
                exclude = f'{exclude} --exclude {ignore_file}'
            params = f"-auv {exclude} {source_study_ftp_folder_path}/* {target_study_folder}/."
        else:
            params = f"-auv {source_study_ftp_folder_path}/* {target_study_folder}/."

        submitter = f"{self.studyId}_do"
        study_log_file = os.path.join(self._get_study_log_folder(), f"{submitter}_{command}.log")
        self.create_empty_file(file_path=study_log_file)

        logger.info("Sending cluster job : " + command + " " + params + " ;For Study :- " + self.studyId)
        status, message, job_out, job_err, log_file = submit_job(False, None, queue=self.app.config.get('LSF_DATAMOVER_Q'),
                                                                 job_cmd=command, job_params=params,
                                                                 submitter=submitter, log=True,
                                                                 log_path=self._get_study_log_datamover_path())
        logger.info("Job output -  " + job_out)
        logger.info("Job error -  " + job_err)
        logger.info("Log file  -  " + study_log_file)
        return status

    def calculate_sync(self, source_ftp_folder: str, ignore_list: List[str] = None) -> bool:
        source_study_ftp_folder_path = self._get_absolute_ftp_private_path(source_ftp_folder)
        target_study_folder = self._get_absolute_study_datamover_path(self.studyId)

        make_dir_with_chmod(self._get_study_log_folder(), 0o777)
        command = "rsync"
        if ignore_list:
            exclude = ''
            for ignore_file in ignore_list:
                exclude = f'{exclude} --exclude {ignore_file}'
            params = f"-aunv {exclude} {source_study_ftp_folder_path}/* {target_study_folder}/."
        else:
            params = f"-aunv {source_study_ftp_folder_path}/* {target_study_folder}/."

        submitter = f"{self.studyId}_calc"
        study_log_file = os.path.join(self._get_study_log_folder(), f"{submitter}_{command}.log")
        self.create_empty_file(file_path=study_log_file)

        logger.info("Sending cluster job : " + command + " " + params + " ;For Study :- " + self.studyId)
        status, message, job_out, job_err, log_file = submit_job(False, None, queue=self.app.config.get('LSF_DATAMOVER_Q'),
                                                                 job_cmd=command, job_params=params,
                                                                 submitter=submitter, log=True,
                                                                 log_path=self._get_study_log_datamover_path())
        logger.info("Job output -  " + job_out)
        logger.info("Job error -  " + job_err)
        logger.info("Log file  -  " + study_log_file)
        return status

    def check_calculate_sync_status(self, source_ftp_folder: str, force: bool = False) -> SyncCalculationTaskResult:
        job_name = f'{self.studyId}_calc_rsync'
        job_no_found = 'is not found in queue'
        result: SyncCalculationTaskResult = SyncCalculationTaskResult()
        study_log_file = os.path.join(self._get_study_log_folder(), f"{self.studyId}_calc_rsync.log")

        status, message, msg_out, msg_err = list_jobs(self.app.config.get('LSF_DATAMOVER_Q'), job_name)
        try:
            if status:
                if job_no_found in msg_err:
                    if not os.path.exists(study_log_file):
                        return self._init_calculate_sync(source_ftp_folder)
                    else:
                        result = self._check_calc_log_file_status(study_log_file, source_ftp_folder, False, force)
                        result.last_update_time = time.ctime(os.path.getmtime(study_log_file))
                        return result
                if 'JOBID' in msg_out:
                    job_id = 'NONE'
                    splitted_value = msg_out.split('\n')
                    if len(splitted_value) > 1 and splitted_value[1]:
                        job_out = splitted_value[1]
                        splitted = job_out.split(' ')
                        if len(splitted) > 1 and splitted[0]:
                            job_id = splitted[0]

                    result = self._check_calc_log_file_status(study_log_file, source_ftp_folder, True, force)
                    result.last_update_time = time.ctime(os.path.getmtime(study_log_file))
                    result.description = job_id
                    return result
                else:
                    result.status = SyncCalculationStatus.UNKNOWN
            else:
                result.status = SyncCalculationStatus.UNKNOWN
        except Exception as e:
            message = f'Could not check the job status for study sync  - {self.studyId}'
            logger.error(message + ' ;  reason  :-' + str(e))
            result.status = SyncCalculationStatus.UNKNOWN
        return result

    def _init_calculate_sync(self, source_ftp_folder: str) -> SyncCalculationTaskResult:
        result: SyncCalculationTaskResult = SyncCalculationTaskResult()
        try:
            status = self.calculate_sync(source_ftp_folder, None)
            if status:
                result.status = SyncCalculationStatus.CALCULATING
                result.last_update_time = datetime.now().strftime("%d/%m/%y %H:%M:%S.%f")
            else:
                result.status = SyncCalculationStatus.UNKNOWN
                result.last_update_time = datetime.now().strftime("%d/%m/%y %H:%M:%S.%f")
        except:
            result.status = SyncCalculationStatus.UNKNOWN
            result.last_update_time = datetime.now().strftime("%d/%m/%y %H:%M:%S.%f")
        return result

    def _check_calc_log_file_status(self, study_log_file: str, source_ftp_folder: str, job_found: bool, force: bool) -> SyncCalculationTaskResult:
        result: SyncCalculationTaskResult = SyncCalculationTaskResult()
        if not job_found:
            # check for one day case
            logfile_time = os.path.getmtime(study_log_file)
            seconds_since_epoch = datetime.now().timestamp()
            difference = seconds_since_epoch - logfile_time
            if difference > 86400:
                # More than day since log updated
                logger.info("Logfile updated since more than a day. So init calc request !")
                return self._init_calculate_sync(source_ftp_folder)

            sync_log_file = os.path.join(self._get_study_log_folder(), f"{self.studyId}_do_rsync.log")
            sync_log_file_time = os.path.getmtime(sync_log_file)
            if sync_log_file_time > logfile_time:
                # Sync happened after calculation
                logger.info("Logfile outdated as sync happened recently, so init calc request !")
                return self._init_calculate_sync(source_ftp_folder)

            if force:
                return self._init_calculate_sync(source_ftp_folder)
            else:
                if self.str_in_file(file_path=study_log_file, word='Successfully completed'):
                    # Read output
                    first_line = self.read_first_line(study_log_file)
                    if 'sending incremental file list' in first_line:
                        read_second_line = self.read_second_line(study_log_file)
                        if len(read_second_line) == 0:
                            result.status = SyncCalculationStatus.SYNC_NOT_NEEDED
                        else:
                            result.status = SyncCalculationStatus.SYNC_NEEDED
                            result.description = self.read_lines(study_log_file)
                elif self.str_in_file(file_path=study_log_file, word='Exited with exit code'):
                    logger.info("Last calculation was failure !")
                    result.status = SyncCalculationStatus.NOT_FOUND
                elif os.path.getsize(study_log_file) < 1:
                    result.status = SyncCalculationStatus.CALCULATION_FAILURE
                else:
                    result.status = SyncCalculationStatus.UNKNOWN
        else:
            if os.path.getsize(study_log_file) > 1:
                result.status = SyncCalculationStatus.CALCULATING
            else:
                result.status = SyncCalculationStatus.PENDING

        return result

    def create_ftp_folder(self, folder_path_list: Union[str, List[str]], chmod: int = 0o770, exist_ok: bool = True) -> bool:
        """
        Create FTP folder and RAW_FILES and DERIVED_FILES folders
        """
        paths = []
        if isinstance(folder_path_list, str):
            paths.append(folder_path_list)
        else:
            paths = folder_path_list

        if paths:
            study_ftp_private_paths = list()
            for file in paths:
                valid = self.check_for_invalid_values(file)
                if valid:
                    abs_path = self._get_absolute_ftp_private_path(file)
                    study_ftp_private_paths.append(abs_path)
            if not study_ftp_private_paths:
                return False

            joined_paths = " ".join(study_ftp_private_paths)

            command = f"new_dirs=({joined_paths}) ; for i in $(seq $#new_dirs[@]); do mkdir -p chmod={chmod} exist_ok={exist_ok} new_dirs[$i]; done"
            params = ''
            #command = "mkdir"
            #params = f"-p chmod={chmod} exist_ok={exist_ok} {study_ftp_private_path}"

            output: CommandOutput = self._execute_and_get_result(command, params)
            return output.execution_status
        else:
            return False

    def does_folder_exist(self, ftp_folder_name: str) -> bool:
        """
        Create FTP folder and RAW_FILES and DERIVED_FILES folders
        """
        if ftp_folder_name:
            ftp_private_path = self._get_absolute_ftp_private_path(ftp_folder_name)

            command = "ls"
            params = "-lrt " + ftp_private_path

            output: CommandOutput = self._execute_and_get_result(command, params)
            return True if output.execution_status else False
        else:
            return False

    def delete_ftp_folder(self, study_ftp_folder_name: str) -> bool:
        """
        Delete FTP study folder
        """
        if self.check_for_invalid_values(study_ftp_folder_name):
            study_ftp_private_path = self._get_absolute_ftp_private_path(study_ftp_folder_name)

            command = "rm"
            params = "-rf " + study_ftp_private_path

            output: CommandOutput = self._execute_and_get_result(command, params)
            return output.execution_status
        else:
            return False

    def move_ftp_folder(self, study_ftp_folder_name: str, target_path) -> bool:
        """
        Move FTP study folder to other path
        """
        if self.check_for_invalid_values(study_ftp_folder_name):
            study_ftp_private_path = self._get_absolute_ftp_private_path(study_ftp_folder_name)
            target_study_ftp_folder_path = self._get_absolute_ftp_private_path(target_path)

            command = "mv"
            params = study_ftp_private_path + " " + target_study_ftp_folder_path + "/."

            output: CommandOutput = self._execute_and_get_result(command, params)
            return output.execution_status
        else:
            return False

    def update_ftp_folder_permission(self, study_ftp_folder_name: str, chmod: int = 0o770, guid: bool = False) -> bool:

        if self.check_for_invalid_values(study_ftp_folder_name):
            study_ftp_private_path = self._get_absolute_ftp_private_path(study_ftp_folder_name)
            chmod_string = str(oct(chmod & 0o777)).replace('0o', '')
            command = "chmod"
            guid_value = '2' if guid else ''
            params = f"-R {guid_value}{chmod_string} {study_ftp_private_path}"

            output: CommandOutput = self._execute_and_get_result(command, params)
            return output.execution_status
        else:
            return False

    def get_ftp_folder_permission(self, study_ftp_folder_name: str, chmod: int = 770, exist_ok: bool = True) -> str:
        """
        Create FTP folder and RAW_FILES and DERIVED_FILES folders
        """
        if self.check_for_invalid_values(study_ftp_folder_name):
            study_ftp_private_path = self._get_absolute_ftp_private_path(study_ftp_folder_name)
            command = "stat"
            params = f"--format '%a' {study_ftp_private_path}"
            output: CommandOutput = self._execute_and_get_result(command, params)
            if output.execution_status:
                return output.execution_output
            else:
                return ''
        else:
            return ''

    def check_folder_sync_status(self) -> SyncTaskResult:

        job_name = f'{self.studyId}_do_rsync'
        job_no_found = 'is not found in queue'
        result: SyncTaskResult = SyncTaskResult()
        study_log_file = os.path.join(self._get_study_log_folder(), f"{self.studyId}_do_rsync.log")

        status, message, msg_out, msg_err = list_jobs(self.app.config.get('LSF_DATAMOVER_Q'), job_name)
        try:
            if status:
                if job_no_found in msg_err:
                    if not os.path.exists(study_log_file):
                        result.status = SyncTaskStatus.NO_TASK
                        result.last_update_time = "NONE"
                        result.description = "NONE"
                        return result
                    else:
                        result.status = self._check_sync_log_file_status(study_log_file, False)
                        result.last_update_time = time.ctime(os.path.getmtime(study_log_file))
                        result.description = "NONE"
                        return result
                if 'JOBID' in msg_out:
                    job_id = 'NONE'
                    splitted_value = msg_out.split('\n')
                    if len(splitted_value) > 1 and splitted_value[1]:
                        job_out = splitted_value[1]
                        splitted = job_out.split(' ')
                        if len(splitted) > 1 and splitted[0]:
                            job_id = splitted[0]

                    result.status = self._check_sync_log_file_status(study_log_file, True)
                    result.last_update_time = time.ctime(os.path.getmtime(study_log_file))
                    result.description = job_id
                    return result
                else:
                    result.status = SyncTaskStatus.UNKNOWN
                    result.last_update_time = 'NONE'
                    result.description = 'NONE'
            else:
                result.status = SyncTaskStatus.UNKNOWN
                result.last_update_time = 'NONE'
                result.description = 'NONE'
        except Exception as e:
            message = f'Could not check the job status for study sync  - {self.studyId}'
            logger.error(message + ' ;  reason  :-' + str(e))
            result.status = SyncTaskStatus.UNKNOWN
            result.last_update_time = 'NONE'
            result.description = 'NONE'
        return result

    def _check_sync_log_file_status(self, study_log_file: str, job_found: bool) -> SyncTaskStatus:
        if not job_found:
            if self.str_in_file(file_path=study_log_file, word='Successfully completed'):
                return SyncTaskStatus.COMPLETED_SUCCESS
            if self.str_in_file(file_path=study_log_file, word='Exited with exit code'):
                return SyncTaskStatus.SYNC_FAILURE
            if os.path.getsize(study_log_file) < 1:
                return SyncTaskStatus.START_FAILURE
            else:
                return SyncTaskStatus.UNKNOWN
        else:
            if os.path.getsize(study_log_file) > 1:
                return SyncTaskStatus.RUNNING
            else:
                return SyncTaskStatus.PENDING

    def _execute_and_get_result(self, command, params) -> CommandOutput:
        study_log_folder = self._get_study_log_folder()

        make_dir_with_chmod(study_log_folder, 0o777)
        self.create_empty_file(file_path=self._get_study_log_file(command=command))

        logger.info("Sending cluster job : " + command + " " + params + " ;For Study :- " + self.studyId)
        status, message, job_out, job_err, log_file = submit_job(False, None, queue=self.app.config.get('LSF_DATAMOVER_Q'),
                                                                 job_cmd=command, job_params=params,
                                                                 submitter=self.requestor, log=True,
                                                                 log_path=self._get_study_log_datamover_path())
        log_file_name = os.path.basename(log_file)
        log_file_study_path = os.path.join(study_log_folder, log_file_name)
        status1 = self.check_if_job_successful(status=status, job_out=job_out, log_file_study_path=log_file_study_path)
        logger.info("Job output -  " + job_out)
        logger.info("Job error -  " + job_err)
        logger.info("Log file  -  " + log_file_study_path)

        if command == 'stat':
            output = self.read_first_line(log_file_study_path)
        else:
            output = "None"

        return CommandOutput(execution_status=status1, execution_output=output)

    def _get_absolute_ftp_private_path(self, relative_path: str) -> str:
        return os.path.join(self.ftp_user_home_path, relative_path.lstrip('/'))

    def _get_absolute_study_datamover_path(self, relative_path: str) -> str:
        return os.path.join(self.studies_root_path_datamover, relative_path.lstrip('/'))

    def _get_study_log_folder(self) -> str:
        return os.path.join(self.source_study_path, 'audit', 'logs')

    def _get_study_log_datamover_path(self) -> str:
        return os.path.join(self.app.config.get('LSF_DATAMOVER_STUDY_PATH'), self.studyId, "audit", "logs")

    def _get_study_log_file(self, command: str) -> str:
        return os.path.join(self._get_study_log_folder(), self.requestor + "_" + command + ".log")

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
                output = output + lines[1].rstrip() + ","
            if len(lines) > 2:
                output = output + lines[2].rstrip() + ","
            if len(lines) > 3:
                output = output + lines[3].rstrip() + ","
            return output
        except OSError:
            logger.error('Failed to read file')
            return None

    def create_empty_file(self, file_path):
        try:
            with open(file_path, 'w'):
                pass
        except OSError:
            logger.error('Failed to create the file')

    def check_if_job_successful(self, status, job_out, log_file_study_path):
        if status:
            if "is submitted to queue" in job_out:
                for x in range(0, 10):
                    if self.str_in_file(file_path=log_file_study_path, word='Successfully completed'):
                        return True
                    if self.str_in_file(file_path=log_file_study_path, word='Exited with exit code'):
                        return False
                    time.sleep(1)
                logger.error('Failed to read the file content in 5 seconds')
                return False
            else:
                logger.error('Job was not submitted to queue')
                return False
        else:
            logger.error('Job submission failed!')
            return False

    def check_for_invalid_values(self, value):
        if not value:
            return False
        if value.startswith('mtbls'):
            return True
        else:
            return False
