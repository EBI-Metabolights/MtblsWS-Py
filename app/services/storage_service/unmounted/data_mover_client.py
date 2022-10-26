import os
import time
from typing import List

from app.services.storage_service.models import SyncCalculationTaskResult, SyncTaskResult
#from app.ws.cluster_jobs import submit_job
from flask import current_app as app
import logging

logger = logging.getLogger('wslog_datamover')


class DataMoverAvailableStorage():

    def __init__(self, requestor, study_id):
        self.requestor = requestor
        self.studyId = study_id
        self.source_study_path = app.config.get('STUDY_PATH') + study_id
        self.ftp_user_home_path = app.config.get('LSF_DATAMOVER_FTP_PRIVATE_HOME')
        self.studies_root_path_datamover = app.config.get('LSF_DATAMOVER_STUDY_PATH')
        self.datamover_absolute_studies_path = os.path.join(self.ftp_user_home_path,
                                                            self.studyId)

    def sync_from_studies_folder(self, target_ftp_folder: str, ignore_list: List[str] = None,
                                 **kwargs):
        target_study_ftp_folder_path = self._get_absolute_ftp_private_path(target_ftp_folder)

        if not os.path.exists(self._get_study_log_folder()):
            os.makedirs(self._get_study_log_folder(), mode=777, exist_ok=True)
        command = "rsync"
        params = "-auv " + self._get_absolute_study_datamover_path(self.studyId) + "/* " + target_study_ftp_folder_path + "/."
        logger.info("Sending cluster job : " + command + "; For Study :- " + self.studyId)
        self.create_empty_file(file_path=self._get_study_log_file(command=command))

        status, message, job_out, job_err, log_file = submit_job(False, None, queue=app.config.get('LSF_DATAMOVER_Q'),
                                                                 job_cmd=command, job_params=params,
                                                                 submitter='study_to_ftp', log=True,
                                                                 log_path=self._get_study_log_datamover_path())
        return status, log_file

    def sync_from_ftp_folder(self, source_ftp_folder: str, ignore_list: List[str] = None,
                             **kwargs):
        source_study_ftp_folder_path = self._get_absolute_ftp_private_path(source_ftp_folder)
        target_study_folder =self._get_absolute_study_datamover_path(self.studyId)
        if not os.path.exists(self._get_study_log_folder()):
            os.makedirs(self._get_study_log_folder(), mode=777, exist_ok=True)
        command = "rsync"
        params = "-auv " + source_study_ftp_folder_path + "/* " + target_study_folder + "/."
        self.create_empty_file(file_path=self._get_study_log_file(command=command))

        logger.info("Sending cluster job : " + command + "; For Study :- " + self.studyId)
        status, message, job_out, job_err, log_file = submit_job(False, None, queue=app.config.get('LSF_DATAMOVER_Q'),
                                                                 job_cmd=command, job_params=params,
                                                                 submitter='ftp_to_study', log=True,
                                                                 log_path=self._get_study_log_datamover_path())
        return status, log_file

    def check_sync_status(self, log_file: str):
        log_file_name = os.path.basename(log_file)
        study_log_folder = self._get_study_log_folder()
        study_log_file_path = os.path.join(study_log_folder, log_file_name)
        if not os.path.exists(study_log_file_path):
            return 'NOT_STARTED', 'NONE'
        last_modified = time.ctime(os.path.getmtime(study_log_file_path))

        if self.str_in_file(file_path=study_log_file_path, word='Successfully completed'):
            return 'COMPLETED', last_modified
        if self.str_in_file(file_path=study_log_file_path, word='Exited with exit code'):
            return 'FAILED', last_modified
        else:
            return 'STARTED', last_modified

    def create_ftp_folder(self, study_ftp_folder_name: str, chmod: int = 770, exist_ok: bool = True) -> bool:
        """
        Create FTP folder and RAW_FILES and DERIVED_FILES folders
        """

        study_ftp_private_path = self._get_absolute_ftp_private_path(study_ftp_folder_name)

        command = "mkdir"
        params = f"-p chmod={chmod} exist_ok={exist_ok} {study_ftp_private_path}"

        return self._execute_sync_command(command, params)

    def does_folder_exist(self, ftp_folder_name: str) -> bool:
        """
        Create FTP folder and RAW_FILES and DERIVED_FILES folders
        """
        ftp_private_path = self._get_absolute_ftp_private_path(ftp_folder_name)

        command = "ls"
        params = "-lrt " + ftp_private_path

        return self._execute_sync_command(command, params)

    def delete_ftp_folder(self, study_ftp_folder_name: str) -> bool:
        """
        Delete FTP study folder
        """

        study_ftp_private_path = self._get_absolute_ftp_private_path(study_ftp_folder_name)

        command = "rm"
        params = "-rf " + study_ftp_private_path

        return self._execute_sync_command(command, params)

    def move_ftp_folder(self, study_ftp_folder_name: str, target_path) -> bool:
        """
        Move FTP study folder to other path
        """

        study_ftp_private_path = self._get_absolute_ftp_private_path(study_ftp_folder_name)
        target_study_ftp_folder_path = self._get_absolute_ftp_private_path(target_path)

        command = "mv"
        params = study_ftp_private_path + " " + target_study_ftp_folder_path + "/."

        return self._execute_sync_command(command, params)

    def update_ftp_folder_permission(self, study_ftp_folder_name: str, chmod: int = 770) -> bool:

        study_ftp_private_path = self._get_absolute_ftp_private_path(study_ftp_folder_name)

        command = "chmod"
        params = f"-R {chmod} {study_ftp_private_path}"

        return self._execute_sync_command(command, params)

    def get_ftp_folder_permission(self, study_ftp_folder_name: str, chmod: int = 770, exist_ok: bool = True) -> str:
        """
        Create FTP folder and RAW_FILES and DERIVED_FILES folders
        """

        study_ftp_private_path = self._get_absolute_ftp_private_path(study_ftp_folder_name)

        command = "stat"
        params = f"--format '%a' {study_ftp_private_path}"
        # TODO Implement
        return None


    def calculate_sync_status(self, study_id: str) -> SyncCalculationTaskResult:
        # TODO Implement
        pass

    def check_folder_sync_status(self, study_id: str) -> SyncTaskResult:
        # TODO Implement
        pass

    def _execute_sync_command(self, command, params) -> bool:
        study_log_folder = self._get_study_log_folder()

        if not os.path.exists(study_log_folder):
            os.makedirs(study_log_folder, mode=777, exist_ok=True)

        self.create_empty_file(file_path=self._get_study_log_file(command=command))

        logger.info("Sending cluster job : " + command + "; For Study :- " + self.studyId)
        status, message, job_out, job_err, log_file = submit_job(False, None, queue=app.config.get('LSF_DATAMOVER_Q'),
                                                                 job_cmd=command, job_params=params,
                                                                 submitter=self.requestor, log=True,
                                                                 log_path=self._get_study_log_datamover_path())
        log_file_name = os.path.basename(log_file)
        log_file_study_path = os.path.join(study_log_folder, log_file_name)
        return self.check_if_job_successful(status=status, job_out=job_out, log_file_study_path=log_file_study_path)

    def _get_absolute_ftp_private_path(self, relative_path: str) -> str:
        return os.path.join(self.ftp_user_home_path, relative_path.lstrip('/'))

    def _get_absolute_study_datamover_path(self, relative_path: str) -> str:
        return os.path.join(self.studies_root_path_datamover, relative_path.lstrip('/'))

    def _get_study_log_folder(self) -> str:
        return os.path.join(self.source_study_path, 'audit', 'logs')

    def _get_study_log_datamover_path(self) -> str:
        return os.path.join(app.config.get('LSF_DATAMOVER_STUDY_PATH'), self.studyId, "audit", "logs")

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

    def create_empty_file(self, file_path):
        try:
            with open(file_path, 'w'):
                pass
        except OSError:
            logger.error('Failed to create the file')

    def check_if_job_successful(self, status, job_out, log_file_study_path):
        if status:
            if "is submitted to queue" in job_out:
                for x in range(0, 5):
                    if self.str_in_file(file_path=log_file_study_path, word='Successfully completed'):
                        return True
                    if self.str_in_file(file_path=log_file_study_path, word='Exited with exit code'):
                        return False
                    time.sleep(1)
                return False
            else:
                return False
        else:
            return False

