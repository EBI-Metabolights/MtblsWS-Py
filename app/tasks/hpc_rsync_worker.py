import pathlib
from typing import List, Union

from pydantic import BaseModel
from app.config import get_settings
from app.services.storage_service.acl import Acl
from app.services.storage_service.models import (
    SyncCalculationStatus,
    SyncCalculationTaskResult,
    SyncTaskResult,
    SyncTaskStatus,
)
from app.tasks.bash_client import BashExecutionResult, CapturedBashExecutionResult, LoggedBashExecutionResult
from app.tasks.datamover_tasks.basic_tasks.file_management import create_folders
from app.tasks.hpc_worker_bash_runner import BashExecutionTaskStatus, HpcWorkerBashRunner
from app.tasks.utils import get_current_utc_time_string, get_utc_time_string_from_timestamp
from app.utils import current_time


class RsyncResult(BaseModel):
    valid_result: bool = False
    dry_run: bool = False
    returncode: int = -1
    files: List[str] = []
    number_of_files: int = 0
    total_bytes: int = 0
    total_size_str: str = ""
    error_message: str = ""
    success_message: str = ""

def create_remote_path(target_path: Union[str, List[str]], acl: Union[int, Acl] = Acl.AUTHORIZED_READ_WRITE):
    
    inputs = {"folder_paths": target_path, "exist_ok": True, "acl": acl}
    task = create_folders.apply_async(kwargs=inputs, expires=10)
    try:
        task.get(timeout=get_settings().hpc_cluster.configuration.task_get_timeout_in_seconds)
    except Exception as ex:
        print(f"Failed to create remote path {target_path}. {str(ex)}")
    

class HpcRsyncWorker:
    @staticmethod
    def start_rsync(
        task_name,
        study_id,
        source_path,
        target_path,
        include_list=None,
        exclude_list=None,
        rsync_arguments="-auv",
        stdout_log_file_path: Union[None, str] = None,
        stderr_log_file_path: Union[None, str] = None,
        identity_file: Union[None, str] = None,
    ) -> SyncTaskResult:
        create_remote_path([source_path, target_path])
        command = HpcRsyncWorker.build_rsync_command(
            source_path, target_path, include_list, exclude_list, rsync_arguments=rsync_arguments, identity_file=identity_file
        )
        runner = HpcWorkerBashRunner(
            task_name=task_name,
            study_id=study_id,
            command=command,
            stdout_log_file_path=stdout_log_file_path,
            stderr_log_file_path=stderr_log_file_path,
        )
        task_status: BashExecutionTaskStatus = runner.get_bash_execution_status(result_only=False)

        return HpcRsyncWorker.get_sync_task_result(task_status)

    @staticmethod
    def get_rsync_status(task_name, study_id) -> SyncTaskResult:
        runner = HpcWorkerBashRunner(task_name=task_name, study_id=study_id)
        task_status: BashExecutionTaskStatus = runner.get_bash_execution_status(result_only=True)
        return HpcRsyncWorker.get_sync_task_result(task_status)

    @staticmethod
    def start_rsync_dry_run(
        task_name,
        study_id,
        source_path,
        target_path,
        include_list=None,
        exclude_list=None,
        rsync_arguments="-aunv",
        stdout_log_file_path: Union[None, str] = None,
        stderr_log_file_path: Union[None, str] = None,
        identity_file: Union[None, str] = None
    ) -> SyncCalculationTaskResult:
        
        create_remote_path([source_path, target_path])
        command = HpcRsyncWorker.build_rsync_command(
            source_path,
            target_path,
            include_list,
            exclude_list,
            rsync_arguments=rsync_arguments,
            identity_file=identity_file
        )
        runner = HpcWorkerBashRunner(
            task_name=task_name,
            study_id=study_id,
            command=command,
            stdout_log_file_path=stdout_log_file_path,
            stderr_log_file_path=stderr_log_file_path,
        )
        task_status: BashExecutionTaskStatus = runner.get_bash_execution_status(result_only=False)

        return HpcRsyncWorker.get_sync_calculation_result(task_status)

    @staticmethod
    def get_rsync_dry_run_status(task_name, study_id) -> SyncCalculationTaskResult:
        runner = HpcWorkerBashRunner(task_name=task_name, study_id=study_id)
        task_status: BashExecutionTaskStatus = runner.get_bash_execution_status(result_only=True)
        return HpcRsyncWorker.get_sync_calculation_result(task_status)

    @staticmethod
    def get_sync_calculation_result(task_status: BashExecutionTaskStatus) -> SyncCalculationTaskResult:
        UTC_SIMPLE_DATE_FORMAT='%Y-%m-%d %H:%M:%S'
        
        task_description = task_status.description
        sync_required = False
        
        last_upate_time_val = current_time()
        last_update_time = last_upate_time_val.strftime(UTC_SIMPLE_DATE_FORMAT)
        last_update_timestamp = last_upate_time_val.timestamp()
        calc_result = SyncCalculationTaskResult()
        if task_description and task_description.task_id:
            calc_result.description = f"Task id: {task_status.description.task_id}"
            calc_result.task_id = task_status.description.task_id
        calc_result.new_task = task_status.new_task
        calc_result.last_update_time = last_update_time
        calc_result.last_update_timestamp = last_update_timestamp
        if task_status.result_ready and task_status.result:
            calc_result.task_done_timestamp  = task_status.description.task_done_time
            calc_result.task_done_time_str = get_utc_time_string_from_timestamp(calc_result.task_done_timestamp)
            rsync_result = HpcRsyncWorker.evaluate_rsync_result(task_status)
            if rsync_result.valid_result:
                
                if rsync_result.returncode > 0:
                    calc_result.status = SyncCalculationStatus.CALCULATION_FAILURE
                    calc_result.description = rsync_result.error_message
                else:
                    if rsync_result.number_of_files > 0:
                        calc_result.status = SyncCalculationStatus.SYNC_NEEDED
                    else:
                        calc_result.status = SyncCalculationStatus.SYNC_NOT_NEEDED

                    calc_result.description = rsync_result.success_message
                    
                return calc_result
            else:
                calc_result.status = SyncCalculationStatus.CALCULATION_FAILURE
                calc_result.description = rsync_result.error_message
        else:
            if task_description:
                last_update_time = get_utc_time_string_from_timestamp(task_description.last_update_time)
                calc_result.last_update_time = last_update_time
        if not task_description:
            status = SyncCalculationStatus.NO_TASK
        elif task_description.last_status == "SUCCESS":
            if sync_required:
                status = SyncCalculationStatus.SYNC_NEEDED
            status = SyncCalculationStatus.SYNC_NOT_NEEDED
        elif task_description.last_status == "FAILURE":
            status = SyncCalculationStatus.CALCULATION_FAILURE
        elif task_description.last_status == "INITIATED":
            status = SyncCalculationStatus.CALCULATING
        elif task_description.last_status == "RETRY" or task_description.last_status == "STARTED":
            status = SyncCalculationStatus.CALCULATING
        else:
            status = SyncCalculationStatus.UNKNOWN
        calc_result.status = status
        
        return calc_result

    @staticmethod
    def evaluate_rsync_result(task_status: BashExecutionTaskStatus, trimmed_files_count: int = -1) -> RsyncResult:
        rsync_result = RsyncResult()
        if not task_status.result_ready or not task_status.result:
            return rsync_result
        rsync_result.valid_result = True
        result: BashExecutionResult = task_status.result
        stdout_lines = []
        stderr_lines = []
        if isinstance(result, LoggedBashExecutionResult):
            logged_result: LoggedBashExecutionResult = result
            stdout_lines = pathlib.Path(logged_result.stdout_log_file_path).read_text().split("\n")
            stderr_lines = pathlib.Path(logged_result.stderr_log_file_path).read_text().split("\n")
        elif isinstance(result, CapturedBashExecutionResult):
            captured_result: CapturedBashExecutionResult = result
            stdout_lines = captured_result.stdout
            stderr_lines = captured_result.stderr

        rsync_result.returncode = result.returncode
        if result.returncode > 0:
            rsync_result.error_message = f"{stderr_lines[0]}..." if stdout_lines else f"Error code {result.returncode}"
            return rsync_result
        else:
            messages = []
            if len(stdout_lines) > 5:
                if stdout_lines[1].startswith("created directory"):
                    rsync_result.files = stdout_lines[3:-4]
                else:
                    rsync_result.files = stdout_lines[2:-4]
                rsync_result.number_of_files = len(rsync_result.files)
                messages.append(f"Number of new/updated files: {len(rsync_result.files)}.")

            size_line = stdout_lines[-2]
            if size_line.startswith("total size is"):
                try:
                    rsync_result.total_bytes = int(size_line.split()[3].replace(",", ""))
                    rsync_result.total_size_str = f"{round(rsync_result.total_bytes/(1.0*1024*1024), 2):02} MB"

                    messages.append(f"Total size: {rsync_result.total_size_str}.")
                    if rsync_result.number_of_files == 0 and rsync_result.total_bytes > 0:
                        messages.append("Files are empty.")
                except Exception as ex:
                    print(f"There is no 'total size is' line. {str(ex)}")

            if trimmed_files_count >= 0 and len(rsync_result.files) > trimmed_files_count:
                trimmed_files = stdout_lines[:trimmed_files_count]
                rsync_result.files = trimmed_files
                messages.append(f"First {trimmed_files_count} files: {', '.join(trimmed_files)} ...")
            elif len(rsync_result.files) > 0:
                messages.append(f"Files: {', '.join(rsync_result.files)}.")

            if rsync_result.number_of_files > 0:
                rsync_result.success_message = " ".join(messages)
            else:
                rsync_result.success_message = "There is no file to synchronise"

            return rsync_result

    @staticmethod
    def get_sync_task_result(task_status: BashExecutionTaskStatus) -> SyncTaskResult:
        task_description = task_status.description
        if not task_description:
            status = SyncTaskStatus.NO_TASK
        elif task_description.last_status == "SUCCESS":
            status = SyncTaskStatus.COMPLETED_SUCCESS
        elif task_description.last_status == "FAILURE":
            status = SyncTaskStatus.COMPLETED_SUCCESS
        elif task_description.last_status == "INITIATED":
            status = SyncTaskStatus.JOB_SUBMITTED
        elif task_description.last_status == "RETRY" or task_description.last_status == "STARTED":
            status = SyncTaskStatus.RUNNING
        else:
            status = SyncTaskStatus.UNKNOWN

        if task_description:
            last_update_time = get_utc_time_string_from_timestamp(task_description.last_update_time)
            last_update_timestamp = task_description.last_update_time
            task_done_time_str = get_utc_time_string_from_timestamp(task_description.task_done_time)
            task_done_timestamp = task_description.task_done_time
        else:
            last_update_time = get_current_utc_time_string()
            task_done_time_str = ""
            task_done_timestamp = 0
            last_update_timestamp = 0
            
        task_result: SyncTaskResult = SyncTaskResult(
            status=status, description="...", 
            last_update_time=last_update_time, 
            last_update_timestamp=last_update_timestamp, 
            task_done_time_str=task_done_time_str,
            task_done_timestamp=task_done_timestamp,  
            dry_run=False
        )
        if task_description and task_description.task_id:
            task_result.task_id = task_status.description.task_id
        if task_status.result_ready and task_status.result:
            rsync_result = HpcRsyncWorker.evaluate_rsync_result(task_status)
            if rsync_result.valid_result:
                if rsync_result.returncode > 0:
                    task_result.description = rsync_result.error_message
                    task_result.status = SyncTaskStatus.SYNC_FAILURE
                else:
                    task_result.description = rsync_result.success_message
                    task_result.status = SyncTaskStatus.COMPLETED_SUCCESS
            else:
                task_result.description = rsync_result.error_message
                task_result.status = SyncTaskStatus.SYNC_FAILURE
        return task_result

    @staticmethod
    def build_rsync_command(
        source_path: str, target_path: str, include_list=None, exclude_list=None, rsync_arguments: str = "-aunv", 
        identity_file: Union[None, str] = None, created_remote_path: Union[None, str] = None
    ):
        source_path = source_path.rstrip("/")
        target_path = target_path.rstrip(".").rstrip("/")

        command_terms = ["rsync"]
        command_terms.append(rsync_arguments)
        
        if identity_file and (":" in source_path or ":" in target_path):
            command_terms.append("-e")
            ssh_command = f"ssh -i {identity_file}"
            command_terms.append(f'\"{ssh_command}\"')
        if created_remote_path:
            command_terms.append(f'-–rsync-path="mkdir -p {created_remote_path}/ && rsync"')
        if include_list:
            for item in include_list:
                command_terms.append(f"--include='{item}'")
        if exclude_list:
            for item in exclude_list:
                command_terms.append(f"--exclude='{item}'")
        command_terms.append(f'"{source_path}/."')
        command_terms.append(f'"{target_path}/"')

        command = " ".join(command_terms)
        return command
