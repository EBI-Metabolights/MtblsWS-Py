from collections import OrderedDict
import datetime
import json
import os
import pathlib
from typing import Dict
from app.config import get_settings
from app.study_folder_utils import FileDescriptor
from app.tasks.hpc_worker_bash_runner import BashExecutionTaskStatus, HpcWorkerBashRunner
from app import application_path
from app.tasks.file_utils import index_data_files


def start_data_file_index(study_id, data_files_path, index_file_root_path):
    log_path = os.path.join(index_file_root_path)
    command_terms = [f"PYTHONPATH={str(application_path)}", 
                     "python3", 
                     index_data_files.__file__,
                    #  "app/tasks/utils/index_data_files.py", 
                     study_id, 
                     data_files_path, 
                     index_file_root_path]
    command = " ".join(command_terms)
    task_name = f"{study_id}_index_data_files"
    # date_timestamp = datetime.datetime.now(datetime.UTC).strftime("%y-%m-%d-%m_%H:%M:%S")
    stdout_log_filename = f"{task_name}.sout.txt"
    stderr_log_filename = f"{task_name}.err.txt"
    stdout_log_file_path = os.path.join(log_path, stdout_log_filename)
    stderr_log_file_path = os.path.join(log_path, stderr_log_filename)
        
    runner = HpcWorkerBashRunner(
        task_name=task_name,
        study_id=study_id,
        command=command,
        stdout_log_file_path=stdout_log_file_path,
        stderr_log_file_path=stderr_log_file_path,
        )
    task_status: BashExecutionTaskStatus = runner.get_bash_execution_status(result_only=False)
    print(task_status)