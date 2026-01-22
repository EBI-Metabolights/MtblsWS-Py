import datetime
import logging
import os
import pathlib
import re
import time
import uuid
from typing import List, Union

from app.config import get_settings
from app.config.model.hpc_cluster import HpcClusterDefaultSettings
from app.services.cluster.hpc_client import HpcClient, HpcJob, SubmittedJobResult
from app.tasks.bash_client import BashClient, CapturedBashExecutionResult
from app.utils import MetabolightsException

logger = logging.getLogger("wslog")


class SlurmClient(HpcClient):
    def __init__(self, settings: HpcClusterDefaultSettings) -> None:
        if (
            not settings
            or not settings.workload_manager
            or not isinstance(settings, HpcClusterDefaultSettings)
        ):
            raise MetabolightsException("Invalid settings for Slurm client")
        if settings.workload_manager.lower() != "slurm":
            raise MetabolightsException("Invalid workload manager for Slurm client")
        super(SlurmClient, self).__init__(settings)

    def convert_to_runtime_limit(self, time_in_seconds: int):
        hours = time_in_seconds // 3600
        minutes = (time_in_seconds % 3600) // 60
        secs = time_in_seconds % 60
        return f"{int(hours):02}:{int(minutes):02}:{int(secs):02}"

    def get_job_name_env_variable(self):
        return "SLURM_JOB_NAME"

    def submit_hpc_job(
        self,
        script_path: str,
        job_name: str,
        output_file=None,
        error_file=None,
        account=None,
        queue: Union[None, str] = None,
        timeout: Union[None, float] = 30.0,
        runtime_limit: Union[None, str] = None,
        cpu: int = 1,
        mem: str = "",
        mail_type="START,END,FAIL",
    ) -> SubmittedJobResult:
        logger.info("===Submit HPC job request received===")
        if not queue:
            queue = self.settings.default_queue
        hpc_command = self._get_submit_command(
            script_path=script_path,
            queue=queue,
            job_name=job_name,
            output_file=output_file,
            error_file=error_file,
            account=account,
            runtime_limit=runtime_limit,
            cpu=cpu,
            mem=mem,
            mail_type=mail_type,
        )
        max_retries = 10
        delay_period_in_seconds = 20
        for iteration in range(max_retries):
            logger.info("%s. submission attempt...", (iteration + 1))
            result: CapturedBashExecutionResult = BashClient.execute_command(
                hpc_command, timeout=timeout
            )
            stdout = result.stdout
            status_line = result.stdout[0] if result.stdout else ""
            job_id = ""
            pattern = re.compile(r"Submitted batch job (.+).*", re.IGNORECASE)
            match = pattern.match(status_line)
            if match:
                match_result = match.groups()
                if not match_result[0] or not match_result[0].isnumeric():
                    logger.error(result)
                    time.sleep(delay_period_in_seconds)
                    continue
                else:
                    job_id = int(match_result[0])

                message = f"Script was sumbitted: Job name: {job_name}, queue: {queue}, job id: {match_result[0]}"
                logger.info(message)
                logger.info(result)
            else:
                logger.error(result)
                time.sleep(delay_period_in_seconds)
                continue

            return SubmittedJobResult(
                return_code=result.returncode,
                job_ids=[job_id],
                stdout=stdout if stdout else [],
                stderr=result.stderr if result.stderr else [],
            )
        raise MetabolightsException(
            message=f"Job submission failed after  {max_retries} retries."
        )

    def kill_jobs(
        self,
        job_id_list: List[str],
        failing_gracefully=False,
        timeout: Union[None, float] = 30.0,
    ) -> SubmittedJobResult:
        kill_command = f"scancel {' '.join(job_id_list)}"
        command = self._get_hpc_ssh_command(kill_command)
        result: CapturedBashExecutionResult = BashClient.execute_command(
            command, timeout=timeout
        )
        if result.returncode == 0:
            return SubmittedJobResult(
                return_code=result.returncode,
                job_ids=job_id_list,
                stdout=result.stdout if result.stdout else [],
                stderr=result.stderr if result.stderr else [],
            )

        if not failing_gracefully:
            raise MetabolightsException(message="No job id is defined.")
        return SubmittedJobResult(
            return_code=result.returncode,
            job_ids=[],
            stdout=result.stdout if result.stdout else [],
            stderr=result.stderr if result.stderr else [],
        )

    def get_job_status(
        self,
        job_names: Union[None, str, List[str]] = None,
        timeout: Union[None, float] = 30.0,
    ) -> List[HpcJob]:
        if not job_names:
            job_names = []
        elif isinstance(job_names, str):
            job_names = [job_names]

        command = self._get_job_status_command()

        result: CapturedBashExecutionResult = BashClient.execute_command(
            command, timeout=timeout
        )
        results = []
        if result and result.stdout:
            delimeter = self.settings.job_prefix_demimeter
            lines = result.stdout
            for line in lines:
                if line.strip():
                    columns = line.split("::")
                    if len(columns) < 7:
                        raise MetabolightsException(
                            message="Return format is not valid."
                        )
                    job_name = ""
                    if (
                        not columns[6]
                        .strip()
                        .startswith(f"{self.settings.job_prefix}{delimeter}")
                    ):
                        continue
                    else:
                        job_name = (
                            columns[6]
                            .strip()
                            .replace(f"{self.settings.job_prefix}{delimeter}", "")
                        )
                    status = columns[2]
                    job_id = columns[0]
                    partition = columns[1]
                    submit_time_str = " ".join(columns[7:]) if columns[7] else ""
                    submit_time = 0
                    try:
                        submit_datetime = datetime.datetime.strptime(
                            submit_time_str, self.settings.stdout_datetime_format
                        )
                        submit_time = submit_datetime.timestamp()

                    except Exception as ex:
                        pass
                    item = HpcJob(
                        job_id=job_id,
                        status=status,
                        name=columns[6].strip(),
                        submit_time=submit_time,
                        queue=partition,
                    )
                    if job_names:
                        if job_name in job_names:
                            results.append(item)
                    else:
                        results.append(item)
            return results
        else:
            raise MetabolightsException(
                message=f"No result returned for the command:\n'{command}'"
            )

    def _get_job_status_command(self):
        name_delimeter = self.settings.job_prefix_demimeter
        # slurm does not support project name, so we filter jobs by {project name}--- prefix
        # we need full job name to find the job
        # squeue parameters
        # delimeter: "::"
        # -h: no header
        # --format: format of the output
        # %i: job id
        # %P: partition
        # %T: job state
        # %u: user
        # %l: job name
        # %A: job account
        # %j: job name
        # %V: job submit time
        command = f'squeue -h --format=%i::%P::%T::%u::%l::%A::%j::%V | grep "{self.settings.job_prefix}{name_delimeter}"'
        return self._get_hpc_ssh_command(command)

    def _get_submit_command(
        self,
        script_path: str,
        job_name: str,
        queue=None,
        output_file=None,
        error_file=None,
        account=None,
        runtime_limit: Union[None, str] = None,
        cpu: int = 1,
        mem: str = "",
        mail_type: str = "START,END,FAIL",
    ) -> int:
        script_file_path = self._prepare_script_to_submit_on_hpc(
            script_path=script_path,
            queue=queue,
            job_name=job_name,
            output_file=output_file,
            error_file=error_file,
            account=account,
            runtime_limit=runtime_limit,
            cpu=cpu,
            mem=mem,
            mail_type=mail_type,
        )
        print(pathlib.Path(script_file_path).read_text())
        submission_command = f"sbatch < {script_file_path}"
        return self._get_hpc_ssh_command(submission_command)

    def _prepare_script_to_submit_on_hpc(
        self,
        script_path: str,
        job_name: str,
        queue=None,
        output_file=None,
        error_file=None,
        account=None,
        runtime_limit: Union[None, str] = None,
        cpu: int = 1,
        mem: str = "",
        mail_type: str = "START,END,FAIL",
    ):
        if not os.path.exists(script_path):
            raise MetabolightsException(
                message=f"Script path {script_path} does not exist."
            )
        lines = []
        with open(script_path, "r") as f:
            lines = f.readlines()

        if not lines:
            raise MetabolightsException(message=f"Script {script_path} is empty.")

        if lines[0].strip() == "#!/bin/bash":
            lines[0] = "\n"

        inputs = [x.strip() for x in lines]

        hpc_comments = []

        hpc_comments.append(
            f"-J {self.settings.job_prefix}{self.settings.job_prefix_demimeter}{job_name}"
        )
        if queue:
            hpc_comments.append(f"-p {queue}")
        if account:
            hpc_comments.append(f"--mail-user={account}")
            hpc_comments.append(f"--mail-type={mail_type}")
        if output_file:
            hpc_comments.append(f"-o {output_file}")
        if error_file:
            hpc_comments.append(f"-e {error_file}")
        if runtime_limit:
            hpc_comments.append(f"--time={runtime_limit}")
        else:
            default_limit_in_secs = self.settings.job_default_runtime_limit_in_secs
            default_limit = self.convert_to_runtime_limit(default_limit_in_secs)
            hpc_comments.append(f"--time={default_limit}")
        if mem:
            hpc_comments.append(f"--mem={mem}")
        else:
            hpc_comments.append(
                f"--mem={str(self.settings.job_default_memory_in_mb)}MB"
            )
        hpc_comments.append(f"-n {str(cpu)}")

        hpc_comments = [f"#SBATCH {x}" for x in hpc_comments]
        content = ["#!/bin/bash"]
        content.extend(hpc_comments)
        content.append("")
        content.extend(inputs)
        basename = os.path.basename(script_path).replace(".", "_")
        content_str = "\n".join(content)
        logger.debug(f"Script content:\n{content_str}")
        content = [f"{x}\n" for x in content]
        temp_file_name = f"{basename}_slurm_script_{str(uuid.uuid4())}.sh"
        file_input_path = os.path.join(
            get_settings().server.temp_directory_path, temp_file_name
        )

        with open(file_input_path, "w") as f:
            f.writelines(content)
        return file_input_path
