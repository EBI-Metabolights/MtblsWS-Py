import os
import time
from typing import List, Tuple, Union
from app.config import get_settings

from app.file_utils import make_dir_with_chmod
from app.services.storage_service.models import (
    SyncCalculationTaskResult,
    SyncTaskResult,
    CommandOutput,
    SyncTaskStatus,
    SyncCalculationStatus,
)
from app.utils import current_time
from app.ws.cluster_jobs import submit_job, list_jobs
import logging
from app.ws.settings.utils import get_cluster_settings, get_study_settings

logger = logging.getLogger("wslog_datamover")


class DataMoverAvailableStorage(object):
    def __init__(self, requestor, study_id, app=None):
        self.requestor = requestor
        self.studyId = study_id
        self.settings = get_settings()
        self.cluster_settings = get_cluster_settings()
        self.study_settings = get_study_settings()
        self.meta_folder_type = "metadata"
        self.rdfiles_folder_type = "rawderived"
        self.calc_metedata_task = "calc_meta_study"
        self.calc_rdfiles_task = "calc_rdfiles_study"
        self.sync_meta_task = "rsync_meta_study"
        self.sync_rdfiles_task = "rsync_rdfiles_study"
        mounted_paths = self.settings.hpc_cluster.datamover.mounted_paths
        self.datamover_queue = self.settings.hpc_cluster.datamover.default_queue
        self.read_timeout = self.cluster_settings.job_status_read_timeout
        self.study_metadata_files_path = (
            self.settings.study.mounted_paths.study_metadata_files_root_path
        )
        self.study_readonly_files_actual_path = (
            self.settings.study.mounted_paths.study_readonly_files_actual_root_path
        )
        self.study_internal_files_path = (
            self.settings.study.mounted_paths.study_internal_files_root_path
        )
        self.study_logs_folder_name = self.study_settings.internal_logs_folder_name
        self.ftp_private_root_path = mounted_paths.cluster_private_ftp_root_path
        self.ftp_public_root_path = mounted_paths.cluster_public_ftp_root_path
        self.cluster_study_metadata_files_root_path = (
            mounted_paths.cluster_study_metadata_files_root_path
        )
        self.cluster_study_readonly_files_actual_root_path = (
            mounted_paths.cluster_study_readonly_files_actual_root_path
        )
        self.cluster_study_internal_files_root_path = (
            mounted_paths.cluster_study_internal_files_root_path
        )
        self.cluster_study_readonly_audit_files_root_path = (
            mounted_paths.cluster_study_readonly_audit_files_root_path
        )
        self.chebi_annotation_sub_folder = (
            self.settings.chebi.pipeline.chebi_annotation_sub_folder
        )

    def sync_from_studies_folder(
        self,
        target_ftp_folder: str,
        ignore_list: Union[None, List[str]] = None,
        **kwargs,
    ):
        command = "rsync"
        meta_sync_task = "rsync_meta_prv_ftp"
        files_sync_task = "rsync_rdfiles_prv_ftp"
        chebi_sync_task = "rsync_chebi_subs_only"
        meta_sync_status = None
        files_sync_status = None
        chebi_sync_status = None

        study_id = self.studyId
        target_folder = self._get_absolute_ftp_private_path(target_ftp_folder).rstrip(
            os.sep
        )
        exclude = self.build_exclusion_list(ignore_list=ignore_list)
        sync_chebi_annotation = True
        if "sync_chebi_annotation" in kwargs:
            sync_chebi_annotation = kwargs["sync_chebi_annotation"]

        make_dir_with_chmod(self._get_study_log_folder(study_id=study_id), 0o777)
        if sync_chebi_annotation:
            chebi_sync_result: SyncTaskResult = self._check_folder_sync_result(
                task_name=chebi_sync_task
            )
            if (
                chebi_sync_result.status != SyncTaskStatus.RUNNING
                and chebi_sync_result.status != SyncTaskStatus.PENDING
            ):
                source_folder = self._get_chebi_sub_folder(study_id).rstrip(os.sep)
                params = f"-aurv {source_folder} {target_folder}/"
                study_log_file = self._get_study_log_file_path(
                    study_id=study_id, task_name=chebi_sync_task
                )
                datamover_log_file = self._get_study_log_datamover_path(
                    study_id=study_id, task_name=chebi_sync_task
                )

                logger.info(
                    "sync_from_studies_folder: Syncing chebi-annotation sub folder only. Study [{study_id}]"
                )
                self.create_empty_file(file_path=study_log_file)
                logger.info(f"Sending cluster job by requestor - {self.requestor}")
                logger.info(
                    f" Task - [{chebi_sync_task}]; Command -[{command}]; params - [{params}]"
                )
                chebi_sync_job, message, job_out, job_err, log_file = submit_job(
                    email=False,
                    account=None,
                    queue=self.datamover_queue,
                    job_cmd=command,
                    job_params=params,
                    identifier=study_id,
                    taskname=chebi_sync_task,
                    log=True,
                    log_path=datamover_log_file,
                )
                self._log_job_output(
                    status=chebi_sync_job,
                    job_out=job_out,
                    job_err=job_err,
                    log_file_study_path=study_log_file,
                )
                if chebi_sync_job:
                    chebi_sync_status = SyncTaskStatus.JOB_SUBMITTED
                else:
                    chebi_sync_status = SyncTaskStatus.JOB_SUBMISSION_FAILED
            else:
                chebi_sync_status = chebi_sync_result.status
        else:
            meta_sync_result: SyncTaskResult = self._check_folder_sync_result(
                task_name=meta_sync_task
            )
            if (
                meta_sync_result.status != SyncTaskStatus.RUNNING
                and meta_sync_result.status != SyncTaskStatus.PENDING
            ):
                source_folder = self._get_absolute_cluster_study_metadata_files_path(
                    study_id
                ).rstrip(os.sep)
                params = f"-auv {source_folder}/. {target_folder}/"
                study_log_file = self._get_study_log_file_path(
                    study_id=study_id, task_name=meta_sync_task
                )
                datamover_log_file = self._get_study_log_datamover_path(
                    study_id=study_id, task_name=meta_sync_task
                )

                logger.info(
                    f"sync_from_studies_folder : Syncing  Metadata files for study[{study_id}]"
                )
                self.create_empty_file(file_path=study_log_file)
                logger.info(f"Sending cluster job by requestor - {self.requestor}")
                logger.info(
                    f" Task - [{meta_sync_task}]; Command -[{command}]; params - [{params}]"
                )
                meta_sync_job, message, job_out, job_err, log_file = submit_job(
                    email=False,
                    account=None,
                    queue=self.datamover_queue,
                    job_cmd=command,
                    job_params=params,
                    identifier=study_id,
                    taskname=meta_sync_task,
                    log=True,
                    log_path=datamover_log_file,
                )
                self._log_job_output(
                    status=meta_sync_job,
                    job_out=job_out,
                    job_err=job_err,
                    log_file_study_path=study_log_file,
                )
                if meta_sync_job:
                    meta_sync_status = SyncTaskStatus.JOB_SUBMITTED
                else:
                    meta_sync_status = SyncTaskStatus.JOB_SUBMISSION_FAILED
            else:
                meta_sync_status = meta_sync_result.status
            files_sync_result: SyncTaskResult = self._check_folder_sync_result(
                task_name=files_sync_task
            )
            if (
                files_sync_result.status != SyncTaskStatus.RUNNING
                and files_sync_result.status != SyncTaskStatus.PENDING
            ):
                source_folder = (
                    self._get_absolute_cluster_study_readonly_files_actual_path(
                        study_id
                    ).rstrip(os.sep)
                )
                params = f"-auv {source_folder}/. {target_folder}/"
                study_log_file = self._get_study_log_file_path(
                    study_id=study_id, task_name=files_sync_task
                )
                datamover_log_file = self._get_study_log_datamover_path(
                    study_id=study_id, task_name=files_sync_task
                )
                logger.info(
                    f"sync_from_studies_folder Syncing  RAW/DERIVED files for study[{study_id}]"
                )
                self.create_empty_file(file_path=study_log_file)
                logger.info(f"Sending cluster job by requestor - {self.requestor}")
                logger.info(
                    f" Task - [{files_sync_task}]; Command -[{command}]; params - [{params}]"
                )
                files_sync_job, message, job_out2, job_err2, log_file = submit_job(
                    email=False,
                    account=None,
                    queue=self.datamover_queue,
                    job_cmd=command,
                    job_params=params,
                    identifier=study_id,
                    taskname=files_sync_task,
                    log=True,
                    log_path=datamover_log_file,
                )
                self._log_job_output(
                    status=files_sync_job,
                    job_out=job_out2,
                    job_err=job_err2,
                    log_file_study_path=study_log_file,
                )
                if files_sync_job:
                    files_sync_status = SyncTaskStatus.JOB_SUBMITTED
                else:
                    files_sync_status = SyncTaskStatus.JOB_SUBMISSION_FAILED
            else:
                files_sync_status = files_sync_result.status

        return meta_sync_status, files_sync_status, chebi_sync_status

    def sync_public_study_to_ftp(
        self,
        source_study_folder: Union[None, str] = None,
        target_ftp_folder: Union[None, str] = None,
        ignore_list: Union[None, List[str]] = None,
        **kwargs,
    ):
        command = "rsync"
        meta_sync_task = "rsync_meta_pub_ftp"
        files_sync_task = "rsync_rdfiles_pub_ftp"
        meta_sync_status = None
        files_sync_status = None
        study_id = self.studyId
        target_folder = self._get_absolute_ftp_public_study_path(study_id).rstrip(
            os.sep
        )
        exclude = self.build_exclusion_list(ignore_list=ignore_list)
        make_dir_with_chmod(self._get_study_log_folder(study_id=study_id), 0o777)

        meta_sync_result: SyncTaskResult = self._check_folder_sync_result(
            task_name=meta_sync_task
        )
        if (
            meta_sync_result.status != SyncTaskStatus.RUNNING
            and meta_sync_result.status != SyncTaskStatus.PENDING
        ):
            source_folder = self._get_absolute_cluster_study_metadata_files_path(
                study_id
            ).rstrip(os.sep)
            params = f"-auv --include='*.txt' --include='*.tsv' --exclude='*' --no-links --delete {source_folder}/. {target_folder}/"
            study_log_file = self._get_study_log_file_path(
                study_id=study_id, task_name=meta_sync_task
            )
            datamover_log_file = self._get_study_log_datamover_path(
                study_id=study_id, task_name=meta_sync_task
            )
            logger.info(
                "sync_from_studies_folder Syncing  Metadata files for study : "
                + study_id
            )
            self.create_empty_file(file_path=study_log_file)
            logger.info(f"Sending cluster job by requestor - {self.requestor}")
            logger.info(
                f" Task - [{meta_sync_task}]; Command -[{command}]; params - [{params}]"
            )
            meta_sync_job, message, job_out, job_err, log_file = submit_job(
                email=False,
                account=None,
                queue=self.datamover_queue,
                job_cmd=command,
                job_params=params,
                identifier=study_id,
                taskname=meta_sync_task,
                log=True,
                log_path=datamover_log_file,
            )
            self._log_job_output(
                status=meta_sync_status,
                job_out=job_out,
                job_err=job_err,
                log_file_study_path=study_log_file,
            )
            if meta_sync_job:
                meta_sync_status = SyncTaskStatus.JOB_SUBMITTED
            else:
                meta_sync_status = SyncTaskStatus.JOB_SUBMISSION_FAILED
        else:
            meta_sync_status = meta_sync_result.status

        files_sync_result: SyncTaskResult = self._check_folder_sync_result(
            task_name=files_sync_task
        )
        if (
            files_sync_result.status != SyncTaskStatus.RUNNING
            and files_sync_result.status != SyncTaskStatus.PENDING
        ):
            source_folder = self._get_absolute_cluster_study_readonly_files_actual_path(
                study_id
            ).rstrip(os.sep)
            params = f"-auv {source_folder}/. {target_folder}/"
            study_log_file = self._get_study_log_file_path(
                study_id=study_id, task_name=files_sync_task
            )
            datamover_log_file = self._get_study_log_datamover_path(
                study_id=study_id, task_name=files_sync_task
            )
            logger.info(
                "sync_from_studies_folder Syncing  RAW/DERIVED files for study : "
                + study_id
            )
            self.create_empty_file(file_path=study_log_file)
            logger.info(f"Sending cluster job by requestor - {self.requestor}")
            logger.info(
                f" Task - [{files_sync_task}]; Command -[{command}]; params - [{params}]"
            )
            files_sync_job, message, job_out, job_err, log_file = submit_job(
                email=False,
                account=None,
                queue=self.datamover_queue,
                job_cmd=command,
                job_params=params,
                identifier=study_id,
                taskname=files_sync_task,
                log=True,
                log_path=datamover_log_file,
            )
            self._log_job_output(
                status=files_sync_status,
                job_out=job_out,
                job_err=job_err,
                log_file_study_path=study_log_file,
            )
            if files_sync_job:
                files_sync_status = SyncTaskStatus.JOB_SUBMITTED
            else:
                files_sync_status = SyncTaskStatus.JOB_SUBMISSION_FAILED
        else:
            files_sync_status = files_sync_result.status

        return meta_sync_status, files_sync_status

    def sync_from_ftp_folder(
        self,
        source_ftp_folder: str,
        ignore_list: Union[None, List[str]] = None,
        **kwargs,
    ) -> bool:
        command = "rsync"
        meta_sync_status = None
        rdfiles_sync_status = None
        study_id = self.studyId
        source_folder = self._get_absolute_ftp_private_path(source_ftp_folder).rstrip(
            os.sep
        )
        exclude = self.build_exclusion_list(ignore_list=ignore_list)
        make_dir_with_chmod(self._get_study_log_folder(study_id=study_id), 0o777)

        meta_sync_result: SyncTaskResult = self._check_folder_sync_result(
            task_name=self.sync_meta_task
        )
        if (
            meta_sync_result.status != SyncTaskStatus.RUNNING
            and meta_sync_result.status != SyncTaskStatus.PENDING
        ):
            target_folder = self._get_absolute_cluster_study_metadata_files_path(
                study_id
            ).rstrip(os.sep)
            params = f"-auv --include='*.txt' --include='*.tsv' --exclude='*' {source_folder}/. {target_folder}/"
            study_log_file = self._get_study_log_file_path(
                study_id=study_id, task_name=self.sync_meta_task
            )
            datamover_log_file = self._get_study_log_datamover_path(
                study_id=study_id, task_name=self.sync_meta_task
            )
            logger.info(
                f"sync_from_ftp_folder : Syncing  Metadata files for study[{study_id}]"
            )
            self.create_empty_file(file_path=study_log_file)
            logger.info(f"Sending cluster job by requestor - {self.requestor}")
            logger.info(
                f" Task - [{self.sync_meta_task}]; Command -[{command}]; params - [{params}]"
            )
            meta_sync_job, message, job_out, job_err, log_file = submit_job(
                email=False,
                account=None,
                queue=self.datamover_queue,
                job_cmd=command,
                job_params=params,
                identifier=study_id,
                taskname=self.sync_meta_task,
                log=True,
                log_path=datamover_log_file,
            )
            self._log_job_output(
                status=meta_sync_job,
                job_out=job_out,
                job_err=job_err,
                log_file_study_path=study_log_file,
            )
            if meta_sync_job:
                meta_sync_status = SyncTaskStatus.JOB_SUBMITTED
            else:
                meta_sync_status = SyncTaskStatus.JOB_SUBMISSION_FAILED
        else:
            meta_sync_status = meta_sync_result.status

        rdfiles_sync_result: SyncTaskResult = self._check_folder_sync_result(
            task_name=self.sync_rdfiles_task
        )
        if (
            rdfiles_sync_result.status != SyncTaskStatus.RUNNING
            and rdfiles_sync_result.status != SyncTaskStatus.PENDING
        ):
            target_folder = self._get_absolute_cluster_study_readonly_files_actual_path(
                study_id
            ).rstrip(os.sep)
            params = f"-auv {exclude} --exclude='*.txt' --exclude='*.tsv' {source_folder}/. {target_folder}/"
            study_log_file = self._get_study_log_file_path(
                study_id=study_id, task_name=self.sync_rdfiles_task
            )
            datamover_log_file = self._get_study_log_datamover_path(
                study_id=study_id, task_name=self.sync_rdfiles_task
            )
            logger.info(
                f"sync_from_ftp_folder Syncing  RAW/DERIVED files for study[{study_id}]"
            )
            self.create_empty_file(file_path=study_log_file)
            logger.info(f"Sending cluster job by requestor - {self.requestor}")
            logger.info(
                f" Task - [{self.sync_rdfiles_task}]; Command -[{command}]; params - [{params}]"
            )
            files_sync_job, message, job_out, job_err, log_file = submit_job(
                email=False,
                account=None,
                queue=self.datamover_queue,
                job_cmd=command,
                job_params=params,
                identifier=study_id,
                taskname=self.sync_rdfiles_task,
                log=True,
                log_path=datamover_log_file,
            )
            self._log_job_output(
                status=files_sync_job,
                job_out=job_out,
                job_err=job_err,
                log_file_study_path=study_log_file,
            )
            if files_sync_job:
                rdfiles_sync_status = SyncTaskStatus.JOB_SUBMITTED
            else:
                rdfiles_sync_status = SyncTaskStatus.JOB_SUBMISSION_FAILED
        else:
            rdfiles_sync_status = rdfiles_sync_result.status
        return meta_sync_status, rdfiles_sync_status

    def build_exclusion_list(self, ignore_list):
        rsync_exclude_list = get_settings().file_filters.rsync_exclude_list
        ignore_set = set()
        if ignore_list:
            ignore_set = ignore_set.union(set(ignore_list))
        if rsync_exclude_list:
            ignore_set = ignore_set.union(set(rsync_exclude_list))
        exclude = ""
        if ignore_set:
            for ignore_file in ignore_set:
                exclude = f"{exclude} --exclude '{ignore_file}'"
        return exclude

    def _sync_analysis_metafiles(
        self, source_ftp_folder: str, ignore_list: Union[None, List[str]] = None
    ) -> bool:
        command = "rsync"
        status = None
        study_id = self.studyId
        source_folder = self._get_absolute_ftp_private_path(source_ftp_folder).rstrip(
            os.sep
        )
        exclude = self.build_exclusion_list(ignore_list=ignore_list)

        target_folder = self._get_absolute_cluster_study_metadata_files_path(
            study_id
        ).rstrip(os.sep)
        params = f"-aunv --include='*.txt' --include='*.tsv' --exclude='*' {source_folder}/. {target_folder}/"
        study_log_file = self._get_study_log_file_path(
            study_id=study_id, task_name=self.calc_metedata_task
        )
        datamover_log_file = self._get_study_log_datamover_path(
            study_id=study_id, task_name=self.calc_metedata_task
        )
        logger.info(
            f"sync_from_ftp_folder : Calculating sync status of Metadata files for study[{study_id}]"
        )
        self.create_empty_file(file_path=study_log_file)
        logger.info(f"Sending cluster job by requestor - {self.requestor}")
        logger.info(
            f" Task - [{self.calc_metedata_task}]; Command -[{command}]; params - [{params}]"
        )
        status, message, job_out, job_err, log_file = submit_job(
            email=False,
            account=None,
            queue=self.datamover_queue,
            job_cmd=command,
            job_params=params,
            identifier=study_id,
            taskname=self.calc_metedata_task,
            log=True,
            log_path=datamover_log_file,
        )
        self._log_job_output(
            status=status,
            job_out=job_out,
            job_err=job_err,
            log_file_study_path=study_log_file,
        )
        return status

    def _sync_analysis_rdfiles(
        self, source_ftp_folder: str, ignore_list: Union[None, List[str]] = None
    ) -> bool:
        command = "rsync"
        status = None
        study_id = self.studyId
        source_folder = self._get_absolute_ftp_private_path(source_ftp_folder).rstrip(
            os.sep
        )
        exclude = self.build_exclusion_list(ignore_list=ignore_list)

        target_folder = self._get_absolute_cluster_study_readonly_files_actual_path(
            study_id
        ).rstrip(os.sep)
        params = f"-aunv {exclude} --exclude='*.txt' --exclude='*.tsv' {source_folder}/. {target_folder}/"
        study_log_file = self._get_study_log_file_path(
            study_id=study_id, task_name=self.calc_rdfiles_task
        )
        datamover_log_file = self._get_study_log_datamover_path(
            study_id=study_id, task_name=self.calc_rdfiles_task
        )
        logger.info(
            f"sync_from_ftp_folder Calculating Syncing status  RAW/DERIVED files for study[{study_id}]"
        )
        self.create_empty_file(file_path=study_log_file)
        logger.info(f"Sending cluster job by requestor - {self.requestor}")
        logger.info(
            f" Task - [{self.calc_rdfiles_task}]; Command -[{command}]; params - [{params}]"
        )
        status, message, job_out, job_err, log_file = submit_job(
            email=False,
            account=None,
            queue=self.datamover_queue,
            job_cmd=command,
            job_params=params,
            identifier=study_id,
            taskname=self.calc_rdfiles_task,
            log=True,
            log_path=datamover_log_file,
        )
        self._log_job_output(
            status=status,
            job_out=job_out,
            job_err=job_err,
            log_file_study_path=study_log_file,
        )

        return status

    def sync_anaysis_job_results(
        self,
        source_ftp_folder: str,
        force: bool = True,
        ignore_list: Union[None, List[str]] = None,
    ) -> Tuple[SyncCalculationTaskResult, SyncCalculationTaskResult]:
        study_id = self.studyId
        calc_meta_job = f"{study_id}_{self.calc_metedata_task}"
        calc_rdfiles_job = f"{study_id}_{self.calc_rdfiles_task}"

        meta_calc_log_file = self._get_study_log_file_path(
            study_id=study_id, task_name=self.calc_metedata_task
        )
        meta_sync_log_file = self._get_study_log_file_path(
            study_id=study_id, task_name=self.sync_meta_task
        )
        meta_calc_result = self._check_job_result(
            calc_log_file=meta_calc_log_file,
            sync_log_file=meta_sync_log_file,
            job_name=calc_meta_job,
            folder_type=self.meta_folder_type,
            source_ftp_folder=source_ftp_folder,
            ignore_list=ignore_list,
            force=force,
        )

        rdfiles_calc_log_file = self._get_study_log_file_path(
            study_id=study_id, task_name=self.calc_rdfiles_task
        )
        rdfiles_sync_log_file = self._get_study_log_file_path(
            study_id=study_id, task_name=self.sync_rdfiles_task
        )
        rdfiles_calc_result = self._check_job_result(
            calc_log_file=rdfiles_calc_log_file,
            sync_log_file=rdfiles_sync_log_file,
            job_name=calc_rdfiles_job,
            folder_type=self.rdfiles_folder_type,
            source_ftp_folder=source_ftp_folder,
            ignore_list=ignore_list,
            force=force,
        )

        return meta_calc_result, rdfiles_calc_result

    def _check_job_result(
        self,
        calc_log_file=None,
        sync_log_file=None,
        job_name=None,
        folder_type=None,
        source_ftp_folder=None,
        ignore_list=None,
        force=True,
    ):
        job_no_found = "is not found in queue"
        status, message, msg_out, msg_err = list_jobs(self.datamover_queue, job_name)
        try:
            if status:
                if job_no_found in msg_err:
                    if not os.path.exists(calc_log_file):
                        return self._init_sync_analysis(
                            folder_type=folder_type,
                            source_ftp_folder=source_ftp_folder,
                            ignore_list=ignore_list,
                        )
                    else:
                        result = self._check_calc_log_file_status(
                            calc_log_file=calc_log_file,
                            sync_log_file=sync_log_file,
                            folder_type=folder_type,
                            source_ftp_folder=source_ftp_folder,
                            job_found=False,
                            force=force,
                            ignore_list=ignore_list,
                        )
                        result.last_update_time = time.ctime(
                            os.path.getmtime(calc_log_file)
                        )
                        return result
                if "JOBID" in msg_out:
                    job_id = "NONE"
                    splitted_value = msg_out.split("\n")
                    if len(splitted_value) > 1 and splitted_value[1]:
                        job_out = splitted_value[1]
                        splitted = job_out.split(" ")
                        if len(splitted) > 1 and splitted[0]:
                            job_id = splitted[0]

                    result = self._check_calc_log_file_status(
                        calc_log_file=calc_log_file,
                        sync_log_file=sync_log_file,
                        folder_type=folder_type,
                        source_ftp_folder=source_ftp_folder,
                        job_found=True,
                        force=force,
                        ignore_list=ignore_list,
                    )
                    result.last_update_time = time.ctime(
                        os.path.getmtime(calc_log_file)
                    )
                    result.description = f"Job ID : {job_id}"
                    return result
                else:
                    result.status = SyncCalculationStatus.UNKNOWN
            else:
                result.status = SyncCalculationStatus.UNKNOWN
                # raise MetabolightsException(message=message, http_code=500)
        except Exception as e:
            message = f"Could not check the Sync analysis status for study sync  - {self.studyId}"
            logger.error(message + " ;  reason  :-" + str(e))
            result.status = SyncCalculationStatus.UNKNOWN
            # raise MetabolightsException(message=message, http_code=500, exception=e)
        return result

    def _init_sync_analysis(
        self,
        folder_type: str = "metadata",
        source_ftp_folder: str = "NONE",
        ignore_list: Union[None, List] = None,
    ) -> SyncCalculationTaskResult:
        result: SyncCalculationTaskResult = SyncCalculationTaskResult()
        try:
            make_dir_with_chmod(
                self._get_study_log_folder(study_id=self.studyId), 0o777
            )
            if folder_type == self.meta_folder_type:
                status = self._sync_analysis_metafiles(source_ftp_folder, ignore_list)
                if status:
                    result.status = SyncCalculationStatus.CALCULATING
                    result.last_update_time = current_time().strftime(
                        "%d/%m/%y %H:%M:%S.%f"
                    )
                else:
                    result.status = SyncCalculationStatus.UNKNOWN
                    result.last_update_time = current_time().strftime(
                        "%d/%m/%y %H:%M:%S.%f"
                    )
            else:
                status = self._sync_analysis_rdfiles(source_ftp_folder, ignore_list)
                if status:
                    result.status = SyncCalculationStatus.CALCULATING
                    result.last_update_time = current_time().strftime(
                        "%d/%m/%y %H:%M:%S.%f"
                    )
                else:
                    result.status = SyncCalculationStatus.UNKNOWN
                    result.last_update_time = current_time().strftime(
                        "%d/%m/%y %H:%M:%S.%f"
                    )
        except Exception as e:
            result.status = SyncCalculationStatus.UNKNOWN
            result.last_update_time = current_time().strftime("%d/%m/%y %H:%M:%S.%f")
            # raise MetabolightsException(message="Error while calculating ftp folder sync status", http_code=500, exception=e)
        return result

    def _check_calc_log_file_status(
        self,
        calc_log_file: str,
        sync_log_file: str,
        folder_type: str = "metadata",
        source_ftp_folder: str = "NONE",
        job_found: bool = False,
        force: bool = True,
        ignore_list: Union[None, List] = None,
    ) -> SyncCalculationTaskResult:
        result: SyncCalculationTaskResult = SyncCalculationTaskResult()
        if not job_found:
            # check for one day case
            logfile_time = os.path.getmtime(calc_log_file)
            seconds_since_epoch = current_time().timestamp()
            difference = seconds_since_epoch - logfile_time
            if difference > 86400:
                # More than day since log updated
                logger.info(
                    "Logfile updated since more than a day. So init calc request !"
                )
                return self._init_sync_analysis(
                    folder_type=folder_type,
                    source_ftp_folder=source_ftp_folder,
                    ignore_list=ignore_list,
                )

            if os.path.exists(sync_log_file):
                sync_log_file_time = os.path.getmtime(sync_log_file)
                if sync_log_file_time > logfile_time:
                    # Sync happened after calculation
                    logger.info(
                        "Logfile outdated as sync happened recently, so init calc request !"
                    )
                    return self._init_sync_analysis(
                        folder_type=folder_type,
                        source_ftp_folder=source_ftp_folder,
                        ignore_list=ignore_list,
                    )

            if force:
                return self._init_sync_analysis(
                    folder_type=folder_type,
                    source_ftp_folder=source_ftp_folder,
                    ignore_list=ignore_list,
                )
            else:
                if self.str_in_file(
                    file_path=calc_log_file, word="Successfully completed"
                ):
                    # Read output
                    first_line = self.read_first_line(calc_log_file)
                    if "sending incremental file list" in first_line:
                        read_second_line = self.read_second_line(calc_log_file)
                        if len(read_second_line) == 0:
                            result.status = SyncCalculationStatus.SYNC_NOT_NEEDED
                        else:
                            result.status = SyncCalculationStatus.SYNC_NEEDED
                            result.description = self.read_lines(calc_log_file)
                elif self.str_in_file(
                    file_path=calc_log_file, word="Exited with exit code"
                ):
                    logger.info("Last calculation was failure !")
                    result.status = SyncCalculationStatus.NOT_FOUND
                elif os.path.getsize(calc_log_file) < 1:
                    result.status = SyncCalculationStatus.CALCULATION_FAILURE
                else:
                    result.status = SyncCalculationStatus.UNKNOWN
        else:
            if os.path.getsize(calc_log_file) > 1:
                result.status = SyncCalculationStatus.CALCULATING
            else:
                result.status = SyncCalculationStatus.PENDING

        return result

    def create_ftp_folder(
        self,
        folder_paths: Union[str, List[str]],
        chmod: int = 0o770,
        exist_ok: bool = True,
    ) -> bool:
        """
        Create FTP folder and RAW_FILES and DERIVED_FILES folders
        """
        paths = []
        if isinstance(folder_paths, str):
            paths.append(folder_paths)
        else:
            paths = folder_paths

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
            chmod_string = "2" + str(oct(chmod & 0o777)).replace("0o", "")
            command = "mkdir"
            exist_ok_param = "-p" if exist_ok else ""
            params = f"{exist_ok_param} -m {chmod_string} {joined_paths}"
            task_name = "create-ftp-folder"
            output: CommandOutput = self._execute_and_get_result(
                task_name=task_name, command=command, params=params
            )
            return output.execution_status
        else:
            return False

    def does_folder_exist(self, ftp_folder_name: str) -> bool:
        """
        Create FTP folder and RAW_FILES and DERIVED_FILES folders
        """
        if ftp_folder_name:
            ftp_private_path = self._get_absolute_ftp_private_path(ftp_folder_name)

            output: CommandOutput = self.check_folder_exists(ftp_private_path)
            return True if output.execution_status else False
        else:
            return False

    def delete_ftp_folder(self, study_ftp_folder_name: str) -> bool:
        """
        Delete FTP study folder
        """
        if self.check_for_invalid_values(study_ftp_folder_name):
            study_ftp_private_path = self._get_absolute_ftp_private_path(
                study_ftp_folder_name
            )

            command = "rm"
            params = "-rf " + study_ftp_private_path
            task_name = "delete-ftp-folder"
            output: CommandOutput = self._execute_and_get_result(
                task_name=task_name, command=command, params=params
            )
            return output.execution_status
        else:
            return False

    def move_ftp_folder(self, study_ftp_folder_name: str, target_path) -> bool:
        """
        Move FTP study folder to other path
        """
        if self.check_for_invalid_values(study_ftp_folder_name):
            study_ftp_private_path = self._get_absolute_ftp_private_path(
                study_ftp_folder_name
            )
            target_study_ftp_folder_path = self._get_absolute_ftp_private_path(
                target_path
            )

            command = "mv"
            params = study_ftp_private_path + " " + target_study_ftp_folder_path + "/."
            task_name = "move-ftp-folder"
            output: CommandOutput = self._execute_and_get_result(
                task_name=task_name, command=command, params=params
            )
            return output.execution_status
        else:
            return False

    def update_ftp_folder_permission(
        self, study_ftp_folder_name: str, chmod: int = 0o770, guid: bool = False
    ) -> bool:
        if self.check_for_invalid_values(study_ftp_folder_name):
            study_ftp_private_path = self._get_absolute_ftp_private_path(
                study_ftp_folder_name
            )
            chmod_string = str(oct(chmod & 0o777)).replace("0o", "")
            command = "chmod"
            guid_value = "2" if guid else ""
            params = f"-R {guid_value}{chmod_string} {study_ftp_private_path}"
            task_name = "update-ftp-permission"
            output: CommandOutput = self._execute_and_get_result(
                task_name=task_name, command=command, params=params
            )
            return output.execution_status
        else:
            return False

    def get_ftp_folder_permission(
        self, study_ftp_folder_name: str, chmod: int = 770, exist_ok: bool = True
    ) -> str:
        """
        Create FTP folder and RAW_FILES and DERIVED_FILES folders
        """
        if self.check_for_invalid_values(study_ftp_folder_name):
            study_ftp_private_path = self._get_absolute_ftp_private_path(
                study_ftp_folder_name
            )
            command = "stat"
            params = f"--format '%a' {study_ftp_private_path}"
            task_name = "ftp-folder-permission"
            output: CommandOutput = self._execute_and_get_result(
                task_name=task_name, command=command, params=params
            )
            if output.execution_status:
                return output.execution_output
            else:
                return ""
        else:
            return ""

    def get_folder_sync_results(self) -> Tuple[SyncTaskResult, SyncTaskResult]:
        sync_metafiles_result = self._check_folder_sync_result(
            task_name=self.sync_meta_task
        )
        sync_rdfiles_result = self._check_folder_sync_result(
            task_name=self.sync_rdfiles_task
        )
        return sync_metafiles_result, sync_rdfiles_result

    def _check_folder_sync_result(self, task_name=None) -> SyncTaskResult:
        job_no_found = "is not found in queue"
        job_name = f"{self.studyId}_{task_name}"
        queue = self.datamover_queue
        study_log_file = self._get_study_log_file_path(
            study_id=self.studyId, task_name=task_name
        )
        result: SyncTaskResult = SyncTaskResult()
        status, message, msg_out, msg_err = list_jobs(queue=queue, job_name=job_name)
        try:
            if status:
                if job_no_found in msg_err:
                    if not os.path.exists(study_log_file):
                        result.status = SyncTaskStatus.NO_TASK
                        result.last_update_time = "NONE"
                        result.description = "NONE"
                        return result
                    else:
                        result.status = self._check_sync_log_file_status(
                            study_log_file, False
                        )
                        result.last_update_time = time.ctime(
                            os.path.getmtime(study_log_file)
                        )
                        result.description = "NONE"
                        return result
                if "JOBID" in msg_out:
                    job_id = "NONE"
                    splitted_value = msg_out.split("\n")
                    if len(splitted_value) > 1 and splitted_value[1]:
                        job_out = splitted_value[1]
                        splitted = job_out.split(" ")
                        if len(splitted) > 1 and splitted[0]:
                            job_id = splitted[0]

                    result.status = self._check_sync_log_file_status(
                        study_log_file, True
                    )
                    result.last_update_time = time.ctime(
                        os.path.getmtime(study_log_file)
                    )
                    result.description = job_id
                    return result
                else:
                    result.status = SyncTaskStatus.UNKNOWN
                    result.last_update_time = "NONE"
                    result.description = "NONE"
            else:
                result.status = SyncTaskStatus.UNKNOWN
                result.last_update_time = "NONE"
                result.description = "NONE"
        except Exception as e:
            message = f"Could not check the job status for study sync  - {self.studyId}"
            logger.error(message + " ;  reason  :-" + str(e))
            result.status = SyncTaskStatus.UNKNOWN
            result.last_update_time = "NONE"
            result.description = "NONE"
        return result

    def _check_sync_log_file_status(
        self, study_log_file: str, job_found: bool
    ) -> SyncTaskStatus:
        if not job_found:
            if self.str_in_file(
                file_path=study_log_file, word="Successfully completed"
            ):
                return SyncTaskStatus.COMPLETED_SUCCESS
            if self.str_in_file(file_path=study_log_file, word="Exited with exit code"):
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

    def _execute_and_get_result(
        self, task_name=None, command=None, params=None
    ) -> CommandOutput:
        try:
            study_id = self.studyId
            queue = self.datamover_queue
            job_log_file = self._get_study_log_file_path(
                study_id=study_id, task_name=task_name
            )
            datamover_log_file = self._get_study_log_datamover_path(
                study_id=study_id, task_name=task_name
            )
            make_dir_with_chmod(
                self._get_study_log_folder(study_id=self.studyId), 0o777
            )
            self.create_empty_file(file_path=job_log_file)

            logger.info(f"Sending cluster job by requestor - {self.requestor}")
            logger.info(f"Task - {task_name} ; Command - {command} ; params {params}")
            status, message, job_out, job_err, log_file = submit_job(
                False,
                None,
                queue=queue,
                job_cmd=command,
                job_params=params,
                identifier=study_id,
                taskname=task_name,
                log=True,
                log_path=datamover_log_file,
            )
            status1 = self.check_if_job_successful(
                status=status, job_out=job_out, log_file_study_path=job_log_file
            )
            self._log_job_output(
                status=status,
                job_out=job_out,
                job_err=job_err,
                log_file_study_path=job_log_file,
            )

            if command == "stat":
                output = self.read_first_line(job_log_file)
            else:
                output = "None"

            return CommandOutput(execution_status=status1, execution_output=output)
        except Exception as e:
            message = f"Could not execute and get results for task[{task_name}] ; study[{self.studyId}]"
            logger.error(message + " ;  reason  :-" + str(e))
        return CommandOutput(execution_status=False, execution_output="UNKNOWN")

    def check_folder_exists(self, check_folder: str) -> CommandOutput:
        try:
            command = "test"
            task_name = "check-dir"
            params = f"-d {check_folder}"
            queue = self.datamover_queue
            study_id = self.studyId
            queue = self.datamover_queue
            job_log_file = self._get_study_log_file_path(
                study_id=study_id, task_name=task_name
            )
            datamover_log_file = self._get_study_log_datamover_path(
                study_id=study_id, task_name=task_name
            )
            make_dir_with_chmod(
                self._get_study_log_folder(study_id=self.studyId), 0o777
            )
            self.create_empty_file(file_path=job_log_file)

            logger.info(f"Sending cluster job by requestor - {self.requestor}")
            logger.info(f"Task - {task_name} ; Command - {command} ; params {params}")
            status, message, job_out, job_err, log_file = submit_job(
                False,
                None,
                queue=queue,
                job_cmd=command,
                job_params=params,
                identifier=self.studyId,
                taskname=task_name,
                log=True,
                log_path=datamover_log_file,
            )
            status = self.check_if_job_successful(
                status=status, job_out=job_out, log_file_study_path=job_log_file
            )
            self._log_job_output(
                status=status,
                job_out=job_out,
                job_err=job_err,
                log_file_study_path=job_log_file,
            )

            if status:
                return CommandOutput(execution_status=True, execution_output=None)
            else:
                if self.str_in_file(
                    file_path=job_log_file, word="Exited with exit code"
                ):
                    return CommandOutput(
                        execution_status=False, execution_output="NOT_PRESENT"
                    )
        except Exception as e:
            message = (
                f"Could not check the folder existence for Study  - {self.studyId}"
            )
            logger.error(message + " ;  reason  :-" + str(e))

        return CommandOutput(execution_status=False, execution_output="UNKNOWN")

    def _log_job_output(self, status, job_out, job_err, log_file_study_path):
        logger.info("----------------------- ")
        logger.info("Requestor " + self.requestor)
        logger.info("Job execution status -  " + str(status))
        logger.info("Job output -  " + job_out)
        logger.info("Job error -  " + job_err)
        logger.info("Log file  -  " + log_file_study_path)
        logger.info("----------------------- ")

    def _get_absolute_ftp_private_path(self, relative_path: str) -> str:
        return os.path.join(self.ftp_private_root_path, relative_path.lstrip("/"))

    def _get_absolute_ftp_public_study_path(self, relative_path: str) -> str:
        return os.path.join(self.ftp_public_root_path, relative_path.lstrip("/"))

    # Don't use this method!
    def _get_absolute_study_datamover_path(self, relative_path: str) -> str:
        return os.path.join(
            "self.studies_root_path_datamover", relative_path.lstrip("/")
        )

    def _get_absolute_cluster_study_metadata_files_path(
        self, relative_path: str
    ) -> str:
        return os.path.join(
            self.cluster_study_metadata_files_root_path, relative_path.lstrip("/")
        )

    def _get_absolute_cluster_study_readonly_files_actual_path(
        self, relative_path: str
    ) -> str:
        return os.path.join(
            self.cluster_study_readonly_files_actual_root_path,
            relative_path.lstrip("/"),
        )

    def _get_absolute_cluster_study_audit_files_path(self, relative_path: str) -> str:
        return os.path.join(
            self.cluster_study_readonly_audit_files_root_path, relative_path.lstrip("/")
        )

    def _get_study_log_folder(self, study_id: str) -> str:
        return os.path.join(
            self.study_internal_files_path, study_id, self.study_logs_folder_name
        )

    def _get_chebi_sub_folder(self, study_id: str) -> str:
        return os.path.join(
            self.study_internal_files_path, study_id, self.chebi_annotation_sub_folder
        )

    def _get_study_log_datamover_path(self, study_id: str, task_name: str) -> str:
        return os.path.join(
            self._get_study_log_datamover_folder(study_id=study_id),
            study_id + "_" + task_name + ".log",
        )

    def _get_study_log_datamover_folder(self, study_id: str):
        return os.path.join(
            self.cluster_study_internal_files_root_path,
            study_id,
            self.study_logs_folder_name,
        )

    def _get_study_log_file_path(self, study_id: str, task_name: str) -> str:
        return os.path.join(
            self._get_study_log_folder(study_id=study_id),
            study_id + "_" + task_name + ".log",
        )

    def str_in_file(self, file_path, word):
        try:
            with open(file_path, "r") as file:
                # read all content of a file
                content = file.read()
                # check if string present in a file
                if word in content:
                    return True
                else:
                    return False
        except OSError:
            logger.error("Failed to read file")
            return False

    def read_first_line(self, file_path):
        try:
            fline = open(file_path).readline().rstrip()
            return fline
        except OSError:
            logger.error("Failed to read file")
            return None

    def read_second_line(self, file_path):
        try:
            f = open(file_path)
            lines = f.readlines()
            return lines[1].rstrip()
        except OSError:
            logger.error("Failed to read file")
            return None

    def read_lines(self, file_path):
        try:
            output = ""
            f = open(file_path)
            lines = f.readlines()
            if len(lines) > 1:
                output = output + lines[1].rstrip()
            if len(lines) > 2:
                if "sent" not in lines[2]:
                    output = output + "," + lines[2].rstrip()
            if len(lines) > 3:
                if "sent" not in lines[3]:
                    output = output + "," + lines[3].rstrip()
            return output
        except OSError:
            logger.error("Failed to read file")
            return None

    def create_empty_file(self, file_path):
        try:
            with open(file_path, "w"):
                pass
        except OSError:
            logger.error("Failed to create the file")

    def check_if_job_successful(self, status, job_out, log_file_study_path):
        if status:
            if "is submitted to queue" in job_out:
                for x in range(0, self.read_timeout):
                    if self.str_in_file(
                        file_path=log_file_study_path, word="Successfully completed"
                    ):
                        return True
                    if self.str_in_file(
                        file_path=log_file_study_path, word="Exited with exit code"
                    ):
                        return False
                    time.sleep(1)
                logger.error(
                    f"Failed to read the file content in {self.read_timeout} seconds"
                )
                return False
            else:
                logger.error("Job was not submitted to queue")
                return False
        else:
            logger.error("Job submission failed!")
            return False

    def check_for_invalid_values(self, value):
        if not value:
            return False
        if value.startswith("mtbls"):
            return True
        else:
            return False
