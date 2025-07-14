import json
import logging
import os
import time
from typing import Any

from celery import chain
import requests

from app.config import get_settings
from app.tasks.common_tasks.basic_tasks.send_email import (
    get_principal_investigator_emails,
    get_study_contacts,
    get_submitters,
    send_email_for_new_accession_number,
    send_generic_email,
)
from app.tasks.common_tasks.curation_tasks.study_revision import (
    prepare_study_revision,
    sync_study_revision,
)
from app.tasks.common_tasks.curation_tasks.submission_model import (
    MakeStudyPrivateParameters,
    MakeStudyPublicParameters,
    StudySubmissionError,
)
from isatools import model

from app.tasks.datamover_tasks.curation_tasks.submission_pipeline import (
    index_study_data_files_task,
    make_ftp_folder_readonly_task,
    revert_ftp_folder_permission_task,
    backup_metadata_files_on_private_ftp,
)
from app.tasks.worker import (
    MetabolightsTask,
    celery,
    report_internal_technical_issue,
)
from app.utils import MetabolightsException
from app.ws.db.types import StudyStatus
from app.ws.db_connection import update_study_status
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from app.ws.email.email_service import EmailService
from app.ws.isaApiClient import IsaApiClient
from app.ws.study.study_revision_service import StudyRevisionService
from app.ws.study.study_service import StudyService
from isatools import model as isa_model

from app.ws.study_status_utils import StudyStatusHelper
from app.ws.tasks.db_track import delete_task

logger = logging.getLogger(__name__)
iac = IsaApiClient()


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=1,
    max_retries=1,
    name="app.tasks.common_tasks.curation_tasks.submission_pipeline.validate_study_task",
)
def validate_study_task(self, params: dict[str, Any]):
    try:
        # model = RevalidateStudyParameters.model_validate(params)
        study_id = params.get("study_id")
        current_status = params.get("current_status")
        task_name = params.get("task_name", "")
        if task_name is None:
            raise MetabolightsException("validate_study task: task_name is not valid")

        if current_status is None:
            raise MetabolightsException(
                "validate_study task: current_status is not valid"
            )
        current_status = StudyStatus(current_status)

        if not study_id:
            raise MetabolightsException("validate_study task: Study id is not valid")
        logger.info(f"{study_id} validate_study_task is running...")

        if params.get("test"):
            logger.info(
                f"{study_id} from_provisional_to_private is in test mode. Skipping..."
            )
            return params

        target_status = params.get("target_status")

        if target_status is None:
            target_status = current_status

        target_status = StudyStatus(target_status)

        user_token = get_settings().auth.service_account.api_token
        service_email = get_settings().auth.service_account.email
        validation_endpoint = (
            get_settings().external_dependencies.api.validation_service_url
        )
        url = f"{validation_endpoint}/auth/v1/token"
        token_params = {}
        token_headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = f"grant_type=password&client_secret={user_token}"
        response = requests.post(
            url=url, params=token_params, headers=token_headers, data=data
        )
        token = ""
        response_json = ""
        if response.status_code == 200:
            response_json = response.json()
            if response_json and response_json.get("access_token"):
                token = response_json.get("access_token")

        if not token:
            raise StudySubmissionError(
                f"Validation token request failed. {response.text or ''}"
            )

        url = f"{validation_endpoint}/submissions/v2/validations/{study_id}"
        validation_params = {"run_metadata_modifiers": True}
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        logger.info(f"Start validation for {study_id}")
        response = requests.post(
            url=url, params=validation_params, headers=headers, data={}
        )
        task_id = ""
        if response.status_code == 200:
            response_json = response.json()
            if response_json and response_json.get("content"):
                task_id = response_json.get("content", {}).get("task", {}).get("taskId")
        if not task_id:
            logger.error("Study validation task id not found in response.")
            raise StudySubmissionError(
                f"Study validation does not start. {response.text or ''}"
            )
        sleep_period = 10
        max_retry = 60
        retry = 0
        poll_url = (
            f"{validation_endpoint}/submissions/v2/validations/{study_id}/{task_id}"
        )

        validation_status = None
        latest_response = {}
        task_result = {}
        while retry < max_retry:
            logger.info(
                f"Polling validation result for {study_id} (Attempt {retry + 1}/{max_retry})..."
            )
            response = requests.get(url=poll_url, headers=headers)

            if response.status_code == 200:
                response_json = response.json()
                latest_response = response_json
                if response_json:
                    if response_json.get("content"):
                        execution_status = response_json.get("status", "")
                        content: dict[str, Any] = response_json.get("content", {})
                        task = content.get("task", {})
                        task_result: dict[str, Any] = content.get("taskResult", {})

                        ready = task.get("ready", False)
                        successful = task.get("isSuccessful", False)

                        if "success" not in execution_status.lower():
                            raise StudySubmissionError(
                                "Validation task failed.", response.text
                            )

                        if ready and not successful:
                            raise StudySubmissionError(
                                "Validation task failed.", response.text
                            )
                        if ready and task_result:
                            task_result_status: str = task_result.get("status", "")
                            logger.info(
                                "Validation task ended and validation result: "
                                f"{task_result_status}"
                            )

                            validation_status = "error"
                            if task_result_status.lower() in {"success", "warning"}:
                                validation_status = task_result_status.lower()
                            break
                else:
                    break
            else:
                break

            retry += 1
            time.sleep(sleep_period)

        if validation_status is None:
            logger.error(
                f"Study validation status polling failed for {study_id}. "
                f"Latest response: {latest_response}"
            )
            raise StudySubmissionError(
                f"Study validation status polling failed. {str(latest_response)}"
            )

        if validation_status in {"error"}:
            if not params.get("test") and current_status != target_status:
                submitters = get_submitters(study_id=study_id)
                to_addresses = [x.email for x in submitters]
                created_by = params.get("created_by")
                if created_by and created_by not in to_addresses:
                    to_addresses = [created_by]

                submitter_names = [f"{x.first_name} {x.last_name}" for x in submitters]

                if len(submitter_names) == 1:
                    submitter_fullname = submitter_names[0]
                else:
                    submitter_fullname = ", ".join(
                        [
                            x.full_name
                            for i, x in enumerate(submitter_names)
                            if i < len(submitter_names) - 1
                        ]
                    )
                    submitter_fullname += " and " + submitter_names[-1]
                logger.info(
                    f"Sending validation failed email to {', '.join(to_addresses)}"
                )
                metadata_root_path = (
                    get_settings().study.mounted_paths.study_metadata_files_root_path
                )
                study_location = os.path.join(metadata_root_path, study_id)
                _, inv_study, _ = iac.get_isa_study(
                    study_id=study_id,
                    api_token=user_token,
                    study_location=study_location,
                    skip_load_tables=True,
                )
                if not inv_study:
                    raise StudySubmissionError(
                        "Study investigation file can not be found"
                    )
                inv_study: model.Study = inv_study
                EmailService.get_instance().send_email_study_validation_failed(
                    task_name=task_name,
                    study_id=study_id,
                    current_status=current_status,
                    next_status=target_status,
                    user_email=service_email,
                    submitters_mail_addresses=to_addresses,
                    submitter_fullname=submitter_fullname,
                    study_title=inv_study.title,
                )

                raise StudySubmissionError(
                    f"Study validation failed. {str(latest_response)}"
                )
        logger.info(f"Validation task for {study_id} completed successfully.")
        params["validate_study_task_status"] = True
        return params
    except Exception as ex:
        params["validate_study_task_status"] = False
        revert_ftp_folder_permission_task.apply_async(kwargs={"params": params})
        params_str = json.dumps(params, indent=2)
        params_str = params_str.replace("\n", "<br/>")
        logger.error(
            f"Validate {study_id} task failed. <br/>{str(ex)}. <br/>{params_str}"
        )
        report_internal_technical_issue(
            f"{task_name} task failed.",
            f"Validate {study_id} task failed. <br/>{str(ex)}. <br/>{params_str}",
        )
        raise ex


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=1,
    max_retries=1,
    name="app.tasks.common_tasks.curation_tasks.submission_pipeline.from_provisional_to_private",
)
def from_provisional_to_private(self, params: dict[str, Any]):
    study_id = params.get("study_id")
    task_name = params.get("task_name", "")
    if task_name is None:
        raise MetabolightsException(
            "from_provisional_to_private task: task_name is not valid"
        )
    if not study_id:
        raise MetabolightsException(
            "from_provisional_to_private task: Study id is not valid"
        )

    api_token = params.get("api_token")
    if not api_token:
        raise MetabolightsException(
            "from_provisional_to_private task: api_token id isnot valid"
        )

    if params.get("test"):
        logger.info(
            f"{study_id} from_provisional_to_private is in test mode. Skipping..."
        )
        return params

    current_study_status = None
    try:
        study = StudyService.get_instance().get_study_by_req_or_mtbls_id(study_id)
        current_study_status = StudyStatus.from_int(study.status)
    except Exception as e:
        raise e

    try:
        logger.info(f"{study_id} from_provisional_to_private_in_db task is running...")
        status = update_study_status(
            study.acc,
            study_status="private",
            first_public_date=study.first_public_date,
            first_private_date=study.first_private_date,
        )
        if not status:
            raise StudySubmissionError(
                f"{study.acc} status not updated to private in database."
            )

        requested_study_status = StudyStatus.PRIVATE
        study_id = study.acc
        obfuscation_code = study.obfuscationcode
        study_location = os.path.join(
            get_settings().study.mounted_paths.study_metadata_files_root_path, study_id
        )
        StudyStatusHelper.update_db_study_id(
            study_id,
            current_study_status,
            requested_study_status,
            study.reserved_accession,
        )
        study = StudyService.get_instance().get_study_by_req_or_mtbls_id(study_id)
        updated_study_id = study.acc
        if study_id != updated_study_id:
            isa_study: isa_model.Study = StudyStatusHelper.refactor_study_folder(
                study, study_location, None, study_id, updated_study_id
            )
            try:
                ElasticsearchService.get_instance()._delete_study_index(
                    study_id, ignore_errors=True
                )
            except Exception as ex:
                logger.error(str(ex))

            if (
                study.acc == study.reserved_accession
                and study.reserved_submission_id == study_id
            ):
                study_title = isa_study.title
                additional_cc_emails = get_principal_investigator_emails(isa_study)
                study_contacts = get_study_contacts(isa_study)
                release_date = (
                    study.releasedate.strftime("%Y-%m-%d")
                    if study.first_private_date
                    else study.releasedate.strftime("%Y-%m-%d")
                )
                inputs = {
                    "user_token": api_token,
                    "provisional_id": study_id,
                    "study_id": updated_study_id,
                    "obfuscation_code": obfuscation_code,
                    "study_title": study_title,
                    "release_date": release_date,
                    "additional_cc_emails": additional_cc_emails,
                    "study_contacts": study_contacts,
                }
                send_email_for_new_accession_number.apply_async(kwargs=inputs)
        StudyRevisionService.update_investigation_file_from_db(updated_study_id)
        if study.reserved_accession:
            delete_task(
                study_id=study.reserved_accession, task_name="UPDATE_STUDY_STATUS"
            )
        delete_task(
            study_id=study.reserved_submission_id, task_name="UPDATE_STUDY_STATUS"
        )
        params["from_provisional_to_private"] = True
        return params
    except Exception as ex:
        revert_ftp_folder_permission_task.apply_async(kwargs={"params": params})
        params_str = json.dumps(params, indent=2)
        params_str = params_str.replace("\n", "<br/>")
        logger.error(
            f"Validate {study_id} task failed. <br/>{str(ex)}. <br/>{params_str}"
        )
        report_internal_technical_issue(
            f"{task_name} task failed.",
            f"From provisional to private db status update task failed. <br/>{str(ex)}. <br/>{params_str}",
        )
        raise ex


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=1,
    max_retries=1,
    name="app.tasks.common_tasks.curation_tasks.submission_pipeline.reindex_study",
)
def reindex_study(self, params: dict[str, Any]):
    task_name = params.get("task_name", "")
    if task_name is None:
        raise MetabolightsException("reindex_study task: task_name is not valid")
    try:
        study_id = params.get("study_id")
        if not study_id:
            raise MetabolightsException("reindex_study task: Study id is not valid")

        if params.get("test"):
            logger.info(f"{study_id} reindex_study task is in test mode. Skipping...")
            return params
        es_service = ElasticsearchService.get_instance()
        es_service.reindex_study_with_task(
            study_id=study_id,
            user_token=None,
            include_validation_results=False,
            sync=False,
        )
        logger.info(f"Reindex {study_id} task completed")
        params["reindex_study_task_status"] = True
        return params
    except Exception as ex:
        params["reindex_study_task_status"] = False
        params_str = json.dumps(params, indent=2)
        params_str = params_str.replace("\n", "<br/>")
        logger.error(
            f"Validate {study_id} task failed. <br/>{str(ex)}. <br/>{params_str}"
        )
        report_internal_technical_issue(
            f"{task_name} task failed.",
            f"Reindex {study_id} task failed. <br/>{str(ex)}. <br/>{params_str}",
        )
        raise ex


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=10,
    max_retries=1,
    name="app.tasks.common_tasks.curation_tasks.submission_pipeline.make_study_private_on_success_callback",
)
def make_study_private_on_success_callback(self, params: dict[str, Any]):
    model = MakeStudyPrivateParameters.model_validate(params)
    if params.get("test"):
        logger.info(
            f"{model.study_id} start_make_study_private_pipeline task executed in test mode successfully"
        )
    else:
        logger.info(
            f"{model.study_id} start_make_study_private_pipeline task ended successfully"
        )


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=10,
    max_retries=1,
    name="app.tasks.common_tasks.curation_tasks.submission_pipeline.make_study_private_on_failure_callback",
)
def make_study_private_on_failure_callback(self, task_id: str, *args, **kwargs):
    logger.info(f"make_study_private task '{task_id}' failed")


@celery.task(
    bind=True,
    base=MetabolightsTask,
    max_retries=1,
    name="app.tasks.common_tasks.curation_tasks.submission_pipeline.start_make_study_private_pipeline",
)
def start_make_study_private_pipeline(self, params: dict[str, Any]):
    model = MakeStudyPrivateParameters.model_validate(params)
    pipeline = chain(
        make_ftp_folder_readonly_task.s(),
        backup_metadata_files_on_private_ftp.s(),
        index_study_data_files_task.s(),
        validate_study_task.s(),
        from_provisional_to_private.s(),
        reindex_study.s(),
    )
    params = model.model_dump()
    task = pipeline.apply_async(
        link=make_study_private_on_success_callback.s(),
        link_error=make_study_private_on_failure_callback.s(),
        kwargs={"params": params},
    )
    logger.info(
        f"{model.study_id} start_make_study_private_pipeline {task.task_id} is running..."
    )
    return task.task_id


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=10,
    max_retries=1,
    name="app.tasks.common_tasks.curation_tasks.submission_pipeline.make_study_public_on_success_callback",
)
def make_study_public_on_success_callback(self, params: dict[str, Any]):
    model = MakeStudyPublicParameters.model_validate(params)
    logger.info(f"{model.study_id} make_study_public task is successfull")


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=10,
    max_retries=1,
    name="app.tasks.common_tasks.curation_tasks.submission_pipeline.make_study_public_on_failure_callback",
)
def make_study_public_on_failure_callback(self, task_id: str, *args, **kwargs):
    logger.info(f"make_study_public task '{task_id}' failed")


@celery.task(
    bind=True,
    base=MetabolightsTask,
    max_retries=1,
    name="app.tasks.common_tasks.curation_tasks.submission_pipeline.start_new_public_revision_pipeline",
)
def start_new_public_revision_pipeline(self, params: dict[str, Any]):
    model = MakeStudyPublicParameters.model_validate(params)
    pipeline = chain(
        make_ftp_folder_readonly_task.s(),
        backup_metadata_files_on_private_ftp.s(),
        index_study_data_files_task.s(),
        validate_study_task.s(),
        prepare_study_revision.s(),
        sync_study_revision.s(),
    )
    params = model.model_dump()
    task = pipeline.apply_async(
        link=make_study_public_on_success_callback.s(),
        link_error=make_study_public_on_failure_callback.s(),
        kwargs={"params": params},
    )
    logger.info(
        f"{model.study_id} start_new_public_revision_pipeline {task.task_id} is running..."
    )
    return task.task_id
