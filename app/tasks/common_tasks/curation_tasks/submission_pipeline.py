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

from app.tasks.datamover_tasks.curation_tasks.submission_pipeline import (
    index_study_data_files_task,
    make_ftp_folder_readonly_task,
    revert_ftp_folder_permission_task,
    rsync_metadata_files,
)
from app.tasks.worker import (
    MetabolightsTask,
    celery,
    report_internal_technical_issue,
    send_email,
)
from app.utils import MetabolightsException
from app.ws.db.types import StudyStatus
from app.ws.db_connection import query_study_submitters, update_study_status
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from app.ws.isaApiClient import IsaApiClient
from app.ws.study.study_revision_service import StudyRevisionService
from app.ws.study.study_service import StudyService
from isatools import model as isa_model

from app.ws.study_status_utils import StudyStatusHelper

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

        if not study_id:
            raise MetabolightsException("validate_study task: Study id is not valid")
        logger.info(f"{study_id} validate_study_task is running...")

        if params.get("test"):
            return params
        user_token = get_settings().auth.service_account.api_token
        validation_endpoint = (
            get_settings().external_dependencies.api.validation_service_url
        )
        url = f"{validation_endpoint}/auth/token"
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

        url = f"{validation_endpoint}/validations/{study_id}"
        validation_params = {"run_metadata_modifiers": True}
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        response = requests.post(
            url=url, params=validation_params, headers=headers, data={}
        )
        task_id = ""
        if response.status_code == 200:
            response_json = response.json()
            if response_json and response_json.get("content"):
                task_id = response_json.get("content").get("task_id")
        if not task_id:
            raise StudySubmissionError(
                f"Study validation does not start. {response.text or ''}"
            )
        sleep_period = 10
        max_retry = 60
        retry = 0
        poll_url = f"{validation_endpoint}/validations/{study_id}/result"
        poll_params = {"summary_messages": False}
        validation_status = None
        latest_response = {}
        task_result = {}
        while retry < max_retry:
            response = requests.get(url=poll_url, params=poll_params, headers=headers)

            if response.status_code == 200:
                response_json = response.json()
                latest_response = response_json
                if response_json:
                    if response_json.get("content"):
                        content = response_json.get("content")
                        task_status = content.get("task_status", "")
                        task_result = content.get("task_result", {})
                        task_message = content.get("errorMessage", "")
                        if "not ready" not in task_message.lower():
                            if task_status.lower() not in {"success", "not ready"}:
                                raise StudySubmissionError(
                                    "Validation task failed.", response.text
                                )
                        if (
                            task_status
                            and task_status.lower() == "success"
                            and task_result
                        ):
                            validation_status = "error"
                            status = task_result.get("status", "")
                            if status.lower() in {"success", "warning"}:
                                validation_status = status.lower()
                            break
                else:
                    break
            elif response.status_code == 409:
                pass
            else:
                break

            retry += 1
            time.sleep(sleep_period)

        if validation_status is None:
            raise StudySubmissionError(
                f"Study validation status polling failed. {str(latest_response)}"
            )

        if validation_status in {"error"}:
            if not params.get("test"):
                email_settings = get_settings().email.email_service.configuration
                no_reply = email_settings.no_reply_email_address
                technical_email = email_settings.technical_issue_recipient_email_address

                submitter_emails = query_study_submitters(study_id)
                submitters_email_list = [
                    submitter[0] for submitter in submitter_emails if submitter
                ]
                # to_addresses = submitters_email_list
                to_addresses = [technical_email]
                kwargs = {
                    "subject": f"{study_id} Validation Failed",
                    "body": f"Validation result:\n\n {json.dumps(task_result, indent=2)}",
                    "from_address": no_reply,
                    "to_addresses": to_addresses,
                    "cc_addresses": None,
                }
                send_generic_email.apply_async(kwargs=kwargs)
                raise StudySubmissionError(
                    f"Study validation failed. {str(latest_response)}"
                )

        params["validate_study_task_status"] = True
        return params
    except Exception as ex:
        params["validate_study_task_status"] = False
        revert_ftp_folder_permission_task.apply_async(kwargs={"params": params})
        report_internal_technical_issue(
            f"Validate {study_id} task failed.",
            f"Validate {study_id} task failed. {str(ex)}.",
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

    if not study_id:
        raise MetabolightsException("validate_study task: Study id is not valid")

    api_token = params.get("api_token")
    if not api_token:
        raise MetabolightsException("validate_study task: api_token id isnot valid")

    if params.get("test"):
        return params
    # model = MakeStudyPrivateParameters.model_validate(params)
    # base_study_id = None
    # base_first_private_date = None
    # base_first_public_date = None
    # base_release_date  = None
    # base_status = None
    # base_update_time = None
    current_study_status = None
    try:
        study = StudyService.get_instance().get_study_by_acc(study_id)
        # base_study_id = study.acc
        # base_first_private_date = study.first_private_date
        # base_first_public_date = study.first_public_date
        # base_release_date = study.releasedate
        # base_status = study.status
        # base_update_time = study.updatedate
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
        updated_study_id = StudyStatusHelper.update_db_study_id(
            study_id,
            current_study_status,
            requested_study_status,
            study.reserved_accession,
        )

        if study_id != updated_study_id:
            StudyStatusHelper.refactor_study_folder(
                study, study_location, None, study_id, updated_study_id
            )
            isa_study_input, isa_inv, std_path = iac.get_isa_study(
                study_id=updated_study_id,
                api_key=api_token,
                skip_load_tables=True,
                study_location=study_location,
            )
            isa_study: isa_model.Study = isa_study_input
            try:
                ElasticsearchService.get_instance()._delete_study_index(
                    study_id, ignore_errors=True
                )
            except Exception as ex:
                logger.error(str(ex))

            if updated_study_id.startswith(
                get_settings().study.accession_number_prefix
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
        params["from_provisional_to_private"] = True
        return params
    except Exception as ex:
        revert_ftp_folder_permission_task.apply_async(kwargs={"params": params})
        report_internal_technical_issue(
            f"{study_id} status update to private task failed.",
            f"{study_id} status update to private task failed. {str(ex)}.",
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
    try:
        study_id = params.get("study_id")
        if not study_id:
            raise MetabolightsException("reindex_study task: Study id is not valid")

        if params.get("test"):
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
        report_internal_technical_issue(
            f"{study_id} reindex task failed.",
            f"{study_id} reindex task failed. {str(ex)}.",
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
    logger.info(f"{model.study_id} make_study_private task is successfull")


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
        rsync_metadata_files.s(),
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
        rsync_metadata_files.s(),
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
