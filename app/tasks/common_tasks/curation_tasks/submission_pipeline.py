import logging
import time
from typing import Any

from celery import chain
import requests

from app.config import get_settings
from app.tasks.common_tasks.curation_tasks.submission_model import (
    MakeStudyPrivateParameters,
    RevalidateStudyParameters,
    StudySubmissionError,
)
from app.tasks.datamover_tasks.curation_tasks.submission_pipeline import (
    index_study_data_files_task,
    make_ftp_folder_readonly_task,
    make_ftp_folder_writable_task,
    rsync_metadata_files,
)
from app.tasks.worker import MetabolightsTask, celery, report_internal_technical_issue
from app.ws.db_connection import update_study_status
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from app.ws.study.study_service import StudyService

logger = logging.getLogger(__name__)


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=1,
    max_retries=1,
    name="app.tasks.common_tasks.curation_tasks.submission_pipeline.validate_study_task",
)
def validate_study_task(self, params: dict[str, Any]):
    try:
        model = RevalidateStudyParameters.model_validate(params)
        logger.info(f"{model.study_id} validate_study_task is running...")
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

        url = f"{validation_endpoint}/validations/{model.study_id}"
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
        poll_url = f"{validation_endpoint}/validations/{model.study_id}/result"
        poll_params = {"summary_messages": False}
        validation_status = None
        latest_response = {}
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
            if not model.test:
                raise StudySubmissionError(
                    f"Study validation failed. {str(latest_response)}"
                )

        model.validate_study_task_status = True
        return model.model_dump()
    except Exception as ex:
        model.validate_study_task_status = False
        make_ftp_folder_writable_task.apply_async(kwargs={"params": params})
        report_internal_technical_issue(
            f"Validate {model.study_id} task failed.",
            f"Validate {model.study_id} task failed. {str(ex)}.",
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
    try:
        model = MakeStudyPrivateParameters.model_validate(params)
        study = StudyService.get_instance().get_study_by_acc(model.study_id)
        logger.info(
            f"{model.study_id} from_provisional_to_private_in_db task is running..."
        )
        if model.test:
            return params
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
        return model.model_dump()
    except Exception as ex:
        make_ftp_folder_writable_task.apply_async(kwargs={"params": params})
        report_internal_technical_issue(
            f"{model.study_id} status update to private task failed.",
            f"{model.study_id} status update to private task failed. {str(ex)}.",
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
        model = MakeStudyPrivateParameters.model_validate(params)
        es_service = ElasticsearchService.get_instance()
        es_service.reindex_study_with_task(
            study_id=model.study_id,
            user_token=None,
            include_validation_results=False,
            sync=False,
        )
        logger.info(f"Reindex {model.study_id} task completed")
        model.reindex_study_task_status = True
        return model.model_dump()
    except Exception as ex:
        model.reindex_study_task_status = False
        report_internal_technical_issue(
            f"{model.study_id} reindex task failed.",
            f"{model.study_id} reindex task failed. {str(ex)}.",
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
    name="app.tasks.common_tasks.curation_tasks.submission_pipeline.make_study_private",
)
def make_study_private(self, params: dict[str, Any]):
    model = RevalidateStudyParameters.model_validate(params)
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
    logger.info(f"{model.study_id} make_study_private {task.task_id} is running...")
    return task.task_id
