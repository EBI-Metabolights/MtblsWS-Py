import logging
import os
import celery
from app.services.external.eb_eye_search import EbEyeSearchService
from app.tasks.worker import (MetabolightsTask, celery)
from app.tasks.worker import MetabolightsTask


logger = logging.getLogger('wslog')


@celery.task(bind=True, base=MetabolightsTask, default_retry_delay=10, max_retries=3, soft_time_limit=60*15, name="app.tasks.common_tasks.report_tasks.eb_eye_search.eb_eye_build_public_studies")
def eb_eye_build_public_studies(self, user_token: str, thomson_reuters: bool):
    logger.info("Received request to process EB EYE search public studies!")
    return EbEyeSearchService.export_public_studies(user_token=user_token, thomson_reuters=thomson_reuters)