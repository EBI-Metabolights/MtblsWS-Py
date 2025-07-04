import logging
import os
import celery
from app.services.external.eb_eye_search import EbEyeSearchService
from app.tasks.worker import (MetabolightsTask, celery)
from app.tasks.worker import MetabolightsTask


logger = logging.getLogger('wslog')


@celery.task(bind=True, base=MetabolightsTask, default_retry_delay=10, max_retries=3, soft_time_limit=60*120, name="app.tasks.common_tasks.report_tasks.europe_pmc.europe_publication_report")
def europe_pmc_publication_report(self, user_token: str):
    logger.info("Received request to process EuropePMC study publication search !")
    return EbEyeSearchService.europe_publication_report(user_token=user_token)