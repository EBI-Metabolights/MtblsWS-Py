import logging
import os
import celery
from app.services.external.eb_eye_search import EbEyeSearchService
from app.tasks.worker import (MetabolightsTask, celery)
from app.tasks.worker import MetabolightsTask


logger = logging.getLogger('wslog')


@celery.task(bind=True, base=MetabolightsTask, default_retry_delay=10, max_retries=3, soft_time_limit=60*15, name="app.tasks.common_tasks.report_tasks.eb_eye_search.eb_eye_build_public_studies")
def eb_eye_build_public_studies(self, user_token: str, thomson_reuters: bool):
    if thomson_reuters:
        logger.info("Received request to process public studies for Thomson Reuters!")
    else:
        logger.info("Received request to process EB EYE search public studies!")
    return EbEyeSearchService.export_public_studies(user_token=user_token, thomson_reuters=thomson_reuters)

@celery.task(bind=True, base=MetabolightsTask, default_retry_delay=10, max_retries=3, soft_time_limit=60*15, name="app.tasks.common_tasks.report_tasks.eb_eye_search.eb_eye_build_compounds")
def eb_eye_build_compounds(self, user_token: str):
    logger.info("Received request to process EB EYE search compounds!")
    return EbEyeSearchService.export_compounds(user_token=user_token)

@celery.task(bind=True, base=MetabolightsTask, default_retry_delay=10, max_retries=3, soft_time_limit=60*15, name="app.tasks.common_tasks.report_tasks.eb_eye_search.build_studies_for_europe_pmc")
def build_studies_for_europe_pmc(self, user_token: str):
    logger.info("Received request to process Studies for EuropePMC!")
    return EbEyeSearchService.export_europe_pmc(user_token=user_token)