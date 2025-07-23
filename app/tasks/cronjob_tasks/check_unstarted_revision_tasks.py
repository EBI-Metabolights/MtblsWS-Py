import logging
import sys
from app.tasks.common_tasks.curation_tasks.study_revision import (
    check_not_started_study_revisions,
)

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s [%(module)s] %(message)s",
        datefmt="%d/%b/%Y %H:%M:%S",
        stream=sys.stdout,
        force=True,
    )
    logger.setLevel(logging.DEBUG)
    check_not_started_study_revisions()
