from app.tasks.common_tasks.curation_tasks.submission_model import (
    MakeStudyPrivateParameters,
)
from app.tasks.common_tasks.curation_tasks.submission_pipeline import make_study_private

if __name__ == "__main__":
    params = MakeStudyPrivateParameters(
        study_id="MTBLS9776",
        obfuscation_code="xxx",
        test=False,
    )
    make_study_private.apply_async(kwargs={"params": params.model_dump()})
