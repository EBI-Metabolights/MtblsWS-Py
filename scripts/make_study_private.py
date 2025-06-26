from app.config import get_settings
from app.tasks.common_tasks.curation_tasks.submission_model import (
    MakeStudyPublicParameters,
)
from app.tasks.common_tasks.curation_tasks.submission_pipeline import (
    start_new_public_revision_pipeline,
)
from app.ws.study.study_service import StudyService


if __name__ == "__main__":
    user_token = get_settings().auth.service_account.api_token
    study_id = "MTBLS30008953"
    study = StudyService.get_instance().get_study_by_acc(study_id)
    # params = MakeStudyPrivateParameters(
    #     study_id=study_id,
    #     obfuscation_code=study.obfuscationcode,
    #     current_status=study.status,
    #     api_token=user_token,
    #     test=True,
    # )
    # start_make_study_private_pipeline.apply_async(kwargs={"params": params.model_dump()})
    params = MakeStudyPublicParameters(
        study_id=study_id,
        obfuscation_code=study.obfuscationcode,
        current_status=study.status,
        api_token=user_token,
        revision_comment="test",
        created_by="test@ebi.ac.uk",
    )
    start_new_public_revision_pipeline.apply_async(
        kwargs={"params": params.model_dump()}
    )
