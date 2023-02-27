from app.ws.elasticsearch.elastic_service import ElasticsearchService
from app.tasks.worker import MetabolightsTask, celery, get_flask_app


@celery.task(base=MetabolightsTask, name="app.tasks.common.elasticsearch.reindex_study")
def reindex_study(user_token, study_id):
    flask_app = get_flask_app()
    with flask_app.app_context():

        ElasticsearchService.get_instance(flask_app).reindex_study(
            study_id, user_token, False, True
        )
        return {"study_id": study_id}
