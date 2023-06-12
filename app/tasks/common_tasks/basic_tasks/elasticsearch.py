import datetime
import json
import os
from app.utils import MetabolightsDBException
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import RefMetabolite, Study, User
from app.ws.db.types import StudyStatus
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from app.tasks.worker import MetabolightsTask, celery, send_email


@celery.task(base=MetabolightsTask, name="app.tasks.common_tasks.basic_tasks.elasticsearch.reindex_study")
def reindex_study(user_token, study_id):
    ElasticsearchService.get_instance().reindex_study(study_id, user_token, False, True)
    return {"study_id": study_id}


@celery.task(base=MetabolightsTask, name="app.tasks.common_tasks.basic_tasks.elasticsearch.delete_study_index")
def delete_study_index(user_token, study_id):
    ElasticsearchService.get_instance().delete_study_index(user_token, study_id)
    return {"study_id": study_id}


@celery.task(base=MetabolightsTask, name="app.tasks.common_tasks.basic_tasks.elasticsearch.reindex_all_studies")
def reindex_all_public_studies(user_token, send_email_to_submitter=False):
    try:
        studies = []

        with DBManager.get_instance().session_maker() as db_session:
            user = db_session.query(User.email).filter(User.apitoken == user_token).first()
            if not user:
                raise MetabolightsDBException("No user")
            email = user.email

            result = (
                db_session.query(Study.acc, Study.updatedate).filter(Study.status == StudyStatus.PUBLIC.value).all()
            )

            if not result:
                raise MetabolightsDBException(f"No compound found on db.")
            for study in result:
                studies.append(study)

        result = reindex_studies_in_list(user_token, studies)
        result_str = json.dumps(result, indent=4)
        result_str = result_str.replace("\n", "<p>")
        if send_email_to_submitter:
            send_email("Result of the task: reindex all studies", result_str, None, email, None)
    except Exception as ex:
        if send_email_to_submitter:
            result_str = str(ex).replace("\n", "<p>")
            send_email("Reindex all studies task was failed", result_str, None, email, None)
            raise ex


@celery.task(base=MetabolightsTask, name="app.tasks.common_tasks.basic_tasks.elasticsearch.reindex_all_studies")
def reindex_all_studies(user_token, send_email_to_submitter=False):
    try:
        studies = []

        with DBManager.get_instance().session_maker() as db_session:
            user = db_session.query(User.email).filter(User.apitoken == user_token).first()
            if not user:
                raise MetabolightsDBException("No user")
            email = user.email

            result = db_session.query(Study.acc, Study.updatedate).all()

            if not result:
                raise MetabolightsDBException(f"No compound found on db.")
            for study in result:
                studies.append(study)

        result = reindex_studies_in_list(user_token, studies)
        result_str = json.dumps(result, indent=4)
        result_str = result_str.replace("\n", "<p>")
        if send_email_to_submitter:
            send_email("Result of the task: reindex all studies", result_str, None, email, None)
    except Exception as ex:
        if send_email_to_submitter:
            result_str = str(ex).replace("\n", "<p>")
            send_email("Reindex all studies task was failed", result_str, None, email, None)
            raise ex


def reindex_studies_in_list(user_token, studies):
    es = ElasticsearchService.get_instance()
    failed_indexed_studies = {}
    indexed_studies = []

    try:
        for item in studies:
            try:
                es._reindex_study(item["acc"], user_token, include_validation_results=False, sync=True)
                indexed_studies.append(item["acc"])
            except Exception as ex:
                failed_indexed_studies[item["acc"]] = str(ex)
    except Exception as exc:
        raise exc
    return {
        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "executed_on": os.uname().nodename,
        "total_studies": len(studies),
        "indexed_studies": len(indexed_studies),
        "failed_indexed_studies": failed_indexed_studies,
    }


@celery.task(base=MetabolightsTask, name="app.tasks.common_tasks.basic_tasks.elasticsearch.reindex_compound")
def reindex_compound(user_token, compound_id):
    ElasticsearchService.get_instance().reindex_compound(user_token, compound_id)
    return {"compound_id": compound_id}


@celery.task(base=MetabolightsTask, name="app.tasks.common_tasks.basic_tasks.elasticsearch.delete_compound_index")
def delete_compound_index(user_token, compound_id):
    ElasticsearchService.get_instance().delete_compound_index(user_token, compound_id)
    return {"compound_id": compound_id}


@celery.task(base=MetabolightsTask, name="app.tasks.common_tasks.basic_tasks.elasticsearch.reindex_all_compounds")
def reindex_all_compounds(user_token, send_email_to_submitter=False):
    try:
        compounds = []

        with DBManager.get_instance().session_maker() as db_session:
            user = db_session.query(User.email).filter(User.apitoken == user_token).first()
            if not user:
                raise MetabolightsDBException("No user")
            email = user.email

            metabolites = db_session.query(RefMetabolite.acc, RefMetabolite.updated_date).all()

            if not metabolites:
                raise MetabolightsDBException(f"No compound found on db.")
            for metabolite in metabolites:
                compounds.append(metabolite)

        es = ElasticsearchService.get_instance()
        failed_indexed_compounds = {}
        indexed_compounds = []

        try:
            for item in compounds:
                try:
                    es._reindex_compound(item["acc"])
                    indexed_compounds.append(item["acc"])
                except Exception as ex:
                    failed_indexed_compounds[item["acc"]] = str(ex)

        except Exception as exc:
            raise exc

        result = {
            "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executed_on": os.uname().nodename,
            "total_compounds": len(compounds),
            "indexed_compounds": len(indexed_compounds),
            "failed_index_compounds": failed_indexed_compounds,
        }
        result_str = json.dumps(result, indent=4)
        result_str = result_str.replace("\n", "<p>")
        if send_email_to_submitter:
            send_email("Result of the task: reindex all compounds", result_str, None, email, None)
    except Exception as ex:
        if send_email_to_submitter:
            result_str = str(ex).replace("\n", "<p>")
            send_email("Reindex all compounds task was failed", result_str, None, email, None)
        raise ex
