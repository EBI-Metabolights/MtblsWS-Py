import datetime
import json
import logging
import os

from app.tasks.worker import (MetabolightsTask, celery, get_flask_app,
                              send_email)
from app.utils import MetabolightsDBException
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study, User
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from app.ws.study.user_service import UserService

logger = logging.getLogger(__name__)
from app.tasks.worker import get_flask_app


@celery.task(base=MetabolightsTask, name="app.tasks.common_tasks.admin_tasks.es_and_db_study_syncronization.sync_studies_on_es_and_db")
def sync_studies_on_es_and_db(user_token: str, send_email_to_submitter=False):
    try:
        flask_app = get_flask_app()
        UserService.get_instance(flask_app).validate_user_has_curator_role(user_token)

        with flask_app.app_context():
            studies_dict = {}
            
            with DBManager.get_instance(flask_app).session_maker() as db_session:
                user = db_session.query(User.email).filter(User.apitoken == user_token).first()
                if not user:
                    raise MetabolightsDBException("No user")
                
                email = user.email
                
                result = db_session.query(Study.acc, Study.updatedate).all()

                if not result:
                    raise MetabolightsDBException(f"No study found on db.") 
                for study in result:
                    studies_dict[study["acc"]] = study["updatedate"] 
                    
            indexed_studies= {} 
            unindexed_studies= []
            out_of_date_studies= []
            studies_not_in_db = []
            es = ElasticsearchService.get_instance(flask_app)
            result = es.get_all_study_ids()
            if "hits" in result and result["hits"] and "hits" in result["hits"]:
                studies= result["hits"]["hits"]
                try:
                    for studies in studies:
                        studies_id = None
                        updated_date = None
                        if "_id" in studies:
                            studies_id = studies["_id"]
                        try:
                            updated_date = studies["fields"]["updateDate"][0]
                        except Exception as ex:
                            pass
                        if studies_id:
                            indexed_studies[studies_id] = updated_date
                            
                            if studies_id not in studies_dict:
                                studies_not_in_db.append(studies_id)
                            else:
                                if updated_date:
                                    if studies_dict[studies_id]:
                                        time = int(studies_dict[studies_id].timestamp()*1000)
                                        if  updated_date < time:
                                            out_of_date_studies.append(studies_id)
                                else:
                                    out_of_date_studies.append(studies_id)
                except Exception as exc: 
                    raise exc
            
            for db_studies in studies_dict:
                if db_studies not in indexed_studies:
                    unindexed_studies.append(db_studies)
            try:                
                # print("Unindex studies: " + ", ".join(unindexed_studies))
                for item in unindexed_studies:
                    # print(f"inserting new studies index {item}")
                    try:
                        es._reindex_study(item, user_token, include_validation_results=False, sync=True)
                    except Exception as ex:
                        logger.error(f'Error while adding new index {item}: {str(ex)}')
                
                if studies_not_in_db:
                    # print("studiesnot in db: " + ", ".join(studies_not_in_db))
                    for item in studies_not_in_db:
                        # print(f"deleting studies index {item}")
                        try:
                            es._delete_study_index(item)
                        except Exception as ex:
                            logger.error(f'Error while deleting index {item}: {str(ex)}')
                
                if out_of_date_studies:
                    # print("Out of date studies: " + ", ".join(out_of_date_studies))
                    for item in out_of_date_studies:
                        try:
                            es._reindex_study(item, user_token, include_validation_results=False, sync=True)
                        except Exception as ex:
                            logger.error(f'Error while reindexing {item}: {str(ex)}')
            except Exception as exc:
                raise exc
        
        updated = True
        if not out_of_date_studies and not unindexed_studies and not studies_not_in_db:
            updated = False
        result = {"time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                  "executed_on":  os.uname().nodename,
                "status": f'{"UPDATED" if updated else "NO CHANGE"}',
                "reindexed_studies": str(out_of_date_studies),
                "added_studies": str(unindexed_studies),
                "deleted_studies": str(studies_not_in_db)}
        
        if send_email_to_submitter:
            result_str = json.dumps(result, indent=4)
            result_str = result_str.replace("\n", "<p>")
            send_email("Result of the task: sync MetaboLights studies on elasticsearch and database", result_str, None, email, None)
            
        return result
    except Exception as ex:
        if send_email_to_submitter:
            result_str = str(ex).replace("\n", "<p>")
            send_email("A task was failed: sync MetaboLights studies on elasticsearch and database", result_str, None, email, None)
        raise ex        
    