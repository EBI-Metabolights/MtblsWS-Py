import datetime
import json
import logging
import os

from app.tasks.worker import (MetabolightsTask, celery, get_flask_app,
                              send_email)
from app.utils import MetabolightsDBException
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import RefMetabolite, User
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from app.ws.study.user_service import UserService

logger = logging.getLogger(__name__)
from app.tasks.worker import get_flask_app


@celery.task(base=MetabolightsTask, name="app.tasks.periodic_tasks.compound.sync_compounds_on_es_and_db")
def sync_compound_on_es_and_db(user_token: str, send_email_to_submitter=False):
    try:
        flask_app = get_flask_app()
        UserService.get_instance(flask_app).validate_user_has_curator_role(user_token)
        with flask_app.app_context():
            compounds_dict = {}
            
            with DBManager.get_instance(flask_app).session_maker() as db_session:
                    metabolites = db_session.query(RefMetabolite.acc, RefMetabolite.updated_date).all()
                    user = db_session.query(User.email).filter(User.apitoken == user_token).first()
                    if not user:
                        raise MetabolightsDBException("No user")
                    email = user.email
                    if not metabolites:
                        raise MetabolightsDBException(f"{compound_id} does not exist") 
                    for metabolite in metabolites:
                        compounds_dict[metabolite['acc']] = metabolite['updated_date'] 
            indexed_compounds = {} 
            unindexed_compounds = []
            out_of_date_compounds = []
            compounds_not_in_db = []
            es = ElasticsearchService.get_instance(flask_app)
            result = es.get_all_compound_ids()
            if "hits" in result and result["hits"] and "hits" in result["hits"]:
                compounds = result["hits"]["hits"]
                try:
                    for compound in compounds:
                        compound_id = None
                        updated_date = None
                        if "_id" in compound:
                            compound_id = compound["_id"]
                        try:
                            updated_date = compound["fields"]["updatedDate"][0]
                        except Exception as ex:
                            pass
                        if compound_id:
                            indexed_compounds[compound_id] = updated_date
                            
                            if compound_id not in compounds_dict:
                                compounds_not_in_db.append(compound_id)
                            else:
                                if updated_date:
                                    if compounds_dict[compound_id]:
                                        item_update_date = compounds_dict[compound_id].strftime("%Y-%m-%d")
                                        if  updated_date != item_update_date:
                                            out_of_date_compounds.append(compound_id)
                                else:
                                    out_of_date_compounds.append(compound_id)
                except Exception as exc: 
                    raise exc
            
            for db_compound in compounds_dict:
                if db_compound not in indexed_compounds:
                    unindexed_compounds.append(db_compound)
            try:                
                # print("Unindex compounds: " + ", ".join(unindexed_compounds))
                for item in unindexed_compounds:
                    # print(f"inserting new compound index {item}")
                    try:
                        es._reindex_compound(item)
                    except Exception as ex:
                        logger.error(f'Error while adding new index {item}: {str(ex)}')
                
                if compounds_not_in_db:
                    # print("Compounds not in db: " + ", ".join(compounds_not_in_db))
                    for item in compounds_not_in_db:
                        # print(f"deleting compound index {item}")
                        try:
                            es._delete_compound_index(item)
                        except Exception as ex:
                            logger.error(f'Error while deleting index {item}: {str(ex)}')
                
                if out_of_date_compounds:
                    # print("Out of date compounds: " + ", ".join(out_of_date_compounds))
                    for item in out_of_date_compounds:
                        try:
                            es._reindex_compound(item)
                        except Exception as ex:
                            logger.error(f'Error while reindexing {item}: {str(ex)}')
            except Exception as exc:
                raise exc
        status = "UPDATED" if out_of_date_compounds or unindexed_compounds or compounds_not_in_db else "NO CHANGE"
        result = {"time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                  "status": status,
                  "executed_on":  os.uname().nodename,
                  "reindexed_compounds": str(out_of_date_compounds),
                  "added_compounds": str(unindexed_compounds),
                  "deleted_compounds": str(compounds_not_in_db)}

        if send_email_to_submitter:
            result_str = json.dumps(result, indent=4)
            result_str = result_str.replace("\n", "<p>")
            send_email("Result of the task: sync MetaboLights compounds on elasticsearch and database", result_str, None, email, None)
            
        return result

    except Exception as ex:
        if send_email_to_submitter:
            result_str = str(ex).replace("\n", "<p>")
            send_email("A task was failed: sync MetaboLights compounds on elasticsearch and database", result_str, None, email, None)
        raise ex        