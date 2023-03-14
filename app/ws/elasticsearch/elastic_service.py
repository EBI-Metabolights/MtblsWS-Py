import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from app.utils import MetabolightsDBException, MetabolightsException
from app.ws.db import models
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import RefMetabolite, StudyTask
from app.ws.db.settings import DirectorySettings, get_directory_settings
from app.ws.db.types import StudyTaskName, StudyTaskStatus
from app.ws.elasticsearch.settings import (ElasticsearchSettings,
                                           get_elasticsearch_settings)
from app.ws.study.study_service import StudyService
from app.ws.study.user_service import UserService
from flask import current_app as app

from elasticsearch import Elasticsearch

logger = logging.getLogger('wslog')


class ElasticsearchService(object):
    INDEX_NAME = "metabolights"
    DOC_TYPE_STUDY = "study"
    DOC_TYPE_COMPOUND = "compound"

    def __init__(self, settings: ElasticsearchSettings, db_manager: DBManager, directory_settings: DirectorySettings):
        self.settings = settings
        self.db_manager = db_manager
        self.directory_settings = directory_settings
        self._client = None  # lazy load
        self.thread_pool_executor = ThreadPoolExecutor(max_workers=5)

    def initialize_client(self):
        settings = self.settings
        if settings.elasticsearch_use_tls:
            url = f"https://{settings.elasticsearch_host}:{settings.elasticsearch_port}"
            http_auth = (settings.elasticsearch_user_name, settings.elasticsearch_user_password)
            self._client = Elasticsearch(url, http_auth=http_auth, verify_certs=False)
        else:
            url = f"http://{settings.elasticsearch_host}:{settings.elasticsearch_port}"
            self._client = Elasticsearch(url)
        with open(settings.elasticsearch_all_mappings_json) as f:
            mappings = json.load(f)
        body = json.dumps(mappings)
        if not self._client.indices.exists(self.INDEX_NAME):
            self._client.indices.create(index=self.INDEX_NAME, ignore=400, body=body)
        
        with open(settings.elasticsearch_compound_mappings_json) as f:
            mappings = json.load(f)
        body = json.dumps(mappings)
        self._client.indices.put_mapping(index=self.INDEX_NAME, doc_type=self.DOC_TYPE_COMPOUND, body=body, ignore=[404,400])

        with open(settings.elasticsearch_study_mappings_json) as f:
            mappings = json.load(f)
        body = json.dumps(mappings)
        self._client.indices.put_mapping(index=self.INDEX_NAME, doc_type=self.DOC_TYPE_STUDY, body=body, ignore=[404,400])
        
    instance = None

    @classmethod
    def get_instance(cls, application):
        if not application:
            application = app
        if not cls.instance:
            settings = get_elasticsearch_settings(application)
            directory_settings = get_directory_settings(application)
            db_mananager = DBManager.get_instance(application)
            cls.instance = ElasticsearchService(settings=settings, db_manager=db_mananager,
                                                directory_settings=directory_settings)
        return cls.instance

    def get_all_compound_ids(self):
        query = '{  "query": {  "match_all": {} }, "fields": ["_id", "updatedDate"]}'

        result = self.client.search(index=self.INDEX_NAME, doc_type=self.DOC_TYPE_COMPOUND, body=query, params={"size":1000000}, _source=False)
        return result

    def get_all_study_ids(self):
        query = '{  "query": {  "match_all": {} }, "fields": ["_id", "updateDate"]}'

        result = self.client.search(index=self.INDEX_NAME, doc_type=self.DOC_TYPE_STUDY, body=query, params={"size":1000000}, _source=False)
        return result
    
    def delete_compound_index(self, user_token, compound_id):
        UserService.get_instance(app).validate_user_has_curator_role(user_token)
        self.client.delete(index=self.INDEX_NAME, doc_type=self.DOC_TYPE_COMPOUND, id=compound_id)

    def _delete_compound_index(self, compound_id):
        self.client.delete(index=self.INDEX_NAME, doc_type=self.DOC_TYPE_COMPOUND, id=compound_id)
               
    def delete_study_index(self, user_token, study_id):
        UserService.get_instance(app).validate_user_has_curator_role(user_token)
        self.client.delete(index=self.INDEX_NAME, doc_type=self.DOC_TYPE_STUDY, id=study_id)

    def _delete_study_index(self, study_id):
        self.client.delete(index=self.INDEX_NAME, doc_type=self.DOC_TYPE_STUDY, id=study_id)
                
    @property
    def client(self) -> Elasticsearch:
        if not self._client:
            self.initialize_client()
        return self._client

    def reindex_study(self, study_id, user_token, include_validation_results: bool = False, sync: bool = False):
        # Revalidate user permission
        UserService.get_instance(app).validate_user_has_submitter_or_super_user_role(user_token)
        self._reindex_study(study_id, user_token, include_validation_results, sync)

    def _reindex_study(self, study_id, user_token, include_validation_results: bool = False, sync: bool = False):
        try:
            self.reindex_study_with_task(study_id, user_token, include_validation_results, sync)
            return study_id
        except Exception as e:
            raise MetabolightsException(f"Error while reindexing.", exception=e, http_code=500)
        
    def reindex_compound(self, user_token, compound_id):
        UserService.get_instance(app).validate_user_has_curator_role(user_token)
        compound_id = self._reindex_compound(compound_id)
        return compound_id      

    def _reindex_compound(self, compound_id):
        try:
            with DBManager.get_instance(app).session_maker() as db_session:
                metabolite = db_session.query(RefMetabolite).filter(RefMetabolite.acc == compound_id).first()

                if not metabolite:
                    raise MetabolightsDBException(f"{compound_id} does not exist")

                compound = models.MetaboLightsCompoundIndexModel.from_orm(metabolite)
                organisms = set()
                if compound.metSpecies:
                    for item in compound.metSpecies:
                        if item and item.species and item.species.species: 
                            organisms.add(item.species.species)
                compound.name = compound.name
                for organism in organisms:
                    organism_item = models.OrganismModel(organismName=organism)
                    compound.organism.append(organism_item)
                           
                document = compound.dict()
                params = {"request_timeout": 120}
                self.client.index(index=self.INDEX_NAME, doc_type=self.DOC_TYPE_COMPOUND, body=document,
                                    id=compound_id, params=params)
                return compound_id
        except Exception as e:
            raise MetabolightsException(f"Error while reindexing.", exception=e, http_code=500)
        
    def get_study(self, study_id):
        params = {"request_timeout": 10}
        result = self.client.get(index=self.INDEX_NAME, id=study_id, doc_type=self.DOC_TYPE_STUDY, params=params)
        return result

    def reindex_study_with_task(self, study_id, user_token, include_validation_results, sync: bool):
        task_name = StudyTaskName.REINDEX
        tasks = StudyService.get_instance(app).get_study_tasks(study_id=study_id, task_name=task_name)

        with self.db_manager.session_maker() as db_session:
            if tasks:
                task = tasks[0]
            else:
                now = datetime.now()
                task = StudyTask()
                task.study_acc = study_id
                task.task_name = task_name
                task.last_request_time = now
                task.last_execution_time = now
                task.last_request_executed = now
                task.last_execution_status = StudyTaskStatus.NOT_EXECUTED
                task.last_execution_message = 'Task is initiated to reindex.'

            task.last_execution_status = StudyTaskStatus.EXECUTING
            task.last_execution_time = task.last_request_time
            task.last_execution_message = ''
            db_session.add(task)
            db_session.commit()
            logger.info(f'Indexing is started for {study_id}')
        

        def index_study():
            try:
                validations = include_validation_results
                m_study = StudyService.get_instance().get_study_from_db_and_folder(study_id, user_token,
                                                                                   optimize_for_es_indexing=True,
                                                                                   revalidate_study=validations,
                                                                                   include_maf_files=False)
                m_study.indexTimestamp = int(time.time())
                document = m_study.dict()
                params = {"request_timeout": 120}
                self.client.index(index=self.INDEX_NAME, doc_type=self.DOC_TYPE_STUDY, body=document,
                                  id=m_study.studyIdentifier, params=params)
                message = f'{study_id} is indexed.'
                tasks = StudyService.get_instance(app).get_study_tasks(study_id=study_id, task_name=task_name)
                if tasks:
                    task = tasks[0]
                    with self.db_manager.session_maker() as db_session:
                        task.last_request_time = datetime.now()
                        task.last_execution_status = StudyTaskStatus.EXECUTION_SUCCESSFUL
                        task.last_execution_time = datetime.now()
                        task.last_execution_message = message
                        db_session.add(task)
                        db_session.commit()
                logger.info(message)
            except Exception as e:
                tasks = StudyService.get_instance(app).get_study_tasks(study_id=study_id, task_name=task_name)

                message = f'{study_id} reindex is failed: {str(e)}'
                if tasks:
                    task = tasks[0]
                    with self.db_manager.session_maker() as db_session:
                        task.last_request_time = datetime.now()
                        task.last_execution_status = StudyTaskStatus.EXECUTION_FAILED
                        task.last_execution_time = datetime.now()
                        task.last_execution_message = message
                        db_session.add(task)
                        db_session.commit()
                logger.error(message)
                if sync:
                    raise e
        if not sync:
            self.thread_pool_executor.submit(index_study)
        else:
            index_study()

