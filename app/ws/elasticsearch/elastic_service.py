import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict
from app.config import get_settings
from app.config.model.elasticsearch import ElasticsearchSettings
from app.config.model.study import StudySettings

from app.utils import MetabolightsDBException, MetabolightsException, current_time
from app.ws.db import models
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import RefMetabolite, StudyTask
from app.ws.db.types import StudyTaskName, StudyTaskStatus
from app.ws.elasticsearch.schemes import Booster, Facet, FacetLine, SearchQuery, SearchResult
from app.ws.study.study_service import StudyService
from app.ws.study.user_service import UserService

from elasticsearch import Elasticsearch

logger = logging.getLogger('wslog')

compound_boolean_field_names = {"hasLiterature", "hasReactions", "hasSpecies", "hasPathways", "hasNMR", "hasMS"}

class ElasticsearchService(object):
    INDEX_NAME = "metabolights"
    DOC_TYPE_STUDY = "study"
    DOC_TYPE_COMPOUND = "compound"

    def __init__(self, settings: ElasticsearchSettings, db_manager: DBManager, study_settings: StudySettings):
        self.settings = settings
        self.study_settings = study_settings
        self._client = None  # lazy load
        self.thread_pool_executor = ThreadPoolExecutor(max_workers=5)

    def initialize_client(self):
        settings = self.settings
        if settings.connection.use_tls:
            url = f"https://{settings.connection.host}:{settings.connection.port}"
            http_auth = (settings.connection.username, settings.connection.password)
            self._client = Elasticsearch(url, http_auth=http_auth, verify_certs=False)
        else:
            url = f"http://{settings.connection.host}:{settings.connection.port}"
            self._client = Elasticsearch(url)
        with open(settings.configuration.elasticsearch_all_mappings_json) as f:
            mappings = json.load(f)
        body = json.dumps(mappings)
        if not self._client.indices.exists(self.INDEX_NAME):
            self._client.indices.create(index=self.INDEX_NAME, ignore=400, body=body)
        
        with open(settings.configuration.elasticsearch_compound_mappings_json) as f:
            mappings = json.load(f)
        body = json.dumps(mappings)
        self._client.indices.put_mapping(index=self.INDEX_NAME, doc_type=self.DOC_TYPE_COMPOUND, body=body, ignore=[404,400])

        with open(settings.configuration.elasticsearch_study_mappings_json) as f:
            mappings = json.load(f)
        body = json.dumps(mappings)
        self._client.indices.put_mapping(index=self.INDEX_NAME, doc_type=self.DOC_TYPE_STUDY, body=body, ignore=[404,400])
        
    instance = None

    @classmethod
    def get_instance(cls):
        if not cls.instance:
            
            settings = get_settings().elasticsearch
            study_settings = get_settings().study
            db_mananager = DBManager.get_instance()
            cls.instance = ElasticsearchService(settings=settings, db_manager=db_mananager,
                                                study_settings=study_settings)
        return cls.instance

    def search(self, query: SearchQuery):
        if not query:
            query = self.get_empty_query()
        search_body = self.build_search_body(query)
        result = self.client.search(index=self.INDEX_NAME, body=search_body)
        search_result = SearchResult()
        facets: Dict[str, Dict[str, Any]] = {}
        for facet in query.facets:
            facets[facet.name] = {}
            facets[facet.name]["lines"] = {}
            facets[facet.name]["facet"] = facet
            for line in facet.lines:
                facets[facet.name]["lines"][line.value] = line
                
            
            
        search_result.query = query
        if result and "hits" in result and result["hits"]:
            search_result.query.pagination.itemsCount = result["hits"]["total"]
            if "hits" in result["hits"] and result["hits"]["hits"]:
                for item in result["hits"]["hits"]:
                    result_item = None
                    if item["_type"] and "_source" in item and item["_source"]:
                        if item["_type"].lower() == "study":
                            result_item = models.StudyModel.parse_obj(item["_source"])
                        elif item["_type"].lower() == "compound":
                            result_item = models.ESMetaboLightsCompound.parse_obj(item["_source"])
                    if result_item:
                        search_result.results.append(result_item)

            if result and "aggregations" in result and result["aggregations"]:
                for facet_name in result["aggregations"]:
                    if facet_name in facets:
                        current_facet = facets[facet_name]
                        if "buckets" in result["aggregations"][facet_name] and result["aggregations"][facet_name]["buckets"]:
                            buckets = result["aggregations"][facet_name]["buckets"]
                            for bucket in buckets:
                                if "key" in bucket and "doc_count" in bucket:
                                    if bucket['key'] not in current_facet["lines"]:
                                        new_facet_line = FacetLine(value=bucket['key'])
                                        current_facet["lines"][bucket['key']] = new_facet_line
                                        current_facet["facet"].lines.append(new_facet_line)
                                    current_facet["lines"][bucket['key']].count = bucket["doc_count"]
                                
                    # search_result.reportLines.append(facet_item)
            
        return {"content": search_result.dict(), "message": "result successfull", "error": None}
    
    def build_search_body(self, query: SearchQuery) -> str:
        search_text = query.text.replace("'", "") if query.text else ""
        body = {}
        page = query.pagination.page if query.pagination.page > 0 else 1
        body["from"] = (page - 1) * query.pagination.pageSize
        body["size"] = query.pagination.pageSize
        
        filtered = True
        if query.searchUser.isAdmin:
            filtered = False
        
        if not search_text:
            if filtered:
                body["query"] = {"filtered": {"query": {"match_all": {}}}}
            else:
                body["query"] = {"match_all": {}}
            body["sort"] = [{"studyPublicReleaseDate": {"order": "desc"}}]
        else:
            if filtered:
                body["query"] = {"filtered": {"query": {"bool": {"must": { "query_string": {"query": search_text}}}}}}
            else:
                body["query"] = {"bool": {"must": { "query_string": {"query": search_text}}}}
            
            boosters = []
            for boost in query.boosters:
                boosters.append({"term": { boost.fieldName: {"value": search_text, "boost": boost.boost}}})
            if boosters:
                if filtered:
                    body["query"]["filtered"]["query"]["bool"]["should"] = boosters
                else:
                    body["query"]["bool"]["should"] = boosters
                
       
        post_filters = []
        aggregations = {}
        for item in query.facets:
            aggregations[item.name] = { "terms": { "field": item.name, "size": 0 }}
            lines = []
            for line in item.lines:
                if line.checked:
                    if item.name in compound_boolean_field_names:
                        if line.value.lower() == "t":
                            lines.append({"term": { item.name: True}})
                        else:
                            lines.append({"term": { item.name: False}})
                    else:
                        lines.append({"term": { item.name: line.value}})
            filter_item = None
            if len(lines) > 1:
                filter_item = {"or": {"filters": lines}}
            elif len(lines) == 1:
                filter_item = lines[0]
            if filter_item:
                post_filters.append(filter_item)
            
                
        if post_filters:
            body["post_filter"] = {"and": { "filters": post_filters }}
        
        if not query.searchUser.isAdmin:
            if "filtered" not in body["query"]:
                body["query"]["filtered"] = {}
            filter = {"or": { "filters": [ {"term": {"studyStatus": "PUBLIC"}}, {"term": {"users.userName": query.searchUser.id}}]}}
            body["query"]["filtered"]["filter"] = filter
        
        if aggregations:
            body["aggregations"] = aggregations
        value = json.dumps(body, indent=4)
        return body
            
    def get_empty_query(self) -> SearchQuery:
        query = SearchQuery()
        query.facets.append(Facet(name="ObjectType"))
        query.facets.append(Facet(name="assays.technology"))
        query.facets.append(Facet(name="compound.hasSpecies"))
        query.facets.append(Facet(name="compound.hasPathways"))
        query.facets.append(Facet(name="compound.hasReactions"))
        query.facets.append(Facet(name="compound.hasNMR"))
        query.facets.append(Facet(name="compound.hasMS"))
        query.facets.append(Facet(name="studyStatus"))
        query.facets.append(Facet(name="organism.organismName"))
        query.facets.append(Facet(name="organism.organismPart"))
        # query.facets.append(Facet(name="factors.name"))
        # query.facets.append(Facet(name="users.fullName"))
        # query.facets.append(Facet(name="descriptors.description"))
        # query.facets.append(Facet(name="validations.status"))
        # query.facets.append(Facet(name="validations.entries.statusExt"))
        query.boosters.append(Booster(fieldName="_id",boost=2))
        query.boosters.append(Booster(fieldName="title",boost=1))
        query.boosters.append(Booster(fieldName="name",boost=1))
        
        return query
        
    def get_all_compound_ids(self):
        query = '{  "query": {  "match_all": {} }, "fields": ["_id", "updatedDate"]}'

        result = self.client.search(index=self.INDEX_NAME, doc_type=self.DOC_TYPE_COMPOUND, body=query, params={"size":1000000}, _source=False)
        return result

    def get_all_study_ids(self):
        query = '{  "query": {  "match_all": {} }, "fields": ["_id", "updateDate"]}'

        result = self.client.search(index=self.INDEX_NAME, doc_type=self.DOC_TYPE_STUDY, body=query, params={"size":1000000}, _source=False)
        return result
    
    def delete_compound_index(self, user_token, compound_id):
        UserService.get_instance().validate_user_has_curator_role(user_token)
        self.client.delete(index=self.INDEX_NAME, doc_type=self.DOC_TYPE_COMPOUND, id=compound_id)

    def _delete_compound_index(self, compound_id):
        self.client.delete(index=self.INDEX_NAME, doc_type=self.DOC_TYPE_COMPOUND, id=compound_id)
               
    def delete_study_index(self, user_token, study_id):
        UserService.get_instance().validate_user_has_curator_role(user_token)
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
        UserService.get_instance().validate_user_has_submitter_or_super_user_role(user_token)
        self._reindex_study(study_id, user_token, include_validation_results, sync)

    def _reindex_study(self, study_id, user_token, include_validation_results: bool = False, sync: bool = False):
        try:
            self.reindex_study_with_task(study_id, user_token, include_validation_results, sync)
            return study_id
        except Exception as e:
            raise MetabolightsException(f"Error while reindexing.", exception=e, http_code=500)
        
    def reindex_compound(self, user_token, compound_id):
        UserService.get_instance().validate_user_has_curator_role(user_token)
        compound_id = self._reindex_compound(compound_id)
        return compound_id      

    def _reindex_compound(self, compound_id):
        try:
            with DBManager.get_instance().session_maker() as db_session:
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

    def get_study(self, study_id):
        params = {"request_timeout": 10}
        result = self.client.get(index=self.INDEX_NAME, id=study_id, doc_type=self.DOC_TYPE_STUDY, params=params)
        return result
        
    def reindex_study_with_task(self, study_id, user_token, include_validation_results, sync: bool):
        task_name = StudyTaskName.REINDEX
        tasks = StudyService.get_instance().get_study_tasks(study_id=study_id, task_name=task_name)

        with DBManager.get_instance().session_maker() as db_session:
            if tasks:
                task = tasks[0]
            else:
                now = current_time()
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
                tasks = StudyService.get_instance().get_study_tasks(study_id=study_id, task_name=task_name)
                if tasks:
                    task: StudyTask = tasks[0]
                    with DBManager.get_instance().session_maker() as db_session:
                        task.last_request_time = current_time()
                        task.last_execution_status = StudyTaskStatus.EXECUTION_SUCCESSFUL
                        task.last_execution_time = current_time()
                        task.last_execution_message = message
                        db_session.add(task)
                        db_session.commit()
                logger.info(message)
            except Exception as e:
                tasks = StudyService.get_instance().get_study_tasks(study_id=study_id, task_name=task_name)

                message = f'{study_id} reindex is failed: {str(e)}'
                if tasks:
                    task = tasks[0]
                    with DBManager.get_instance().session_maker() as db_session:
                        task.last_request_time = current_time()
                        task.last_execution_status = StudyTaskStatus.EXECUTION_FAILED
                        task.last_execution_time = current_time()
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

