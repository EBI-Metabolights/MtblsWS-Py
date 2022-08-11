import time

from elasticsearch import Elasticsearch
from flask import current_app as app

from app.utils import MetabolightsException
from app.ws.db.dbmanager import DBManager
from app.ws.db.settings import DirectorySettings, get_directory_settings
from app.ws.elasticsearch.settings import ElasticsearchSettings, get_elasticsearch_settings
from app.ws.study.study_service import StudyService
from app.ws.study.user_service import UserService


class ElasticsearchService(object):
    INDEX_NAME = "metabolights"
    DOC_TYPE_STUDY = "study"
    DOC_TYPE_COMPOUND = "compound"

    def __init__(self, settings: ElasticsearchSettings, db_manager: DBManager, directory_settings: DirectorySettings):
        self.settings = settings
        self.db_manager = db_manager
        self.directory_settings = directory_settings
        self._client = None  # lazy load

    def initialize_client(self):
        settings = self.settings
        if settings.elasticsearch_use_tls:
            url = f"https://{settings.elasticsearch_host}:{settings.elasticsearch_port}"
            http_auth = (settings.elasticsearch_user_name, settings.elasticsearch_user_password)
            self._client = Elasticsearch(url, http_auth=http_auth, verify_certs=False)
        else:
            url = f"http://{settings.elasticsearch_host}:{settings.elasticsearch_port}"
            self._client = Elasticsearch(url)
        self._client.indices.create(index=self.INDEX_NAME, ignore=400)

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

    @property
    def client(self):
        if not self._client:
            self.initialize_client()
        return self._client

    def reindex_study(self, study_id, user_token, include_validation_results: bool = False):
        # Revalidate user permission
        UserService.get_instance(app).validate_user_has_submitter_or_super_user_role(user_token)
        try:
            validations = include_validation_results
            m_study = StudyService.get_instance().get_study_from_db_and_folder(study_id, user_token,
                                                                               optimize_for_es_indexing=True,
                                                                               revalidate_study=validations,
                                                                               include_maf_files=False)
            m_study.indexTimestamp = time.time()
            document = m_study.dict()
            params = {"request_timeout": 120}
            self.client.index(index=self.INDEX_NAME, doc_type=self.DOC_TYPE_STUDY, body=document,
                              id=m_study.studyIdentifier, params=params)
            return m_study
        except Exception as e:
            raise MetabolightsException("Error while indexing", exception=e)
