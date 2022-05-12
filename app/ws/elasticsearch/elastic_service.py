from elasticsearch import Elasticsearch
from flask import current_app as app

from app.ws.db.dbmanager import DBManager
from app.ws.db.settings import DirectorySettings, get_database_settings, get_directory_settings
from app.ws.elasticsearch.settings import ElasticsearchSettings, get_elasticsearch_settings
from app.ws.study.study_service import StudyService


class ElasticsearchService(object):
    INDEX_NAME = "metabolights"
    DOC_TYPE_STUDY = "study"
    DOC_TYPE_COMPOUND = "compund"

    def __init__(self, settings: ElasticsearchSettings, db_manager: DBManager, directory_settings: DirectorySettings):
        self.settings = settings
        self.db_manager = db_manager
        self.directory_settings = directory_settings
        if settings.elasticsearch_use_tls:
            url = f"https://{settings.elasticsearch_host}:{settings.elasticsearch_port}"
            http_auth = (settings.elasticsearch_user_name, settings.elasticsearch_user_password)
            self.client = Elasticsearch(url, http_auth=http_auth, verify_certs=False)
        else:
            url = f"http://{settings.elasticsearch_host}:{settings.elasticsearch_port}"
            self.client = Elasticsearch(url)

        self.client.indices.create(index=self.INDEX_NAME, ignore=400)

    instance = None

    @classmethod
    def get_instance(cls):
        if not cls.instance:
            settings = get_elasticsearch_settings(app)
            db_settings = get_database_settings(app)
            directory_settings = get_directory_settings(app)
            db_mananager = DBManager(db_settings)
            cls.instance = ElasticsearchService(settings=settings, db_manager=db_mananager,
                                                directory_settings=directory_settings)
        return cls.instance

    def reindex_study(self, study_id, user_token):

        m_study = StudyService.get_instance().get_study_from_db_and_folder(study_id, user_token,
                                                                           optimize_for_es_indexing=True,
                                                                           revalidate_study=True,
                                                                           include_maf_files=False)
        try:
            document = m_study.dict()
            self.client.index(index=self.INDEX_NAME, doc_type=self.DOC_TYPE_STUDY, body=document, id=m_study.studyIdentifier)
            return True, "success"
        except Exception as e:
            return False, f"Error while indexing: {str(e)}"

    def reindex_compound_by_chebi_id(self, chebi_id, user_token):

        m_compound = None   # TODO complete here to create Compound Model to index in ES
        try:
            document = m_compound.dict()
            self.client.index(index=self.INDEX_NAME, doc_type=self.DOC_TYPE_COMPOUND, body=document, id=m_compound.accession)
            return True, "success"
        except Exception as e:
            return False, f"Error while indexing: {str(e)}"