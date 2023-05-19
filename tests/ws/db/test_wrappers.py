from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study
from app.ws.db.wrappers import create_study_model_from_db_study, update_study_model_from_directory
from app.ws.settings.utils import get_study_settings


class TestWrappers(object):

    def test_create_mtbls_file_obj_01(self, flask_app, sensitive_data):
        study_id = "MTBLS1"
        with flask_app.app_context():
            db_manager = DBManager.get_instance(flask_app)
            with db_manager.session_maker() as db_session:
                db_study_obj = db_session.query(Study).filter(Study.acc == study_id).first()
                study = create_study_model_from_db_study(db_study_obj)
            studies_root_path = get_study_settings().study_metadata_files_root_path
            update_study_model_from_directory(study, studies_root_path,
                                              optimize_for_es_indexing=False,
                                              revalidate_study=True,
                                              user_token_to_revalidate=sensitive_data.super_user_token_001,
                                              include_maf_files=False)

        assert len(study.protocols) >= 1
        assert len(study.users) >= 1
        assert len(study.publications) >= 1
        assert len(study.organism) >= 1
        assert len(study.contacts) >= 1
        assert len(study.descriptors) >= 1
        assert len(study.assays) >= 1
        assert len(study.factors) >= 1
        assert len(study.assays[0].assayTable.data) >= 1
        assert len(study.assays[0].assayTable.fields) >= 1

    def test_create_mtbls_file_obj_01_optimized(self, flask_app, sensitive_data):
        study_id = "MTBLS1"
        with flask_app.app_context():
            db_manager = DBManager.get_instance(flask_app)
            with db_manager.session_maker() as db_session:
                db_study_obj = db_session.query(Study).filter(Study.acc == study_id).first()
                study = create_study_model_from_db_study(db_study_obj)
            studies_root_path = get_study_settings().study_metadata_files_root_path
            update_study_model_from_directory(study, studies_root_path,
                                              optimize_for_es_indexing=True,
                                              revalidate_study=True,
                                              user_token_to_revalidate=sensitive_data.super_user_token_001,
                                              include_maf_files=False)

        assert not hasattr(study, "protocols")
        assert not hasattr(study, "contacts")
        assert not hasattr(study, "sampleTable")
        assert len(study.factors) >= 1

        assert len(study.users) >= 1
        assert len(study.publications) >= 1
        assert len(study.organism) >= 1
        assert len(study.descriptors) >= 1
        assert len(study.assays) >= 1
        assert not hasattr(study.assays[0], "assayTable")

    def test_create_mtbls_file_obj_02_optimized(self, flask_app, sensitive_data):
        study_id = "MTBLS2435"
        with flask_app.app_context():
            db_manager = DBManager.get_instance(flask_app)
            with db_manager.session_maker() as db_session:
                db_study_obj = db_session.query(Study).filter(Study.acc == study_id).first()
                study = create_study_model_from_db_study(db_study_obj)
            studies_root_path = get_study_settings().study_metadata_files_root_path
            update_study_model_from_directory(study, studies_root_path,
                                              optimize_for_es_indexing=True,
                                              revalidate_study=True,
                                              user_token_to_revalidate=sensitive_data.super_user_token_001,
                                              include_maf_files=False)

        assert not hasattr(study, "protocols")
        assert not hasattr(study, "contacts")
        assert not hasattr(study, "sampleTable")
        assert len(study.users) >= 1
        assert len(study.assays) >= 1
        assert not hasattr(study.assays[0], "assayTable")
