import pytest
from flask import Flask

from app.utils import MetabolightsException
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from tests.fixtures import SensitiveDatastorage


class TestElasticService(object):
    def test_reindex_study_01(
        self,
        flask_app: Flask,
        elasticsearch_service: ElasticsearchService,
        sensitive_data: SensitiveDatastorage,
        mocker,
    ):
        with flask_app.app_context():
            study_id = "MTBLS1"
            mock_index_method = mocker.Mock()
            elasticsearch_service.client = mock_index_method
            mock_index_method.index = mocker.Mock()
            study = elasticsearch_service.reindex_study(
                study_id=study_id, user_token=sensitive_data.super_user_token_001
            )

            assert study is not None
            mock_index_method.index.assert_called()

    def test_reindex_study_valid_authorization_02(
        self,
        flask_app: Flask,
        elasticsearch_service: ElasticsearchService,
        sensitive_data: SensitiveDatastorage,
        mocker,
    ):
        with flask_app.app_context():
            study_id = "MTBLS2435"
            mock_index_method = mocker.Mock()
            elasticsearch_service.client = mock_index_method
            mock_index_method.index = mocker.Mock()
            study = elasticsearch_service.reindex_study(
                study_id=study_id, user_token=sensitive_data.super_user_token_001
            )

            assert study is not None
            mock_index_method.index.assert_called()

    def test_reindex_study_invalid_study_01(
        self,
        flask_app: Flask,
        elasticsearch_service: ElasticsearchService,
        sensitive_data: SensitiveDatastorage,
        mocker,
    ):
        with flask_app.app_context():
            study_id = "MTBLS4654333322"
            mock_index_method = mocker.Mock()
            elasticsearch_service.client = mock_index_method
            mock_index_method.index = mocker.Mock()
            with pytest.raises(MetabolightsException) as context:
                study = elasticsearch_service.reindex_study(
                    study_id=study_id, user_token=sensitive_data.submitter_token_001
                )

            assert context.value is not None
            mock_index_method.index.assert_not_called()

    def test_reindex_study_invalid_user_token_01(
        self,
        flask_app: Flask,
        elasticsearch_service: ElasticsearchService,
        sensitive_data: SensitiveDatastorage,
        mocker,
    ):
        with flask_app.app_context():
            study_id = "MTBLS1"
            mock_index_method = mocker.Mock()
            elasticsearch_service.client = mock_index_method
            mock_index_method.index = mocker.Mock()
            with pytest.raises(MetabolightsException) as context:
                study = elasticsearch_service.reindex_study(
                    study_id=study_id, user_token="xyzs-invalid"
                )

            assert context.value is not None
            mock_index_method.index.assert_not_called()

    def test_reindex_study_sumitter_user_token_01(
        self,
        flask_app: Flask,
        elasticsearch_service: ElasticsearchService,
        sensitive_data: SensitiveDatastorage,
        mocker,
    ):
        with flask_app.app_context():
            study_id = "MTBLS1"
            mock_index_method = mocker.Mock()
            elasticsearch_service.client = mock_index_method
            mock_index_method.index = mocker.Mock()
            study = elasticsearch_service.reindex_study(
                study_id=study_id, user_token=sensitive_data.submitter_token_001
            )

            assert study is not None
            mock_index_method.index.assert_called()
