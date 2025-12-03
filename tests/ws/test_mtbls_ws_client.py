import json

import pytest
from flask import Flask

from app.utils import MetabolightsException
from app.ws.mtblsWSclient import WsClient
from tests.fixtures import SensitiveDatastorage
from tests.ws.test_data.utils import delete_test_study_from_db


class TestWebServiceClient(object):
    def test_get_maf_search_1(
        self, flask_app: Flask, email_service_isolated_ws_client: WsClient
    ):
        with flask_app.app_context():
            ws_client = email_service_isolated_ws_client

            search_type = "databaseId"
            search_value = "CHEBI:10225"
            expected_value = "CHEBI:10225"
            result = ws_client.get_maf_search(search_type, search_value)
            assert result is not None
            data = result
            assert 1 == len(data["content"])
            assert "databaseId" in data["content"][0]
            assert expected_value == data["content"][0]["databaseId"]

    def test_get_maf_search_2(
        self, flask_app: Flask, email_service_isolated_ws_client: WsClient
    ):
        with flask_app.app_context():
            ws_client = email_service_isolated_ws_client

            search_type = "inchi"
            search_value = "NLDDIKRKFXEWBK-AWEZNQCLSA-N"
            expected_value = "CHEBI:10136"
            result = ws_client.get_maf_search(search_type, search_value)
            assert result is not None
            data = result
            assert "content" in data
            assert 1 == len(data["content"])
            assert "databaseId" in data["content"][0]
            assert expected_value == data["content"][0]["databaseId"]

    def test_get_maf_search_3(
        self, flask_app: Flask, email_service_isolated_ws_client: WsClient
    ):
        with flask_app.app_context():
            ws_client = email_service_isolated_ws_client

            search_type = "smiles"
            search_value = "CO"
            expected_value = "CHEBI:17790"
            result = ws_client.get_maf_search(search_type, search_value)
            assert result is not None
            data = result
            assert "content" in data
            assert 1 == len(data["content"])
            assert "databaseId" in data["content"][0]
            assert expected_value == data["content"][0]["databaseId"]

    def test_get_maf_search_4_name(
        self, flask_app: Flask, email_service_isolated_ws_client: WsClient
    ):
        with flask_app.app_context():
            ws_client = email_service_isolated_ws_client

            search_type = "name"
            search_value = "(5S)-5-hydroxy-1-(4-hydroxy-3-methoxyphenyl)decan-3-one"
            expected_value = "CHEBI:10136"
            result = ws_client.get_maf_search(search_type, search_value)
            assert result is not None
            data = result
            assert "content" in data
            assert 1 == len(data["content"])
            assert "databaseId" in data["content"][0]
            assert expected_value == data["content"][0]["databaseId"]

    def test_get_all_studies_for_user_1(
        self,
        flask_app: Flask,
        email_service_isolated_ws_client: WsClient,
        sensitive_data: SensitiveDatastorage,
    ):
        with flask_app.app_context():
            ws_client = email_service_isolated_ws_client

            result = ws_client.get_all_studies_for_user(
                sensitive_data.invalid_user_token_001
            )
            assert result is not None
            data = json.loads(result)
            assert len(data) == 0

    def test_get_all_studies_for_user_2(
        self,
        flask_app: Flask,
        email_service_isolated_ws_client: WsClient,
        sensitive_data: SensitiveDatastorage,
    ):
        with flask_app.app_context():
            ws_client = email_service_isolated_ws_client

            result = ws_client.get_all_studies_for_user(
                sensitive_data.submitter_token_001
            )
            assert result is not None
            data = json.loads(result)
            assert len(data) > 0

    def test_reindex_1(
        self,
        flask_app: Flask,
        email_service_isolated_ws_client: WsClient,
        sensitive_data: SensitiveDatastorage,
        mocker,
    ):
        with flask_app.app_context():
            ws_client = email_service_isolated_ws_client
            study_id = None
            user_token = sensitive_data.super_user_token_001
            try:
                mock_index_method = mocker.Mock()
                ws_client.elasticsearch_service.client = mock_index_method
                mock_index_method.index = mocker.Mock()
                study_id = ws_client.reindex_study("MTBLS1", user_token)
                assert study_id is not None
                mock_index_method.index.assert_called_once()

            finally:
                if study_id:
                    delete_test_study_from_db(study_id)

    def test_reindex_unauthorized_1(
        self,
        flask_app: Flask,
        email_service_isolated_ws_client: WsClient,
        sensitive_data: SensitiveDatastorage,
        mocker,
    ):
        with flask_app.app_context():
            ws_client = email_service_isolated_ws_client
            study_id = None
            try:
                with pytest.raises(MetabolightsException):
                    mock_index_method = mocker.Mock()
                    ws_client.elasticsearch_service.client = mock_index_method
                    mock_index_method.index = mocker.Mock()
                    study_id = ws_client.reindex_study("MTBLS1", "<invalid token>")

                    mock_index_method.index.assert_not_called()
            finally:
                if study_id:
                    delete_test_study_from_db(study_id)
