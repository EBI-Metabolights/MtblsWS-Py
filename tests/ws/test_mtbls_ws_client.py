import json
import os
import pathlib
import shutil

import pytest
from flask import Flask

from app.utils import MetabolightsException
from app.ws.db_connection import get_email, execute_query_with_parameter, study_submitters
from app.ws.mtblsWSclient import WsClient
from tests.fixtures import SensitiveDatastorage
from tests.ws.test_data.utils import delete_test_study_from_db


class TestWebServiceClient(object):

    def test_get_study_location_not_authorized_1(self, flask_app: Flask,
                                                 email_service_isolated_ws_client: WsClient,
                                                 sensitive_data: SensitiveDatastorage):
        with flask_app.app_context():
            ws_client = email_service_isolated_ws_client
            with pytest.raises(Exception) as context:
                ws_client.get_study_location("MTBLS9999", sensitive_data.invalid_user_token_001)
        exception_info = context.value
        assert exception_info.code == 403

    def test_get_study_location_public_1(self, flask_app: Flask, email_service_isolated_ws_client: WsClient,
                                         sensitive_data: SensitiveDatastorage):
        with flask_app.app_context():
            ws_client = email_service_isolated_ws_client
            actual = ws_client.get_study_location("MTBLS1", sensitive_data.super_user_token_001)

            expected = os.path.join(flask_app.config.get('STUDY_PATH'), "MTBLS1")
            assert expected == actual

    def test_get_maf_search_1(self, flask_app: Flask, email_service_isolated_ws_client: WsClient):
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

    def test_get_maf_search_2(self, flask_app: Flask, email_service_isolated_ws_client: WsClient):
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

    def test_get_maf_search_3(self, flask_app: Flask, email_service_isolated_ws_client: WsClient):
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

    def test_get_maf_search_4_name(self, flask_app: Flask, email_service_isolated_ws_client: WsClient):
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

    def test_get_all_studies_for_user_1(self, flask_app: Flask,
                                        email_service_isolated_ws_client: WsClient,
                                        sensitive_data: SensitiveDatastorage):
        with flask_app.app_context():
            ws_client = email_service_isolated_ws_client

            result = ws_client.get_all_studies_for_user(sensitive_data.invalid_user_token_001)
            assert result is not None
            data = json.loads(result)
            assert len(data) == 0

    def test_get_all_studies_for_user_2(self, flask_app: Flask,
                                        email_service_isolated_ws_client: WsClient,
                                        sensitive_data: SensitiveDatastorage):
        with flask_app.app_context():
            ws_client = email_service_isolated_ws_client

            result = ws_client.get_all_studies_for_user(sensitive_data.submitter_token_001)
            assert result is not None
            data = json.loads(result)
            assert len(data) > 0

    def test_get_queue_folder_1(self, flask_app: Flask,
                                email_service_isolated_ws_client: WsClient,
                                sensitive_data: SensitiveDatastorage):
        with flask_app.app_context():
            ws_client = email_service_isolated_ws_client

            actual = ws_client.get_queue_folder()
            expected = flask_app.config.get('STUDY_QUEUE_FOLDER')
            assert expected == actual

    def test_create_upload_folder_valid_study_01(self, flask_app: Flask,
                                                 email_service_isolated_ws_client: WsClient,
                                                 sensitive_data: SensitiveDatastorage):
        with flask_app.app_context():
            ws_client = email_service_isolated_ws_client
            input_study_id = "MTBLS1"
            input_obfusucation_code = "XYZTaamss"
            expected_path = os.path.join(flask_app.config.get('MTBLS_FTP_ROOT'),
                                         input_study_id.lower() + "-" + input_obfusucation_code)
            if os.path.exists(expected_path):
                created_path = pathlib.Path(expected_path)
                shutil.rmtree(created_path)
            try:
                actual = ws_client.create_upload_folder(input_study_id, input_obfusucation_code,
                                                        sensitive_data.super_user_token_001)

                assert actual is not None
                assert expected_path == actual['os_upload_path']
                ws_client.email_service.send_email.assert_called()

            finally:
                if os.path.exists(expected_path):
                    created_path = pathlib.Path(expected_path)
                    shutil.rmtree(created_path)

    def test_create_upload_folder_invalid_study_01(self, flask_app: Flask,
                                                   email_service_isolated_ws_client: WsClient,
                                                   sensitive_data: SensitiveDatastorage):
        with flask_app.app_context():
            ws_client = email_service_isolated_ws_client
            input_study_id = "MTBLS999991"
            input_obfusucation_code = "XYZTaamss"
            expected_path = os.path.join(flask_app.config.get('MTBLS_FTP_ROOT'),
                                         input_study_id.lower() + "-" + input_obfusucation_code)

            try:
                with pytest.raises(MetabolightsException):
                    ws_client.create_upload_folder(input_study_id, input_obfusucation_code,
                                                   sensitive_data.super_user_token_001)
                    ws_client.email_service.send_email.assert_not_called()
            finally:
                if os.path.exists(expected_path):
                    created_path = pathlib.Path(expected_path)
                    shutil.rmtree(created_path)

    def test_add_empty_study_1(self, flask_app: Flask,
                               email_service_isolated_ws_client: WsClient,
                               sensitive_data: SensitiveDatastorage):
        with flask_app.app_context():
            ws_client = email_service_isolated_ws_client
            study_id = None
            user_token = sensitive_data.super_user_token_001
            try:
                actual = ws_client.add_empty_study(user_token)
                study_id = actual
                assert actual is not None
                ws_client.email_service.send_email.assert_called()

            finally:
                if study_id:
                    user_email = get_email(user_token)
                    study_submitters(study_id, user_email, "delete")
                    sql = "delete from study where acc = %(acc)s;"
                    params = {"acc": study_id}
                    execute_query_with_parameter(sql, params)

    def test_reindex_1(self, flask_app: Flask,
                       email_service_isolated_ws_client: WsClient,
                       sensitive_data: SensitiveDatastorage, mocker):
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


    def test_reindex_unauthorized_1(self, flask_app: Flask,
                                    email_service_isolated_ws_client: WsClient,
                                    sensitive_data: SensitiveDatastorage, mocker):
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