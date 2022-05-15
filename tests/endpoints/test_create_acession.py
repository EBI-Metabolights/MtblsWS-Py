import os
import shutil
from unittest.mock import patch

import flask_mail

from app.ws.db_connection import create_empty_study
from tests.ws.test_data.utils import delete_test_study_from_db

context_path = "/metabolights/ws"

test_study_list = ["MTBLS1000", "MTBLS1001"]


class TestCreateAccession(object):

    def test_get_invalid_token_01(self, flask_app_with_test_data_01, invalid_user_1):
        flask_app = flask_app_with_test_data_01
        with flask_app.test_client() as c:
            headers = {"user_token": invalid_user_1.user_token}
            result = c.get(f"{context_path}/studies/create", headers=headers, json={})
            assert result is not None
            assert 401 == result.status_code

    def test_get_valid_token_01(self, flask_app_with_test_data_01, super_user_01):
        """
        Verifies these method updates
            WsClient add_empty_study
            WsClient create_upload_folder
            WsClient reindex_study
            get_permissions
        """
        flask_app = flask_app_with_test_data_01
        with flask_app.test_client() as c:
            with patch.object(flask_mail.Mail, "send", return_value="") as mock_mail_send:
                headers = {"user_token": super_user_01.user_token}
                result = c.get(f"{context_path}/studies/create", headers=headers, json={})
                assert result is not None
                assert result.status_code in (200, 201)
                mock_mail_send.assert_called()

    def test_get_valid_token_with_study_id_not_in_db_01(self, flask_app_with_test_data_01, super_user_01):
        flask_app = flask_app_with_test_data_01
        with flask_app.test_client() as c:
            # create study in db before run test and than delete these in db
            study_id_1 = create_empty_study(super_user_01.user_token)
            assert study_id_1 is not None
            delete_test_study_from_db(study_id_1)

            path = os.path.join(flask_app.config.get("STUDY_PATH"), study_id_1)
            if os.path.exists(path):
                shutil.rmtree(path)
            with patch.object(flask_mail.Mail, "send", return_value="") as mock_mail_send:
                headers = {"user_token": super_user_01.user_token, "study_id": study_id_1}
                result = c.get(f"{context_path}/studies/create", headers=headers, json={})
                delete_test_study_from_db(study_id_1)
                if os.path.exists(path):
                    shutil.rmtree(path)

                assert result is not None
                assert result.status_code in (200, 201)
                mock_mail_send.assert_called()

    def test_get_unauthorized_token_with_study_id_not_in_db_01(self, flask_app_with_test_data_01, submitter_1):
        flask_app = flask_app_with_test_data_01
        with flask_app.test_client() as c:
            # create study in db before run test and than delete these in db
            study_id_1 = create_empty_study(submitter_1.user_token)
            assert study_id_1 is not None
            delete_test_study_from_db(study_id_1)

            path = os.path.join(flask_app.config.get("STUDY_PATH"), study_id_1)
            if os.path.exists(path):
                shutil.rmtree(path)
            with patch.object(flask_mail.Mail, "send", return_value="") as mock_mail_send:
                headers = {"user_token": submitter_1.user_token, "study_id": study_id_1}
                result = c.get(f"{context_path}/studies/create", headers=headers, json={})
                delete_test_study_from_db(study_id_1)
                if os.path.exists(path):
                    shutil.rmtree(path)

                assert result is not None
                assert result.status_code in (401,)
                mock_mail_send.assert_not_called()

    def test_get_valid_token_with_existing_study_in_db_01(self, flask_app_with_test_data_01, super_user_01):

        flask_app = flask_app_with_test_data_01
        with flask_app.test_client() as c:
            study_id = "MTBLS1"
            path = os.path.join(flask_app.config.get("STUDY_PATH"), study_id)
            with patch.object(flask_mail.Mail, "send", return_value="") as mock_mail_send:
                headers = {"user_token": super_user_01.user_token, "study_id": "MTBLS1"}
                result = c.get(f"{context_path}/studies/create", headers=headers, json={})
                assert result is not None
                assert result.status_code in (400,)
                mock_mail_send.assert_not_called()
