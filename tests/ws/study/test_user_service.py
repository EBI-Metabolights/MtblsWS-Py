import pytest

from app.utils import MetabolightsException
from app.ws.study.user_service import UserService


class TestUserService(object):

    def test_validate_user_01(self, flask_app_with_test_data_01, super_user_01):
        flask_app = flask_app_with_test_data_01
        with flask_app.app_context():
            user_token = super_user_01.user_token
            user_service = UserService.get_instance(flask_app)
            result = user_service.validate_user_by_token(user_token, [1])
            assert result is not None
            assert result.username is not None

    def test_validate_user_02(self, flask_app_with_test_data_01, submitter_1):
        flask_app = flask_app_with_test_data_01
        with flask_app.app_context():
            user_token = submitter_1.user_token
            user_service = UserService.get_instance(flask_app)
            result = user_service.validate_user_by_token(user_token, [0])
            assert result is not None
            assert result.username is not None

    def test_validate_user_invalid_token_01(self, flask_app_with_test_data_01, invalid_user_1):
        flask_app = flask_app_with_test_data_01
        with flask_app.app_context():
            user_token = invalid_user_1.user_token
            user_service = UserService.get_instance(flask_app)
            with pytest.raises(MetabolightsException) as context:
                user_service.validate_user_by_token(user_token, [1])
            assert context.value.message is not None

    def test_validate_user_invalid_role_01(self, flask_app_with_test_data_01, submitter_1):
        flask_app = flask_app_with_test_data_01
        with flask_app.app_context():
            user_token = submitter_1.user_token
            user_service = UserService.get_instance(flask_app)
            with pytest.raises(MetabolightsException) as context:
                user_service.validate_user_by_token(user_token, [1])
            assert context.value.message is not None

    def test_validate_user_has_write_access_invalid_01(self, flask_app_with_test_data_01, submitter_1):
        flask_app = flask_app_with_test_data_01
        with flask_app.app_context():
            user_token = submitter_1.user_token
            user_service = UserService.get_instance(flask_app)
            with pytest.raises(MetabolightsException) as context:
                user_service.validate_user_has_write_access(user_token, "MTBLS3000")
            assert context.value.message is not None

    def test_validate_user_has_write_access_invalid_acess_token_01(self, flask_app_with_test_data_01, invalid_user_1):
        flask_app = flask_app_with_test_data_01
        with flask_app.app_context():
            user_token = invalid_user_1.user_token
            user_service = UserService.get_instance(flask_app)
            with pytest.raises(MetabolightsException) as context:
                user_service.validate_user_has_write_access(user_token, "MTBLS3000")
            assert context.value.message is not None

    def test_validate_user_has_write_access_invalid_study_id_01(self, flask_app_with_test_data_01, submitter_1):
        flask_app = flask_app_with_test_data_01
        with flask_app.app_context():
            user_token = submitter_1.user_token
            user_service = UserService.get_instance(flask_app)
            with pytest.raises(MetabolightsException) as context:
                user_service.validate_user_has_write_access(user_token, "MTBLSX3000")
            assert context.value.message is not None

    def test_validate_user_has_write_access_invalid_study_id_02(self, flask_app_with_test_data_01, super_user_01):
        flask_app = flask_app_with_test_data_01
        with flask_app.app_context():
            user_token = super_user_01.user_token
            user_service = UserService.get_instance(flask_app)
            with pytest.raises(MetabolightsException) as context:
                user_service.validate_user_has_write_access(user_token, "MTBLSX3000")
            assert context.value.message is not None

    def test_validate_user_has_write_access_valid_01(self, flask_app_with_test_data_01, submitter_1):
        flask_app = flask_app_with_test_data_01
        with flask_app.app_context():
            user_token = submitter_1.user_token
            user_service = UserService.get_instance(flask_app)
            result = user_service.validate_user_has_write_access(user_token, submitter_1.studies[0])
            assert result is not None
            assert result.username is not None

    def test_validate_user_has_write_access_valid_super_user_01(self, flask_app_with_test_data_01, submitter_1,
                                                                super_user_01):
        flask_app = flask_app_with_test_data_01
        with flask_app.app_context():
            user_token = super_user_01.user_token
            user_service = UserService.get_instance(flask_app)
            result = user_service.validate_user_has_write_access(user_token, submitter_1.studies[0])
            assert result is not None
            assert result.username is not None

    def test_validate_user_has_write_access_valid_super_user_02(self, flask_app_with_test_data_01, super_user_01,
                                                                submitter_1):
        flask_app = flask_app_with_test_data_01
        with flask_app.app_context():
            user_token = super_user_01.user_token
            user_service = UserService.get_instance(flask_app)
            result = user_service.validate_user_has_write_access(user_token, submitter_1.studies[1])
            assert result is not None
            assert result.username is not None
