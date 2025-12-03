import json

context_path = "/metabolights/ws"


class TestMtblsStudies(object):
    def test_get_public_studies_01_super_user(self, flask_app, sensitive_data):
        with flask_app.test_client() as c:
            headers = {"user-token": sensitive_data.super_user_token_001}
            result = c.get(f"{context_path}/studies/private", headers=headers, json={})
            assert result is not None
            studies = json.loads(result.data)
            assert studies is not None

    def test_get_public_studies_01_invalid_token(self, flask_app, sensitive_data):
        with flask_app.test_client() as c:
            headers = {"user-token": sensitive_data.invalid_user_token_001}
            result = c.get(f"{context_path}/studies", headers=headers, json={})
            assert result is not None
            studies = json.loads(result.data)
            assert studies is not None

    def test_get_public_studies_01_without_token(self, flask_app):
        with flask_app.test_client() as c:
            headers = {}
            result = c.get(f"{context_path}/studies", headers=headers, json={})
            assert result is not None
            studies = json.loads(result.data)
            assert studies is not None
