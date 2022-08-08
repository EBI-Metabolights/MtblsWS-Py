import json

context_path = "/metabolights/ws"


class TestMtblsPrivateStudies(object):

    def test_get_private_studies_01(self, flask_app, sensitive_data):
        with flask_app.test_client() as c:
            headers = {"user_token": sensitive_data.super_user_token_001}
            result = c.get(f"{context_path}/studies/private", headers=headers, json={})
            assert result is not None
            studies = json.loads(result.data)
            assert studies is not None

    def test_get_private_studies_unauthorized_01(self, flask_app, sensitive_data):
        with flask_app.test_client() as c:
            headers = {"user_token": sensitive_data.submitter_token_001}
            result = c.get(f"{context_path}/studies/private", headers=headers, json={})
            assert 401 == result.status_code
