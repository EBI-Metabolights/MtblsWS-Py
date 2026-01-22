context_path = "/metabolights/ws"

test_study_list = ["MTBLS1000", "MTBLS1001"]


class TestCreateAccession(object):
    def test_get_invalid_token_01(self, flask_app_with_test_data_01, invalid_user_1):
        flask_app = flask_app_with_test_data_01
        with flask_app.test_client() as c:
            headers = {"user-token": invalid_user_1.user_token}
            result = c.get(f"{context_path}/studies/create", headers=headers, json={})
            assert result is not None
            assert 401 == result.status_code
