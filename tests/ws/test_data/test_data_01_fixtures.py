import uuid
from typing import List

import pytest

from app.ws.db_connection import create_empty_study
from tests.ws.test_data import utils


@pytest.fixture
def data_01_study_id_01_list():
    return ["MTBLS999999", "MTBLS1000001"]


@pytest.fixture
def super_user_01(data_01_study_id_01_list):
    return utils.UserTestData(str(uuid.uuid4()), "mtbls-test-super@ebi.ac.uk", 9999998, 1, 2)


@pytest.fixture
def submitter_1(data_01_study_id_01_list):
    return utils.UserTestData(str(uuid.uuid4()), "mtbls-submitter1@ebi.ac.uk", 9999999, 0, 2, data_01_study_id_01_list)


@pytest.fixture
def invalid_user_1(data_01_study_id_01_list):
    return utils.UserTestData("xxxxxxxsss", "mtbls-invalid@ebi.ac.uk", 1111111111, 0, 2, [])


@pytest.fixture
def data_01_user_list(super_user_01, submitter_1):
    return [super_user_01, submitter_1]


@pytest.fixture
def flask_app_with_test_data_01(flask_app, data_01_user_list: List[utils.UserTestData]):
    with flask_app.app_context():
        for user in data_01_user_list:
            utils.delete_test_user_from_db(user)
            for study_id in user.studies:
                utils.delete_test_study_from_db(study_id)
            utils.create_user_in_db(user)
            for study_id in user.studies:
                create_empty_study(user.user_token, study_id)
        yield flask_app
        for user in data_01_user_list:
            utils.delete_test_user_from_db(user)
            for study_id in user.studies:
                utils.delete_test_study_from_db(study_id)
