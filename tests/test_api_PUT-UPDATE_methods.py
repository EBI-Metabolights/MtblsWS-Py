import json
import urllib.request
import urllib.error
import unittest
import time

import instance.config

url_about = instance.config.TEST_URL_ABOUT
public_study_id = instance.config.TEST_PUB_STUDY_ID
private_study_id = instance.config.TEST_PRIV_STUDY_ID
bad_study_id = instance.config.TEST_BAD_STUDY_ID
auth_id = instance.config.TEST_AUTH_ID
wrong_auth_token = instance.config.TEST_WRONG_AUTH_TOKEN
url_base = instance.config.TEST_URL_BASE
url_pub_id = url_base + "/" + public_study_id
url_priv_id = url_base + "/" + private_study_id
url_null_id = url_base + "/"
url_wrong_id = url_base + bad_study_id
public_source_id = instance.config.TEST_PUB_SOURCE_ID
private_source_id = instance.config.TEST_PRIV_SOURCE_ID
bad_source_id = instance.config.TEST_BAD_SOURCE_ID
public_sample_id = instance.config.TEST_PUB_SAMPLE_ID
private_sample_id = instance.config.TEST_PRIV_SAMPLE_ID
bad_sample_id = instance.config.TEST_BAD_SAMPLE_ID


class WsTests(unittest.TestCase):

    def add_common_headers(self, request):
        request.add_header('Accept', 'application/json')
        request.add_header('Content-Type', 'application/json')

    def check_header_common(self, header):
        self.assertIn('Access-Control-Allow-Origin', header)
        self.assertIn('Content-Type', header)

    def check_body_common(self, body):
        self.assertIsNotNone(body)


# ############################################
# All tests that UPDATE data below this point
# ############################################

class UpdateStudyTitleTests(WsTests):

    data_new_title = b'{ "title": "New Study title..." }'

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

    # Update Study Title - Pub - Auth -> 200
    def test_update_title_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/title', data=self.data_new_title, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('title', body)

    # Update Study Title - Pub - NoAuth -> 403
    def test_update_title_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/title', data=self.data_new_title, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Update Study Title - Priv - Auth -> 200
    def test_update_title_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/title', data=self.data_new_title, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('title', body)

    # Update Study Title - Priv - NoAuth -> 403
    def test_update_title_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/title', data=self.data_new_title, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Update Study Title - NullId -> 404
    def test_update_title_nullId(self):
        request = urllib.request.Request(url_null_id + '/title', data=self.data_new_title, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Title - BadId -> 404
    def test_update_title_badId(self):
        request = urllib.request.Request(url_wrong_id + '/title', data=self.data_new_title, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Title - Pub - Auth - NoSave -> 200
    def test_update_title_pub_auth_noSave(self):
        request = urllib.request.Request(url_pub_id + '/title', data=self.data_new_title, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('title', body)

    # Update Study Title - Pub - NoAuth - NoSave -> 403
    def test_update_title_pub_noAuth_noSave(self):
        request = urllib.request.Request(url_priv_id + '/title', data=self.data_new_title, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        request.add_header('save_audit_copy', 'False')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Update Study Title - Priv - Auth - NoSave -> 200
    def test_update_title_priv_auth_noSave(self):
        request = urllib.request.Request(url_priv_id + '/title', data=self.data_new_title, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('title', body)

    # Update Study Title - Priv - NoAuth - NoSave -> 403
    def test_update_title_priv_noAuth_noSave(self):
        request = urllib.request.Request(url_priv_id + '/title', data=self.data_new_title, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        request.add_header('save_audit_copy', 'False')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Update Study Title - NullId - NoSave -> 404
    def test_update_title_nullId_noSave(self):
        request = urllib.request.Request(url_null_id + '/title', data=self.data_new_title, method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Title - BadId - NoSave -> 404
    def test_update_title_badId_noSave(self):
        request = urllib.request.Request(url_wrong_id + '/title', data=self.data_new_title, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


class UpdateStudyDescriptionTests(WsTests):

    data_new_description = b'{ "description": "New Study description..." }'

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

    # Update Study Description - Pub - Auth -> 200
    def test_update_description_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/description', data=self.data_new_description, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('description', body)

    # Update Study Description - Pub - NoAuth -> 403
    def test_update_description_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/description', data=self.data_new_description, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Update Study Description - Priv - Auth -> 200
    def test_update_description_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/description', data=self.data_new_description, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('description', body)

    # Update Study Description - Priv - NoAuth -> 403
    def test_update_description_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/description', data=self.data_new_description, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Update Study Description - NullId -> 404
    def test_update_description_nullId(self):
        request = urllib.request.Request(url_null_id + '/description', data=self.data_new_description, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Description - BadId -> 404
    def test_update_description_badId(self):
        request = urllib.request.Request(url_wrong_id + '/description', data=self.data_new_description, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Description - Pub - Auth - NoSave -> 200
    def test_update_description_pub_auth_noSave(self):
        request = urllib.request.Request(url_pub_id + '/description', data=self.data_new_description, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('description', body)

    # Update Study Description - Pub - NoAuth - NoSave -> 403
    def test_update_description_pub_noAuth_noSave(self):
        request = urllib.request.Request(url_pub_id + '/description', data=self.data_new_description, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        request.add_header('save_audit_copy', 'False')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Update Study Description - Priv - Auth - NoSave -> 200
    def test_update_description_priv_auth_noSave(self):
        request = urllib.request.Request(url_priv_id + '/description', data=self.data_new_description, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('description', body)

    # Update Study Description - Priv - NoAuth - NoSave -> 403
    def test_update_description_priv_noAuth_noSave(self):
        request = urllib.request.Request(url_priv_id + '/description', data=self.data_new_description, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        request.add_header('save_audit_copy', 'False')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Update Study Description - NullId - NoSave -> 404
    def test_update_description_nullId_noSave(self):
        request = urllib.request.Request(url_null_id + '/description', data=self.data_new_description, method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Description - BadId - NoSave -> 404
    def test_update_description_badId_noSave(self):
        request = urllib.request.Request(url_wrong_id + '/description', data=self.data_new_description, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


class UpdateStudyContactTests(WsTests):

    valid_id = instance.config.VALID_CONTACT_ID
    bad_id = instance.config.BAD_CONTACT_ID
    valid_data = instance.config.TEST_DATA_VALID_CONTACT
    missing_data = instance.config.TEST_DATA_MISSING_CONTACT
    no_data = b''

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

    def check_Person_class(self, obj):
        self.assertIsNotNone(obj['firstName'])
        # self.assertIsNotNone(person['midInitials'])
        self.assertIsNotNone(obj['lastName'])
        self.assertIsNotNone(obj['email'])
        self.assertIsNotNone(obj['affiliation'])
        self.assertIsNotNone(obj['phone'])
        self.assertIsNotNone(obj['address'])
        self.assertIsNotNone(obj['fax'])
        self.assertIsNotNone(obj['comments'])
        for role in obj['roles']:
            self.assertIsNotNone(role['annotationValue'])
            self.assertIsNotNone(role['termAccession'])
            self.assertIsNotNone(role['comments'])

    def pre_create_contact(self, url):
        request = urllib.request.Request(url + '/contacts',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            if err.code != 409:
                raise Exception(err)

    # Update Study Contact - Pub - Auth - GoodId -> 200
    def test_update_Contact_pub_auth(self):
        # first, create the contact to ensure it will exists
        self.pre_create_contact(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to update the contact
        request = urllib.request.Request(url_pub_id + '/contacts'
                                         + '?email=' + self.valid_id,
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('contact', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['contact'])
            self.check_Person_class(j_resp['contact'])

    # Update Study Contact - Pub - Auth - NoData -> 400
    def test_update_Contact_pub_auth_noData(self):
        request = urllib.request.Request(url_pub_id + '/contacts'
                                         + '?email=' + self.valid_id,
                                         data=self.no_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 400)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('BAD REQUEST', err.msg)
            self.assertEqual('BAD REQUEST', err.reason)

    # Update Study Contact - Pub - Auth - MissingRequiredData -> 400
    def test_update_Contact_pub_auth_missingData(self):
        request = urllib.request.Request(url_pub_id + '/contacts'
                                         + '?email=' + self.valid_id,
                                         data=self.missing_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 400)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('BAD REQUEST', err.msg)
            self.assertEqual('BAD REQUEST', err.reason)

    # Update Study Contact - Pub - NoToken -> 401
    def test_update_Contact_pub_noToken(self):
        request = urllib.request.Request(url_pub_id + '/contacts'
                                         + '?email=' + self.valid_id,
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Contact - Pub - NoAuth -> 403
    def test_update_Contact_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/contacts'
                                         + '?email=' + self.valid_id,
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Update Study Contact - Pub - Auth - BadId -> 404
    def test_update_Contact_pub_auth_badId(self):
        request = urllib.request.Request(url_pub_id + '/contacts'
                                         + '?email=' + self.bad_id,
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Contact - Pub - Auth - NullId -> 404
    def test_update_Contact_pub_auth_nullId(self):
        request = urllib.request.Request(url_pub_id + '/contacts'
                                         + '?email=',
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Contact - Priv - Auth - GoodId -> 200
    def test_update_Contact_priv_auth(self):
        # first, create the contact to ensure it will exists
        self.pre_create_contact(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to update the contact
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=' + self.valid_id,
                                         data=self.valid_data, method='PUT'
                                         )
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('contact', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['contact'])
            self.check_Person_class(j_resp['contact'])

    # Update Study Contact - Priv - Auth - NoData -> 400
    def test_update_Contact_priv_auth_noData(self):
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=' + self.valid_id,
                                         data=self.no_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 400)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('BAD REQUEST', err.msg)
            self.assertEqual('BAD REQUEST', err.reason)

    # Update Study Contact - Priv - Auth - MissingRequiredData -> 400
    def test_update_Contact_priv_auth_missingData(self):
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=' + self.valid_id,
                                         data=self.missing_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 400)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('BAD REQUEST', err.msg)
            self.assertEqual('BAD REQUEST', err.reason)

    # Update Study Contact - Priv - NoToken -> 401
    def test_update_Contact_priv_noToken(self):
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=' + self.valid_id,
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Contact - Priv - NoAuth -> 403
    def test_update_Contact_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=' + self.valid_id,
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Update Study Contact - Priv - Auth - BadId -> 404
    def test_update_Contact_priv_auth_badId(self):
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=' + self.bad_id,
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Contact - Priv - Auth - NullId -> 404
    def test_update_Contact_priv_auth_nullId(self):
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=',
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


class UpdateStudyProtocolTests(WsTests):

    valid_id = instance.config.VALID_PROTOCOL_ID
    bad_id = instance.config.BAD_PROTOCOL_ID
    valid_data = instance.config.TEST_DATA_VALID_PROTOCOL
    missing_data = instance.config.TEST_DATA_MISSING_PROTOCOL
    no_data = b''

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

    def check_Protocol_class(self, obj):
        self.assertIsNotNone(obj['name'])
        self.assertIsNotNone(obj['protocolType'])
        self.assertIsNotNone(obj['description'])
        self.assertIsNotNone(obj['uri'])
        self.assertIsNotNone(obj['version'])
        self.assertIsNotNone(obj['parameters'])
        self.assertIsNotNone(obj['components'])

    def pre_create_protocol(self, url):
        request = urllib.request.Request(url + '/protocols',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            if err.code != 409:
                raise Exception(err)

    # Update Study Protocol - Pub - Auth - GoodId -> 200
    def test_update_Protocol_pub_auth(self):
        # first, create the protocol to ensure it will exists
        self.pre_create_protocol(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to update the protocol
        request = urllib.request.Request(url_pub_id + '/protocols'
                                         + '?name=' + self.valid_id,
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('protocol', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['protocol'])
            self.check_Protocol_class(j_resp['protocol'])

    # Update Study Protocol - Pub - Auth - NoData -> 400
    def test_update_Protocol_pub_auth_noData(self):
        request = urllib.request.Request(url_pub_id + '/protocols'
                                         + '?name=' + self.valid_id,
                                         data=self.no_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 400)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('BAD REQUEST', err.msg)
            self.assertEqual('BAD REQUEST', err.reason)

    # Update Study Protocol - Pub - Auth - MissingRequiredData -> 400
    def test_update_Protocol_pub_auth_missingData(self):
        request = urllib.request.Request(url_pub_id + '/protocols'
                                         + '?name=' + self.valid_id,
                                         data=self.missing_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 400)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('BAD REQUEST', err.msg)
            self.assertEqual('BAD REQUEST', err.reason)

    # Update Study Protocol - Pub - NoToken -> 401
    def test_update_Protocol_pub_noToken(self):
        request = urllib.request.Request(url_pub_id + '/protocols'
                                         + '?name=' + self.valid_id,
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Protocol - Pub - NoAuth -> 403
    def test_update_Protocol_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/protocols'
                                         + '?name=' + self.valid_id,
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Update Study Protocol - Pub - Auth - BadId -> 404
    def test_update_Protocol_pub_auth_badId(self):
        request = urllib.request.Request(url_pub_id + '/protocols'
                                         + '?name=' + self.bad_id,
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Protocol - Pub - Auth - NullId -> 404
    def test_update_Protocol_pub_auth_nullId(self):
        request = urllib.request.Request(url_pub_id + '/protocols'
                                         + '?name=',
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Protocol - Priv - Auth - GoodId -> 200
    def test_update_Protocol_priv_auth(self):
        # first, create the protocol to ensure it will exists
        self.pre_create_protocol(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to update the protocol
        request = urllib.request.Request(url_priv_id + '/protocols'
                                         + '?name=' + self.valid_id,
                                         data=self.valid_data, method='PUT'
                                         )
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('protocol', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['protocol'])
            self.check_Protocol_class(j_resp['protocol'])

    # Update Study Protocol - Priv - Auth - NoData -> 400
    def test_update_Protocol_priv_auth_noData(self):
        request = urllib.request.Request(url_priv_id + '/protocols'
                                         + '?name=' + self.valid_id,
                                         data=self.no_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 400)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('BAD REQUEST', err.msg)
            self.assertEqual('BAD REQUEST', err.reason)

    # Update Study Protocol - Priv - Auth - MissingRequiredData -> 400
    def test_update_Protocol_priv_auth_missingData(self):
        request = urllib.request.Request(url_priv_id + '/protocols'
                                         + '?name=' + self.valid_id,
                                         data=self.missing_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 400)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('BAD REQUEST', err.msg)
            self.assertEqual('BAD REQUEST', err.reason)

    # Update Study Protocol - Priv - NoToken -> 401
    def test_update_Protocol_priv_noToken(self):
        request = urllib.request.Request(url_priv_id + '/protocols'
                                         + '?name=' + self.valid_id,
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Protocol - Priv - NoAuth -> 403
    def test_update_Protocol_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/protocols'
                                         + '?name=',
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Update Study Protocol - Priv - Auth - BadId -> 404
    def test_update_Protocol_priv_auth_badId(self):
        request = urllib.request.Request(url_priv_id + '/protocols'
                                         + '?name=' + self.bad_id,
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Protocol - Priv - Auth - NullId -> 404
    def test_update_Protocol_priv_auth_nullId(self):
        request = urllib.request.Request(url_priv_id + '/protocols'
                                         + '?name=',
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


class UpdateStudyFactorTests(WsTests):

    valid_id = instance.config.VALID_FACTOR_ID
    bad_id = instance.config.BAD_FACTOR_ID
    valid_data = instance.config.TEST_DATA_VALID_FACTOR
    missing_data = instance.config.TEST_DATA_MISSING_FACTOR
    no_data = b''

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

    def check_Factor_class(self, obj):
        self.assertIsNotNone(obj['factorName'])
        self.assertIsNotNone(obj['factorType'])
        self.assertIsNotNone(obj['comments'])

    def pre_create_factor(self, url):
        request = urllib.request.Request(url + '/factors',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            if err.code != 409:
                raise Exception(err)

    # Update Study Factor - Pub - Auth - GoodId -> 200
    def test_update_Factor_pub_auth(self):
        # first, create the factor to ensure it will exists
        self.pre_create_factor(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to update the factor
        request = urllib.request.Request(url_pub_id + '/factors'
                                         + '?name=' + self.valid_id,
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('factor', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['factor'])
            self.check_Factor_class(j_resp['factor'])

    # Update Study Factor - Pub - Auth - NoData -> 400
    def test_update_Factor_pub_auth_noData(self):
        request = urllib.request.Request(url_pub_id + '/factors'
                                         + '?name=' + self.valid_id,
                                         data=self.no_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 400)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('BAD REQUEST', err.msg)
            self.assertEqual('BAD REQUEST', err.reason)

    # Update Study Factor - Pub - Auth - MissingRequiredData -> 400
    def test_update_Factor_pub_auth_missingData(self):
        request = urllib.request.Request(url_pub_id + '/factors'
                                         + '?name=' + self.valid_id,
                                         data=self.missing_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 400)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('BAD REQUEST', err.msg)
            self.assertEqual('BAD REQUEST', err.reason)

    # Update Study Factor - Pub - NoToken -> 401
    def test_update_Factor_pub_noToken(self):
        request = urllib.request.Request(url_pub_id + '/factors'
                                         + '?name=' + self.valid_id,
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Factor - Pub - NoAuth -> 403
    def test_update_Factor_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/factors'
                                         + '?name=' + self.valid_id,
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Update Study Factor - Pub - Auth - BadId -> 404
    def test_update_Factor_pub_auth_badId(self):
        request = urllib.request.Request(url_pub_id + '/factors'
                                         + '?name=' + self.bad_id,
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Factor - Pub - Auth - NullId -> 404
    def test_update_Factor_pub_auth_nullId(self):
        request = urllib.request.Request(url_pub_id + '/factors'
                                         + '?name=',
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Factor - Priv - Auth - GoodId -> 200
    def test_update_Factor_priv_auth(self):
        # first, create the factor to ensure it will exists
        self.pre_create_factor(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to update the factor
        request = urllib.request.Request(url_priv_id + '/factors'
                                         + '?name=' + self.valid_id,
                                         data=self.valid_data, method='PUT'
                                         )
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('factor', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['factor'])
            self.check_Factor_class(j_resp['factor'])

    # Update Study Factor - Priv - Auth - NoData -> 400
    def test_update_Factor_priv_auth_noData(self):
        request = urllib.request.Request(url_priv_id + '/factors'
                                         + '?name=' + self.valid_id,
                                         data=self.no_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 400)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('BAD REQUEST', err.msg)
            self.assertEqual('BAD REQUEST', err.reason)

    # Update Study Factor - Priv - Auth - MissingRequiredData -> 400
    def test_update_Factor_priv_auth_missingData(self):
        request = urllib.request.Request(url_priv_id + '/factors'
                                         + '?name=' + self.valid_id,
                                         data=self.missing_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 400)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('BAD REQUEST', err.msg)
            self.assertEqual('BAD REQUEST', err.reason)

    # Update Study Factor - Priv - NoToken -> 401
    def test_update_Factor_priv_noToken(self):
        request = urllib.request.Request(url_priv_id + '/factors'
                                         + '?name=' + self.valid_id,
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Factor - Priv - NoAuth -> 403
    def test_update_Factor_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/factors'
                                         + '?name=',
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Update Study Factor - Priv - Auth - BadId -> 404
    def test_update_Factor_priv_auth_badId(self):
        request = urllib.request.Request(url_priv_id + '/factors'
                                         + '?name=' + self.bad_id,
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Factor - Priv - Auth - NullId -> 404
    def test_update_Factor_priv_auth_nullId(self):
        request = urllib.request.Request(url_priv_id + '/factors'
                                         + '?name=',
                                         data=self.valid_data, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)






# class UpdateStudyFactorsTests(WsTests):
#
#     data_new_factors = instance.config.TEST_DATA_STUDYFACTORS
#
#     def tearDown(self):
#         time.sleep(1)  # sleep time in seconds
#
#     def check_Factors_class(self, obj):
#         self.assertIsNotNone(obj['factors'])
#         for factor in obj['factors']:
#             self.assertIsNotNone(factor['factorName'])
#             self.assertIsNotNone(factor['factorType'])
#             self.assertIsNotNone(factor['comments'])
#
#     # Update Study Factors - Pub - Auth -> 200
#     def test_update_factors_pub_auth(self):
#         request = urllib.request.Request(url_pub_id + '/factors', data=self.data_new_factors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('factors', body)
#             self.check_Factors_class(j_resp)
#
#     # Update Study Factors - Pub - NoAuth -> 403
#     def test_update_factors_pub_noAuth(self):
#         request = urllib.request.Request(url_pub_id + '/factors', data=self.data_new_factors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Factors - Priv - Auth -> 200
#     def test_update_factors_priv_auth(self):
#         request = urllib.request.Request(url_priv_id + '/factors', data=self.data_new_factors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('factors', body)
#             self.check_Factors_class(j_resp)
#
#     # Update Study Factors - Priv - NoAuth -> 403
#     def test_update_factors_priv_noAuth(self):
#         request = urllib.request.Request(url_priv_id + '/factors', data=self.data_new_factors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Factors - NullId -> 404
#     def test_update_factors_nullId(self):
#         request = urllib.request.Request(url_null_id + '/factors', data=self.data_new_factors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # Update Study Factors - BadId -> 404
#     def test_update_factors_badId(self):
#         request = urllib.request.Request(url_wrong_id + '/factors', data=self.data_new_factors, method='PUT')
#         request.add_header('user_token', auth_id)
#         self.add_common_headers(request)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # Update Study Factors - Pub - Auth - NoSave -> 200
#     def test_update_factors_pub_auth_noSave(self):
#         request = urllib.request.Request(url_pub_id + '/factors', data=self.data_new_factors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         request.add_header('save_audit_copy', 'False')
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('factors', body)
#             self.check_Factors_class(j_resp)
#
#     # Update Study Factors - Pub - NoAuth - NoSave -> 403
#     def test_update_factors_pub_noAuth_noSave(self):
#         request = urllib.request.Request(url_pub_id + '/factors', data=self.data_new_factors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         request.add_header('save_audit_copy', 'False')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Factors - Priv - Auth - NoSave -> 200
#     def test_update_factors_priv_auth_noSave(self):
#         request = urllib.request.Request(url_priv_id + '/factors', data=self.data_new_factors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         request.add_header('save_audit_copy', 'False')
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('factors', body)
#             self.check_Factors_class(j_resp)
#
#     # Update Study Factors - Priv - NoAuth - NoSave -> 403
#     def test_update_factors_priv_noAuth_noSave(self):
#         request = urllib.request.Request(url_priv_id + '/factors', data=self.data_new_factors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         request.add_header('save_audit_copy', 'False')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Factors - NullId - NoSave -> 404
#     def test_update_factors_nullId_noSave(self):
#         request = urllib.request.Request(url_null_id + '/factors', data=self.data_new_factors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('save_audit_copy', 'False')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # Update Study Factors - BadId - NoSave -> 404
#     def test_update_factors_badId_noSave(self):
#         request = urllib.request.Request(url_wrong_id + '/factors', data=self.data_new_factors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         request.add_header('save_audit_copy', 'False')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#
# class UpdateStudyDescriptorsTests(WsTests):
#
#     data_new_descriptors = instance.config.TEST_DATA_DESCRIPTORS
#
#     def tearDown(self):
#         time.sleep(1)  # sleep time in seconds
#
#     def check_Descriptors_class(self, obj):
#         self.assertIsNotNone(obj['descriptors'])
#         for descriptor in obj['descriptors']:
#             self.assertIsNotNone(descriptor['annotationValue'])
#             self.assertIsNotNone(descriptor['termAccession'])
#             self.assertIsNotNone(descriptor['termSource'])
#             self.assertIsNotNone(descriptor['comments'])
#
#     # Update Study Descriptors - Pub - Auth -> 200
#     def test_update_descriptors_pub_auth(self):
#         request = urllib.request.Request(url_pub_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('descriptors', body)
#             self.check_Descriptors_class(j_resp)
#
#     # Update Study Descriptors - Pub - NoAuth -> 403
#     def test_update_descriptors_pub_noAuth(self):
#         request = urllib.request.Request(url_pub_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Descriptors - Priv - Auth -> 200
#     def test_update_descriptors_priv_auth(self):
#         request = urllib.request.Request(url_priv_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('descriptors', body)
#             self.check_Descriptors_class(j_resp)
#
#     # Update Study Descriptors - Priv - NoAuth -> 403
#     def test_update_descriptors_priv_noAuth(self):
#         request = urllib.request.Request(url_priv_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Descriptors - NullId -> 404
#     def test_update_descriptors_nullId(self):
#         request = urllib.request.Request(url_null_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
#         self.add_common_headers(request)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # Update Study Descriptors - BadId -> 404
#     def test_update_descriptors_badId(self):
#         request = urllib.request.Request(url_wrong_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # Update Study Descriptors - Pub - Auth - NoSave -> 200
#     def test_update_descriptors_pub_auth_noSave(self):
#         request = urllib.request.Request(url_pub_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         request.add_header('save_audit_copy', 'False')
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('descriptors', body)
#             self.check_Descriptors_class(j_resp)
#
#     # Update Study Descriptors - Pub - NoAuth - NoSave -> 403
#     def test_update_descriptors_pub_noAuth_noSave(self):
#         request = urllib.request.Request(url_pub_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         request.add_header('save_audit_copy', 'False')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Descriptors - Priv - Auth - NoSave -> 200
#     def test_update_descriptors_priv_auth_noSave(self):
#         request = urllib.request.Request(url_priv_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         request.add_header('save_audit_copy', 'False')
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('descriptors', body)
#             self.check_Descriptors_class(j_resp)
#
#     # Update Study Descriptors - Priv - NoAuth - NoSave -> 403
#     def test_update_descriptors_priv_noAuth_noSave(self):
#         request = urllib.request.Request(url_priv_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         request.add_header('save_audit_copy', 'False')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Descriptors - NullId - NoSave -> 404
#     def test_update_descriptors_nullId_noSave(self):
#         request = urllib.request.Request(url_null_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('save_audit_copy', 'False')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # Update Study Descriptors - BadId - NoSave -> 404
#     def test_update_descriptors_badId_noSave(self):
#         request = urllib.request.Request(url_wrong_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         request.add_header('save_audit_copy', 'False')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#
# class UpdateStudyPublicationsTests(WsTests):
#
#     data_new_publications = instance.config.TEST_DATA_PUBLICATIONS
#
#     def tearDown(self):
#         time.sleep(1)  # sleep time in seconds
#
#     def check_Publications_class(self, obj):
#         self.assertIsNotNone(obj['publications'])
#         for publication in obj['publications']:
#             self.assertIsNotNone(publication['title'])
#             self.assertIsNotNone(publication['status'])
#             self.assertIsNotNone(publication['doi'])
#             self.assertIsNotNone(publication['authorList'])
#             self.assertIsNotNone(publication['pubMedID'])
#             self.assertIsNotNone(publication['comments'])
#
#     # Update Study Publications - Pub - Auth -> 200
#     def test_update_publications_pub_auth(self):
#         request = urllib.request.Request(url_pub_id + '/publications', data=self.data_new_publications, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('publications', body)
#             self.check_Publications_class(j_resp)
#
#     # Update Study Publications - Pub - NoAuth -> 403
#     def test_update_publications_pub_noAuth(self):
#         request = urllib.request.Request(url_pub_id + '/publications', data=self.data_new_publications, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Publications - Priv - Auth -> 200
#     def test_update_publications_priv_auth(self):
#         request = urllib.request.Request(url_priv_id + '/publications', data=self.data_new_publications, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('publications', body)
#             self.check_Publications_class(j_resp)
#
#     # Update Study Publications - Priv - NoAuth -> 403
#     def test_update_publications_priv_noAuth(self):
#         request = urllib.request.Request(url_priv_id + '/publications', data=self.data_new_publications, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Publications - NullId -> 404
#     def test_update_publications_nullId(self):
#         request = urllib.request.Request(url_null_id + '/publications', data=self.data_new_publications, method='PUT')
#         self.add_common_headers(request)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # Update Study Publications - BadId -> 404
#     def test_update_publications_badId(self):
#         request = urllib.request.Request(url_wrong_id + '/publications', data=self.data_new_publications, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # Update Study Publications - Pub - Auth - NoSave -> 200
#     def test_update_publications_pub_auth_noSave(self):
#         request = urllib.request.Request(url_pub_id + '/publications', data=self.data_new_publications, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         request.add_header('save_audit_copy', 'False')
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('publications', body)
#             self.check_Publications_class(j_resp)
#
#     # Update Study Publications - Pub - NoAuth - NoSave -> 403
#     def test_update_publications_pub_noAuth_noSave(self):
#         request = urllib.request.Request(url_pub_id + '/publications', data=self.data_new_publications, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Publications - Priv - Auth - NoSave -> 200
#     def test_update_publications_priv_auth_noSave(self):
#         request = urllib.request.Request(url_priv_id + '/publications', data=self.data_new_publications, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         request.add_header('save_audit_copy', 'False')
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('publications', body)
#             self.check_Publications_class(j_resp)
#
#     # Update Study Publications - Priv - NoAuth - NoSave -> 403
#     def test_update_publications_priv_noAuth_noSave(self):
#         request = urllib.request.Request(url_priv_id + '/publications', data=self.data_new_publications, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         request.add_header('save_audit_copy', 'False')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Publications - NullId - NoSave -> 404
#     def test_update_publications_nullId_noSave(self):
#         request = urllib.request.Request(url_null_id + '/publications', data=self.data_new_publications, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('save_audit_copy', 'False')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # Update Study Publications - BadId - NoSave -> 404
#     def test_update_publications_badId_noSave(self):
#         request = urllib.request.Request(url_wrong_id + '/publications', data=self.data_new_publications, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         request.add_header('save_audit_copy', 'False')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#
# class UpdateStudySourceTests(WsTests):
#
#     data_updated_pub_source = instance.config.TEST_DATA_PUB_SOURCE
#     data_updated_priv_source = instance.config.TEST_DATA_PRIV_SOURCE
#
#     def tearDown(self):
#         time.sleep(1)  # sleep time in seconds
#
#     def check_Source_class(self, obj):
#         self.assertIsNotNone(obj['source'])
#         source = obj['source']
#         self.assertIsNotNone(source['name'])
#         self.assertIsNotNone(source['characteristics'])
#         self.assertIsNotNone(source['comments'])
#
#     # Update Study Source - Pub - Auth -> 200
#     def test_update_source_pub_auth(self):
#         request = urllib.request.Request(url_pub_id + '/sources/' + public_source_id,
#                                          data=self.data_updated_pub_source,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('source', body)
#             self.check_Source_class(j_resp)
#
#     # Update Study Source - Pub - NoAuth -> 403
#     def test_update_source_pub_noAuth(self):
#         request = urllib.request.Request(url_pub_id + '/sources/' + public_source_id,
#                                          data=self.data_updated_pub_source,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Source - Priv - Auth -> 200
#     def test_update_source_priv_auth(self):
#         request = urllib.request.Request(url_priv_id + '/sources/' + private_source_id,
#                                          data=self.data_updated_priv_source,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('source', body)
#             self.check_Source_class(j_resp)
#
#     # Update Study Source - Priv - NoAuth -> 403
#     def test_update_source_priv_noAuth(self):
#         request = urllib.request.Request(url_priv_id + '/sources/' + private_source_id,
#                                          data=self.data_updated_priv_source,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Source - NullId -> 404
#     def test_update_source_nullId(self):
#         request = urllib.request.Request(url_null_id + '/sources/' + public_source_id,
#                                          data=self.data_updated_pub_source,
#                                          method='PUT')
#         self.add_common_headers(request)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # Update Study Source - BadId -> 404
#     def test_update_source_badId(self):
#         request = urllib.request.Request(url_wrong_id + '/sources/' + public_source_id,
#                                          data=self.data_updated_pub_source,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # Update Study Source - Pub - Auth - NoSave -> 200
#     def test_update_source_pub_auth_noSave(self):
#         request = urllib.request.Request(url_pub_id + '/sources/' + public_source_id,
#                                          data=self.data_updated_pub_source,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         request.add_header('save_audit_copy', 'False')
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('source', body)
#             self.check_Source_class(j_resp)
#
#     # Update Study Source - Pub - NoAuth - NoSave -> 403
#     def test_update_source_pub_noAuth_noSave(self):
#         request = urllib.request.Request(url_pub_id + '/sources/' + public_source_id,
#                                          data=self.data_updated_pub_source,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Source - Priv - Auth - NoSave -> 200
#     def test_update_source_priv_auth_noSave(self):
#         request = urllib.request.Request(url_priv_id + '/sources/' + private_source_id,
#                                          data=self.data_updated_priv_source,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         request.add_header('save_audit_copy', 'False')
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('source', body)
#             self.check_Source_class(j_resp)
#
#     # Update Study Source - Priv - NoAuth - NoSave -> 403
#     def test_update_source_priv_noAuth_noSave(self):
#         request = urllib.request.Request(url_priv_id + '/sources/' + private_source_id,
#                                          data=self.data_updated_priv_source,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         request.add_header('save_audit_copy', 'False')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Source - NullId - NoSave -> 404
#     def test_update_source_nullId_noSave(self):
#         request = urllib.request.Request(url_null_id + '/sources/' + public_source_id,
#                                          data=self.data_updated_pub_source,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('save_audit_copy', 'False')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # Update Study Source - BadId - NoSave -> 404
#     def test_update_source_badId_noSave(self):
#         request = urllib.request.Request(url_wrong_id + '/sources/' + public_source_id,
#                                          data=self.data_updated_pub_source,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         request.add_header('save_audit_copy', 'False')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#
# class UpdateStudySampleTests(WsTests):
#
#     data_updated_pub_sample = instance.config.TEST_DATA_PUB_SAMPLE
#     data_updated_priv_sample = instance.config.TEST_DATA_PRIV_SAMPLE
#
#     def tearDown(self):
#         time.sleep(1)  # sleep time in seconds
#
#     def check_Sample_class(self, obj):
#         self.assertIsNotNone(obj['sample'])
#         sample = obj['sample']
#         self.assertIsNotNone(sample['name'])
#         self.assertIsNotNone(sample['derives_from'])
#         self.assertIsNotNone(sample['characteristics'])
#         self.assertIsNotNone(sample['factorValues'])
#         self.assertIsNotNone(sample['comments'])
#
#     # Update Study Sample - Pub - Auth -> 200
#     def test_update_sample_pub_auth(self):
#         request = urllib.request.Request(url_pub_id + '/samples/' + public_sample_id,
#                                          data=self.data_updated_pub_sample,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('sample', body)
#             self.check_Sample_class(j_resp)
#
#     # Update Study Sample - Pub - NoAuth -> 403
#     def test_update_sample_pub_noAuth(self):
#         request = urllib.request.Request(url_pub_id + '/samples/' + public_sample_id,
#                                          data=self.data_updated_pub_sample,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Sample - Priv - Auth -> 200
#     def test_update_sample_priv_auth(self):
#         request = urllib.request.Request(url_priv_id + '/samples/' + private_sample_id,
#                                          data=self.data_updated_priv_sample,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('sample', body)
#             self.check_Sample_class(j_resp)
#
#     # Update Study Sample - Priv - NoAuth -> 403
#     def test_update_sample_priv_noAuth(self):
#         request = urllib.request.Request(url_priv_id + '/samples/' + private_sample_id,
#                                          data=self.data_updated_priv_sample,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Sample - NullId -> 404
#     def test_update_sample_nullId(self):
#         request = urllib.request.Request(url_null_id + '/samples/' + public_sample_id,
#                                          data=self.data_updated_pub_sample,
#                                          method='PUT')
#         self.add_common_headers(request)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # Update Study Sample - BadId -> 404
#     def test_update_sample_badId(self):
#         request = urllib.request.Request(url_wrong_id + '/samples/' + public_sample_id,
#                                          data=self.data_updated_pub_sample,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # Update Study Sample - Pub - Auth - NoSave -> 200
#     def test_update_sample_pub_auth_noSave(self):
#         request = urllib.request.Request(url_pub_id + '/samples/' + public_sample_id,
#                                          data=self.data_updated_pub_sample,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         request.add_header('save_audit_copy', 'False')
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('sample', body)
#             self.check_Sample_class(j_resp)
#
#     # Update Study Sample - Pub - NoAuth - NoSave -> 403
#     def test_update_sample_pub_noAuth_noSave(self):
#         request = urllib.request.Request(url_pub_id + '/samples/' + public_sample_id,
#                                          data=self.data_updated_pub_sample,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         request.add_header('save_audit_copy', 'False')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Sample - Priv - Auth - NoSave -> 200
#     def test_update_sample_priv_auth_noSave(self):
#         request = urllib.request.Request(url_priv_id + '/samples/' + private_sample_id,
#                                          data=self.data_updated_priv_sample,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         request.add_header('save_audit_copy', 'False')
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('sample', body)
#             self.check_Sample_class(j_resp)
#
#     # Update Study Sample - Priv - NoAuth - NoSave -> 403
#     def test_update_sample_priv_noAuth_noSave(self):
#         request = urllib.request.Request(url_priv_id + '/samples/' + private_sample_id,
#                                          data=self.data_updated_priv_sample,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', wrong_auth_token)
#         request.add_header('save_audit_copy', 'False')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 403)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('FORBIDDEN', err.msg)
#             self.assertEqual('FORBIDDEN', err.reason)
#
#     # Update Study Sample - NullId - NoSave -> 404
#     def test_update_sample_nullId_noSave(self):
#         request = urllib.request.Request(url_null_id + '/samples/' + public_sample_id,
#                                          data=self.data_updated_pub_sample,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('save_audit_copy', 'False')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # Update Study Sample - BadId - NoSave -> 404
#     def test_update_sample_badId_noSave(self):
#         request = urllib.request.Request(url_wrong_id + '/samples/' + public_sample_id,
#                                          data=self.data_updated_pub_sample,
#                                          method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         request.add_header('save_audit_copy', 'False')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)


if __name__ == '__main__':
    unittest.main()