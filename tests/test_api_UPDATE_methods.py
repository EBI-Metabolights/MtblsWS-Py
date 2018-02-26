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
valid_contact_id = instance.config.VALID_CONTACT_ID
bad_contact_id = instance.config.BAD_CONTACT_ID

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
# All tests that modify data below this point
# ############################################

class PostSingleStudyTests(WsTests):
    """
    This is actually an ISA-JSON Investigation object
    that contains a list of studies, samples and assays
    """
    data_new_study = instance.config.TEST_DATA_NEW_STUDY

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

    def check_Investigation_class(self, obj):
        self.assertIsNotNone(obj['investigation'])
        inv = obj['investigation']
        self.assertIsNotNone(inv['comments'])
        self.assertIsNotNone(inv['description'])
        self.assertIsNotNone(inv['filename'])
        self.assertIsNotNone(inv['id'])
        self.assertIsNotNone(inv['identifier'])
        # self.assertIsNotNone(inv['ontologySourceReferences'])
        # self.assertIsNotNone(inv['people'])
        self.assertIsNotNone(inv['publicReleaseDate'])
        self.assertIsNotNone(inv['publications'])
        self.assertIsNotNone(inv['studies'])
        self.assertIsNotNone(inv['submissionDate'])
        self.assertIsNotNone(inv['title'])

    # POST New Study - Auth -> 201
    def test_post_new_study_auth(self):
        request = urllib.request.Request(instance.config.TEST_URL_BASE, data=self.data_new_study, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 201)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('investigation', body)
            self.check_Investigation_class(j_resp)

    # New Study - Null Req -> 400
    def test_post_new_study_nullReq(self):
        request = urllib.request.Request(instance.config.TEST_URL_BASE, data=b'', method='POST')
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

    # New Study - Bad req -> 400
    def test_post_new_study_badReq(self):
        request = urllib.request.Request(instance.config.TEST_URL_BASE,
                                         data=b'{"title": "Study title"}',
                                         method='POST')
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

    # New Study - NoUser -> 401
    def test_post_new_study_noUser(self):
        request = urllib.request.Request(instance.config.TEST_URL_BASE, data=self.data_new_study, method='POST')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # New Study - NoAuth -> 403
    def test_post_new_study_noAuth(self):
        request = urllib.request.Request(instance.config.TEST_URL_BASE, data=self.data_new_study, method='POST')
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

    data_new_contact = instance.config.TEST_DATA_CONTACT

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

    # Update Study Contact - Pub - Auth - GoodId -> 200
    def test_update_Contact_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/contacts'
                                         + '?email=' + valid_contact_id,
                                         data=self.data_new_contact, method='PUT')
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

    # Update Study Contact - Pub - Auth - BadId -> 404
    def test_update_Contact_pub_auth_badId(self):
        request = urllib.request.Request(url_pub_id + '/contacts'
                                         + '?email=' + bad_contact_id,
                                         data=self.data_new_contact, method='PUT')
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
                                         data=self.data_new_contact, method='PUT')
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

    # Update Study Contact - Pub - NoAuth -> 403
    def test_update_Contact_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/contacts'
                                         + '?email=' + valid_contact_id,
                                         data=self.data_new_contact, method='PUT')
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

    # Update Study Contact - Priv - Auth - GoodId -> 200
    def test_update_Contact_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=' + valid_contact_id,
                                         data=self.data_new_contact, method='PUT'
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

    # Update Study Contact - Priv - Auth - BadId -> 404
    def test_update_Contact_priv_auth_badId(self):
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=' + bad_contact_id,
                                         data=self.data_new_contact, method='PUT')
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
                                         data=self.data_new_contact, method='PUT')
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

    # Update Study Contact - Priv -> 403
    def test_update_Contact_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=',
                                         data=self.data_new_contact, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Update Study Contact - Priv - NoAuth -> 403
    def test_update_Contact_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=',
                                         data=self.data_new_contact, method='PUT')
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

#
# class UpdateStudyProtocolsTests(WsTests):
#
#     data_new_protocol = instance.config.TEST_DATA_PROTOCOLS
#
#     def tearDown(self):
#         time.sleep(1)  # sleep time in seconds
#
#     def check_Protocols_class(self, obj):
#         self.assertIsNotNone(obj['protocols'])
#         for protocol in obj['protocols']:
#             self.assertIsNotNone(protocol['name'])
#             self.assertIsNotNone(protocol['protocolType'])
#             self.assertIsNotNone(protocol['description'])
#             self.assertIsNotNone(protocol['uri'])
#             self.assertIsNotNone(protocol['version'])
#             self.assertIsNotNone(protocol['parameters'])
#             self.assertIsNotNone(protocol['components'])
#
#     # Update Study Protocols - Pub - Auth -> 200
#     def test_update_protocols_pub_auth(self):
#         request = urllib.request.Request(url_pub_id + '/protocols', data=self.data_new_protocol, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('protocols', body)
#             self.check_Protocols_class(j_resp)
#             self.assertIn('Updated with MtblsWs-Py', body)
#
#     # Update Study Protocols - Pub - NoAuth -> 403
#     def test_update_protocols_pub_noAuth(self):
#         request = urllib.request.Request(url_pub_id + '/protocols', data=self.data_new_protocol, method='PUT')
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
#     # Update Study Protocols - Priv - Auth -> 200
#     def test_update_protocols_priv_auth(self):
#         request = urllib.request.Request(url_priv_id + '/protocols', data=self.data_new_protocol, method='PUT')
#         self.add_common_headers(request)
#         request.add_header('user_token', auth_id)
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('protocols', body)
#             self.check_Protocols_class(j_resp)
#             self.assertIn('Updated with MtblsWs-Py', body)
#
#     # Update Study Protocols - Priv - NoAuth -> 403
#     def test_update_protocols_priv_noAuth(self):
#         request = urllib.request.Request(url_priv_id + '/protocols', data=self.data_new_protocol, method='PUT')
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
#     # Update Study Protocols - NullId -> 404
#     def test_update_protocols_nullId(self):
#         request = urllib.request.Request(url_null_id + '/protocols', data=self.data_new_protocol, method='PUT')
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
#     # Update Study Protocols - BadId -> 404
#     def test_update_protocols_badId(self):
#         request = urllib.request.Request(url_wrong_id + '/protocols', data=self.data_new_protocol, method='PUT')
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
#     # Update Study Protocols - Pub - Auth - NoSave -> 200
#     def test_update_protocols_pub_auth_noSave(self):
#         request = urllib.request.Request(url_pub_id + '/protocols', data=self.data_new_protocol, method='PUT')
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
#             self.assertIn('protocols', body)
#             self.check_Protocols_class(j_resp)
#             self.assertIn('Updated with MtblsWs-Py', body)
#
#     # Update Study Protocols - Pub - NoAuth - NoSave -> 403
#     def test_update_protocols_pub_noAuth_noSave(self):
#         request = urllib.request.Request(url_pub_id + '/protocols', data=self.data_new_protocol, method='PUT')
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
#     # Update Study Protocols - Priv - Auth - NoSave -> 200
#     def test_update_protocols_priv_auth_noSave(self):
#         request = urllib.request.Request(url_priv_id + '/protocols', data=self.data_new_protocol, method='PUT')
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
#             self.assertIn('protocols', body)
#             self.check_Protocols_class(j_resp)
#             self.assertIn('Updated with MtblsWs-Py', body)
#
#     # Update Study Protocols - Priv - NoAuth - NoSave -> 403
#     def test_update_protocols_priv_noAuth_noSave(self):
#         request = urllib.request.Request(url_priv_id + '/protocols', data=self.data_new_protocol, method='PUT')
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
#     # Update Study Protocols - NullId - NoSave -> 404
#     def test_update_protocols_nullId_noSave(self):
#         request = urllib.request.Request(url_null_id + '/protocols', data=self.data_new_protocol, method='PUT')
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
#     # Update Study Protocols - BadId - NoSave -> 404
#     def test_update_protocols_badId_noSave(self):
#         request = urllib.request.Request(url_wrong_id + '/protocols', data=self.data_new_protocol, method='PUT')
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