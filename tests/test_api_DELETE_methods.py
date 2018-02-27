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
# All tests that DELETE data below this point
# ############################################


class DeleteStudyContactTests(WsTests):

    valid_contact = instance.config.TEST_DATA_CONTACT
    missingData_new_contact = instance.config.TEST_DATA_CONTACT_MISSING
    noData_new_contact = b''

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
                                         data=self.valid_contact, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            if err.code != 409:
                raise Exception(err)

    def pre_delete_contact(self, url):
        request = urllib.request.Request(url + '/contacts'
                                         + '?email=' + valid_contact_id,
                                         method='DELETE')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            if err.code != 404:
                raise Exception(err)

    # Delete Study Contact - Pub - Auth -> 200
    def test_delete_Contact_pub_auth(self):
        # first, create the contact to ensure it will exists
        self.pre_create_contact(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to delete the contact
        request = urllib.request.Request(url_pub_id + '/contacts'
                                         + '?email=' + valid_contact_id,
                                         method='DELETE')
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

    # Delete Study Contact - Pub - NoToken -> 401
    def test_delete_Contact_pub_auth_noToken(self):
        request = urllib.request.Request(url_pub_id + '/contacts'
                                         + '?email=' + valid_contact_id,
                                         method='DELETE')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Delete Study Contact - Pub - NoAuth -> 403
    def test_delete_Contact_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/contacts'
                                         + '?email=' + valid_contact_id,
                                         method='DELETE')
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

    # Delete Study Contact - Pub - Auth - NoParams -> 404
    def test_delete_Contact_pub_auth_noParams(self):
        request = urllib.request.Request(url_pub_id + '/contacts',
                                         method='DELETE')
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

    # Delete Study Contact - Pub - Auth - NoData -> 404
    def test_delete_Contact_pub_auth_noData(self):
        request = urllib.request.Request(url_pub_id + '/contacts'
                                         + '?email=',
                                         method='DELETE')
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

    # Delete Study Contact - Pub - Auth - unknownContact -> 404
    def test_delete_Contact_pub_auth_unknownContact(self):
        request = urllib.request.Request(url_pub_id + '/contacts'
                                         + '?email=' + bad_contact_id,
                                         method='DELETE')
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

    # Delete Study Contact - Priv - Auth - NewData -> 200
    def test_delete_Contact_priv_auth_newData(self):
        # first, create the contact to ensure it will exists
        self.pre_create_contact(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to delete the contact
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=' + valid_contact_id,
                                         method='DELETE')
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

    # Delete Study Contact - Priv - NoToken -> 401
    def test_delete_Contact_priv_auth_noToken(self):
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=' + valid_contact_id,
                                         method='DELETE')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Delete Study Contact - Priv - NoAuth -> 403
    def test_delete_Contact_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=' + valid_contact_id,
                                         method='DELETE')
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

    # Delete Study Contact - Priv - Auth - NoParams -> 404
    def test_delete_Contact_priv_auth_noParams(self):
        request = urllib.request.Request(url_priv_id + '/contacts',
                                         method='DELETE')
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

    # Delete Study Contact - Priv - Auth - NoData -> 404
    def test_delete_Contact_piv_auth_noData(self):
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=',
                                         method='DELETE')
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

    # Delete Study Contact - Priv - Auth - unknownContact -> 404
    def test_delete_Contact_priv_auth_unknownContact(self):
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=' + bad_contact_id,
                                         method='DELETE')
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

