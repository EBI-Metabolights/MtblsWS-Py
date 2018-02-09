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
valid_person_id = instance.config.VALID_PERSON_ID
bad_person_id = instance.config.BAD_PERSON_ID


class WsTests(unittest.TestCase):

    def add_common_headers(self, request):
        request.add_header('Accept', 'application/json')
        request.add_header('Content-Type', 'application/json')

    def check_header_common(self, header):
        self.assertIn('Access-Control-Allow-Origin', header)
        self.assertIn('Content-Type', header)

    def check_body_common(self, body):
        self.assertIsNotNone(body)


class AboutTests(WsTests):

    # GET About
    def test_request_about_OK(self):
        # assert self.app
        request = urllib.request.Request(url_about)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('AboutWS', body)
            self.assertIn('WsApp', body)
            self.assertIn('WsName', body)
            self.assertIn('WsURL', body)
            self.assertIn('WsVersion', body)
            self.assertIn('WsDescription', body)
            self.assertIn('WsApi', body)
            self.assertIn('ApiSpecification', body)
            self.assertIn('ApiVersion', body)
            self.assertIn('ApiDocumentation', body)


class GetStudiesTests(WsTests):

    # Get All Public Studies - Pub -> 200
    def test_get_studies(self):
        request = urllib.request.Request(url_base, method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('content', body)


class GetSingleStudyTests(WsTests):

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

    def check_Investigation_class(self, obj):
        self.assertIsNotNone(obj['investigation'])
        inv = obj['investigation']
        self.assertIsNotNone(inv['comments'])
        self.assertIsNotNone(inv['description'])
        # self.assertIsNotNone(inv['filename'])
        # self.assertIsNotNone(inv['id'])
        self.assertIsNotNone(inv['identifier'])
        # self.assertIsNotNone(inv['ontologySourceReferences'])
        # self.assertIsNotNone(inv['people'])
        # self.assertIsNotNone(inv['publicReleaseDate'])
        self.assertIsNotNone(inv['publications'])
        self.assertIsNotNone(inv['studies'])
        # self.assertIsNotNone(inv['submissionDate'])
        self.assertIsNotNone(inv['title'])

    # Get Study - Pub -> 200
    def test_get_study(self):
        request = urllib.request.Request(url_pub_id, method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('investigation', body)
            j_resp = json.loads(body)
            self.check_Investigation_class(j_resp)

    # Get Study - Pub - Auth -> 200
    def test_get_study_pub_auth(self):
        request = urllib.request.Request(url_pub_id, method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('investigation', body)
            j_resp = json.loads(body)
            self.check_Investigation_class(j_resp)

    # Get Study - Pub - NoAuth -> 200
    def test_get_study_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id, method='GET')
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('investigation', body)
            j_resp = json.loads(body)
            self.check_Investigation_class(j_resp)

    # Get Study - Priv - Auth -> 200
    def test_get_study_priv_auth(self):
        request = urllib.request.Request(url_priv_id, method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('investigation', body)
            j_resp = json.loads(body)
            self.check_Investigation_class(j_resp)

    # Get Study - Priv -> 401
    def test_get_study_priv(self):
        request = urllib.request.Request(url_priv_id, method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Get Study - Priv - NoAuth -> 403
    def test_get_study_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id, method='GET')
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # GET Study - NullId -> 404
    def test_get_study_nullId(self):
        request = urllib.request.Request(url_null_id, method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # GET Study - BadId -> 404
    def test_get_study_badId(self):
        request = urllib.request.Request(url_wrong_id, method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


class GetStudyTitleTests(WsTests):

    # GET Study Title - Pub -> 200
    def test_get_title_pub(self):
        request = urllib.request.Request(url_pub_id + '/title', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['title'])

    # GET Study Title - Pub - Auth -> 200
    def test_get_title_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/title', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['title'])

    # GET Study Title - Pub - NoAuth -> 200
    def test_get_title_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/title', method='GET')
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['title'])

    # GET Study Title - Priv - Auth -> 200
    def test_get_title_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/title', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['title'])

    # GET Study Title - Priv -> 401
    def test_get_title_priv(self):
        request = urllib.request.Request(url_priv_id + '/title', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # GET Study Title - Priv - NoAuth -> 403
    def test_get_title_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/title', method='GET')
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # GET Study Title - NullId -> 404
    def test_get_title_nullId(self):
        request = urllib.request.Request(url_null_id + '/title', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # GET Study Title - BadId -> 404
    def test_get_title_badId(self):
        request = urllib.request.Request(url_wrong_id + '/title', method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


class GetStudyDescriptionTests(WsTests):

    # GET Study Description - Pub -> 200
    def test_get_Description(self):
        request = urllib.request.Request(url_pub_id + '/description', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['description'])

    # GET Study Description - Pub - Auth -> 200
    def test_get_Description_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/description', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['description'])

    # GET Study Description - Pub - NoAuth -> 200
    def test_get_Description_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/description', method='GET')
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['description'])

    # GET Study Description - Priv - Auth -> 200
    def test_get_Description_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/description', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['description'])

    # GET Study Description - Priv -> 401
    def test_get_Description_priv(self):
        request = urllib.request.Request(url_priv_id + '/description', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # GET Study Description - Priv - NoAuth -> 403
    def test_get_Description_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/description', method='GET')
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # GET Study Description - NullId -> 404
    def test_get_Description_nullId(self):
        request = urllib.request.Request(url_null_id + '/description', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # GET Study Description - BadId -> 404
    def test_get_Description_badId(self):
        request = urllib.request.Request(url_wrong_id + '/description', method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


class GetStudyPeopleTests(WsTests):

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

    # Get Study People - Pub -> 200
    def test_get_People_pub(self):
        request = urllib.request.Request(url_pub_id + '/people', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('people', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['people'])
            for person in j_resp['people']:
                self.check_Person_class(person)

    # Get Study People - Pub - Auth -> 200
    def test_get_People_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/people', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('people', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['people'])
            for person in j_resp['people']:
                self.check_Person_class(person)

    # Get Study People - Pub - NoAuth -> 200
    def test_get_People_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/people', method='GET')
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('people', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['people'])
            for person in j_resp['people']:
                self.check_Person_class(person)

    # Get Study People - Priv - Auth -> 200
    def test_get_People_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/people', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('people', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['people'])
            for person in j_resp['people']:
                self.check_Person_class(person)

    # Get Study People - Priv -> 401
    def test_get_People_priv(self):
        request = urllib.request.Request(url_priv_id + '/people', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Get Study People - Priv - NoAuth -> 403
    def test_get_People_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/people', method='GET')
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # GET Study People - NullId -> 404
    def test_get_People_nullId(self):
        request = urllib.request.Request(url_null_id + '/people', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # GET Study People - BadId -> 404
    def test_get_People_badId(self):
        request = urllib.request.Request(url_wrong_id + '/people', method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


class GetStudyPersonTests(WsTests):

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

    # Get Study Person - Pub -> 200
    def test_get_Person_pub(self):
        request = urllib.request.Request(url_pub_id + '/people' + '/' + valid_person_id, method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('person', body)
            self.check_Person_class(j_resp['person'])

    # Get Study Person - Pub - BadPersonId -> 404
    def test_get_Person_pub_badPersonId(self):
        request = urllib.request.Request(url_pub_id + '/people' + '/' + bad_person_id, method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Person - Pub - NoAuth -> 200
    def test_get_Person_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/people' + '/' + valid_person_id, method='GET')
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('person', body)
            self.check_Person_class(j_resp['person'])

    # Get Study Person - Priv - Auth -> 200
    def test_get_Person_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/people' + '/' + valid_person_id, method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('person', body)
            self.check_Person_class(j_resp['person'])

    # Get Study Person - Priv - BadPersonId - Auth -> 404
    def test_get_Person_priv_badPersonId_auth(self):
        request = urllib.request.Request(url_priv_id + '/people' + '/' + bad_person_id, method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Person - Priv -> 401
    def test_get_Person_priv(self):
        request = urllib.request.Request(url_priv_id + '/people' + '/' + valid_person_id, method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Get Study Person - Priv - NoAuth -> 403
    def test_get_Person_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/people' + '/' + valid_person_id, method='GET')
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # GET Study Person - NullPersonId -> 404
    def test_get_Person_nullId(self):
        request = urllib.request.Request(url_pub_id + '/people' + '/', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


class GetStudyProtocolsTests(WsTests):

    def check_Protocols_class(self, obj):
        self.assertIsNotNone(obj['protocols'])
        for protocol in obj['protocols']:
            self.assertIsNotNone(protocol['name'])
            self.assertIsNotNone(protocol['protocolType'])
            self.assertIsNotNone(protocol['description'])
            self.assertIsNotNone(protocol['uri'])
            self.assertIsNotNone(protocol['version'])
            self.assertIsNotNone(protocol['parameters'])
            self.assertIsNotNone(protocol['components'])

    # Get Study Protocols - Pub -> 200
    def test_get_protocols(self):
        request = urllib.request.Request(url_pub_id + '/protocols', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('protocols', body)
            self.check_Protocols_class(j_resp)

    # Get Study Protocols - Pub - Auth -> 200
    def test_get_protocols_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/protocols', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('protocols', body)
            self.check_Protocols_class(j_resp)

    # Get Study Protocols - Pub - NoAuth -> 200
    def test_get_protocols_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/protocols', method='GET')
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('protocols', body)
            self.check_Protocols_class(j_resp)

    # Get Study Protocols - Priv - Auth -> 200
    def test_get_protocols_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/protocols', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('protocols', body)
            self.check_Protocols_class(j_resp)

    # Get Study Protocols - Priv -> 401
    def test_get_protocols_priv(self):
        request = urllib.request.Request(url_priv_id + '/protocols', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Get Study Protocols - Priv - NoAuth -> 403
    def test_get_protocols_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/protocols', method='GET')
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # GET Study Protocols - NullId -> 404
    def test_get_protocols_nullId(self):
        request = urllib.request.Request(url_null_id + '/protocols', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # GET Study Protocols - BadId -> 404
    def test_get_protocols_badId(self):
        request = urllib.request.Request(url_wrong_id + '/protocols', method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


class GetStudyFactorsTests(WsTests):

    def check_Factors_class(self, obj):
        self.assertIsNotNone(obj['factors'])
        for factor in obj['factors']:
            self.assertIsNotNone(factor['factorName'])
            self.assertIsNotNone(factor['factorType'])
            self.assertIsNotNone(factor['comments'])

    # Get Study Factors - Pub -> 200
    def test_get_factors(self):
        request = urllib.request.Request(url_pub_id + '/factors', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('factors', body)
            self.check_Factors_class(j_resp)

    # Get Study Factors - Pub - Auth -> 200
    def test_get_factors_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/factors', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('factors', body)
            self.check_Factors_class(j_resp)

    # Get Study Factors - Pub - NoAuth -> 200
    def test_get_factors_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/factors', method='GET')
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('factors', body)
            self.check_Factors_class(j_resp)

    # Get Study Factors - Priv - Auth -> 200
    def test_get_factors_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/factors', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('factors', body)
            self.check_Factors_class(j_resp)

    # Get Study Factors - Priv -> 401
    def test_get_factors_priv(self):
        request = urllib.request.Request(url_priv_id + '/factors', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Get Study Factors - Priv - NoAuth -> 403
    def test_get_factors_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/factors', method='GET')
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # GET Study Factors - NullId -> 404
    def test_get_factors_nullId(self):
        request = urllib.request.Request(url_null_id + '/factors', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # GET Study Factors - BadId -> 404
    def test_get_factors_badId(self):
        request = urllib.request.Request(url_wrong_id + '/factors', method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


class GetStudyDescriptorsTests(WsTests):

    def check_Descriptors_class(self, obj):
        self.assertIsNotNone(obj['descriptors'])
        for descriptor in obj['descriptors']:
            self.assertIsNotNone(descriptor['annotationValue'])
            self.assertIsNotNone(descriptor['termAccession'])
            self.assertIsNotNone(descriptor['termSource'])
            self.assertIsNotNone(descriptor['comments'])

    # Get Study Descriptors - Pub -> 200
    def test_get_descriptors(self):
        request = urllib.request.Request(url_pub_id + '/descriptors', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('descriptors', body)
            self.check_Descriptors_class(j_resp)

    # Get Study Descriptors - Pub - Auth -> 200
    def test_get_descriptors_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/descriptors', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('descriptors', body)
            self.check_Descriptors_class(j_resp)

    # Get Study Descriptors - Pub - NoAuth -> 200
    def test_get_descriptors_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/descriptors', method='GET')
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('descriptors', body)
            self.check_Descriptors_class(j_resp)

    # Get Study Descriptors - Priv - Auth -> 200
    def test_get_descriptors_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/descriptors', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('descriptors', body)
            self.check_Descriptors_class(j_resp)

    # Get Study Descriptors - Priv -> 401
    def test_get_descriptors_priv(self):
        request = urllib.request.Request(url_priv_id + '/descriptors', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Get Study Descriptors - Priv - NoAuth -> 403
    def test_get_descriptors_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/descriptors', method='GET')
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # GET Study Descriptors - NullId -> 404
    def test_get_descriptors_nullId(self):
        request = urllib.request.Request(url_null_id + '/descriptors', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # GET Study Descriptors - BadId -> 404
    def test_get_descriptors_badId(self):
        request = urllib.request.Request(url_wrong_id + '/descriptors', method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


class GetStudyPublicationsTests(WsTests):
    # tests for GET Method for Study Publications
    #
    # pubmed_id (str, NoneType):
    # doi (str, NoneType):
    # author_list (str, NoneType):
    # title (str, NoneType):
    # status (str, OntologyAnnotation, NoneType):
    # comments (list, Comment):

    def check_Publications_class(self, obj):
        self.assertIsNotNone(obj['publications'])
        for publication in obj['publications']:
            self.assertIsNotNone(publication['pubMedID'])
            self.assertIsNotNone(publication['doi'])
            self.assertIsNotNone(publication['authorList'])
            self.assertIsNotNone(publication['title'])
            self.assertIsNotNone(publication['status'])
            self.assertIsNotNone(publication['comments'])

    # Get Study Publications - Pub -> 200
    def test_get_descriptors(self):
        request = urllib.request.Request(url_pub_id + '/publications', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('publications', body)
            self.check_Publications_class(j_resp)

    # Get Study Publications - Pub - Auth -> 200
    def test_get_descriptors_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/publications', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('publications', body)
            self.check_Publications_class(j_resp)

    # Get Study Publications - Pub - NoAuth -> 200
    def test_get_descriptors_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/publications', method='GET')
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('publications', body)
            self.check_Publications_class(j_resp)

    # Get Study Publications - Priv - Auth -> 200
    def test_get_descriptors_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/publications', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('publications', body)
            self.check_Publications_class(j_resp)

    # Get Study Publications - Priv -> 401
    def test_get_descriptors_priv(self):
        request = urllib.request.Request(url_priv_id + '/publications', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Get Study Publications - Priv - NoAuth -> 403
    def test_get_descriptors_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/publications', method='GET')
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # GET Study Publications - NullId -> 404
    def test_get_descriptors_nullId(self):
        request = urllib.request.Request(url_null_id + '/publications', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # GET Study Publications - BadId -> 404
    def test_get_descriptors_badId(self):
        request = urllib.request.Request(url_wrong_id + '/publications', method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


class GetStudySourcesTests(WsTests):
    # tests for GET Method for list of sources names associated with the Study
    #
    # name (str):

    def check_Sources_class(self, obj):
        self.assertIsNotNone(obj['sources'])
        for sample in obj['sources']:
            self.assertIsNotNone(sample['name'])

    # Get Study list of Sources
    def test_get_sources(self):
        request = urllib.request.Request(url_pub_id + '/sources', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('sources', body)
            self.check_Sources_class(j_resp)


class GetStudySourceTests(WsTests):
    # tests for GET Method for Study Source, by name
    #
    # name (str):
    # characteristics (list, OntologyAnnotation):
    # comments (list, Comment):

    def check_Source_class(self, obj):
        self.assertIsNotNone(obj['source'])
        source = obj['source']
        self.assertIsNotNone(source['name'])
        self.assertIsNotNone(source['characteristics'])
        self.assertIsNotNone(source['comments'])

    # Get Study Source - Pub -> 200
    def test_get_source(self):
        request = urllib.request.Request(url_pub_id + '/sources/' + public_source_id, method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('source', body)
            self.check_Source_class(j_resp)

    # Get Study Source - Pub - Auth -> 200
    def test_get_source_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/sources/' + public_source_id, method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('source', body)
            self.check_Source_class(j_resp)

    # Get Study Source - Pub - NoAuth -> 200
    def test_get_source_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/sources/' + public_source_id, method='GET')
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('source', body)
            self.check_Source_class(j_resp)

    # Get Study Source - Priv - Auth -> 200
    def test_get_source_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/sources/' + private_source_id, method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('source', body)
            self.check_Source_class(j_resp)

    # Get Study Source - Priv -> 401
    def test_get_source_priv(self):
        request = urllib.request.Request(url_priv_id + '/sources/' + private_source_id, method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Get Study Source - Priv - NoAuth -> 403
    def test_get_source_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/sources/' + private_source_id, method='GET')
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # GET Study Source - NullId -> 404
    def test_get_source_nullId(self):
        request = urllib.request.Request(url_null_id + '/sources/' + public_source_id, method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # GET Study Source - BadId -> 404
    def test_get_source_badId(self):
        request = urllib.request.Request(url_wrong_id + '/sources/' + public_source_id, method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


class GetStudySamplesTests(WsTests):
    # tests for GET Method for list of samples names associated with the Study
    #
    # name (str):

    def check_Samples_class(self, obj):
        self.assertIsNotNone(obj['samples'])
        for sample in obj['samples']:
            self.assertIsNotNone(sample['name'])

    # Get Study list of Samples
    def test_get_samples(self):
        request = urllib.request.Request(url_pub_id + '/samples', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('samples', body)
            self.check_Samples_class(j_resp)


class GetStudySampleTests(WsTests):
    # tests for GET Method for Study Sample, by name
    #
    # name (str):
    # characteristics (list, OntologyAnnotation):
    # derives_from (Source):
    # factorValues (FactorValues):
    # comments (list, Comment):

    def check_Sample_class(self, obj):
        self.assertIsNotNone(obj['sample'])
        sample = obj['sample']
        self.assertIsNotNone(sample['name'])
        self.assertIsNotNone(sample['derives_from'])
        self.assertIsNotNone(sample['characteristics'])
        self.assertIsNotNone(sample['factorValues'])
        self.assertIsNotNone(sample['comments'])

    # Get Study Sample - Pub -> 200
    def test_get_sample(self):
        request = urllib.request.Request(url_pub_id + '/samples/' + public_sample_id, method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('sample', body)
            self.check_Sample_class(j_resp)

    # Get Study Sample - Pub - Auth -> 200
    def test_get_sample_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/samples/' + public_sample_id, method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('sample', body)
            self.check_Sample_class(j_resp)

    # Get Study Sample - Pub - NoAuth -> 200
    def test_get_sample_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/samples/' + public_sample_id, method='GET')
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('sample', body)
            self.check_Sample_class(j_resp)

    # Get Study Sample - Priv - Auth -> 200
    def test_get_sample_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/samples/' + private_sample_id, method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('sample', body)
            self.check_Sample_class(j_resp)

    # Get Study Samples - Priv -> 401
    def test_get_sample_priv(self):
        request = urllib.request.Request(url_priv_id + '/samples/' + private_sample_id, method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Get Study Sample - Priv - NoAuth -> 403
    def test_get_sample_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/samples/' + private_sample_id, method='GET')
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # GET Study Sample - NullId -> 404
    def test_get_sample_nullId(self):
        request = urllib.request.Request(url_null_id + '/samples/' + public_sample_id, method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # GET Study Sample - BadId -> 404
    def test_get_sample_badId(self):
        request = urllib.request.Request(url_wrong_id + '/samples/' + public_sample_id, method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


if __name__ == '__main__':
    unittest.main()
