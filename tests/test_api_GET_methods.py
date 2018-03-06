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

    def check_OntologySource_class(self, obj):
        self.assertIsNotNone(obj['name'])
        self.assertIsNotNone(obj['description'])
        self.assertIsNotNone(obj['file'])
        self.assertIsNotNone(obj['version'])

    def check_OntologyAnnotation_class(self, obj):
        self.assertIsNotNone(obj['annotationValue'])
        if obj['termSource']:
            self.check_OntologySource_class(obj['termSource'])
        self.assertIsNotNone(obj['termAccession'])
        self.assertIsNotNone(obj['comments'])


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

    # Get Study - Priv -> 403
    def test_get_study_priv(self):
        request = urllib.request.Request(url_priv_id, method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

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

    # GET Study Title - Priv -> 403
    def test_get_title_priv(self):
        request = urllib.request.Request(url_priv_id + '/title', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

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

    # GET Study Description - Priv -> 403
    def test_get_Description_priv(self):
        request = urllib.request.Request(url_priv_id + '/description', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

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


class GetStudyContactsTests(WsTests):

    valid_id = instance.config.VALID_ID_CONTACT
    bad_id = instance.config.BAD_ID_CONTACT
    valid_data = instance.config.TEST_DATA_VALID_CONTACT
    missing_data = instance.config.TEST_DATA_MISSING_CONTACT
    no_data = b''

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
            self.check_OntologyAnnotation_class(role)

    # Get Study Contacts - Pub -> 200
    def test_get_Contacts_pub(self):
        request = urllib.request.Request(url_pub_id + '/contacts', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('contacts', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['contacts'])
            for person in j_resp['contacts']:
                self.check_Person_class(person)

    # Get Study Contacts - Pub - Auth -> 200
    def test_get_Contacts_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/contacts', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('contacts', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['contacts'])
            for person in j_resp['contacts']:
                self.check_Person_class(person)

    # Get Study Contacts - Pub - NoAuth -> 200
    def test_get_Contacts_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/contacts', method='GET')
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('contacts', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['contacts'])
            for person in j_resp['contacts']:
                self.check_Person_class(person)

    # Get Study Contacts - Priv -> 403
    def test_get_Contacts_priv(self):
        request = urllib.request.Request(url_priv_id + '/contacts', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Get Study Contacts - Priv - Auth -> 200
    def test_get_Contacts_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/contacts', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('contacts', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['contacts'])
            for person in j_resp['contacts']:
                self.check_Person_class(person)

    # Get Study Contacts - Priv - NoAuth -> 403
    def test_get_Contacts_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/contacts', method='GET')
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # GET Study Contacts - NullId -> 404
    def test_get_Contacts_nullId(self):
        request = urllib.request.Request(url_null_id + '/contacts', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # GET Study Contacts - BadId -> 404
    def test_get_Contacts_badId(self):
        request = urllib.request.Request(url_wrong_id + '/contacts', method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


class GetStudyContactTests(WsTests):

    valid_id = instance.config.VALID_ID_CONTACT
    bad_id = instance.config.BAD_ID_CONTACT
    valid_data = instance.config.TEST_DATA_VALID_CONTACT
    missing_data = instance.config.TEST_DATA_MISSING_CONTACT
    no_data = b''

    def check_Person_class(self, obj):
        self.assertIsNotNone(obj['firstName'])
        # self.assertIsNotNone(person['midInitials'])
        self.assertIsNotNone(obj['lastName'])
        self.assertIsNotNone(obj['email'])
        self.assertIsNotNone(obj['affiliation'])
        self.assertIsNotNone(obj['phone'])
        self.assertIsNotNone(obj['address'])
        self.assertIsNotNone(obj['fax'])
        for role in obj['roles']:
            self.check_OntologyAnnotation_class(role)
        self.assertIsNotNone(obj['comments'])

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

    # Get Study Contact - Pub - ContacId -> 200
    def test_get_Contacts_pub(self):
        # first, create the contact to ensure it will exists
        self.pre_create_contact(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to get the contact
        request = urllib.request.Request(url_pub_id + '/contacts'
                                         + '?email=' + self.valid_id,
                                         method='GET')
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

    # Get Study Contact - Pub - BadContacId -> 404
    def test_get_Contact_pub_badId(self):
        request = urllib.request.Request(url_wrong_id + '/contacts'
                                         + '?email=' + self.bad_id,
                                         method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Contact - Pub - NullId -> 404
    def test_get_Contact_pub_nullId(self):
        request = urllib.request.Request(url_null_id + '/contacts'
                                         + '?email=',
                                         method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Contact - Priv - Auth - ContactId -> 200
    def test_get_Contact_priv_auth(self):
        # first, create the contact to ensure it will exists
        self.pre_create_contact(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to get the contact
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=' + self.valid_id,
                                         method='GET')
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

    # Get Study Contact - Priv - Auth - BadContacId -> 404
    def test_get_Contact_priv_Auth_badId(self):
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=' + self.bad_id,
                                         method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Contact - Priv - Auth - NullId -> 404
    def test_get_Contact_priv_Auth_nullId(self):
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=',
                                         method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study People - Priv -> 403
    def test_get_Contact_priv(self):
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=' + self.valid_id,
                                         method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Get Study Contact - Priv - NoAuth -> 403
    def test_get_Contact_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=' + self.valid_id,
                                         method='GET')
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)


class GetStudyProtocolsTests(WsTests):

    valid_id = instance.config.VALID_ID_PROTOCOL
    bad_id = instance.config.BAD_ID_PROTOCOL
    valid_data = instance.config.TEST_DATA_VALID_PROTOCOL
    missing_data = instance.config.TEST_DATA_MISSING_PROTOCOL
    no_data = b''

    def check_ProtocolParameter_class(self, obj):
        if obj['parameterName']:
            self.check_OntologyAnnotation_class(obj['parameterName'])
        self.assertIsNotNone(obj['comments'])

    def check_Protocol_class(self, obj):
        self.assertIsNotNone(obj['name'])
        if obj['protocolType']:
            self.check_OntologyAnnotation_class(obj['protocolType'])
        self.assertIsNotNone(obj['description'])
        self.assertIsNotNone(obj['uri'])
        self.assertIsNotNone(obj['version'])
        for param in obj['parameters']:
            self.check_ProtocolParameter_class(param)
        if obj['components']:
            self.check_OntologyAnnotation_class(obj['components'])
        self.assertIsNotNone(obj['comments'])

    # Get Study Protocols - Pub -> 200
    def test_get_protocols(self):
        request = urllib.request.Request(url_pub_id + '/protocols', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('protocols', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['protocols'])
            for protocol in j_resp['protocols']:
                self.check_Protocol_class(protocol)

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
            self.assertIn('protocols', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['protocols'])
            for protocol in j_resp['protocols']:
                self.check_Protocol_class(protocol)

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
            self.assertIn('protocols', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['protocols'])
            for protocol in j_resp['protocols']:
                self.check_Protocol_class(protocol)

    # Get Study Protocols - Priv -> 403
    def test_get_protocols_priv(self):
        request = urllib.request.Request(url_priv_id + '/protocols', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

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
            self.assertIn('protocols', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['protocols'])
            for protocol in j_resp['protocols']:
                self.check_Protocol_class(protocol)

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


class GetStudyProtocolTests(WsTests):

    valid_id = instance.config.VALID_ID_PROTOCOL
    bad_id = instance.config.BAD_ID_PROTOCOL
    valid_data = instance.config.TEST_DATA_VALID_PROTOCOL
    missing_data = instance.config.TEST_DATA_MISSING_PROTOCOL
    no_data = b''

    def check_ProtocolParameter_class(self, obj):
        if obj['parameterName']:
            self.check_OntologyAnnotation_class(obj['parameterName'])
        self.assertIsNotNone(obj['comments'])

    def check_Protocol_class(self, obj):
        self.assertIsNotNone(obj['name'])
        if obj['protocolType']:
            self.check_OntologyAnnotation_class(obj['protocolType'])
        self.assertIsNotNone(obj['description'])
        self.assertIsNotNone(obj['uri'])
        self.assertIsNotNone(obj['version'])
        for param in obj['parameters']:
            self.check_ProtocolParameter_class(param)
        if obj['components']:
            self.check_OntologyAnnotation_class(obj['components'])
        self.assertIsNotNone(obj['comments'])

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

    # Get Study Protocol - Pub - ProtocolId -> 200
    def test_get_protocols_pub(self):
        # first, create the protocol to ensure it will exists
        self.pre_create_protocol(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to get the protocol
        request = urllib.request.Request(url_pub_id + '/protocols'
                                         + '?name=' + self.valid_id,
                                         method='GET')
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

    # Get Study Protocol - Pub - BadProtocolId -> 404
    def test_get_protocol_pub_badId(self):
        request = urllib.request.Request(url_wrong_id + '/protocols'
                                         + '?name=' + self.valid_id,
                                         method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Protocol - Pub - NullId -> 404
    def test_get_protocol_pub_nullId(self):
        request = urllib.request.Request(url_null_id + '/protocols'
                                         + '?name=',
                                         method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Protocol - Priv - Auth - ProtocolId -> 200
    def test_get_protocol_priv_auth(self):
        # first, create the Protocol to ensure it will exists
        self.pre_create_protocol(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to get the Protocol
        request = urllib.request.Request(url_priv_id + '/protocols'
                                         + '?name=' + self.valid_id,
                                         method='GET')
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

    # Get Study Protocol - Priv - Auth - BadProtocolId -> 404
    def test_get_protocol_priv_Auth_badId(self):
        request = urllib.request.Request(url_priv_id + '/protocols'
                                         + '?name=' + self.valid_id,
                                         method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Protocol - Priv - Auth - NullId -> 404
    def test_get_protocol_priv_Auth_nullId(self):
        request = urllib.request.Request(url_priv_id + '/protocols'
                                         + '?name=',
                                         method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Protocol - Priv -> 403
    def test_get_protocol_priv(self):
        request = urllib.request.Request(url_priv_id + '/protocols'
                                         + '?name=' + self.valid_id,
                                         method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Get Study Protocol - Priv - NoAuth -> 403
    def test_get_protocol_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/protocols'
                                         + '?name=' + self.valid_id,
                                         method='GET')
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)


class GetStudyFactorsTests(WsTests):

    valid_id = instance.config.VALID_ID_FACTOR
    bad_id = instance.config.BAD_ID_FACTOR
    valid_data = instance.config.TEST_DATA_VALID_FACTOR
    missing_data = instance.config.TEST_DATA_MISSING_FACTOR
    no_data = b''

    def check_StudyFactor_class(self, obj):
        self.assertIsNotNone(obj['factorName'])
        if obj['factorType']:
            self.check_OntologyAnnotation_class(obj['factorType'])
        self.assertIsNotNone(obj['comments'])

    # Get Study Factors - Pub -> 200
    def test_get_factors(self):
        request = urllib.request.Request(url_pub_id + '/factors', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('factors', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['factors'])
            for protocol in j_resp['factors']:
                self.check_StudyFactor_class(protocol)

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
            self.assertIn('factors', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['factors'])
            for protocol in j_resp['factors']:
                self.check_StudyFactor_class(protocol)

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
            self.assertIn('factors', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['factors'])
            for protocol in j_resp['factors']:
                self.check_StudyFactor_class(protocol)

    # Get Study Factors - Priv -> 403
    def test_get_factors_priv(self):
        request = urllib.request.Request(url_priv_id + '/factors', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

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
            self.assertIn('factors', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['factors'])
            for protocol in j_resp['factors']:
                self.check_StudyFactor_class(protocol)

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


class GetStudyFactorTests(WsTests):

    valid_id = instance.config.VALID_ID_FACTOR
    bad_id = instance.config.BAD_ID_FACTOR
    valid_data = instance.config.TEST_DATA_VALID_FACTOR
    missing_data = instance.config.TEST_DATA_MISSING_FACTOR
    no_data = b''

    def check_StudyFactor_class(self, obj):
        self.assertIsNotNone(obj['factorName'])
        if obj['factorType']:
            self.check_OntologyAnnotation_class(obj['factorType'])
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

    # Get Study Factor - Pub - FactorId -> 200
    def test_get_factors_pub(self):
        # first, create the factor to ensure it will exists
        self.pre_create_factor(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to get the factor
        request = urllib.request.Request(url_pub_id + '/factors'
                                         + '?name=' + self.valid_id,
                                         method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('factor', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['factor'])
            self.check_StudyFactor_class(j_resp['factor'])

    # Get Study Factor - Pub - BadFactorId -> 404
    def test_get_factor_pub_badId(self):
        request = urllib.request.Request(url_wrong_id + '/factors'
                                         + '?name=' + self.valid_id,
                                         method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Factor - Pub - NullId -> 404
    def test_get_factor_pub_nullId(self):
        request = urllib.request.Request(url_null_id + '/factors'
                                         + '?name=',
                                         method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Factor - Priv - Auth - FactorId -> 200
    def test_get_factor_priv_auth(self):
        # first, create the Factor to ensure it will exists
        self.pre_create_factor(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to get the Factor
        request = urllib.request.Request(url_priv_id + '/factors'
                                         + '?name=' + self.valid_id,
                                         method='GET')
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
            self.check_StudyFactor_class(j_resp['factor'])

    # Get Study Factor - Priv - Auth - BadFactorId -> 404
    def test_get_factor_priv_Auth_badId(self):
        request = urllib.request.Request(url_priv_id + '/factors'
                                         + '?name=' + self.valid_id,
                                         method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Factor - Priv - Auth - NullId -> 404
    def test_get_factor_priv_Auth_nullId(self):
        request = urllib.request.Request(url_priv_id + '/factors'
                                         + '?name=',
                                         method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Factor - Priv -> 403
    def test_get_factor_priv(self):
        request = urllib.request.Request(url_priv_id + '/factors'
                                         + '?name=' + self.valid_id,
                                         method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Get Study Factor - Priv - NoAuth -> 403
    def test_get_factor_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/factors'
                                         + '?name=' + self.valid_id,
                                         method='GET')
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)


class GetStudyDesignDescriptorsTests(WsTests):

    valid_id = instance.config.VALID_ID_DESCRIPTOR
    bad_id = instance.config.BAD_ID_DESCRIPTOR
    valid_data = instance.config.TEST_DATA_VALID_DESCRIPTOR
    missing_data = instance.config.TEST_DATA_MISSING_DESCRIPTOR
    no_data = b''

    # Get Study Design Descriptors - Pub -> 200
    def test_get_descriptors(self):
        request = urllib.request.Request(url_pub_id + '/descriptors', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('studyDesignDescriptors', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['studyDesignDescriptors'])
            for descriptor in j_resp['studyDesignDescriptors']:
                self.check_OntologyAnnotation_class(descriptor)

    # Get Study Design Descriptors - Pub - Auth -> 200
    def test_get_descriptors_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/descriptors', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('studyDesignDescriptors', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['studyDesignDescriptors'])
            for descriptor in j_resp['studyDesignDescriptors']:
                self.check_OntologyAnnotation_class(descriptor)

    # Get Study Design Descriptors - Pub - NoAuth -> 200
    def test_get_descriptors_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/descriptors', method='GET')
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('studyDesignDescriptors', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['studyDesignDescriptors'])
            for descriptor in j_resp['studyDesignDescriptors']:
                self.check_OntologyAnnotation_class(descriptor)

    # Get Study Design Descriptors - Priv -> 403
    def test_get_descriptors_priv(self):
        request = urllib.request.Request(url_priv_id + '/descriptors', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Get Study Design Descriptors - Priv - Auth -> 200
    def test_get_descriptors_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/descriptors', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('studyDesignDescriptors', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['studyDesignDescriptors'])
            for descriptor in j_resp['studyDesignDescriptors']:
                self.check_OntologyAnnotation_class(descriptor)

    # Get Study Design Descriptors - Priv - NoAuth -> 403
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

    # GET Study Design Descriptors - NullId -> 404
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

    # GET Study Design Descriptors - BadId -> 404
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


class GetStudyDesignDescriptorTests(WsTests):

    valid_id = instance.config.VALID_ID_DESCRIPTOR
    bad_id = instance.config.BAD_ID_DESCRIPTOR
    valid_data = instance.config.TEST_DATA_VALID_DESCRIPTOR
    missing_data = instance.config.TEST_DATA_MISSING_DESCRIPTOR
    no_data = b''

    def pre_create_descriptor(self, url):
        request = urllib.request.Request(url + '/descriptors',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            if err.code != 409:
                raise Exception(err)

    # Get Study Design Descriptor - Pub - FactorId -> 200
    def test_get_descriptors_pub(self):
        # first, create the descriptor to ensure it will exists
        self.pre_create_descriptor(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to get the descriptor
        request = urllib.request.Request(url_pub_id + '/descriptors'
                                         + '?annotationValue=' + self.valid_id,
                                         method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('studyDesignDescriptor', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['studyDesignDescriptor'])
            self.check_OntologyAnnotation_class(j_resp['studyDesignDescriptor'])

    # Get Study Design Descriptor - Pub - BadFactorId -> 404
    def test_get_descriptor_pub_badId(self):
        request = urllib.request.Request(url_wrong_id + '/descriptors'
                                         + '?annotationValue=' + self.valid_id,
                                         method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Design Descriptor - Pub - NullId -> 404
    def test_get_descriptor_pub_nullId(self):
        request = urllib.request.Request(url_null_id + '/descriptors'
                                         + '?annotationValue=',
                                         method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Design Descriptor - Priv - Auth - FactorId -> 200
    def test_get_descriptor_priv_auth(self):
        # first, create the DesignDescriptor to ensure it will exists
        self.pre_create_descriptor(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to get the DesignDescriptor
        request = urllib.request.Request(url_priv_id + '/descriptors'
                                         + '?annotationValue=' + self.valid_id,
                                         method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('studyDesignDescriptor', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['studyDesignDescriptor'])
            self.check_OntologyAnnotation_class(j_resp['studyDesignDescriptor'])

    # Get Study Design Descriptor - Priv - Auth - BadFactorId -> 404
    def test_get_descriptor_priv_Auth_badId(self):
        request = urllib.request.Request(url_priv_id + '/descriptors'
                                         + '?annotationValue=' + self.valid_id,
                                         method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Design Descriptor - Priv - Auth - NullId -> 404
    def test_get_descriptor_priv_Auth_nullId(self):
        request = urllib.request.Request(url_priv_id + '/descriptors'
                                         + '?annotationValue=',
                                         method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Design Descriptor - Priv -> 403
    def test_get_descriptor_priv(self):
        request = urllib.request.Request(url_priv_id + '/descriptors'
                                         + '?annotationValue=' + self.valid_id,
                                         method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Get Study Design Descriptor - Priv - NoAuth -> 403
    def test_get_descriptor_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/descriptors'
                                         + '?annotationValue=' + self.valid_id,
                                         method='GET')
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)


class GetStudyPublicationsTests(WsTests):

    valid_id = instance.config.VALID_ID_PUBLICATION
    bad_id = instance.config.BAD_ID_PUBLICATION
    valid_data = instance.config.TEST_DATA_VALID_PUBLICATION
    missing_data = instance.config.TEST_DATA_MISSING_PUBLICATION
    no_data = b''

    def check_Publication_class(self, obj):
        self.assertIsNotNone(obj['title'])
        self.assertIsNotNone(obj['authorList'])
        self.assertIsNotNone(obj['pubMedID'])
        self.assertIsNotNone(obj['doi'])
        if obj['status']:
            self.check_OntologyAnnotation_class(obj['status'])
        self.assertIsNotNone(obj['comments'])

    # Get Study Publications - Pub -> 200
    def test_get_publications(self):
        request = urllib.request.Request(url_pub_id + '/publications', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('publications', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['publications'])
            for protocol in j_resp['publications']:
                self.check_Publication_class(protocol)

    # Get Study Publications - Pub - Auth -> 200
    def test_get_publications_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/publications', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('publications', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['publications'])
            for protocol in j_resp['publications']:
                self.check_Publication_class(protocol)

    # Get Study Publications - Pub - NoAuth -> 200
    def test_get_publications_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/publications', method='GET')
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('publications', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['publications'])
            for protocol in j_resp['publications']:
                self.check_Publication_class(protocol)

    # Get Study Publications - Priv -> 403
    def test_get_publications_priv(self):
        request = urllib.request.Request(url_priv_id + '/publications', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Get Study Publications - Priv - Auth -> 200
    def test_get_publications_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/publications', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('publications', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['publications'])
            for protocol in j_resp['publications']:
                self.check_Publication_class(protocol)

    # Get Study Publications - Priv - NoAuth -> 403
    def test_get_publications_priv_noAuth(self):
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
    def test_get_publications_nullId(self):
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
    def test_get_publications_badId(self):
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


class GetStudyPublicationTests(WsTests):

    valid_id = instance.config.VALID_ID_PUBLICATION
    bad_id = instance.config.BAD_ID_PUBLICATION
    valid_data = instance.config.TEST_DATA_VALID_PUBLICATION
    missing_data = instance.config.TEST_DATA_MISSING_PUBLICATION
    no_data = b''

    def check_Publication_class(self, obj):
        self.assertIsNotNone(obj['title'])
        self.assertIsNotNone(obj['authorList'])
        self.assertIsNotNone(obj['pubMedID'])
        self.assertIsNotNone(obj['doi'])
        if obj['status']:
            self.check_OntologyAnnotation_class(obj['status'])
        self.assertIsNotNone(obj['comments'])

    def pre_create_publication(self, url):
        request = urllib.request.Request(url + '/publications',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            if err.code != 409:
                raise Exception(err)

    # Get Study Design Descriptor - Pub - FactorId -> 200
    def test_get_publications_pub(self):
        # first, create the publication to ensure it will exists
        self.pre_create_publication(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to get the publication
        request = urllib.request.Request(url_pub_id + '/publications'
                                         + '?title=' + self.valid_id,
                                         method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('publication', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['publication'])
            self.check_Publication_class(j_resp['publication'])

    # Get Study Design Descriptor - Pub - BadFactorId -> 404
    def test_get_publication_pub_badId(self):
        request = urllib.request.Request(url_wrong_id + '/publications'
                                         + '?title=' + self.valid_id,
                                         method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Design Descriptor - Pub - NullId -> 404
    def test_get_publication_pub_nullId(self):
        request = urllib.request.Request(url_null_id + '/publications'
                                         + '?title=',
                                         method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Design Descriptor - Priv - Auth - FactorId -> 200
    def test_get_publication_priv_auth(self):
        # first, create the Publication to ensure it will exists
        self.pre_create_publication(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to get the Publication
        request = urllib.request.Request(url_priv_id + '/publications'
                                         + '?title=' + self.valid_id,
                                         method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('publication', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['publication'])
            self.check_Publication_class(j_resp['publication'])

    # Get Study Design Descriptor - Priv - Auth - BadFactorId -> 404
    def test_get_publication_priv_Auth_badId(self):
        request = urllib.request.Request(url_priv_id + '/publications'
                                         + '?title=' + self.valid_id,
                                         method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Design Descriptor - Priv - Auth - NullId -> 404
    def test_get_publication_priv_Auth_nullId(self):
        request = urllib.request.Request(url_priv_id + '/publications'
                                         + '?title=',
                                         method='GET')
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Get Study Design Descriptor - Priv -> 403
    def test_get_publication_priv(self):
        request = urllib.request.Request(url_priv_id + '/publications'
                                         + '?title=' + self.valid_id,
                                         method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)

    # Get Study Design Descriptor - Priv - NoAuth -> 403
    def test_get_publication_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/publications'
                                         + '?title=' + self.valid_id,
                                         method='GET')
        request.add_header('user_token', wrong_auth_token)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 403)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('FORBIDDEN', err.msg)
            self.assertEqual('FORBIDDEN', err.reason)


if __name__ == '__main__':
    unittest.main()
