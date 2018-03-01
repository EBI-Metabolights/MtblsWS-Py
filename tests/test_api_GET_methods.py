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
valid_protocol_id = instance.config.VALID_PROTOCOL_ID
bad_protocol_id = instance.config.BAD_PROTOCOL_ID


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

    valid_contact = instance.config.TEST_DATA_CONTACT
    missingData_new_contact = instance.config.TEST_DATA_CONTACT_MISSING
    noData_new_contact = b''

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

    # Get Study Contact - Pub - ContacId -> 200
    def test_get_Contacts_pub(self):
        # first, create the contact to ensure it will exists
        self.pre_create_contact(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to get the contact
        request = urllib.request.Request(url_pub_id + '/contacts'
                                         + '?email=' + valid_contact_id,
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
                                         + '?email=' + bad_contact_id,
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
                                         + '?email=' + valid_contact_id,
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
                                         + '?email=' + bad_contact_id,
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
                                         + '?email=' + valid_contact_id,
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
                                         + '?email=' + valid_contact_id,
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

    data_new_protocol = instance.config.TEST_DATA_PROTOCOL
    valid_protocol_name = instance.config.TEST_PROTOCOL_NAME

    def check_Protocol_class(self, obj):
        self.assertIsNotNone(obj['name'])
        self.assertIsNotNone(obj['protocolType'])
        self.assertIsNotNone(obj['description'])
        self.assertIsNotNone(obj['uri'])
        self.assertIsNotNone(obj['version'])
        self.assertIsNotNone(obj['parameters'])
        self.assertIsNotNone(obj['components'])

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

    valid_protocol = instance.config.TEST_DATA_PROTOCOL
    missingData_new_protocol = instance.config.TEST_DATA_PROTOCOL_MISSING
    noData_new_protocol = b''

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
                                         data=self.valid_protocol, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            if err.code != 409:
                raise Exception(err)

    def pre_delete_protocol(self, url):
        request = urllib.request.Request(url + '/protocols'
                                         + '?name=' + valid_protocol_id,
                                         method='DELETE')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            if err.code != 404:
                raise Exception(err)

    # Get Study Protocol - Pub - ProtocolId -> 200
    def test_get_protocols_pub(self):
        # first, create the protocol to ensure it will exists
        self.pre_create_protocol(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to get the protocol
        request = urllib.request.Request(url_pub_id + '/protocols'
                                         + '?name=' + valid_protocol_id,
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
                                         + '?name=' + bad_protocol_id,
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
                                         + '?name=' + valid_protocol_id,
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
                                         + '?name=' + bad_protocol_id,
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

    # Get Study People - Priv -> 403
    def test_get_protocol_priv(self):
        request = urllib.request.Request(url_priv_id + '/protocols'
                                         + '?name=' + valid_protocol_id,
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
                                         + '?name=' + valid_protocol_id,
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


# class GetStudyFactorsTests(WsTests):
#
#     def check_Factors_class(self, obj):
#         self.assertIsNotNone(obj['factors'])
#         for factor in obj['factors']:
#             self.assertIsNotNone(factor['factorName'])
#             self.assertIsNotNone(factor['factorType'])
#             self.assertIsNotNone(factor['comments'])
#
#     # Get Study Factors - Pub -> 200
#     def test_get_factors(self):
#         request = urllib.request.Request(url_pub_id + '/factors', method='GET')
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
#     # Get Study Factors - Pub - Auth -> 200
#     def test_get_factors_pub_auth(self):
#         request = urllib.request.Request(url_pub_id + '/factors', method='GET')
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
#     # Get Study Factors - Pub - NoAuth -> 200
#     def test_get_factors_pub_noAuth(self):
#         request = urllib.request.Request(url_pub_id + '/factors', method='GET')
#         request.add_header('user_token', wrong_auth_token)
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
#     # Get Study Factors - Priv - Auth -> 200
#     def test_get_factors_priv_auth(self):
#         request = urllib.request.Request(url_priv_id + '/factors', method='GET')
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
#     # Get Study Factors - Priv -> 401
#     def test_get_factors_priv(self):
#         request = urllib.request.Request(url_priv_id + '/factors', method='GET')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 401)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('UNAUTHORIZED', err.msg)
#             self.assertEqual('UNAUTHORIZED', err.reason)
#
#     # Get Study Factors - Priv - NoAuth -> 403
#     def test_get_factors_priv_noAuth(self):
#         request = urllib.request.Request(url_priv_id + '/factors', method='GET')
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
#     # GET Study Factors - NullId -> 404
#     def test_get_factors_nullId(self):
#         request = urllib.request.Request(url_null_id + '/factors', method='GET')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # GET Study Factors - BadId -> 404
#     def test_get_factors_badId(self):
#         request = urllib.request.Request(url_wrong_id + '/factors', method='GET')
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
#
# class GetStudyDescriptorsTests(WsTests):
#
#     def check_Descriptors_class(self, obj):
#         self.assertIsNotNone(obj['descriptors'])
#         for descriptor in obj['descriptors']:
#             self.assertIsNotNone(descriptor['annotationValue'])
#             self.assertIsNotNone(descriptor['termAccession'])
#             self.assertIsNotNone(descriptor['termSource'])
#             self.assertIsNotNone(descriptor['comments'])
#
#     # Get Study Descriptors - Pub -> 200
#     def test_get_descriptors(self):
#         request = urllib.request.Request(url_pub_id + '/descriptors', method='GET')
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
#     # Get Study Descriptors - Pub - Auth -> 200
#     def test_get_descriptors_pub_auth(self):
#         request = urllib.request.Request(url_pub_id + '/descriptors', method='GET')
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
#     # Get Study Descriptors - Pub - NoAuth -> 200
#     def test_get_descriptors_pub_noAuth(self):
#         request = urllib.request.Request(url_pub_id + '/descriptors', method='GET')
#         request.add_header('user_token', wrong_auth_token)
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
#     # Get Study Descriptors - Priv - Auth -> 200
#     def test_get_descriptors_priv_auth(self):
#         request = urllib.request.Request(url_priv_id + '/descriptors', method='GET')
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
#     # Get Study Descriptors - Priv -> 401
#     def test_get_descriptors_priv(self):
#         request = urllib.request.Request(url_priv_id + '/descriptors', method='GET')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 401)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('UNAUTHORIZED', err.msg)
#             self.assertEqual('UNAUTHORIZED', err.reason)
#
#     # Get Study Descriptors - Priv - NoAuth -> 403
#     def test_get_descriptors_priv_noAuth(self):
#         request = urllib.request.Request(url_priv_id + '/descriptors', method='GET')
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
#     # GET Study Descriptors - NullId -> 404
#     def test_get_descriptors_nullId(self):
#         request = urllib.request.Request(url_null_id + '/descriptors', method='GET')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # GET Study Descriptors - BadId -> 404
#     def test_get_descriptors_badId(self):
#         request = urllib.request.Request(url_wrong_id + '/descriptors', method='GET')
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
#
# class GetStudyPublicationsTests(WsTests):
#     # tests for GET Method for Study Publications
#     #
#     # pubmed_id (str, NoneType):
#     # doi (str, NoneType):
#     # author_list (str, NoneType):
#     # title (str, NoneType):
#     # status (str, OntologyAnnotation, NoneType):
#     # comments (list, Comment):
#
#     def check_Publications_class(self, obj):
#         self.assertIsNotNone(obj['publications'])
#         for publication in obj['publications']:
#             self.assertIsNotNone(publication['pubMedID'])
#             self.assertIsNotNone(publication['doi'])
#             self.assertIsNotNone(publication['authorList'])
#             self.assertIsNotNone(publication['title'])
#             self.assertIsNotNone(publication['status'])
#             self.assertIsNotNone(publication['comments'])
#
#     # Get Study Publications - Pub -> 200
#     def test_get_publications(self):
#         request = urllib.request.Request(url_pub_id + '/publications', method='GET')
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
#     # Get Study Publications - Pub - Auth -> 200
#     def test_get_publications_pub_auth(self):
#         request = urllib.request.Request(url_pub_id + '/publications', method='GET')
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
#     # Get Study Publications - Pub - NoAuth -> 200
#     def test_get_publications_pub_noAuth(self):
#         request = urllib.request.Request(url_pub_id + '/publications', method='GET')
#         request.add_header('user_token', wrong_auth_token)
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
#     # Get Study Publications - Priv - Auth -> 200
#     def test_get_publications_priv_auth(self):
#         request = urllib.request.Request(url_priv_id + '/publications', method='GET')
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
#     # Get Study Publications - Priv -> 401
#     def test_get_publications_priv(self):
#         request = urllib.request.Request(url_priv_id + '/publications', method='GET')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 401)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('UNAUTHORIZED', err.msg)
#             self.assertEqual('UNAUTHORIZED', err.reason)
#
#     # Get Study Publications - Priv - NoAuth -> 403
#     def test_get_publications_priv_noAuth(self):
#         request = urllib.request.Request(url_priv_id + '/publications', method='GET')
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
#     # GET Study Publications - NullId -> 404
#     def test_get_publications_nullId(self):
#         request = urllib.request.Request(url_null_id + '/publications', method='GET')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # GET Study Publications - BadId -> 404
#     def test_get_publications_badId(self):
#         request = urllib.request.Request(url_wrong_id + '/publications', method='GET')
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
#
# class GetStudySourcesTests(WsTests):
#     # tests for GET Method for list of sources names associated with the Study
#     #
#     # name (str):
#
#     def check_Sources_class(self, obj):
#         self.assertIsNotNone(obj['sources'])
#         for sample in obj['sources']:
#             self.assertIsNotNone(sample['name'])
#
#     # Get Study list of Sources
#     def test_get_sources(self):
#         request = urllib.request.Request(url_pub_id + '/sources', method='GET')
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('sources', body)
#             self.check_Sources_class(j_resp)
#
#
# class GetStudySourceTests(WsTests):
#     # tests for GET Method for Study Source, by name
#     #
#     # name (str):
#     # characteristics (list, OntologyAnnotation):
#     # comments (list, Comment):
#
#     def check_Source_class(self, obj):
#         self.assertIsNotNone(obj['source'])
#         source = obj['source']
#         self.assertIsNotNone(source['name'])
#         self.assertIsNotNone(source['characteristics'])
#         self.assertIsNotNone(source['comments'])
#
#     # Get Study Source - Pub -> 200
#     def test_get_source(self):
#         request = urllib.request.Request(url_pub_id + '/sources/' + public_source_id, method='GET')
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
#     # Get Study Source - Pub - Auth -> 200
#     def test_get_source_pub_auth(self):
#         request = urllib.request.Request(url_pub_id + '/sources/' + public_source_id, method='GET')
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
#     # Get Study Source - Pub - NoAuth -> 200
#     def test_get_source_pub_noAuth(self):
#         request = urllib.request.Request(url_pub_id + '/sources/' + public_source_id, method='GET')
#         request.add_header('user_token', wrong_auth_token)
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
#     # Get Study Source - Priv - Auth -> 200
#     def test_get_source_priv_auth(self):
#         request = urllib.request.Request(url_priv_id + '/sources/' + private_source_id, method='GET')
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
#     # Get Study Source - Priv -> 401
#     def test_get_source_priv(self):
#         request = urllib.request.Request(url_priv_id + '/sources/' + private_source_id, method='GET')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 401)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('UNAUTHORIZED', err.msg)
#             self.assertEqual('UNAUTHORIZED', err.reason)
#
#     # Get Study Source - Priv - NoAuth -> 403
#     def test_get_source_priv_noAuth(self):
#         request = urllib.request.Request(url_priv_id + '/sources/' + private_source_id, method='GET')
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
#     # GET Study Source - NullId -> 404
#     def test_get_source_nullId(self):
#         request = urllib.request.Request(url_null_id + '/sources/' + public_source_id, method='GET')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # GET Study Source - BadId -> 404
#     def test_get_source_badId(self):
#         request = urllib.request.Request(url_wrong_id + '/sources/' + public_source_id, method='GET')
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
#
# class GetStudySamplesTests(WsTests):
#     # tests for GET Method for list of samples names associated with the Study
#     #
#     # name (str):
#
#     def check_Samples_class(self, obj):
#         self.assertIsNotNone(obj['samples'])
#         for sample in obj['samples']:
#             self.assertIsNotNone(sample['name'])
#
#     # Get Study list of Samples
#     def test_get_samples(self):
#         request = urllib.request.Request(url_pub_id + '/samples', method='GET')
#         with urllib.request.urlopen(request) as response:
#             self.assertEqual(response.code, 200)
#             header = response.info()
#             self.check_header_common(header)
#             body = response.read().decode('utf-8')
#             self.check_body_common(body)
#             j_resp = json.loads(body)
#             self.assertIn('samples', body)
#             self.check_Samples_class(j_resp)
#
#
# class GetStudySampleTests(WsTests):
#     # tests for GET Method for Study Sample, by name
#     #
#     # name (str):
#     # characteristics (list, OntologyAnnotation):
#     # derives_from (Source):
#     # factorValues (FactorValues):
#     # comments (list, Comment):
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
#     # Get Study Sample - Pub -> 200
#     def test_get_sample(self):
#         request = urllib.request.Request(url_pub_id + '/samples/' + public_sample_id, method='GET')
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
#     # Get Study Sample - Pub - Auth -> 200
#     def test_get_sample_pub_auth(self):
#         request = urllib.request.Request(url_pub_id + '/samples/' + public_sample_id, method='GET')
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
#     # Get Study Sample - Pub - NoAuth -> 200
#     def test_get_sample_pub_noAuth(self):
#         request = urllib.request.Request(url_pub_id + '/samples/' + public_sample_id, method='GET')
#         request.add_header('user_token', wrong_auth_token)
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
#     # Get Study Sample - Priv - Auth -> 200
#     def test_get_sample_priv_auth(self):
#         request = urllib.request.Request(url_priv_id + '/samples/' + private_sample_id, method='GET')
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
#     # Get Study Samples - Priv -> 401
#     def test_get_sample_priv(self):
#         request = urllib.request.Request(url_priv_id + '/samples/' + private_sample_id, method='GET')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 401)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('UNAUTHORIZED', err.msg)
#             self.assertEqual('UNAUTHORIZED', err.reason)
#
#     # Get Study Sample - Priv - NoAuth -> 403
#     def test_get_sample_priv_noAuth(self):
#         request = urllib.request.Request(url_priv_id + '/samples/' + private_sample_id, method='GET')
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
#     # GET Study Sample - NullId -> 404
#     def test_get_sample_nullId(self):
#         request = urllib.request.Request(url_null_id + '/samples/' + public_sample_id, method='GET')
#         try:
#             urllib.request.urlopen(request)
#         except urllib.error.HTTPError as err:
#             self.assertEqual(err.code, 404)
#             self.check_header_common(err.headers)
#             self.check_body_common(err.read().decode('utf-8'))
#             self.assertEqual('NOT FOUND', err.msg)
#             self.assertEqual('NOT FOUND', err.reason)
#
#     # GET Study Sample - BadId -> 404
#     def test_get_sample_badId(self):
#         request = urllib.request.Request(url_wrong_id + '/samples/' + public_sample_id, method='GET')
#         request.add_header('user_token', auth_id)
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
