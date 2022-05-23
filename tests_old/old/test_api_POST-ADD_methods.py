#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2018-Jul-03
#  Modified by:   kenneth
#
#  Copyright 2019 EMBL - European Bioinformatics Institute
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

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

    def check_Characteristic_class(self, obj):
        if obj['category']:
            self.check_OntologyAnnotation_class(obj['category'])
        if obj['value']:
            self.check_OntologyAnnotation_class(obj['value'])
        if obj['unit']:
            self.check_OntologyAnnotation_class(obj['unit'])
        self.assertIsNotNone(obj['comments'])


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


class PostNewStudyContactTests(WsTests):

    valid_id = instance.config.VALID_ID_CONTACT
    bad_id = instance.config.BAD_ID_CONTACT
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
            self.check_OntologyAnnotation_class(role)

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

    def pre_delete_contact(self, url):
        request = urllib.request.Request(url + '/contacts'
                                         + '?email=' + self.valid_id,
                                         method='DELETE')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            if err.code != 404:
                raise Exception(err)

    # New Study Contact - Pub - Auth - NewData -> 200
    def test_add_Contact_pub_auth_newData(self):
        # first, delete the contact to ensure it won't exists
        self.pre_delete_contact(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the contact
        request = urllib.request.Request(url_pub_id + '/contacts',
                                         data=self.valid_data, method='POST')
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

    # New Study Contact - Pub - Auth - ExistingContact -> 409
    def test_add_Contact_pub_auth_duplicateData(self):
        # first, create the contact to ensure it will exists
        self.pre_create_contact(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the contact
        request = urllib.request.Request(url_pub_id + '/contacts',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 409)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('CONFLICT', err.msg)
            self.assertEqual('CONFLICT', err.reason)

    # New Study Contact - Pub - Auth - NoData -> 400
    def test_add_Contact_pub_auth_noData(self):
        request = urllib.request.Request(url_pub_id + '/contacts',
                                         data=self.no_data, method='POST')
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

    # New Study Contact - Pub - Auth - MissingRequiredData -> 400
    def test_add_Contact_pub_auth_missingData(self):
        request = urllib.request.Request(url_pub_id + '/contacts',
                                         data=self.missing_data, method='POST')
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

    # New Study Contact - Pub - Auth - ExtraQueryParams -> 400
    def test_add_Contact_pub_auth_extraParams(self):
        request = urllib.request.Request(url_pub_id + '/contacts'
                                         + '?email=' + self.valid_id,
                                         data=self.valid_data, method='POST')
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

    # New Study Contact - Pub - NoToken -> 401
    def test_add_Contact_pub_noToken(self):
        request = urllib.request.Request(url_pub_id + '/contacts',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # New Study Contact - Pub - NoAuth -> 403
    def test_add_Contact_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/contacts',
                                         data=self.valid_data, method='POST')
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

    # New Study Contact - Priv - Auth - NewData -> 200
    def test_add_Contact_priv_auth_newData(self):
        # first, delete the contact to ensure it won't exists
        self.pre_delete_contact(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the contact
        request = urllib.request.Request(url_priv_id + '/contacts',
                                         data=self.valid_data, method='POST')
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

    # New Study Contact - Priv - Auth - ExistingContact -> 409
    def test_add_Contact_priv_auth_duplicateData(self):
        # first, create the contact to ensure it will exists
        self.pre_create_contact(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the contact
        request = urllib.request.Request(url_priv_id + '/contacts',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 409)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('CONFLICT', err.msg)
            self.assertEqual('CONFLICT', err.reason)

    # New Study Contact - Priv - Auth - NoData -> 400
    def test_add_Contact_priv_auth_noData(self):
        request = urllib.request.Request(url_priv_id + '/contacts',
                                         data=self.no_data, method='POST')
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

    # New Study Contact - Priv - Auth - MissingRequiredData -> 400
    def test_add_Contact_priv_auth_missingData(self):
        request = urllib.request.Request(url_priv_id + '/contacts',
                                         data=self.missing_data, method='POST')
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

    # New Study Contact - Priv - Auth - ExtraQueryParams -> 400
    def test_add_Contact_priv_auth_extraParams(self):
        request = urllib.request.Request(url_priv_id + '/contacts'
                                         + '?email=' + self.valid_id,
                                         data=self.valid_data, method='POST')
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

    # New Study Contact - Priv - NoToken -> 401
    def test_add_Contact_priv_noToken(self):
        request = urllib.request.Request(url_priv_id + '/contacts',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # New Study Contact - Priv - NoAuth -> 403
    def test_add_Contact_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/contacts',
                                         data=self.valid_data, method='POST')
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


class PostNewStudyProtocolTests(WsTests):

    valid_id = instance.config.VALID_ID_PROTOCOL
    bad_id = instance.config.BAD_ID_PROTOCOL
    valid_data = instance.config.TEST_DATA_VALID_PROTOCOL
    missing_data = instance.config.TEST_DATA_MISSING_PROTOCOL
    no_data = b''

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

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

    def pre_delete_protocol(self, url):
        request = urllib.request.Request(url + '/protocols'
                                         + '?name=' + self.valid_id,
                                         method='DELETE')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            if err.code != 404:
                raise Exception(err)

    # New Study Protocol - Pub - Auth - NewData -> 200
    def test_add_Protocol_pub_auth_newData(self):
        # first, delete the protocol to ensure it won't exists
        self.pre_delete_protocol(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the protocol
        request = urllib.request.Request(url_pub_id + '/protocols',
                                         data=self.valid_data, method='POST')
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

    # New Study Protocol - Pub - Auth - ExistingProtocol -> 409
    def test_add_Protocol_pub_auth_duplicateData(self):
        # first, create the protocol to ensure it will exists
        self.pre_create_protocol(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the protocol
        request = urllib.request.Request(url_pub_id + '/protocols',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 409)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('CONFLICT', err.msg)
            self.assertEqual('CONFLICT', err.reason)

    # New Study Protocol - Pub - Auth - NoData -> 400
    def test_add_Protocol_pub_auth_noData(self):
        request = urllib.request.Request(url_pub_id + '/protocols',
                                         data=self.no_data, method='POST')
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

    # New Study Protocol - Pub - Auth - MissingRequiredData -> 400
    def test_add_Protocol_pub_auth_missingData(self):
        request = urllib.request.Request(url_pub_id + '/protocols',
                                         data=self.missing_data, method='POST')
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

    # New Study Protocol - Pub - Auth - ExtraQueryParams -> 400
    def test_add_Protocol_pub_auth_extraParams(self):
        request = urllib.request.Request(url_pub_id + '/protocols'
                                         + '?name=' + self.valid_id,
                                         data=self.valid_data, method='POST')
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

    # New Study Protocol - Pub - NoToken -> 401
    def test_add_Protocol_pub_auth_noToken(self):
        request = urllib.request.Request(url_pub_id + '/protocols',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # New Study Protocol - Pub - NoAuth -> 403
    def test_add_Protocol_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/protocols',
                                         data=self.valid_data, method='POST')
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

    # New Study Protocol - Priv - Auth - NewData -> 200
    def test_add_Protocol_priv_auth_newData(self):
        # first, delete the protocol to ensure it won't exists
        self.pre_delete_protocol(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the protocol
        request = urllib.request.Request(url_priv_id + '/protocols',
                                         data=self.valid_data, method='POST')
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

    # New Study Protocol - Priv - Auth - ExistingProtocol -> 409
    def test_add_Protocol_priv_auth_duplicateData(self):
        # first, create the protocol to ensure it will exists
        self.pre_create_protocol(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the protocol
        request = urllib.request.Request(url_priv_id + '/protocols',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 409)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('CONFLICT', err.msg)
            self.assertEqual('CONFLICT', err.reason)

    # New Study Protocol - Priv - Auth - NoData -> 400
    def test_add_Protocol_priv_auth_noData(self):
        request = urllib.request.Request(url_priv_id + '/protocols',
                                         data=self.no_data, method='POST')
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

    # New Study Protocol - Priv - Auth - MissingRequiredData -> 400
    def test_add_Protocol_priv_auth_missingData(self):
        request = urllib.request.Request(url_priv_id + '/protocols',
                                         data=self.missing_data, method='POST')
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

    # New Study Protocol - Priv - Auth - ExtraQueryParams -> 400
    def test_add_Protocol_priv_auth_extraParams(self):
        request = urllib.request.Request(url_priv_id + '/protocols'
                                         + '?name=' + self.valid_id,
                                         data=self.valid_data, method='POST')
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

    # New Study Protocol - Priv - NoToken -> 401
    def test_add_Protocol_priv_auth_noToken(self):
        request = urllib.request.Request(url_priv_id + '/protocols',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # New Study Protocol - Priv - NoAuth -> 403
    def test_add_Protocol_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/protocols',
                                         data=self.valid_data, method='POST')
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


class PostNewStudyFactorTests(WsTests):

    valid_id = instance.config.VALID_ID_FACTOR
    bad_id = instance.config.BAD_ID_FACTOR
    valid_data = instance.config.TEST_DATA_VALID_FACTOR
    missing_data = instance.config.TEST_DATA_MISSING_FACTOR
    no_data = b''

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

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

    def pre_delete_factor(self, url):
        request = urllib.request.Request(url + '/factors'
                                         + '?name=' + self.valid_id,
                                         method='DELETE')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            if err.code != 404:
                raise Exception(err)

    # New Study Factor - Pub - Auth - NewData -> 200
    def test_add_Factor_pub_auth_newData(self):
        # first, delete the factor to ensure it won't exists
        self.pre_delete_factor(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the factor
        request = urllib.request.Request(url_pub_id + '/factors',
                                         data=self.valid_data, method='POST')
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
            self.check_StudyFactor_class(j_resp['factor'])

    # New Study Factor - Pub - Auth - ExistingFactor -> 409
    def test_add_Factor_pub_auth_duplicateData(self):
        # first, create the factor to ensure it will exists
        self.pre_create_factor(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the factor
        request = urllib.request.Request(url_pub_id + '/factors',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 409)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('CONFLICT', err.msg)
            self.assertEqual('CONFLICT', err.reason)

    # New Study Factor - Pub - Auth - NoData -> 400
    def test_add_Factor_pub_auth_noData(self):
        request = urllib.request.Request(url_pub_id + '/factors',
                                         data=self.no_data, method='POST')
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

    # New Study Factor - Pub - Auth - MissingRequiredData -> 400
    def test_add_Factor_pub_auth_missingData(self):
        request = urllib.request.Request(url_pub_id + '/factors',
                                         data=self.missing_data, method='POST')
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

    # New Study Factor - Pub - Auth - ExtraQueryParams -> 400
    def test_add_Factor_pub_auth_extraParams(self):
        request = urllib.request.Request(url_pub_id + '/factors'
                                         + '?name=' + self.valid_id,
                                         data=self.valid_data, method='POST')
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

    # New Study Factor - Pub - NoToken -> 401
    def test_add_Factor_pub_auth_noToken(self):
        request = urllib.request.Request(url_pub_id + '/factors',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # New Study Factor - Pub - NoAuth -> 403
    def test_add_Factor_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/factors',
                                         data=self.valid_data, method='POST')
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

    # New Study Factor - Priv - Auth - NewData -> 200
    def test_add_Factor_priv_auth_newData(self):
        # first, delete the factor to ensure it won't exists
        self.pre_delete_factor(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the factor
        request = urllib.request.Request(url_priv_id + '/factors',
                                         data=self.valid_data, method='POST')
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
            self.check_StudyFactor_class(j_resp['factor'])

    # New Study Factor - Priv - Auth - ExistingFactor -> 409
    def test_add_Factor_priv_auth_duplicateData(self):
        # first, create the factor to ensure it will exists
        self.pre_create_factor(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the factor
        request = urllib.request.Request(url_priv_id + '/factors',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 409)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('CONFLICT', err.msg)
            self.assertEqual('CONFLICT', err.reason)

    # New Study Factor - Priv - Auth - NoData -> 400
    def test_add_Factor_priv_auth_noData(self):
        request = urllib.request.Request(url_priv_id + '/factors',
                                         data=self.no_data, method='POST')
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

    # New Study Factor - Priv - Auth - MissingRequiredData -> 400
    def test_add_Factor_priv_auth_missingData(self):
        request = urllib.request.Request(url_priv_id + '/factors',
                                         data=self.missing_data, method='POST')
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

    # New Study Factor - Priv - Auth - ExtraQueryParams -> 400
    def test_add_Factor_priv_auth_extraParams(self):
        request = urllib.request.Request(url_priv_id + '/factors'
                                         + '?name=' + self.valid_id,
                                         data=self.valid_data, method='POST')
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

    # New Study Factor - Priv - NoToken -> 401
    def test_add_Factor_priv_auth_noToken(self):
        request = urllib.request.Request(url_priv_id + '/factors',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # New Study Factor - Priv - NoAuth -> 403
    def test_add_Factor_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/factors',
                                         data=self.valid_data, method='POST')
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


class PostNewStudyDesignDescriptorTests(WsTests):

    valid_id = instance.config.VALID_ID_DESCRIPTOR
    bad_id = instance.config.BAD_ID_DESCRIPTOR
    valid_data = instance.config.TEST_DATA_VALID_DESCRIPTOR
    missing_data = instance.config.TEST_DATA_MISSING_DESCRIPTOR
    no_data = b''

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

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

    def pre_delete_descriptor(self, url):
        request = urllib.request.Request(url + '/descriptors'
                                         + '?term=' + self.valid_id,
                                         method='DELETE')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            if err.code != 404:
                raise Exception(err)

    # New Study Design Descriptor - Pub - Auth - NewData -> 200
    def test_add_descriptor_pub_auth_newData(self):
        # first, delete the descriptor' to ensure it won't exists
        self.pre_delete_descriptor(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the descriptor'
        request = urllib.request.Request(url_pub_id + '/descriptors',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
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

    # New Study Design Descriptor - Pub - Auth - ExistingFactor -> 409
    def test_add_descriptor_pub_auth_duplicateData(self):
        # first, create the descriptor' to ensure it will exists
        self.pre_create_descriptor(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the descriptor'
        request = urllib.request.Request(url_pub_id + '/descriptors',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 409)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('CONFLICT', err.msg)
            self.assertEqual('CONFLICT', err.reason)

    # New Study Design Descriptor - Pub - Auth - NoData -> 400
    def test_add_descriptor_pub_auth_noData(self):
        request = urllib.request.Request(url_pub_id + '/descriptors',
                                         data=self.no_data, method='POST')
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

    # New Study Design Descriptor - Pub - Auth - MissingRequiredData -> 400
    def test_add_descriptor_pub_auth_missingData(self):
        request = urllib.request.Request(url_pub_id + '/descriptors',
                                         data=self.missing_data, method='POST')
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

    # New Study Design Descriptor - Pub - Auth - ExtraQueryParams -> 400
    def test_add_descriptor_pub_auth_extraParams(self):
        request = urllib.request.Request(url_pub_id + '/descriptors'
                                         + '?term=' + self.valid_id,
                                         data=self.valid_data, method='POST')
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

    # New Study Design Descriptor - Pub - NoToken -> 401
    def test_add_descriptor_pub_auth_noToken(self):
        request = urllib.request.Request(url_pub_id + '/descriptors',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # New Study Design Descriptor - Pub - NoAuth -> 403
    def test_add_descriptor_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/descriptors',
                                         data=self.valid_data, method='POST')
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

    # New Study Design Descriptor - Priv - Auth - NewData -> 200
    def test_add_descriptor_priv_auth_newData(self):
        # first, delete the descriptor' to ensure it won't exists
        self.pre_delete_descriptor(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the descriptor'
        request = urllib.request.Request(url_priv_id + '/descriptors',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
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

    # New Study Design Descriptor - Priv - Auth - ExistingFactor -> 409
    def test_add_descriptor_priv_auth_duplicateData(self):
        # first, create the descriptor' to ensure it will exists
        self.pre_create_descriptor(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the descriptor'
        request = urllib.request.Request(url_priv_id + '/descriptors',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 409)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('CONFLICT', err.msg)
            self.assertEqual('CONFLICT', err.reason)

    # New Study Design Descriptor - Priv - Auth - NoData -> 400
    def test_add_descriptor_priv_auth_noData(self):
        request = urllib.request.Request(url_priv_id + '/descriptors',
                                         data=self.no_data, method='POST')
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

    # New Study Design Descriptor - Priv - Auth - MissingRequiredData -> 400
    def test_add_descriptor_priv_auth_missingData(self):
        request = urllib.request.Request(url_priv_id + '/descriptors',
                                         data=self.missing_data, method='POST')
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

    # New Study Design Descriptor - Priv - Auth - ExtraQueryParams -> 400
    def test_add_descriptor_priv_auth_extraParams(self):
        request = urllib.request.Request(url_priv_id + '/descriptors'
                                         + '?term=' + self.valid_id,
                                         data=self.valid_data, method='POST')
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

    # New Study Design Descriptor - Priv - NoToken -> 401
    def test_add_descriptor_priv_auth_noToken(self):
        request = urllib.request.Request(url_priv_id + '/descriptors',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # New Study Design Descriptor - Priv - NoAuth -> 403
    def test_add_descriptor_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/descriptors',
                                         data=self.valid_data, method='POST')
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


class PostNewStudyPublicationTests(WsTests):

    valid_id = instance.config.VALID_ID_PUBLICATION
    bad_id = instance.config.BAD_ID_PUBLICATION
    valid_data = instance.config.TEST_DATA_VALID_PUBLICATION
    missing_data = instance.config.TEST_DATA_MISSING_PUBLICATION
    no_data = b''

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

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

    def pre_delete_publication(self, url):
        request = urllib.request.Request(url + '/publications'
                                         + '?title=' + self.valid_id,
                                         method='DELETE')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            if err.code != 404:
                raise Exception(err)

    # New Study Publication - Pub - Auth - NewData -> 200
    def test_add_Publication_pub_auth_newData(self):
        # first, delete the publication to ensure it won't exists
        self.pre_delete_publication(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the publication
        request = urllib.request.Request(url_pub_id + '/publications',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
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

    # New Study Publication - Pub - Auth - ExistingPublication -> 409
    def test_add_Publication_pub_auth_duplicateData(self):
        # first, create the publication to ensure it will exists
        self.pre_create_publication(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the publication
        request = urllib.request.Request(url_pub_id + '/publications',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 409)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('CONFLICT', err.msg)
            self.assertEqual('CONFLICT', err.reason)

    # New Study Publication - Pub - Auth - NoData -> 400
    def test_add_Publication_pub_auth_noData(self):
        request = urllib.request.Request(url_pub_id + '/publications',
                                         data=self.no_data, method='POST')
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

    # New Study Publication - Pub - Auth - MissingRequiredData -> 400
    def test_add_Publication_pub_auth_missingData(self):
        request = urllib.request.Request(url_pub_id + '/publications',
                                         data=self.missing_data, method='POST')
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

    # New Study Publication - Pub - Auth - ExtraQueryParams -> 400
    def test_add_Publication_pub_auth_extraParams(self):
        request = urllib.request.Request(url_pub_id + '/publications'
                                         + '?title=' + self.valid_id,
                                         data=self.valid_data, method='POST')
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

    # New Study Publication - Pub - NoToken -> 401
    def test_add_Publication_pub_auth_noToken(self):
        request = urllib.request.Request(url_pub_id + '/publications',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # New Study Publication - Pub - NoAuth -> 403
    def test_add_Publication_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/publications',
                                         data=self.valid_data, method='POST')
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

    # New Study Publication - Priv - Auth - NewData -> 200
    def test_add_Publication_priv_auth_newData(self):
        # first, delete the publication to ensure it won't exists
        self.pre_delete_publication(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the publication
        request = urllib.request.Request(url_priv_id + '/publications',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
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

    # New Study Publication - Priv - Auth - ExistingPublication -> 409
    def test_add_Publication_priv_auth_duplicateData(self):
        # first, create the publication to ensure it will exists
        self.pre_create_publication(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the publication
        request = urllib.request.Request(url_priv_id + '/publications',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 409)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('CONFLICT', err.msg)
            self.assertEqual('CONFLICT', err.reason)

    # New Study Publication - Priv - Auth - NoData -> 400
    def test_add_Publication_priv_auth_noData(self):
        request = urllib.request.Request(url_priv_id + '/publications',
                                         data=self.no_data, method='POST')
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

    # New Study Publication - Priv - Auth - MissingRequiredData -> 400
    def test_add_Publication_priv_auth_missingData(self):
        request = urllib.request.Request(url_priv_id + '/publications',
                                         data=self.missing_data, method='POST')
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

    # New Study Publication - Priv - Auth - ExtraQueryParams -> 400
    def test_add_Publication_priv_auth_extraParams(self):
        request = urllib.request.Request(url_priv_id + '/publications'
                                         + '?title=' + self.valid_id,
                                         data=self.valid_data, method='POST')
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

    # New Study Publication - Priv - NoToken -> 401
    def test_add_Publication_priv_auth_noToken(self):
        request = urllib.request.Request(url_priv_id + '/publications',
                                         data=self.valid_data, method='POST')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # New Study Publication - Priv - NoAuth -> 403
    def test_add_Publication_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/publications',
                                         data=self.valid_data, method='POST')
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


class PostNewStudySourceTests(WsTests):

    # ToDo change for correct values
    # valid_id = instance.config.VALID_ID_SOURCE
    bad_id = instance.config.BAD_ID_SOURCE
    valid_pub_id = instance.config.VALID_PUB_ID_SOURCE
    valid_priv_id = instance.config.VALID_PRIV_ID_SOURCE
    # valid_data = instance.config.TEST_DATA_VALID_SOURCE
    valid_pub_data = instance.config.TEST_DATA_PUB_VALID_SOURCE
    valid_priv_data = instance.config.TEST_DATA_PRIV_VALID_SOURCE
    missing_data = instance.config.TEST_DATA_MISSING_SOURCE
    no_data = b''

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

    def check_Source_class(self, obj):
        self.assertIsNotNone(obj['name'])
        for characteristic in obj['characteristics']:
            self.check_Characteristic_class(characteristic)
        self.assertIsNotNone(obj['comments'])

    def pre_create_source(self, url, data):
        request = urllib.request.Request(url + '/sources',
                                         data=data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            if err.code != 409:
                raise Exception(err)

    def pre_delete_source(self, url, valid_id):
        request = urllib.request.Request(url + '/sources'
                                         + '?name=' + valid_id,
                                         method='DELETE')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            if err.code != 404:
                raise Exception(err)

    # New Study Source - Pub - Auth - NewData -> 200
    def test_add_Source_pub_auth_newData(self):
        # first, delete the source to ensure it won't exists
        self.pre_delete_source(url_pub_id, self.valid_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the source
        request = urllib.request.Request(url_pub_id + '/sources',
                                         data=self.valid_pub_data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('source', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['source'])
            self.check_Source_class(j_resp['source'])

    # New Study Source - Pub - Auth - ExistingSource -> 409
    def test_add_Source_pub_auth_duplicateData(self):
        # first, create the source to ensure it will exists
        self.pre_create_source(url_pub_id, self.valid_pub_data)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the source
        request = urllib.request.Request(url_pub_id + '/sources',
                                         data=self.valid_pub_data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 409)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('CONFLICT', err.msg)
            self.assertEqual('CONFLICT', err.reason)

    # New Study Source - Pub - Auth - NoData -> 400
    def test_add_Source_pub_auth_noData(self):
        request = urllib.request.Request(url_pub_id + '/sources',
                                         data=self.no_data, method='POST')
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

    # New Study Source - Pub - Auth - MissingRequiredData -> 400
    def test_add_Source_pub_auth_missingData(self):
        request = urllib.request.Request(url_pub_id + '/sources',
                                         data=self.missing_data, method='POST')
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

    # New Study Source - Pub - Auth - ExtraQueryParams -> 400
    def test_add_Source_pub_auth_extraParams(self):
        request = urllib.request.Request(url_pub_id + '/sources'
                                         + '?name=' + self.valid_pub_id,
                                         data=self.valid_pub_data, method='POST')
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

    # New Study Source - Pub - NoToken -> 401
    def test_add_Source_pub_auth_noToken(self):
        request = urllib.request.Request(url_pub_id + '/sources',
                                         data=self.valid_pub_data, method='POST')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # New Study Source - Pub - NoAuth -> 403
    def test_add_Source_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/sources',
                                         data=self.valid_pub_data, method='POST')
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

    # New Study Source - Priv - Auth - NewData -> 200
    def test_add_Source_priv_auth_newData(self):
        # first, delete the source to ensure it won't exists
        self.pre_delete_source(url_priv_id, self.valid_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the source
        request = urllib.request.Request(url_priv_id + '/sources',
                                         data=self.valid_priv_data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('source', body)
            j_resp = json.loads(body)
            self.assertIsNotNone(j_resp['source'])
            self.check_Source_class(j_resp['source'])

    # New Study Source - Priv - Auth - ExistingSource -> 409
    def test_add_Source_priv_auth_duplicateData(self):
        # first, create the source to ensure it will exists
        self.pre_create_source(url_priv_id, self.valid_priv_data)
        time.sleep(1)  # sleep time in seconds

        # then, try to add the source
        request = urllib.request.Request(url_priv_id + '/sources',
                                         data=self.valid_priv_data, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 409)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('CONFLICT', err.msg)
            self.assertEqual('CONFLICT', err.reason)

    # New Study Source - Priv - Auth - NoData -> 400
    def test_add_Source_priv_auth_noData(self):
        request = urllib.request.Request(url_priv_id + '/sources',
                                         data=self.no_data, method='POST')
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

    # New Study Source - Priv - Auth - MissingRequiredData -> 400
    def test_add_Source_priv_auth_missingData(self):
        request = urllib.request.Request(url_priv_id + '/sources',
                                         data=self.missing_data, method='POST')
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

    # New Study Source - Priv - Auth - ExtraQueryParams -> 400
    def test_add_Source_priv_auth_extraParams(self):
        request = urllib.request.Request(url_priv_id + '/sources'
                                         + '?name=' + self.valid_priv_id,
                                         data=self.valid_priv_data, method='POST')
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

    # New Study Source - Priv - NoToken -> 401
    def test_add_Source_priv_auth_noToken(self):
        request = urllib.request.Request(url_priv_id + '/sources',
                                         data=self.valid_priv_data, method='POST')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # New Study Source - Priv - NoAuth -> 403
    def test_add_Source_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/sources',
                                         data=self.valid_priv_data, method='POST')
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


if __name__ == '__main__':
    unittest.main()
