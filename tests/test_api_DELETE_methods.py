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


class DeleteStudyContactTests(WsTests):

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

    # Delete Study Contact - Pub - Auth -> 200
    def test_delete_Contact_pub_auth(self):
        # first, create the contact to ensure it will exists
        self.pre_create_contact(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to delete the contact
        request = urllib.request.Request(url_pub_id + '/contacts'
                                         + '?email=' + self.valid_id,
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
                                         + '?email=' + self.valid_id,
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
                                         + '?email=' + self.valid_id,
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
                                         + '?email=' + self.bad_id,
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
                                         + '?email=' + self.valid_id,
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
                                         + '?email=' + self.valid_id,
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
                                         + '?email=' + self.valid_id,
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
                                         + '?email=' + self.bad_id,
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


class DeleteStudyProtocolTests(WsTests):

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
                                         + '?name=' + valid_protocol_id,
                                         method='DELETE')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            if err.code != 404:
                raise Exception(err)

    # Delete Study Protocol - Pub - Auth -> 200
    def test_delete_Protocol_pub_auth(self):
        # first, create the protocol to ensure it will exists
        self.pre_create_protocol(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to delete the protocol
        request = urllib.request.Request(url_pub_id + '/protocols'
                                         + '?name=' + self.valid_id,
                                         method='DELETE')
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

    # Delete Study Protocol - Pub - NoToken -> 401
    def test_delete_Protocol_pub_auth_noToken(self):
        request = urllib.request.Request(url_pub_id + '/protocols'
                                         + '?name=' + self.valid_id,
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

    # Delete Study Protocol - Pub - NoAuth -> 403
    def test_delete_Protocol_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/protocols'
                                         + '?name=' + self.valid_id,
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

    # Delete Study Protocol - Pub - Auth - NoParams -> 404
    def test_delete_Protocol_pub_auth_noParams(self):
        request = urllib.request.Request(url_pub_id + '/protocols',
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

    # Delete Study Protocol - Pub - Auth - NoData -> 404
    def test_delete_Protocol_pub_auth_noData(self):
        request = urllib.request.Request(url_pub_id + '/protocols'
                                         + '?name=',
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

    # Delete Study Protocol - Pub - Auth - unknownProtocol -> 404
    def test_delete_Protocol_pub_auth_unknownProtocol(self):
        request = urllib.request.Request(url_pub_id + '/protocols'
                                         + '?name=' + self.bad_id,
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

    # Delete Study Protocol - Priv - Auth - NewData -> 200
    def test_delete_Protocol_priv_auth_newData(self):
        # first, create the protocol to ensure it will exists
        self.pre_create_protocol(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to delete the protocol
        request = urllib.request.Request(url_priv_id + '/protocols'
                                         + '?name=' + self.valid_id,
                                         method='DELETE')
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

    # Delete Study Protocol - Priv - NoToken -> 401
    def test_delete_Protocol_priv_auth_noToken(self):
        request = urllib.request.Request(url_priv_id + '/protocols'
                                         + '?name=' + self.valid_id,
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

    # Delete Study Protocol - Priv - NoAuth -> 403
    def test_delete_Protocol_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/protocols'
                                         + '?name=' + self.valid_id,
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

    # Delete Study Protocol - Priv - Auth - NoParams -> 404
    def test_delete_Protocol_priv_auth_noParams(self):
        request = urllib.request.Request(url_priv_id + '/protocols',
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

    # Delete Study Protocol - Priv - Auth - NoData -> 404
    def test_delete_Protocol_piv_auth_noData(self):
        request = urllib.request.Request(url_priv_id + '/protocols'
                                         + '?name=',
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

    # Delete Study Protocol - Priv - Auth - unknownProtocol -> 404
    def test_delete_Protocol_priv_auth_unknownProtocol(self):
        request = urllib.request.Request(url_priv_id + '/protocols'
                                         + '?name=' + self.bad_id,
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


class DeleteStudyFactorTests(WsTests):

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
                                         + '?name=' + valid_factor_id,
                                         method='DELETE')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            if err.code != 404:
                raise Exception(err)

    # Delete Study Factor - Pub - Auth -> 200
    def test_delete_Factor_pub_auth(self):
        # first, create the factor to ensure it will exists
        self.pre_create_factor(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to delete the factor
        request = urllib.request.Request(url_pub_id + '/factors'
                                         + '?name=' + self.valid_id,
                                         method='DELETE')
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

    # Delete Study Factor - Pub - NoToken -> 401
    def test_delete_Factor_pub_auth_noToken(self):
        request = urllib.request.Request(url_pub_id + '/factors'
                                         + '?name=' + self.valid_id,
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

    # Delete Study Factor - Pub - NoAuth -> 403
    def test_delete_Factor_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/factors'
                                         + '?name=' + self.valid_id,
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

    # Delete Study Factor - Pub - Auth - NoParams -> 404
    def test_delete_Factor_pub_auth_noParams(self):
        request = urllib.request.Request(url_pub_id + '/factors',
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

    # Delete Study Factor - Pub - Auth - NoData -> 404
    def test_delete_Factor_pub_auth_noData(self):
        request = urllib.request.Request(url_pub_id + '/factors'
                                         + '?name=',
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

    # Delete Study Factor - Pub - Auth - unknownFactor -> 404
    def test_delete_Factor_pub_auth_unknownFactor(self):
        request = urllib.request.Request(url_pub_id + '/factors'
                                         + '?name=' + self.bad_id,
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

    # Delete Study Factor - Priv - Auth - NewData -> 200
    def test_delete_Factor_priv_auth_newData(self):
        # first, create the factor to ensure it will exists
        self.pre_create_factor(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to delete the factor
        request = urllib.request.Request(url_priv_id + '/factors'
                                         + '?name=' + self.valid_id,
                                         method='DELETE')
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

    # Delete Study Factor - Priv - NoToken -> 401
    def test_delete_Factor_priv_auth_noToken(self):
        request = urllib.request.Request(url_priv_id + '/factors'
                                         + '?name=' + self.valid_id,
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

    # Delete Study Factor - Priv - NoAuth -> 403
    def test_delete_Factor_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/factors'
                                         + '?name=' + self.valid_id,
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

    # Delete Study Factor - Priv - Auth - NoParams -> 404
    def test_delete_Factor_priv_auth_noParams(self):
        request = urllib.request.Request(url_priv_id + '/factors',
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

    # Delete Study Factor - Priv - Auth - NoData -> 404
    def test_delete_Factor_piv_auth_noData(self):
        request = urllib.request.Request(url_priv_id + '/factors'
                                         + '?name=',
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

    # Delete Study Factor - Priv - Auth - unknownFactor -> 404
    def test_delete_Factor_priv_auth_unknownFactor(self):
        request = urllib.request.Request(url_priv_id + '/factors'
                                         + '?name=' + self.bad_id,
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


class DeleteStudyDesignDescriptorTests(WsTests):

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

    # Delete Study Descriptor - Pub - Auth -> 200
    def test_delete_descriptor_pub_auth(self):
        # first, create the descriptor to ensure it will exists
        self.pre_create_descriptor(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to delete the descriptor
        request = urllib.request.Request(url_pub_id + '/descriptors'
                                         + '?term=' + self.valid_id,
                                         method='DELETE')
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

    # Delete Study Descriptor - Pub - NoToken -> 401
    def test_delete_descriptor_pub_auth_noToken(self):
        request = urllib.request.Request(url_pub_id + '/descriptors'
                                         + '?term=' + self.valid_id,
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

    # Delete Study Descriptor - Pub - NoAuth -> 403
    def test_delete_descriptor_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/descriptors'
                                         + '?term=' + self.valid_id,
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

    # Delete Study Descriptor - Pub - Auth - NoParams -> 404
    def test_delete_descriptor_pub_auth_noParams(self):
        request = urllib.request.Request(url_pub_id + '/descriptors',
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

    # Delete Study Descriptor - Pub - Auth - NoData -> 404
    def test_delete_descriptor_pub_auth_noData(self):
        request = urllib.request.Request(url_pub_id + '/descriptors'
                                         + '?term=',
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

    # Delete Study Descriptor - Pub - Auth - unknownDescriptor -> 404
    def test_delete_descriptor_pub_auth_unknownDescriptor(self):
        request = urllib.request.Request(url_pub_id + '/descriptors'
                                         + '?term=' + self.bad_id,
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

    # Delete Study Descriptor - Priv - Auth - NewData -> 200
    def test_delete_descriptor_priv_auth_newData(self):
        # first, create the descriptor to ensure it will exists
        self.pre_create_descriptor(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to delete the descriptor
        request = urllib.request.Request(url_priv_id + '/descriptors'
                                         + '?term=' + self.valid_id,
                                         method='DELETE')
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

    # Delete Study Descriptor - Priv - NoToken -> 401
    def test_delete_descriptor_priv_auth_noToken(self):
        request = urllib.request.Request(url_priv_id + '/descriptors'
                                         + '?term=' + self.valid_id,
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

    # Delete Study Descriptor - Priv - NoAuth -> 403
    def test_delete_descriptor_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/descriptors'
                                         + '?term=' + self.valid_id,
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

    # Delete Study Descriptor - Priv - Auth - NoParams -> 404
    def test_delete_descriptor_priv_auth_noParams(self):
        request = urllib.request.Request(url_priv_id + '/descriptors',
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

    # Delete Study Descriptor - Priv - Auth - NoData -> 404
    def test_delete_descriptor_piv_auth_noData(self):
        request = urllib.request.Request(url_priv_id + '/descriptors'
                                         + '?term=',
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

    # Delete Study Descriptor - Priv - Auth - unknownDescriptor -> 404
    def test_delete_descriptor_priv_auth_unknownDescriptor(self):
        request = urllib.request.Request(url_priv_id + '/descriptors'
                                         + '?term=' + self.bad_id,
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


class DeleteStudyPublicationTests(WsTests):

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

    # Delete Publication - Pub - Auth -> 200
    def test_delete_publication_pub_auth(self):
        # first, create the publication to ensure it will exists
        self.pre_create_publication(url_pub_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to delete the publication
        request = urllib.request.Request(url_pub_id + '/publications'
                                         + '?title=' + self.valid_id,
                                         method='DELETE')
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

    # Delete Publication - Pub - NoToken -> 401
    def test_delete_publication_pub_auth_noToken(self):
        request = urllib.request.Request(url_pub_id + '/publications'
                                         + '?title=' + self.valid_id,
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

    # Delete Publication - Pub - NoAuth -> 403
    def test_delete_publication_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/publications'
                                         + '?title=' + self.valid_id,
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

    # Delete Publication - Pub - Auth - NoParams -> 404
    def test_delete_publication_pub_auth_noParams(self):
        request = urllib.request.Request(url_pub_id + '/publications',
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

    # Delete Publication - Pub - Auth - NoData -> 404
    def test_delete_publication_pub_auth_noData(self):
        request = urllib.request.Request(url_pub_id + '/publications'
                                         + '?title=',
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

    # Delete Publication - Pub - Auth - unknownDescriptor -> 404
    def test_delete_publication_pub_auth_unknownDescriptor(self):
        request = urllib.request.Request(url_pub_id + '/publications'
                                         + '?title=' + self.bad_id,
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

    # Delete Publication - Priv - Auth - NewData -> 200
    def test_delete_publication_priv_auth_newData(self):
        # first, create the publication to ensure it will exists
        self.pre_create_publication(url_priv_id)
        time.sleep(1)  # sleep time in seconds

        # then, try to delete the publication
        request = urllib.request.Request(url_priv_id + '/publications'
                                         + '?title=' + self.valid_id,
                                         method='DELETE')
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

    # Delete Publication - Priv - NoToken -> 401
    def test_delete_publication_priv_auth_noToken(self):
        request = urllib.request.Request(url_priv_id + '/publications'
                                         + '?title=' + self.valid_id,
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

    # Delete Publication - Priv - NoAuth -> 403
    def test_delete_publication_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/publications'
                                         + '?title=' + self.valid_id,
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

    # Delete Publication - Priv - Auth - NoParams -> 404
    def test_delete_publication_priv_auth_noParams(self):
        request = urllib.request.Request(url_priv_id + '/publications',
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

    # Delete Publication - Priv - Auth - NoData -> 404
    def test_delete_publication_piv_auth_noData(self):
        request = urllib.request.Request(url_priv_id + '/publications'
                                         + '?title=',
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

    # Delete Publication - Priv - Auth - unknownDescriptor -> 404
    def test_delete_publication_priv_auth_unknownFactor(self):
        request = urllib.request.Request(url_priv_id + '/publications'
                                         + '?title=' + self.bad_id,
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


class DeleteStudySourceTests(WsTests):

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

    # Delete Study Source - Pub - Auth -> 200
    def test_delete_Source_pub_auth(self):
        # first, create the source to ensure it will exists
        self.pre_create_source(url_pub_id, self.valid_pub_data)
        time.sleep(1)  # sleep time in seconds

        # then, try to delete the source
        request = urllib.request.Request(url_pub_id + '/sources'
                                         + '?name=' + self.valid_pub_id,
                                         method='DELETE')
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

    # Delete Study Source - Pub - NoToken -> 401
    def test_delete_Source_pub_auth_noToken(self):
        request = urllib.request.Request(url_pub_id + '/sources'
                                         + '?name=' + self.valid_pub_id,
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

    # Delete Study Source - Pub - NoAuth -> 403
    def test_delete_Source_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/sources'
                                         + '?name=' + self.valid_pub_id,
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

    # Delete Study Source - Pub - Auth - NoParams -> 404
    def test_delete_Source_pub_auth_noParams(self):
        request = urllib.request.Request(url_pub_id + '/sources',
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

    # Delete Study Source - Pub - Auth - NoData -> 404
    def test_delete_Source_pub_auth_noData(self):
        request = urllib.request.Request(url_pub_id + '/sources'
                                         + '?name=',
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

    # Delete Study Source - Pub - Auth - unknownSource -> 404
    def test_delete_Source_pub_auth_unknownSource(self):
        request = urllib.request.Request(url_pub_id + '/sources'
                                         + '?name=' + self.bad_id,
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

    # Delete Study Source - Priv - Auth - NewData -> 200
    def test_delete_Source_priv_auth_newData(self):
        # first, create the source to ensure it will exists
        self.pre_create_source(url_priv_id, self.valid_priv_data)
        time.sleep(1)  # sleep time in seconds

        # then, try to delete the source
        request = urllib.request.Request(url_priv_id + '/sources'
                                         + '?name=' + self.valid_priv_id,
                                         method='DELETE')
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

    # Delete Study Source - Priv - NoToken -> 401
    def test_delete_Source_priv_auth_noToken(self):
        request = urllib.request.Request(url_priv_id + '/sources'
                                         + '?name=' + self.valid_priv_id,
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

    # Delete Study Source - Priv - NoAuth -> 403
    def test_delete_Source_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/sources'
                                         + '?name=' + self.valid_priv_id,
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

    # Delete Study Source - Priv - Auth - NoParams -> 404
    def test_delete_Source_priv_auth_noParams(self):
        request = urllib.request.Request(url_priv_id + '/sources',
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

    # Delete Study Source - Priv - Auth - NoData -> 404
    def test_delete_Source_piv_auth_noData(self):
        request = urllib.request.Request(url_priv_id + '/sources'
                                         + '?name=',
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

    # Delete Study Source - Priv - Auth - unknownSource -> 404
    def test_delete_Source_priv_auth_unknownSource(self):
        request = urllib.request.Request(url_priv_id + '/sources'
                                         + '?name=' + self.bad_id,
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


if __name__ == '__main__':
    unittest.main()
