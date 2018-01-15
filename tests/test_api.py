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
url_pub_id = url_base + public_study_id
url_priv_id = url_base + private_study_id
url_null_id = url_base
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


class AboutTests(WsTests):

    # GET About
    def test_request_about_OK(self):
        # assert self.app
        request = urllib.request.Request(url_about)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('About-WS', body)
            self.assertIn('WS-App', body)
            self.assertIn('WS-Name', body)
            self.assertIn('WS-URL', body)
            self.assertIn('WS-Version', body)
            self.assertIn('WS-Description', body)
            self.assertIn('WS-API', body)
            self.assertIn('API-Specification', body)
            self.assertIn('API-Version', body)
            self.assertIn('API-Documentation', body)


class GetStudyPubListTests(WsTests):

    # Get All Public Studies - Pub -> 200
    def test_get_pub_studies(self):
        request = urllib.request.Request(url_null_id + 'list', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('content', body)


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
            self.assertIn('Study-title', body)

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
            self.assertIn('Study-title', body)

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
            self.assertIn('Study-title', body)

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
            self.assertIn('Study-title', body)

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


class UpdateStudyTitleTests(WsTests):

    data_new_title = b'{ "title": "New Study title..." }'

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

    # PUT Update Study Title - Pub -> 200
    def test_update_title_pub(self):
        request = urllib.request.Request(url_pub_id + '/title', data=self.data_new_title, method='PUT')
        self.add_common_headers(request)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-title', body)

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
            self.assertIn('Study-title', body)

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
            self.assertIn('Study-title', body)

    # Update Study Title - Priv -> 401
    def test_update_title_priv(self):
        request = urllib.request.Request(url_priv_id+ '/title', data=self.data_new_title, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

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
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Title - Pub - NoSave -> 200
    def test_update_title_pub_noSave(self):
        request = urllib.request.Request(url_pub_id + '/title', data=self.data_new_title, method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-title', body)

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
            self.assertIn('Study-title', body)

    # Update Study Title - Pub - NoAuth - NoSave -> 200
    def test_update_title_pub_noAuth_noSave(self):
        request = urllib.request.Request(url_pub_id + '/title', data=self.data_new_title, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-title', body)

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
            self.assertIn('Study-title', body)

    # Update Study Title - Priv - NoSave -> 401
    def test_update_title_priv_noSave(self):
        request = urllib.request.Request(url_priv_id + '/title', data=self.data_new_title, method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

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
        request.add_header('save_audit_copy', 'False')
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
    def test_get_desc(self):
        request = urllib.request.Request(url_pub_id + '/description', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-description', body)

    # GET Study Description - Pub - Auth -> 200
    def test_get_desc_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/description', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-description', body)

    # GET Study Description - Pub - NoAuth -> 200
    def test_get_desc_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/description', method='GET')
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-description', body)

    # GET Study Description - Priv - Auth -> 200
    def test_get_desc_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/description', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-description', body)

    # GET Study Description - Priv -> 401
    def test_get_desc_priv(self):
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
    def test_get_desc_priv_noAuth(self):
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
    def test_get_desc_nullId(self):
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
    def test_get_desc_badId(self):
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


class UpdateStudyDescriptionTests(WsTests):

    data_new_description = b'{ "description": "New Study description..." }'

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

    # PUT Update Study Description - Pub -> 200
    def test_update_description_pub(self):
        request = urllib.request.Request(url_pub_id + '/description', data=self.data_new_description, method='PUT')
        self.add_common_headers(request)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-description', body)

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
            self.assertIn('Study-description', body)

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
            self.assertIn('Study-description', body)

    # Update Study Description - Priv -> 401
    def test_update_description_priv(self):
        request = urllib.request.Request(url_priv_id + '/description', data=self.data_new_description, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

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
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Description - Pub - NoSave -> 200
    def test_update_description_pub_noSave(self):
        request = urllib.request.Request(url_pub_id + '/description', data=self.data_new_description, method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-description', body)

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
            self.assertIn('Study-description', body)

    # Update Study Description - Pub - NoAuth - NoSave -> 200
    def test_update_description_pub_noAuth_noSave(self):
        request = urllib.request.Request(url_pub_id + '/description', data=self.data_new_description, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-description', body)

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
            self.assertIn('Study-description', body)

    # Update Study Description - Priv - NoSave -> 401
    def test_update_description_priv_noSave(self):
        request = urllib.request.Request(url_priv_id + '/description', data=self.data_new_description, method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

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
        request.add_header('save_audit_copy', 'False')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


class PostStudyNew(WsTests):

    data_new_study = instance.config.TEST_DATA_NEW_STUDY

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

    # POST New Study - Auth -> 200
    def test_post_new_study_auth(self):
        request = urllib.request.Request(url_base + 'new', data=self.data_new_study, method='POST')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('title', body)
            self.assertIn('description', body)
            self.assertIn('submissionDate', body)
            self.assertIn('publicReleaseDate', body)

    # New Study - Null Req -> 400
    def test_post_new_study_nullReq(self):
        request = urllib.request.Request(url_base + 'new', data=b'', method='POST')
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
        request = urllib.request.Request(url_base + 'new',
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
        request = urllib.request.Request(url_base + 'new', data=self.data_new_study, method='POST')
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
        request = urllib.request.Request(url_base + 'new', data=self.data_new_study, method='POST')
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


class GetStudyProtocolsTests(WsTests):

    # Get Study Protocols - Pub -> 200
    def test_get_protocols(self):
        request = urllib.request.Request(url_pub_id + '/protocols', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyProtocols', body)

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
            self.assertIn('StudyProtocols', body)

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
            self.assertIn('StudyProtocols', body)

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
            self.assertIn('StudyProtocols', body)

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


class UpdateStudyProtocolsTests(WsTests):

    data_new_protocol = instance.config.TEST_DATA_PROTOCOLS

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

    # PUT Update Study Protocols - Pub -> 200
    def test_update_protocols_pub(self):
        request = urllib.request.Request(url_pub_id + '/protocols', data=self.data_new_protocol, method='PUT')
        self.add_common_headers(request)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyProtocols', body)
            self.assertIn('Updated with MtblsWs-Py', body)

    # Update Study Protocols - Pub - Auth -> 200
    def test_update_protocols_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/protocols', data=self.data_new_protocol, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyProtocols', body)
            self.assertIn('Updated with MtblsWs-Py', body)

    # Update Study Protocols - Pub - NoAuth -> 403
    def test_update_protocols_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/protocols', data=self.data_new_protocol, method='PUT')
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

    # Update Study Protocols - Priv - Auth -> 200
    def test_update_protocols_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/protocols', data=self.data_new_protocol, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyProtocols', body)
            self.assertIn('Updated with MtblsWs-Py', body)

    # Update Study Protocols - Priv -> 401
    def test_update_protocols_priv(self):
        request = urllib.request.Request(url_priv_id + '/protocols', data=self.data_new_protocol, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Protocols - Priv - NoAuth -> 403
    def test_update_protocols_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/protocols', data=self.data_new_protocol, method='PUT')
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

    # Update Study Protocols - NullId -> 404
    def test_update_protocols_nullId(self):
        request = urllib.request.Request(url_null_id + '/protocols', data=self.data_new_protocol, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Protocols - BadId -> 404
    def test_update_protocols_badId(self):
        request = urllib.request.Request(url_wrong_id + '/protocols', data=self.data_new_protocol, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Protocols - Pub - NoSave -> 200
    def test_update_protocols_pub_noSave(self):
        request = urllib.request.Request(url_pub_id + '/protocols', data=self.data_new_protocol, method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyProtocols', body)
            self.assertIn('Updated with MtblsWs-Py', body)

    # Update Study Protocols - Pub - Auth - NoSave -> 200
    def test_update_protocols_pub_auth_noSave(self):
        request = urllib.request.Request(url_pub_id + '/protocols', data=self.data_new_protocol, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyProtocols', body)
            self.assertIn('Updated with MtblsWs-Py', body)

    # Update Study Protocols - Pub - NoAuth - NoSave -> 200
    def test_update_protocols_pub_noAuth_noSave(self):
        request = urllib.request.Request(url_pub_id + '/protocols', data=self.data_new_protocol, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyProtocols', body)
            self.assertIn('Updated with MtblsWs-Py', body)

    # Update Study Protocols - Priv - Auth - NoSave -> 200
    def test_update_protocols_priv_auth_noSave(self):
        request = urllib.request.Request(url_priv_id + '/protocols', data=self.data_new_protocol, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyProtocols', body)
            self.assertIn('Updated with MtblsWs-Py', body)

    # Update Study Protocols - Priv - NoSave -> 401
    def test_update_protocols_priv_noSave(self):
        request = urllib.request.Request(url_priv_id + '/protocols', data=self.data_new_protocol, method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Protocols - Priv - NoAuth - NoSave -> 403
    def test_update_protocols_priv_noAuth_noSave(self):
        request = urllib.request.Request(url_priv_id + '/protocols', data=self.data_new_protocol, method='PUT')
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

    # Update Study Protocols - NullId - NoSave -> 404
    def test_update_protocols_nullId_noSave(self):
        request = urllib.request.Request(url_null_id + '/protocols', data=self.data_new_protocol, method='PUT')
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

    # Update Study Protocols - BadId - NoSave -> 404
    def test_update_protocols_badId_noSave(self):
        request = urllib.request.Request(url_wrong_id + '/protocols', data=self.data_new_protocol, method='PUT')
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


class GetStudyContactsTests(WsTests):

    def check_Person_class(self, obj):
        self.assertIsNotNone(obj['StudyContacts'])
        for person in obj['StudyContacts']:
            self.assertIsNotNone(person['first_name'])
            self.assertIsNotNone(person['mid_initials'])
            self.assertIsNotNone(person['last_name'])
            self.assertIsNotNone(person['email'])
            self.assertIsNotNone(person['affiliation'])
            self.assertIsNotNone(person['phone'])
            self.assertIsNotNone(person['address'])
            self.assertIsNotNone(person['fax'])
            self.assertIsNotNone(person['comments'])
        for role in obj['StudyContacts'][0]['roles']:
            self.assertIsNotNone(role['term'])
            self.assertIsNotNone(role['term_accession'])
            self.assertIsNotNone(role['comments'])

    # Get Study Contacts - Pub -> 200
    def test_get_contacts(self):
        request = urllib.request.Request(url_pub_id + '/contacts', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('StudyContacts', body)
            self.check_Person_class(j_resp)

    # Get Study Contacts - Pub - Auth -> 200
    def test_get_contacts_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/contacts', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('StudyContacts', body)
            self.check_Person_class(j_resp)

    # Get Study Contacts - Pub - NoAuth -> 200
    def test_get_contacts_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/contacts', method='GET')
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('StudyContacts', body)
            self.check_Person_class(j_resp)

    # Get Study Contacts - Priv - Auth -> 200
    def test_get_contacts_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/contacts', method='GET')
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('StudyContacts', body)
            self.check_Person_class(j_resp)

    # Get Study Contacts - Priv -> 401
    def test_get_contacts_priv(self):
        request = urllib.request.Request(url_priv_id + '/contacts', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Get Study Contacts - Priv - NoAuth -> 403
    def test_get_contacts_priv_noAuth(self):
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
    def test_get_contacts_nullId(self):
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
    def test_get_contacts_badId(self):
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


class UpdateStudyContactsTests(WsTests):

    data_new_contacts = b'{"StudyContacts": [' \
                        b'{"phone": "012345678901",' \
                        b'"mid_initials": "A",' \
                        b'"roles": [' \
                        b'{"term_source": {' \
                        b'"file": null, "version": null, "name": null,"comments": [],"description": null},' \
                        b'"term_accession": "http://www.ebi.ac.uk/efo/EFO_0001739",' \
                        b'"term": "investigator", "comments": []}],' \
                        b'"first_name": "John","address": "","affiliation": "Life Sciences Institute",' \
                        b'"last_name":"Doe",' \
                        b'"fax": "","comments": [], "email": "doej@a.mail.com"},' \
                        b'{"phone": "012345678902","mid_initials": "A","roles": [' \
                        b'{"term_source": {"file": null,"version": null,"name": null,"comments": [],' \
                        b'"description": null},"term_accession": "","term": "Bioinformatician",' \
                        b'"comments": []}],' \
                        b'"first_name": "Jack","address": "","affiliation": "Department of Biochemistry",' \
                        b'"last_name": "Smith",' \
                        b'"fax": "","comments": [],"email": "smithj@a.mail.com"' \
                        b'}]}'

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

    # PUT Update Study Contacts - Pub -> 200
    def test_update_contacts_pub(self):
        request = urllib.request.Request(url_pub_id + '/contacts', data=self.data_new_contacts, method='PUT')
        self.add_common_headers(request)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyContacts', body)

    # Update Study Contacts - Pub - Auth -> 200
    def test_update_contacts_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/contacts', data=self.data_new_contacts, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyContacts', body)

    # Update Study Contacts - Pub - NoAuth -> 403
    def test_update_contacts_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/contacts', data=self.data_new_contacts, method='PUT')
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

    # Update Study Contacts - Priv - Auth -> 200
    def test_update_contacts_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/contacts', data=self.data_new_contacts, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyContacts', body)

    # Update Study Contacts - Priv -> 401
    def test_update_contacts_priv(self):
        request = urllib.request.Request(url_priv_id + '/contacts', data=self.data_new_contacts, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Contacts - Priv - NoAuth -> 403
    def test_update_contacts_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/contacts', data=self.data_new_contacts, method='PUT')
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

    # Update Study Contacts - NullId -> 404
    def test_update_contacts_nullId(self):
        request = urllib.request.Request(url_null_id + '/contacts', data=self.data_new_contacts, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Contacts - BadId -> 404
    def test_update_contacts_badId(self):
        request = urllib.request.Request(url_wrong_id + '/contacts', data=self.data_new_contacts, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Contacts - Pub - NoSave -> 200
    def test_update_contacts_pub_noSave(self):
        request = urllib.request.Request(url_pub_id + '/contacts', data=self.data_new_contacts, method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyContacts', body)

    # Update Study Contacts - Pub - Auth - NoSave -> 200
    def test_update_contacts_pub_auth_noSave(self):
        request = urllib.request.Request(url_pub_id + '/contacts', data=self.data_new_contacts, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyContacts', body)

    # Update Study Contacts - Pub - NoAuth - NoSave -> 200
    def test_update_contacts_pub_noAuth_noSave(self):
        request = urllib.request.Request(url_pub_id + '/contacts', data=self.data_new_contacts, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyContacts', body)

    # Update Study Contacts - Priv - Auth - NoSave -> 200
    def test_update_contacts_priv_auth_noSave(self):
        request = urllib.request.Request(url_priv_id + '/contacts', data=self.data_new_contacts, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyContacts', body)

    # Update Study Contacts - Priv - NoSave -> 401
    def test_update_contacts_priv_noSave(self):
        request = urllib.request.Request(url_priv_id + '/contacts', data=self.data_new_contacts, method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Contacts - Priv - NoAuth - NoSave -> 403
    def test_update_contacts_priv_noAuth_noSave(self):
        request = urllib.request.Request(url_priv_id + '/contacts', data=self.data_new_contacts, method='PUT')
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

    # Update Study Contacts - NullId - NoSave -> 404
    def test_update_contacts_nullId_noSave(self):
        request = urllib.request.Request(url_null_id + '/contacts', data=self.data_new_contacts, method='PUT')
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

    # Update Study Contacts - BadId - NoSave -> 404
    def test_update_contacts_badId_noSave(self):
        request = urllib.request.Request(url_wrong_id + '/contacts', data=self.data_new_contacts, method='PUT')
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


class GetStudyFactorsTests(WsTests):

    def check_Factors_class(self, obj):
        self.assertIsNotNone(obj['StudyFactors'])
        for factor in obj['StudyFactors']:
            self.assertIsNotNone(factor['name'])
            self.assertIsNotNone(factor['factor_type'])
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
            self.assertIn('StudyFactors', body)
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
            self.assertIn('StudyFactors', body)
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
            self.assertIn('StudyFactors', body)
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
            self.assertIn('StudyFactors', body)
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


class UpdateStudyFactorsTests(WsTests):

    data_new_factors = instance.config.TEST_DATA_STUDYFACTORS

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

    # PUT Update Study Factors - Pub -> 200
    def test_update_factors_pub(self):
        request = urllib.request.Request(url_pub_id + '/factors', data=self.data_new_factors, method='PUT')
        self.add_common_headers(request)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyFactors', body)

    # Update Study Factors - Pub - Auth -> 200
    def test_update_factors_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/factors', data=self.data_new_factors, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyFactors', body)

    # Update Study Factors - Pub - NoAuth -> 403
    def test_update_factors_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/factors', data=self.data_new_factors, method='PUT')
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

    # Update Study Factors - Priv - Auth -> 200
    def test_update_factors_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/factors', data=self.data_new_factors, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyFactors', body)

    # Update Study Factors - Priv -> 401
    def test_update_factors_priv(self):
        request = urllib.request.Request(url_priv_id + '/factors', data=self.data_new_factors, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Factors - Priv - NoAuth -> 403
    def test_update_factors_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/factors', data=self.data_new_factors, method='PUT')
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

    # Update Study Factors - NullId -> 404
    def test_update_factors_nullId(self):
        request = urllib.request.Request(url_null_id + '/factors', data=self.data_new_factors, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Factors - BadId -> 404
    def test_update_factors_badId(self):
        request = urllib.request.Request(url_wrong_id + '/factors', data=self.data_new_factors, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Factors - Pub - NoSave -> 200
    def test_update_factors_pub_noSave(self):
        request = urllib.request.Request(url_pub_id + '/factors', data=self.data_new_factors, method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyFactors', body)

    # Update Study Factors - Pub - Auth - NoSave -> 200
    def test_update_factors_pub_auth_noSave(self):
        request = urllib.request.Request(url_pub_id + '/factors', data=self.data_new_factors, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyFactors', body)

    # Update Study Factors - Pub - NoAuth - NoSave -> 200
    def test_update_factors_pub_noAuth_noSave(self):
        request = urllib.request.Request(url_pub_id + '/factors', data=self.data_new_factors, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyFactors', body)

    # Update Study Factors - Priv - Auth - NoSave -> 200
    def test_update_factors_priv_auth_noSave(self):
        request = urllib.request.Request(url_priv_id + '/factors', data=self.data_new_factors, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyFactors', body)

    # Update Study Factors - Priv - NoSave -> 401
    def test_update_factors_priv_noSave(self):
        request = urllib.request.Request(url_priv_id + '/factors', data=self.data_new_factors, method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Factors - Priv - NoAuth - NoSave -> 403
    def test_update_factors_priv_noAuth_noSave(self):
        request = urllib.request.Request(url_priv_id + '/factors', data=self.data_new_factors, method='PUT')
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

    # Update Study Factors - NullId - NoSave -> 404
    def test_update_factors_nullId_noSave(self):
        request = urllib.request.Request(url_null_id + '/factors', data=self.data_new_factors, method='PUT')
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

    # Update Study Factors - BadId - NoSave -> 404
    def test_update_factors_badId_noSave(self):
        request = urllib.request.Request(url_wrong_id + '/factors', data=self.data_new_factors, method='PUT')
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


class GetStudyDescriptorsTests(WsTests):

    def check_Descriptors_class(self, obj):
        self.assertIsNotNone(obj['StudyDescriptors'])
        for descriptor in obj['StudyDescriptors']:
            self.assertIsNotNone(descriptor['term'])
            self.assertIsNotNone(descriptor['term_accession'])
            self.assertIsNotNone(descriptor['term_source'])
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
            self.assertIn('StudyDescriptors', body)
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
            self.assertIn('StudyDescriptors', body)
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
            self.assertIn('StudyDescriptors', body)
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
            self.assertIn('StudyDescriptors', body)
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


class UpdateStudyDescriptorsTests(WsTests):

    data_new_descriptors = b'{"StudyDescriptors": [' \
                           b'{"term_source": {"version": null,"description": null,"file": null,' \
                           b'"comments": [],"name": null},' \
                           b'"comments": [{"name": "Updated","value": "Updated with MtblsWs-Py"}],' \
                           b'"term":"nuclear magnetic resonance spectroscopy",' \
                           b'"term_accession":"http://purl.obolibrary.org/obo/CHMO_0000591"},' \
                           b'{"term_source": {"version": null,"description": null,"file": null,' \
                           b'"comments": [],"name": null},' \
                           b'"comments": [{"name": "Updated","value": "Updated with MtblsWs-Py"}],' \
                           b'"term": "metabolic syndrome",' \
                           b'"term_accession": "http://www.ebi.ac.uk/efo/EFO_0000195"' \
                           b'}]}'

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

    def check_Descriptors_class(self, obj):
        self.assertIsNotNone(obj['StudyDescriptors'])
        for descriptor in obj['StudyDescriptors']:
            self.assertIsNotNone(descriptor['term'])
            self.assertIsNotNone(descriptor['term_accession'])
            self.assertIsNotNone(descriptor['term_source'])
            self.assertIsNotNone(descriptor['comments'])

    # PUT Update Study Descriptors - Pub -> 200
    def test_update_descriptors_pub(self):
        request = urllib.request.Request(url_pub_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
        self.add_common_headers(request)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('StudyDescriptors', body)
            self.check_Descriptors_class(j_resp)

    # Update Study Descriptors - Pub - Auth -> 200
    def test_update_descriptors_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('StudyDescriptors', body)
            self.check_Descriptors_class(j_resp)

    # Update Study Descriptors - Pub - NoAuth -> 403
    def test_update_descriptors_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
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

    # Update Study Descriptors - Priv - Auth -> 200
    def test_update_descriptors_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('StudyDescriptors', body)
            self.check_Descriptors_class(j_resp)

    # Update Study Descriptors - Priv -> 401
    def test_update_descriptors_priv(self):
        request = urllib.request.Request(url_priv_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Descriptors - Priv - NoAuth -> 403
    def test_update_descriptors_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
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

    # Update Study Descriptors - NullId -> 404
    def test_update_descriptors_nullId(self):
        request = urllib.request.Request(url_null_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Descriptors - BadId -> 404
    def test_update_descriptors_badId(self):
        request = urllib.request.Request(url_wrong_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Descriptors - Pub - NoSave -> 200
    def test_update_descriptors_pub_noSave(self):
        request = urllib.request.Request(url_pub_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('StudyDescriptors', body)
            self.check_Descriptors_class(j_resp)

    # Update Study Descriptors - Pub - Auth - NoSave -> 200
    def test_update_descriptors_pub_auth_noSave(self):
        request = urllib.request.Request(url_pub_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('StudyDescriptors', body)
            self.check_Descriptors_class(j_resp)

    # Update Study Descriptors - Pub - NoAuth - NoSave -> 200
    def test_update_descriptors_pub_noAuth_noSave(self):
        request = urllib.request.Request(url_pub_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('StudyDescriptors', body)
            self.check_Descriptors_class(j_resp)

    # Update Study Descriptors - Priv - Auth - NoSave -> 200
    def test_update_descriptors_priv_auth_noSave(self):
        request = urllib.request.Request(url_priv_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('StudyDescriptors', body)
            self.check_Descriptors_class(j_resp)

    # Update Study Descriptors - Priv - NoSave -> 401
    def test_update_descriptors_priv_noSave(self):
        request = urllib.request.Request(url_priv_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Descriptors - Priv - NoAuth - NoSave -> 403
    def test_update_descriptors_priv_noAuth_noSave(self):
        request = urllib.request.Request(url_priv_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
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

    # Update Study Descriptors - NullId - NoSave -> 404
    def test_update_descriptors_nullId_noSave(self):
        request = urllib.request.Request(url_null_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
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

    # Update Study Descriptors - BadId - NoSave -> 404
    def test_update_descriptors_badId_noSave(self):
        request = urllib.request.Request(url_wrong_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
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


# tests for GET Method for Study Publications
#
# pubmed_id (str, NoneType):
# doi (str, NoneType):
# author_list (str, NoneType):
# title (str, NoneType):
# status (str, OntologyAnnotation, NoneType):
# comments (list, Comment):
class GetStudyPublicationsTests(WsTests):

    def check_Publications_class(self, obj):
        self.assertIsNotNone(obj['publications'])
        for publication in obj['publications']:
            self.assertIsNotNone(publication['pubMedID'])
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


class UpdateStudyPublicationsTests(WsTests):

    data_new_publications = instance.config.TEST_DATA_PUBLICATIONS

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

    def check_Publications_class(self, obj):
        self.assertIsNotNone(obj['publications'])
        for publication in obj['publications']:
            self.assertIsNotNone(publication['title'])
            self.assertIsNotNone(publication['status'])
            self.assertIsNotNone(publication['doi'])
            self.assertIsNotNone(publication['authorList'])
            self.assertIsNotNone(publication['pubMedID'])
            self.assertIsNotNone(publication['comments'])

    # PUT Update Study Publications - Pub -> 200
    def test_update_publications_pub(self):
        request = urllib.request.Request(url_pub_id + '/publications', data=self.data_new_publications, method='PUT')
        self.add_common_headers(request)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('publications', body)
            self.check_Publications_class(j_resp)

    # Update Study Publications - Pub - Auth -> 200
    def test_update_publications_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/publications', data=self.data_new_publications, method='PUT')
        self.add_common_headers(request)
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

    # Update Study Publications - Pub - NoAuth -> 403
    def test_update_publications_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/publications', data=self.data_new_publications, method='PUT')
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

    # Update Study Publications - Priv - Auth -> 200
    def test_update_publications_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/publications', data=self.data_new_publications, method='PUT')
        self.add_common_headers(request)
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

    # Update Study Publications - Priv -> 401
    def test_update_publications_priv(self):
        request = urllib.request.Request(url_priv_id + '/publications', data=self.data_new_publications, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Publications - Priv - NoAuth -> 403
    def test_update_publications_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/publications', data=self.data_new_publications, method='PUT')
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

    # Update Study Publications - NullId -> 404
    def test_update_publications_nullId(self):
        request = urllib.request.Request(url_null_id + '/publications', data=self.data_new_publications, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Publications - BadId -> 404
    def test_update_publications_badId(self):
        request = urllib.request.Request(url_wrong_id + '/publications', data=self.data_new_publications, method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Publications - Pub - NoSave -> 200
    def test_update_publications_pub_noSave(self):
        request = urllib.request.Request(url_pub_id + '/publications', data=self.data_new_publications, method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('publications', body)
            self.check_Publications_class(j_resp)

    # Update Study Publications - Pub - Auth - NoSave -> 200
    def test_update_publications_pub_auth_noSave(self):
        request = urllib.request.Request(url_pub_id + '/publications', data=self.data_new_publications, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('publications', body)
            self.check_Publications_class(j_resp)

    # Update Study Publications - Pub - NoAuth - NoSave -> 200
    def test_update_publications_pub_noAuth_noSave(self):
        request = urllib.request.Request(url_pub_id + '/publications', data=self.data_new_publications, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('publications', body)
            self.check_Publications_class(j_resp)

    # Update Study Publications - Priv - Auth - NoSave -> 200
    def test_update_publications_priv_auth_noSave(self):
        request = urllib.request.Request(url_priv_id + '/publications', data=self.data_new_publications, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('publications', body)
            self.check_Publications_class(j_resp)

    # Update Study Publications - Priv - NoSave -> 401
    def test_update_publications_priv_noSave(self):
        request = urllib.request.Request(url_priv_id + '/publications', data=self.data_new_publications, method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Publications - Priv - NoAuth - NoSave -> 403
    def test_update_publications_priv_noAuth_noSave(self):
        request = urllib.request.Request(url_priv_id + '/publications', data=self.data_new_publications, method='PUT')
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

    # Update Study Publications - NullId - NoSave -> 404
    def test_update_publications_nullId_noSave(self):
        request = urllib.request.Request(url_null_id + '/publications', data=self.data_new_publications, method='PUT')
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

    # Update Study Publications - BadId - NoSave -> 404
    def test_update_publications_badId_noSave(self):
        request = urllib.request.Request(url_wrong_id + '/publications', data=self.data_new_publications, method='PUT')
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


# tests for GET Method for list of sources names associated with the Study
#
# name (str):
class GetStudySourcesTests(WsTests):

    def check_Sources_class(self, obj):
        self.assertIsNotNone(obj['Study-sources'])
        for sample in obj['Study-sources']:
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
            self.assertIn('Study-sources', body)
            self.check_Sources_class(j_resp)


# tests for GET Method for Study Source, by name
#
# name (str):
# characteristics (list, OntologyAnnotation):
# comments (list, Comment):
class GetStudySourceTests(WsTests):

    def check_Source_class(self, obj):
        self.assertIsNotNone(obj['Study_source'])
        source = obj['Study_source']
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
            self.assertIn('Study_source', body)
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
            self.assertIn('Study_source', body)
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
            self.assertIn('Study_source', body)
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
            self.assertIn('Study_source', body)
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


class UpdateStudySourceTests(WsTests):

    data_updated_pub_source = instance.config.TEST_DATA_PUB_SOURCE
    data_updated_priv_source = instance.config.TEST_DATA_PRIV_SOURCE

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

    def check_Source_class(self, obj):
        self.assertIsNotNone(obj['Updated_source'])
        source = obj['Updated_source']
        self.assertIsNotNone(source['name'])
        self.assertIsNotNone(source['characteristics'])
        self.assertIsNotNone(source['comments'])

    # PUT Update Study Source - Pub -> 200
    def test_update_source_pub(self):
        request = urllib.request.Request(url_pub_id + '/sources/' + public_source_id,
                                         data=self.data_updated_pub_source,
                                         method='PUT')
        self.add_common_headers(request)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('Updated_source', body)
            self.check_Source_class(j_resp)

    # Update Study Source - Pub - Auth -> 200
    def test_update_source_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/sources/' + public_source_id,
                                         data=self.data_updated_pub_source,
                                         method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('Updated_source', body)
            self.check_Source_class(j_resp)

    # Update Study Source - Pub - NoAuth -> 403
    def test_update_source_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/sources/' + public_source_id,
                                         data=self.data_updated_pub_source,
                                         method='PUT')
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

    # Update Study Source - Priv - Auth -> 200
    def test_update_source_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/sources/' + private_source_id,
                                         data=self.data_updated_priv_source,
                                         method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('Updated_source', body)
            self.check_Source_class(j_resp)

    # Update Study Source - Priv -> 401
    def test_update_source_priv(self):
        request = urllib.request.Request(url_priv_id + '/sources/' + private_source_id,
                                         data=self.data_updated_priv_source,
                                         method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Source - Priv - NoAuth -> 403
    def test_update_source_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/sources/' + private_source_id,
                                         data=self.data_updated_priv_source,
                                         method='PUT')
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

    # Update Study Source - NullId -> 404
    def test_update_source_nullId(self):
        request = urllib.request.Request(url_null_id + '/sources/' + public_source_id,
                                         data=self.data_updated_pub_source,
                                         method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Source - BadId -> 404
    def test_update_source_badId(self):
        request = urllib.request.Request(url_wrong_id + '/sources/' + public_source_id,
                                         data=self.data_updated_pub_source,
                                         method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Source - Pub - NoSave -> 200
    def test_update_source_pub_noSave(self):
        request = urllib.request.Request(url_pub_id + '/sources/' + public_source_id,
                                         data=self.data_updated_pub_source,
                                         method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('Updated_source', body)
            self.check_Source_class(j_resp)

    # Update Study Source - Pub - Auth - NoSave -> 200
    def test_update_source_pub_auth_noSave(self):
        request = urllib.request.Request(url_pub_id + '/sources/' + public_source_id,
                                         data=self.data_updated_pub_source,
                                         method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('Updated_source', body)
            self.check_Source_class(j_resp)

    # Update Study Source - Pub - NoAuth - NoSave -> 200
    def test_update_source_pub_noAuth_noSave(self):
        request = urllib.request.Request(url_pub_id + '/sources/' + public_source_id,
                                         data=self.data_updated_pub_source,
                                         method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('Updated_source', body)
            self.check_Source_class(j_resp)

    # Update Study Source - Priv - Auth - NoSave -> 200
    def test_update_source_priv_auth_noSave(self):
        request = urllib.request.Request(url_priv_id + '/sources/' + private_source_id,
                                         data=self.data_updated_priv_source,
                                         method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('Updated_source', body)
            self.check_Source_class(j_resp)

    # Update Study Source - Priv - NoSave -> 401
    def test_update_source_priv_noSave(self):
        request = urllib.request.Request(url_priv_id + '/sources/' + private_source_id,
                                         data=self.data_updated_priv_source,
                                         method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Source - Priv - NoAuth - NoSave -> 403
    def test_update_source_priv_noAuth_noSave(self):
        request = urllib.request.Request(url_priv_id + '/sources/' + private_source_id,
                                         data=self.data_updated_priv_source,
                                         method='PUT')
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

    # Update Study Source - NullId - NoSave -> 404
    def test_update_source_nullId_noSave(self):
        request = urllib.request.Request(url_null_id + '/sources/' + public_source_id,
                                         data=self.data_updated_pub_source,
                                         method='PUT')
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

    # Update Study Source - BadId - NoSave -> 404
    def test_update_source_badId_noSave(self):
        request = urllib.request.Request(url_wrong_id + '/sources/' + public_source_id,
                                         data=self.data_updated_pub_source,
                                         method='PUT')
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


# tests for GET Method for list of samples names associated with the Study
#
# name (str):
class GetStudySamplesTests(WsTests):

    def check_Samples_class(self, obj):
        self.assertIsNotNone(obj['Study-samples'])
        for sample in obj['Study-samples']:
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
            self.assertIn('Study-samples', body)
            self.check_Samples_class(j_resp)


# tests for GET Method for Study Sample, by name
#
# name (str):
# characteristics (list, OntologyAnnotation):
# derives_from (Source):
# factor_values (FactorValues):
# comments (list, Comment):
class GetStudySampleTests(WsTests):

    def check_Sample_class(self, obj):
        self.assertIsNotNone(obj['Study_sample'])
        sample = obj['Study_sample']
        self.assertIsNotNone(sample['name'])
        self.assertIsNotNone(sample['derives_from'])
        self.assertIsNotNone(sample['characteristics'])
        self.assertIsNotNone(sample['factor_values'])
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
            self.assertIn('Study_sample', body)
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
            self.assertIn('Study_sample', body)
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
            self.assertIn('Study_sample', body)
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
            self.assertIn('Study_sample', body)
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


class UpdateStudySampleTests(WsTests):

    data_updated_pub_sample = instance.config.TEST_DATA_PUB_SAMPLE
    data_updated_priv_sample = instance.config.TEST_DATA_PRIV_SAMPLE

    def tearDown(self):
        time.sleep(1)  # sleep time in seconds

    def check_Sample_class(self, obj):
        self.assertIsNotNone(obj['Updated_sample'])
        sample = obj['Updated_sample']
        self.assertIsNotNone(sample['name'])
        self.assertIsNotNone(sample['derives_from'])
        self.assertIsNotNone(sample['characteristics'])
        self.assertIsNotNone(sample['factor_values'])
        self.assertIsNotNone(sample['comments'])

    # PUT Update Study Sample - Pub -> 200
    def test_update_sample_pub(self):
        request = urllib.request.Request(url_pub_id + '/samples/' + public_sample_id,
                                         data=self.data_updated_pub_sample,
                                         method='PUT')
        self.add_common_headers(request)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('Updated_sample', body)
            self.check_Sample_class(j_resp)

    # Update Study Sample - Pub - Auth -> 200
    def test_update_sample_pub_auth(self):
        request = urllib.request.Request(url_pub_id + '/samples/' + public_sample_id,
                                         data=self.data_updated_pub_sample,
                                         method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('Updated_sample', body)
            self.check_Sample_class(j_resp)

    # Update Study Sample - Pub - NoAuth -> 403
    def test_update_sample_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/samples/' + public_sample_id,
                                         data=self.data_updated_pub_sample,
                                         method='PUT')
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

    # Update Study Sample - Priv - Auth -> 200
    def test_update_sample_priv_auth(self):
        request = urllib.request.Request(url_priv_id + '/samples/' + private_sample_id,
                                         data=self.data_updated_priv_sample,
                                         method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('Updated_sample', body)
            self.check_Sample_class(j_resp)

    # Update Study Sample - Priv -> 401
    def test_update_sample_priv(self):
        request = urllib.request.Request(url_priv_id + '/samples/' + private_sample_id,
                                         data=self.data_updated_priv_sample,
                                         method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Sample - Priv - NoAuth -> 403
    def test_update_sample_priv_noAuth(self):
        request = urllib.request.Request(url_priv_id + '/samples/' + private_sample_id,
                                         data=self.data_updated_priv_sample,
                                         method='PUT')
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

    # Update Study Sample - NullId -> 404
    def test_update_sample_nullId(self):
        request = urllib.request.Request(url_null_id + '/samples/' + public_sample_id,
                                         data=self.data_updated_pub_sample,
                                         method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Sample - BadId -> 404
    def test_update_sample_badId(self):
        request = urllib.request.Request(url_wrong_id + '/samples/' + public_sample_id,
                                         data=self.data_updated_pub_sample,
                                         method='PUT')
        self.add_common_headers(request)
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # Update Study Sample - Pub - NoSave -> 200
    def test_update_sample_pub_noSave(self):
        request = urllib.request.Request(url_pub_id + '/samples/' + public_sample_id,
                                         data=self.data_updated_pub_sample,
                                         method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('Updated_sample', body)
            self.check_Sample_class(j_resp)

    # Update Study Sample - Pub - Auth - NoSave -> 200
    def test_update_sample_pub_auth_noSave(self):
        request = urllib.request.Request(url_pub_id + '/samples/' + public_sample_id,
                                         data=self.data_updated_pub_sample,
                                         method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('Updated_sample', body)
            self.check_Sample_class(j_resp)

    # Update Study Sample - Pub - NoAuth - NoSave -> 200
    def test_update_sample_pub_noAuth_noSave(self):
        request = urllib.request.Request(url_pub_id + '/samples/' + public_sample_id,
                                         data=self.data_updated_pub_sample,
                                         method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('Updated_sample', body)
            self.check_Sample_class(j_resp)

    # Update Study Sample - Priv - Auth - NoSave -> 200
    def test_update_sample_priv_auth_noSave(self):
        request = urllib.request.Request(url_priv_id + '/samples/' + private_sample_id,
                                         data=self.data_updated_priv_sample,
                                         method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', auth_id)
        request.add_header('save_audit_copy', 'False')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('Updated_sample', body)
            self.check_Sample_class(j_resp)

    # Update Study Sample - Priv - NoSave -> 401
    def test_update_sample_priv_noSave(self):
        request = urllib.request.Request(url_priv_id + '/samples/' + private_sample_id,
                                         data=self.data_updated_priv_sample,
                                         method='PUT')
        self.add_common_headers(request)
        request.add_header('save_audit_copy', 'False')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 401)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('UNAUTHORIZED', err.msg)
            self.assertEqual('UNAUTHORIZED', err.reason)

    # Update Study Sample - Priv - NoAuth - NoSave -> 403
    def test_update_sample_priv_noAuth_noSave(self):
        request = urllib.request.Request(url_priv_id + '/samples/' + private_sample_id,
                                         data=self.data_updated_priv_sample,
                                         method='PUT')
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

    # Update Study Sample - NullId - NoSave -> 404
    def test_update_sample_nullId_noSave(self):
        request = urllib.request.Request(url_null_id + '/samples/' + public_sample_id,
                                         data=self.data_updated_pub_sample,
                                         method='PUT')
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

    # Update Study Sample - BadId - NoSave -> 404
    def test_update_sample_badId_noSave(self):
        request = urllib.request.Request(url_wrong_id + '/samples/' + public_sample_id,
                                         data=self.data_updated_pub_sample,
                                         method='PUT')
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


if __name__ == '__main__':
    unittest.main()
