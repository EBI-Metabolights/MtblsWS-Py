import json
import urllib.request
import urllib.error
import unittest
import time

url_about = 'http://localhost:5000/mtbls/ws'
public_study_id = 'MTBLS1'
private_study_id = 'MTBLS48'
bad_study_id = '123456'
auth_id = '0f8ceff2-3a39-487b-9b77-6da415c7f3e8'
wrong_auth_token = '1234567890'
url_base = 'http://localhost:5000/mtbls/ws/study/'
url_pub_id = url_base + public_study_id
url_priv_id = url_base + private_study_id
url_null_id = url_base
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

    # Update Study Title - Pub - NoAuth -> 200
    def test_update_title_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/title', data=self.data_new_title, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-title', body)

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

    # Update Study Description - Pub - NoAuth -> 200
    def test_update_description_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/description', data=self.data_new_description, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-description', body)

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

    data_new_study = b'{ "title": "New Study title...", ' \
                     b' "description": "New Study description...",' \
                     b' "submission_date": "2017-05-08",' \
                     b' "public_release_date": "2018-05-08" }'

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
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


class UpdateStudyProtocolsTests(WsTests):

    data_new_protocol = b'{"StudyProtocols": [{' \
                        b'"comments": [{"name": "Updated","value": "Updated with MtblsWs-Py"}],' \
                        b'"components": [],"description": "Protocol description here.",' \
                        b'"name": "Protocol name here.","parameters": [{"comments": [],' \
                        b'"parameter_name": {"comments": [],"term": "Parameter name here.",' \
                        b'"term_accession": "","term_source": null},' \
                        b'"unit": null}],' \
                        b'"protocol_type": {"comments": [],' \
                        b'"term": "Protocol type name here","term_accession": "","term_source": null},' \
                        b'"uri": "",' \
                        b'"version": "{{$randomInt}}"}]}'

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

    # Update Study Protocols - Pub - NoAuth -> 200
    def test_update_protocols_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/protocols', data=self.data_new_protocol, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyProtocols', body)
            self.assertIn('Updated with MtblsWs-Py', body)

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

    # Update Study Contacts - Pub - NoAuth -> 200
    def test_update_contacts_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/contacts', data=self.data_new_contacts, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyContacts', body)

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
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


class UpdateStudyFactorsTests(WsTests):

    data_new_factors = b'{"StudyFactors": [{' \
                       b'"comments": [{"name": "Updated","value": "Updated with MtblsWs-Py"}],' \
                       b'"factor_type": {"term_accession": "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl",' \
                       b'"comments": [],"term": "Gender","term_source": {' \
                       b'"description": null,"name": null,"version": null,"file": null,' \
                       b'"comments": []}},"name": "Gender"},' \
                       b'{"comments": [{"name": "Updated","value": "Updated with MtblsWs-Py"}],' \
                       b'"factor_type": {"term_accession": "","comments": [],' \
                       b'"term": "metabolic syndrome","term_source": {' \
                       b'"description": null,"name": null,"version": null,"file": null,' \
                       b'"comments": []}},"name": "Metabolic syndrome"}]}'

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

    # Update Study Factors - Pub - NoAuth -> 200
    def test_update_factors_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/factors', data=self.data_new_factors, method='PUT')
        self.add_common_headers(request)
        request.add_header('user_token', wrong_auth_token)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('StudyFactors', body)

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

    # Update Study Descriptors - Pub - NoAuth -> 200
    def test_update_descriptors_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/descriptors', data=self.data_new_descriptors, method='PUT')
        self.add_common_headers(request)
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
    def test_update_descriptorspriv_noAuth(self):
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
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)


class UpdateStudyPublicationsTests(WsTests):

    data_new_publications = b'{"publications": [{' \
                           b'"title": "Updated: Title for this publication.",' \
                           b'"status": {' \
                           b' "term_source": {"version": null,"description": null,"file": null,' \
                           b'"comments": [],"name": null},' \
                           b'"comments": [{"name": "Updated","value": "Updated with MtblsWs-Py"}],' \
                           b'"term":"Published",' \
                           b'"term_accession":""},' \
                           b'"comments": [{"name": "Updated","value": "Updated with MtblsWs-Py"}],' \
                           b'"doi": "http://dx.doi.org/10.1105/tpc.109.066670",' \
                           b'"authorList": "Author A, Author B, Author C",' \
                           b'"pubMedID": "123456789"' \
                           b'}]}'

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

    # Update Study Publications - Pub - NoAuth -> 200
    def test_update_publications_pub_noAuth(self):
        request = urllib.request.Request(url_pub_id + '/publications', data=self.data_new_publications, method='PUT')
        self.add_common_headers(request)
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


# tests for GET Method for Study Samples
#
# pubmed_id (str, NoneType):
# doi (str, NoneType):
# author_list (str, NoneType):
# title (str, NoneType):
# status (str, OntologyAnnotation, NoneType):
# comments (list, Comment):
class GetStudySamplesTests(WsTests):

    def check_Samples_class(self, obj):
        self.assertIsNotNone(obj['samples'])
        for publication in obj['publications']:
            self.assertIsNotNone(publication['pubMedID'])
            self.assertIsNotNone(publication['authorList'])
            self.assertIsNotNone(publication['title'])
            self.assertIsNotNone(publication['status'])
            self.assertIsNotNone(publication['comments'])

    # Get Study Samples - Pub -> 200
    def test_get_descriptors(self):
        request = urllib.request.Request(url_pub_id + '/samples', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            j_resp = json.loads(body)
            self.assertIn('publications', body)
            self.check_Samples_class(j_resp)


if __name__ == '__main__':
    unittest.main()
