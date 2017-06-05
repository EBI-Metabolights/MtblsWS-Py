import urllib.request
import urllib.error
import unittest

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
new_study_title = b'"New Study title..."'
new_study_description = b'"New Study description..."'
submission_date = b'"2017-05-08"'
public_release_date = b'"2018-05-08"'
data_new_title = b'{ "title":' + new_study_title + b' }'
data_new_description = b'{ "description": ' + new_study_description + b' }'
data_new_study = b'{ "title": "New Study title...", '\
                 b' "description": ' + new_study_description + b',' \
                 b' "submission_date": ' + submission_date + b',' \
                 b' "public_release_date": ' + public_release_date + b' }'


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

    # PUT Update Study Title - Pub -> 200
    def test_update_title_pub(self):
        request = urllib.request.Request(url_pub_id + '/title', data=data_new_title, method='PUT')
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
        request = urllib.request.Request(url_pub_id + '/title', data=data_new_title, method='PUT')
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
        request = urllib.request.Request(url_pub_id + '/title', data=data_new_title, method='PUT')
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
        request = urllib.request.Request(url_priv_id + '/title', data=data_new_title, method='PUT')
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
        request = urllib.request.Request(url_priv_id+ '/title', data=data_new_title, method='PUT')
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
        request = urllib.request.Request(url_priv_id + '/title', data=data_new_title, method='PUT')
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
        request = urllib.request.Request(url_null_id + '/title', data=data_new_title, method='PUT')
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
        request = urllib.request.Request(url_wrong_id + '/title', data=data_new_title, method='PUT')
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
        request = urllib.request.Request(url_pub_id + '/title', data=data_new_title, method='PUT')
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
        request = urllib.request.Request(url_pub_id + '/title', data=data_new_title, method='PUT')
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
        request = urllib.request.Request(url_pub_id + '/title', data=data_new_title, method='PUT')
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
        request = urllib.request.Request(url_priv_id + '/title', data=data_new_title, method='PUT')
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
        request = urllib.request.Request(url_priv_id + '/title', data=data_new_title, method='PUT')
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
        request = urllib.request.Request(url_priv_id + '/title', data=data_new_title, method='PUT')
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
        request = urllib.request.Request(url_null_id + '/title', data=data_new_title, method='PUT')
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
        request = urllib.request.Request(url_wrong_id + '/title', data=data_new_title, method='PUT')
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

    # PUT Update Study Description - Pub -> 200
    def test_update_description_pub(self):
        request = urllib.request.Request(url_pub_id + '/description', data=data_new_description, method='PUT')
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
        request = urllib.request.Request(url_pub_id + '/description', data=data_new_description, method='PUT')
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
        request = urllib.request.Request(url_pub_id + '/description', data=data_new_description, method='PUT')
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
        request = urllib.request.Request(url_priv_id + '/description', data=data_new_description, method='PUT')
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
        request = urllib.request.Request(url_priv_id + '/description', data=data_new_description, method='PUT')
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
        request = urllib.request.Request(url_priv_id + '/description', data=data_new_description, method='PUT')
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
        request = urllib.request.Request(url_null_id + '/description', data=data_new_description, method='PUT')
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
        request = urllib.request.Request(url_wrong_id + '/description', data=data_new_description, method='PUT')
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
        request = urllib.request.Request(url_pub_id + '/description', data=data_new_description, method='PUT')
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
        request = urllib.request.Request(url_pub_id + '/description', data=data_new_description, method='PUT')
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
        request = urllib.request.Request(url_pub_id + '/description', data=data_new_description, method='PUT')
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
        request = urllib.request.Request(url_priv_id + '/description', data=data_new_description, method='PUT')
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
        request = urllib.request.Request(url_priv_id + '/description', data=data_new_description, method='PUT')
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
        request = urllib.request.Request(url_priv_id + '/description', data=data_new_description, method='PUT')
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
        request = urllib.request.Request(url_null_id + '/description', data=data_new_description, method='PUT')
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
        request = urllib.request.Request(url_wrong_id + '/description', data=data_new_description, method='PUT')
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

    # POST New Study - Auth -> 200
    def test_post_new_study_auth(self):
        request = urllib.request.Request(url_base + 'new', data=data_new_study, method='POST')
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
        request = urllib.request.Request(url_base + 'new', data=data_new_study, method='POST')
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
        request = urllib.request.Request(url_base + 'new', data=data_new_study, method='POST')
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
            self.assertIn('Study-protocols', body)

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
            self.assertIn('Study-protocols', body)

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
            self.assertIn('Study-protocols', body)

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
            self.assertIn('Study-protocols', body)

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


if __name__ == '__main__':
    unittest.main()
