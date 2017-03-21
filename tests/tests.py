import urllib.request
import unittest

url_about = 'http://localhost:5000/mtbls/ws'
PubStudyId = 'MTBLS1'
PrivStudyId = 'MTBLS48'
BadStudyId = '123456'
CuratorID = '0f8ceff2-3a39-487b-9b77-6da415c7f3e8'
WrongAuth = '1234567890'
url_pubId = 'http://localhost:5000/mtbls/ws/study/' + PubStudyId
url_privId = 'http://localhost:5000/mtbls/ws/study/' + PrivStudyId
url_nullId = 'http://localhost:5000/mtbls/ws/study/'
url_badId = 'http://localhost:5000/mtbls/ws/study/' + BadStudyId


class WsTests(unittest.TestCase):

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


class GetStudyTitleTests(WsTests):

    # GET Study Title - Pub -> 200
    def test_get_title(self):
        request = urllib.request.Request(url_pubId + '/title', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-title', body)

    # GET Study Title - Pub - Auth -> 200
    def test_get_title_pub_auth(self):
        request = urllib.request.Request(url_pubId + '/title', method='GET')
        request.add_header('user_token', CuratorID)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-title', body)

    # GET Study Title - Pub - NoAuth -> 200
    def test_get_title_pub_noAuth(self):
        request = urllib.request.Request(url_pubId + '/title', method='GET')
        request.add_header('user_token', WrongAuth)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-title', body)

    # GET Study Title - Priv - Auth -> 200
    def test_get_title_priv_auth(self):
        request = urllib.request.Request(url_privId + '/title', method='GET')
        request.add_header('user_token', CuratorID)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-title', body)

    # GET Study Title - Priv -> 401
    def test_get_title_priv(self):
        request = urllib.request.Request(url_privId + '/title', method='GET')
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
        request = urllib.request.Request(url_privId + '/title', method='GET')
        request.add_header('user_token', WrongAuth)
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
        request = urllib.request.Request(url_nullId + '/title', method='GET')
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
        request = urllib.request.Request(url_badId + '/title', method='GET')
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
            self.check_header_common(err.headers)
            self.check_body_common(err.read().decode('utf-8'))
            self.assertEqual('NOT FOUND', err.msg)
            self.assertEqual('NOT FOUND', err.reason)

    # def test_request_title_update(self):
    #     data = b'{ "title": "New Study title..." }'
    #     request = urllib.request.Request(self.url_update_title, data=data, method='PUT')
    #     request.add_header('user_token', self.user_token)
    #
    #     with urllib.request.urlopen(request) as response:
    #         header = response.headers
    #         self.assertIn('Content-Type', header)
    #
    #         body = response.read().decode('utf-8')
    #         self.assertIn('Study-title', body)


class GetStudyDescriptionTests(WsTests):

    # GET Study Description - Pub -> 200
    def test_get_desc(self):
        request = urllib.request.Request(url_pubId + '/description', method='GET')
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-description', body)

    # GET Study Description - Pub - Auth -> 200
    def test_get_desc_pub_auth(self):
        request = urllib.request.Request(url_pubId + '/description', method='GET')
        request.add_header('user_token', CuratorID)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-description', body)

    # GET Study Description - Pub - NoAuth -> 200
    def test_get_desc_pub_noAuth(self):
        request = urllib.request.Request(url_pubId + '/description', method='GET')
        request.add_header('user_token', WrongAuth)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-description', body)

    # GET Study Description - Priv - Auth -> 200
    def test_get_desc_priv_auth(self):
        request = urllib.request.Request(url_privId + '/description', method='GET')
        request.add_header('user_token', CuratorID)
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.code, 200)
            header = response.info()
            self.check_header_common(header)
            body = response.read().decode('utf-8')
            self.check_body_common(body)
            self.assertIn('Study-description', body)

    # GET Study Description - Priv -> 401
    def test_get_desc_priv(self):
        request = urllib.request.Request(url_privId + '/description', method='GET')
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
        request = urllib.request.Request(url_privId + '/description', method='GET')
        request.add_header('user_token', WrongAuth)
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
        request = urllib.request.Request(url_nullId + '/description', method='GET')
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
        request = urllib.request.Request(url_badId + '/description', method='GET')
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
