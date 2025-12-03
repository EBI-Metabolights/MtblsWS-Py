import json

context_path = "/metabolights/ws"


class TestIsaStudy(object):
    def test_get_study_contacts_01(self, flask_app, sensitive_data):
        study_id = "MTBLS1"
        with flask_app.test_client() as c:
            headers = {"user-token": sensitive_data.super_user_token_001}
            result = c.get(
                f"{context_path}/studies/{study_id}/contacts", headers=headers, json={}
            )
            assert result is not None
            contacts = json.loads(result.data)
            assert contacts is not None

    def test_post_study_contacts_01(self, flask_app, sensitive_data, mocker):
        """
        Verifies  WsClient reindex_study method update
        """
        study_id = "MTBLS1"
        json_data = {
            "contacts": [
                {
                    "comments": [],
                    "firstName": "Reza",
                    "lastName": "Salek",
                    "email": "rms72@cam.ac.uk",
                    "affiliation": "University of Cambridge",
                    "address": "The Department of Biochemistry, The Sanger Building, 80 Tennis Court Road, Cambridge, CB2 1GA, UK.",
                    "fax": "",
                    "midInitials": "M",
                    "phone": "",
                    "roles": [{"annotationValue": "principal investigator role"}],
                }
            ]
        }

        with flask_app.test_client() as c:
            headers = {
                "user-token": sensitive_data.super_user_token_001,
                "save_audit_copy": True,
            }
            mock_elastic = mocker.Mock()
            mocker.patch(
                "app.ws.elasticsearch.elastic_service.ElasticsearchService.client",
                mock_elastic,
            )
            mock_elastic.index.return_value = ""
            result = c.post(
                f"{context_path}/studies/{study_id}/contacts",
                headers=headers,
                json=json_data,
            )
            assert result is not None
            assert result.status_code in (200, 201)
            mock_elastic.assert_called()
            contacts = json.loads(result.data)
            assert contacts is not None
