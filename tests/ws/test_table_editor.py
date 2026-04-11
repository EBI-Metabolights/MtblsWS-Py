import json
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd

from app.config import get_settings
from app.ws.table_editor import ColumnsRows


class TestColumnsRows:
    def test_put_skips_out_of_range_cell_update(self, flask_app, monkeypatch):
        study_id = "MTBLS14133"
        file_name = "a_MTBLS14133_LC-MS_negative_reverse-phase.txt"
        payload = {
            "data": [
                {"row": 0, "column": 0, "value": "ok"},
                {"row": 0, "column": 1, "value": "ok2"},
                {"row": 0, "column": 2, "value": "extra"},
            ]
        }
        table_df = pd.DataFrame([[1, 2]], columns=["c1", "c2"], dtype=object)

        monkeypatch.setattr(
            "app.ws.table_editor.validate_submission_update",
            lambda *args, **kwargs: SimpleNamespace(
                context=SimpleNamespace(study_id=study_id)
            ),
        )
        monkeypatch.setattr(
            "app.ws.table_editor.get_study_metadata_path",
            lambda _study_id: f"/tmp/{study_id}",
        )
        monkeypatch.setattr(
            "app.ws.table_editor.read_tsv",
            lambda _path: table_df,
        )
        write_tsv_mock = Mock(return_value="success")
        monkeypatch.setattr("app.ws.table_editor.write_tsv", write_tsv_mock)

        with flask_app.test_request_context(
            f"{get_settings().server.service.resources_path}/studies/{study_id}/cells/{file_name}",
            method="PUT",
            data=json.dumps(payload),
            content_type="application/json",
            headers={"user-token": "token"},
        ):
            response = ColumnsRows().put(study_id, file_name)

        assert response["success"] is True
        assert response["message"] == "success"
        assert response["skipped"] == [{"row": 0, "column": 2, "value": "extra"}]
        assert table_df.iloc[0, 0] == "ok"
        assert table_df.iloc[0, 1] == "ok2"
        write_tsv_mock.assert_called_once()
