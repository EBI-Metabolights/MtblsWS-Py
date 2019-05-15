import pyopenms
import logging
import os
import json
from flask import current_app as app
from flask import request, abort
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from pyopenms import *

logger = logging.getLogger('wslog')

# MetaboLights (Java-Based) WebService client
wsc = WsClient()


class ExtractMSSpectra(Resource):
    #  See: https://pypi.org/project/pyopenms/

    def get_spectrum(self, filepath, retention_time):
        float_format = "{0:.4f}"
        peak_list = []
        mz_list = []
        path = str.encode(filepath)

        # rt_num = None
        # if retention_time:
        #     try:
        #         rt_num = int(retention_time)
        #     except ValueError:
        #         rt_num = float(retention_time)
        #     rt_num = float_format.format(rt_num)

        try:
            exp = pyopenms.MSExperiment()
            pyopenms.FileHandler().loadExperiment(path, exp)
        except Exception as error:
            print(str(error))
        for spectrum in exp:
            rt = str(spectrum.getRT())
            # So either no rt param passed, or the decimal input in the mzml rt float (no rounding)
            if not retention_time or retention_time in rt:
                for peak in spectrum:
                    peak_list.append({"intensity": peak.getIntensity(), "mz": peak.getMZ(), "rt": rt})
                    mz_list.append(peak.getMZ())

        return peak_list, mz_list

    def create_mtbls_peak_list(self, filepath, retention_time):
        peak_list, mz_list = self.get_spectrum(filepath, retention_time)
        try:
            mz_start = min(mz_list)
            mz_stop = max(mz_list)
        except:
            mz_start = 0
            mz_stop = 0

        return peak_list, mz_list, mz_start, mz_stop

    def write_json(self, filename, data):
        with open(filename, 'w') as outfile:
            json.dump(data, outfile)

    @swagger.operation(
        summary="Generate SpeckTackle spectra (curator only)",
        nickname="SpeckTackle spectra",
        notes="Extract coordinates from mzML files so we can visualise in SpeckTackle",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "mzml_file_name",
                "description": "mzML file name",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "retention_time",
                "description": "Retention time to extract peaks for, (Use up to 3 decimal places)",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "mtbls_compound_id",
                "description": "MetaboLights compound id (MTBLCxxxxxx)",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The Metabolite Annotation File (MAF) is returned"
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def post(self, study_id):

        # param validation
        if study_id is None:
            abort(404, 'Please provide valid parameter for study identifier')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not is_curator:
            abort(403)

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('mzml_file_name', help="mzML file", location="args")
        parser.add_argument('mtbls_compound_id', help="MTBLS compound id", location="args")
        parser.add_argument('retention_time', help="RT for peaks", location="args")
        args = parser.parse_args()
        mzml_file_name = args['mzml_file_name'].strip()
        mtbls_compound_id = args['mtbls_compound_id'].strip()
        retention_time = args['retention_time']
        if retention_time:
            retention_time = retention_time.strip()

        if mzml_file_name:
            full_mzml_file_name = os.path.join(study_location, mzml_file_name)

        peak_list, mz_list, mz_start, mz_stop = self.create_mtbls_peak_list(full_mzml_file_name, retention_time)
        short_name = mzml_file_name.replace(".mzML", "")
        json_file_name = mtbls_compound_id + '-' + short_name + ".json"

        data = {"mzStart": mz_start, "mzStop": mz_stop, "spectrumId": short_name,
                "fileName": json_file_name, "peaks": peak_list}
        self.write_json(os.path.join(study_location, json_file_name), data)

        return {"mzStart": mz_start, "mzStop": mz_stop,
                "spectrumId": short_name,
                "number of peaks": len(peak_list)}
