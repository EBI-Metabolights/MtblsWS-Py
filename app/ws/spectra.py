#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Jun-03
#  Modified by:   kenneth
#
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import pyopenms
import logging
import json
import os
from flask import request, abort
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from pyopenms import *

logger = logging.getLogger('wslog')

# MetaboLights (Java-Based) WebService client
wsc = WsClient()


class ExtractMSSpectra(Resource):
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
                "description": "Extract peaks for this retention time only. Use as many decimal places as you like",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "mtbls_compound_id",
                "description": "MetaboLights compound id (MTBLCxxxxx)",
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
    def get(self, study_id):

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

        peak_list, mz_list, mz_start, mz_stop, intensity_min, intensity_max, rt_list = \
            self.create_mtblc_peak_list(full_mzml_file_name, retention_time)
        short_name = mzml_file_name.replace(".mzML", "")
        json_file_name = mtbls_compound_id + '-' + short_name + ".json"

        data = {"mzStart": mz_start, "mzStop": mz_stop, "spectrumId": short_name,
                "fileName": json_file_name, "peaks": peak_list}
        self.write_json(os.path.join(study_location, json_file_name), data)

        return {"mzStart": mz_start, "mzStop": mz_stop, "spectrumId": short_name,
                "intensityMin": intensity_min, "intensityMax": intensity_max,
                "retentionTimes": rt_list, "numberOfPeaks": len(peak_list)}

    def get_spectrum(self, filepath, retention_time):
        #  See: https://pypi.org/project/pyopenms/
        peak_list = []
        mz_list = []
        rt_list = []
        intensity_list = []
        path = str.encode(filepath)

        try:
            exp = pyopenms.MSExperiment()
            pyopenms.FileHandler().loadExperiment(path, exp)
        except Exception as error:
            print(str(error))
        for spectrum in exp:
            rt = str(spectrum.getRT())
            # So either no rt param passed, or the decimal input exists in the start of the mzml rt float (no rounding)
            if not retention_time or retention_time in rt:
                if rt not in rt_list:
                    rt_list.append(rt)
                for peak in spectrum:
                    mz = peak.getMZ()
                    intensity = peak.getIntensity()
                    peak_list.append({"intensity": intensity, "mz": mz})
                    mz_list.append(mz)
                    intensity_list.append(intensity)

        return peak_list, mz_list, rt_list, intensity_list

    def create_mtblc_peak_list(self, filepath, retention_time):
        peak_list, mz_list, rt_list, intensity_list = self.get_spectrum(filepath, retention_time)
        try:
            mz_start = min(mz_list)
            mz_stop = max(mz_list)
            intensity_min = min(intensity_list)
            intensity_max = max(intensity_list)
        except:
            mz_start = 0
            mz_stop = 0

        return peak_list, mz_list, mz_start, mz_stop, intensity_min, intensity_max, rt_list

    def write_json(self, filename, data):
        with open(filename, 'w') as outfile:
            json.dump(data, outfile)
