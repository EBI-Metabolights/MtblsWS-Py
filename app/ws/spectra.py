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
import datetime
import gc
import json
import logging
import os
import shutil

import pandas
from flask import request
from flask_restful import Resource, abort
from flask_restful_swagger import swagger
from app.utils import current_time
from pyopenms import MSExperiment, FileHandler
from app.config import get_settings

from app.ws.misc_utilities.response_messages import HTTP_200, HTTP_404, HTTP_403, HTTP_401
from app.ws.mtblsWSclient import WsClient
from app.ws.settings.utils import get_study_settings

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
                "name": "user-token",
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
        is_curator, read_access, write_access, obfuscation_code, study_location_deprecated, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not is_curator:
            abort(403)

        # query validation        
        
        mzml_file_name = request.args.get('mzml_file_name').strip()
        mtbls_compound_id = request.args.get('mtbls_compound_id').strip()
        retention_time = request.args.get('retention_time')
        if retention_time:
            retention_time = retention_time.strip()
        full_mzml_file_name = None
        settings = get_study_settings()
        study_metadata_location = os.path.join(settings.mounted_paths.study_metadata_files_root_path, study_id)
        if mzml_file_name:
            full_mzml_file_name = os.path.join(study_metadata_location, mzml_file_name)

        peak_list, mz_list, mz_start, mz_stop, intensity_min, intensity_max, rt_list = \
            self.create_mtblc_peak_list(full_mzml_file_name, retention_time)
        base_file_name = os.path.basename(full_mzml_file_name)
        parent_folder = os.path.dirname(os.path.join(settings.mounted_paths.study_internal_files_root_path, study_id, "spectra", mzml_file_name))
        os.makedirs(parent_folder, exist_ok=True)
        short_name = base_file_name.replace(".mzML", "")
        json_file_name = base_file_name.replace(".mzML", ".json")
        new_file_path = os.path.join(parent_folder, mtbls_compound_id + '-' + json_file_name)

        data = {"mzStart": mz_start, "mzStop": mz_stop, "spectrumId": short_name,
                "fileName": json_file_name, "peaks": peak_list}
        self.write_json(new_file_path, data)

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
            exp = MSExperiment()
            FileHandler().loadExperiment(path, exp)
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
        intensity_min = 0
        intensity_max = 0
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
        try:
            json_object = json.dumps(data, indent=4)
 
            # Writing to sample.json
            with open(filename, "w") as outfile:
                outfile.write(json_object)
        except Exception as e:
            print(f"{str(e)}")


class ZipSpectraFiles(Resource):
    @swagger.operation(
        summary="Generate spectra directory",
        nickname="Grab all spectra files",
        notes="Gets every spectra file / folder, and copies it to a new directory to be later zipped.",
        parameters=[
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            HTTP_200,
            HTTP_401,
            HTTP_403,
            HTTP_404
        ]
    )
    def post(self):

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, __, __, __, study_location, __, __, __ = wsc.get_permissions('MTBLS1', user_token)
        if not is_curator:
            abort(403)

        reporting_path = os.path.join(get_settings().study.mounted_paths.reports_root_path, 
                                      get_settings().report.report_base_folder_name, 
                                      get_settings().report.report_global_folder_name)
        sz = SpectraZipper(
            study_type='NMR',
            reporting_path=reporting_path,
            private_studies_dir=get_settings().study.mounted_paths.study_metadata_files_root_path,
            spectra_dir=f'NMR_spectra_files_{str(current_time())}',
            study_location=study_location
        )
        sz.run()

        logger.info(f'End result of spextra zipper: {len(sz.not_found)}')
        return {
            "status": "completed",
            "missing files": len(sz.not_found),
            "files unable to be copied": len(sz.not_copied)
        }


class SpectraZipper:

    def __init__(self, study_type, reporting_path, private_studies_dir, spectra_dir, study_location):
        """
        Init method

        :param study_type: Analytical method used in the study IE NMR
        :param reporting_path: The location of the reporting directory
        :param private_studies_dir: The root private studies directory
        :param spectra_dir: The name of the spectra directory to be created
        :param study_location: Base study location
        """
        self.study_type = study_type
        self.reporting_path = reporting_path
        self.private_studies_dir = private_studies_dir
        self.spectra_dir = spectra_dir
        self.study_location = study_location
        self.not_found = []
        self.not_copied = []

    def run(self):
        """
        Entry point method for the zipper class. Creates a new directory for the spectra, Opens the NMR report file
        into memory, extracts the columns we want, and then sets to work populating the spectra directory by searching for
        each of those filenames.
        """

        self._create_spectra_dir()

        file_dataframe = pandas.read_csv(
            os.path.join(self.reporting_path, f'{self.study_type}.tsv'), sep='\t', header=0, encoding='unicode_escape'
        )
        derived_file_frame = file_dataframe[['Study', 'Derived.Spectral.Data.File']]
        derived_file_frame = derived_file_frame.rename(columns={
            'Study': 'Study', 'Derived.Spectral.Data.File': 'DerivedSpectralDataFile'
        })

        # This is a large file, so we don't want it lurking in memory
        del file_dataframe
        gc.collect()

        filename_generator = self._get_filenames(derived_file_frame)
        self._populate_spectra_dir(filename_generator)
        self._zip()

    def _zip(self):
        # undecided whether the webservice should do this or I should just do it on the created directory
        # since this a one time or a couple-of-times operation, I am optioning for the manual way - it will save a huge
        # outlay on memory
        pass

    @staticmethod
    def _get_filenames(frame):
        """
        Get the study accession number and derived file name from each row of the table. This method returns a generator
        object.

        :param frame: pandas DataFrame object.
        :yield: a tuple of study accession and derived filename.
        """
        for row in frame.itertuples():
            yield row.Study, row.DerivedSpectralDataFile

    def _create_spectra_dir(self):
        """
        Create a new spectra directory using the name given to the object at init. If this fails it will abort or raise
        an error.
        :return: N/A
        """
        try:
            os.mkdir(os.path.join(self.private_studies_dir, self.spectra_dir))
        except FileExistsError as e:
            logger.error(f'Tried to create a new directory to collate {self.study_type} spectra files but '
                         f'it already exists: {str(e)}')
            abort(500)

        if os.path.exists(os.path.join(self.private_studies_dir, self.spectra_dir)):
            pass
        else:
            raise FileNotFoundError(f'Couldnt create spectra directory at {self.private_studies_dir}{self.spectra_dir}')

    def _populate_spectra_dir(self, generator, shallow=False):
        """
        Populate the newly created spectra directory. For each derived filename yielded by the generator, we check the
        top level directory, then the derived files dir. If shallow is False, we then check every subdirectory
        (however deep) in the study folder. If after all that the file is not found it is marked as so by appending that
         filename to the not found list.

        :param generator: Generator object which yields tuples that hold study accession numbers and derived filenames.
        :param shallow: Flag to indicate whether to check every subdir if the file is not found in the basic places.
        :return: N/A.
        """
        for items in generator:
            study = items[0]
            desired_derived = items[1]
            logger.info(f'hit generator loop with {study} & {desired_derived}')

            copy = repr(self.study_location).strip("'")
            this_study_location = copy.replace("MTBLS1", study)
            top_level = os.listdir(this_study_location)
            derived_path = f'{this_study_location}/DERIVED_FILES/'

            if desired_derived in top_level:
                self._copy(this_study_location, desired_derived)

            elif os.path.exists(derived_path):
                if self._basic_search(derived_path, desired_derived, this_study_location, shallow):
                    break
            else:
                if shallow:
                    self.not_found.append(desired_derived)
                else:
                    self._deep_search(this_study_location, desired_derived)

    def _basic_search(self, derived_path, desired_derived, this_study_location, shallow) -> bool:
        """
        Checks the derived files directory for the desired derived file. if the file is found it is copied to the
        spectra directory. If the file is not found, and we are only searching shallow, we add that file to the not
        found list. Returns a value of true or false, which will trigger a deep search if said return value is false and
         shallow is False.

        :param derived_path: The location of the derived files directory.
        :param desired_derived: The name of the derived file we are after.
        :param this_study_location: The location of the current study folder.
        :param shallow: Flag to indicate whether to check every subdir if the file is not found in the basic places.
        :return: bool indicating success or failure.
        """
        logger.info(derived_path)

        if os.path.exists(derived_path):
            derived = os.listdir(derived_path)
            if desired_derived in derived:
                self._copy(derived_path, desired_derived)
                return True

            else:
                if 'POS' in derived or 'NEG' in derived:
                    pos_path = f'{this_study_location}/DERIVED_FILES/POS/'
                    neg_path = f'{this_study_location}/DERIVED_FILES/NEG/'

                    if os.path.exists(pos_path):
                        pos = os.listdir(pos_path)
                        if desired_derived in pos:
                            self._copy(pos_path, desired_derived)
                            return True
                        else:
                            if shallow:
                                self.not_found.append(desired_derived)

                    if os.path.exists(neg_path):
                        neg = os.listdir(neg_path)
                        if desired_derived in neg:
                            self._copy(neg_path, desired_derived)
                            return True
                        else:
                            if shallow:
                                self.not_found.append(desired_derived)
        if shallow:
            self.not_found.append(desired_derived)
        return False

    def _deep_search(self, this_study_location, desired_derived) -> bool:
        """
        Searches every subdirectory in the study folder for the given file. If this is successful, it breaks out of
        iterating over every subdir and returns true. If not, the filename is added to the not found list.

        :param this_study_location: Location of the current study folder.
        :param desired_derived: The name of the derived file we are after.
        :return: bool indicating success or failure.
        """
        found = False
        for subdir, dirs, files in os.walk(this_study_location):
            if desired_derived in files:
                self._copy(subdir, desired_derived)
                found = True
                break
        if found is False:
            self.not_found.append(desired_derived)
        return found

    def _copy(self, path, derived_file):
        """
        Copies the file from a study folder to the spectra directory. If the copy operation fails it will add that file
        to the list of files that were unsuccessful in attempts to copy.

        :param path: The path to the directory where the derived file is found.
        :param derived_file: The derived filename.
        :return: N/A
        """
        copy_op_result = shutil.copy2(
            os.path.join(path, derived_file),
            os.path.join(self.private_studies_dir, self.spectra_dir)
        )
        if not copy_op_result:
            logger.error(
                f'Could not copy file {derived_file} to {self.private_studies_dir}{self.spectra_dir}'
            )
            self.not_copied.append(derived_file)
