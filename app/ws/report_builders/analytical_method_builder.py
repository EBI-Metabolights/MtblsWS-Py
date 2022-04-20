import datetime
import logging
import os
from typing import List
import gspread
import gspread_dataframe
import pandas
import numpy
from pandas import DataFrame

from flask import current_app as app, abort
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from app.ws.cronjob import setGoogleSheet
from app.ws.mtbls_maf import totuples
from app.ws.performance_and_metrics.builder_performance_tracker import BuilderPerformanceTracker
from app.ws.utils import readDatafromFile
from app.ws.misc_utilities.dataframe_utils import DataFrameUtils

logger = logging.getLogger('wslog')


class AnalyticalMethodBuilder:

    def __init__(
            self,
            original_study_location: str,
            studytype: str,
            slim: bool,
            reporting_path,
            verbose: bool = True,
            g_drive: bool = False
    ):
        """
        Init method

        :param original_study_location: The location in storage of a single study - we then just reuse this by
            formatting in each study ID repeatedly, to save making more calls to the IsaAPI.
        :param studytype: The analytical method used in the study IE NMR.
        :param slim: Flag to indicate the user wants a slim version of the report, where information for each study is
            condensed to a single row.
        :param reporting_path: The location to save the resultant report.
        :param verbose: Flag to indicate whether to give verbose output from the tracker.
        :param g_drive: Flag to indicate whether to also save the report to google drive.
        """
        self.original_study_location = original_study_location
        self.studytype = studytype
        self.slim = slim
        self.reporting_path = reporting_path
        self.verbose = verbose
        self.tracker = BuilderPerformanceTracker(
            assays_causing_errors=[],
            missing_sample_sheets=0,
        )
        self.specified_study_data = self._get_data_from_reporting_directory()
        self.g_drive = g_drive

    def build(self) -> str:
        """
        Entry method for generating a report for a given analytical method type IE NMR. Calls the build_sheet method,
        and returns a message indicating the outcome of the generation process.


        :return: message indicating the outcome of the file generating process.
        """
        logger.info(f'starting build process for type {self.studytype}')
        self.tracker.start_timer('total')
        list_of_samps = []
        list_of_assays = []
        dataframe_generator = self._get_dataframe()

        for samp, assay in dataframe_generator:
            list_of_samps.extend(samp)
            list_of_assays.extend(assay)

        result = self._merge(list_of_assays, list_of_samps)

        self._save(result)

        self.tracker.stop_timer('total')
        message = self._builder_report()
        return message

    def _merge(self, list_of_assays, list_of_samps) -> DataFrame:
        """
        merge the two list-of-dicts to create two tall dataframes - one representing the assay information, the other
        representing the sample information. We then merge these two dataframes together side-by-side to create the
        final dataframe.

        :param list_of_assays: A List of dicts, where each dict represents a row of assay data.
        :param list_of_samps: A List of dicts where each dict represents a row of sample data.
        :return: A merged pandas.DataFrame object.
        """

        merged_samp = None
        merged_assay = None
        result = None

        try:
            merged_assay = pandas.DataFrame(list_of_assays)
        except Exception as e:
            logger.error(f'Problem creating assay dataframe from list of dicts: {str(e)}')

        try:
            merged_samp = pandas.DataFrame(list_of_samps)
        except Exception as e:
            logger.error(f'Problem creating sample dataframe from list of dicts: {str(e)}')

        if merged_samp is None or merged_assay is None:
            logger.error(f'Unable to build a report for analytical method {self.studytype}. Either the assay or sample '
                         f'dataframe was unable to be built.')
            abort(500)

        try:
            result = pandas.merge(merged_samp, merged_assay, on=['Study', 'Sample.Name'])
        except Exception as e:
            logger.error(f'Unable to build merged report dataframe: {str(e)}')
            abort(500)

        return result

    def _get_dataframe(self):
        """
        Pulls all studies that are recorded as being of that detection type (referring to the globals.json report file),
        and iterates over the list, creating a dataframe for each study consisting of the study accession number,
        organism information from the sample sheet and the (relevant) assay sheet. It concatenates all of these
        dataframes together, and attempts to write them to report file under the name of {studytype}.csv.

        There are numerous ways this method can fail, which are accounted for in the implementation, but to offer an
        easy list to refer to when debugging:
        1 The global.json file has been removed from the reporting directory, and has not yet been regenerated. Hit the
        /v2/reports endpoint with query type global to generate the file.
        2 The study type has been misspelled, or punctuation mangled, by the user. IE LC-ms will fail as it will not
        match LC-MS.
        3 There is an issue with the contents of  sample sheet / assay sheet which causes the concatenation to fail.
        This may result in the end dataframe being empty (although this is unlikely).
        4 The sample and assay sheets do not follow expected naming convention (s_File.txt & a_File.txt) and so cannot
        be found.
        5 You do not have permission, for whatever reason, to write to the reporting directory.

        :return: Merged DataFrame object representing the full report.
        """

        for study in self.specified_study_data:
            self.tracker.start_timer(study)
            study_location = repr(self.original_study_location).replace("MTBLS1", study).strip("'")
            logger.info(study_location)
            sample_file_list = [file for file in os.listdir(study_location) if
                                file.startswith('s_') and file.endswith('.txt')]
            if len(sample_file_list) is 0:
                logger.error(
                    'Sample sheet not found. Either it is not present or does not follow the proper naming convention.')
                self.tracker.missing_samplesheets += 1
                # skip this iteration since we cant find the samplesheet
                continue

            try:
                sample_temp = pandas.read_csv(os.path.join(study_location, sample_file_list[0]), sep="\t", header=0,
                                              encoding='unicode_escape')

                # Get rid of empty numerical values
                sample_temp = sample_temp.replace(numpy.nan, '', regex=True)
                sample_temp.insert(0, 'Study', study)

                # we want to remove any columns we don't want
                sample_temp = DataFrameUtils.sample_cleanup(df=sample_temp)
                if self.slim:
                    sample_temp = DataFrameUtils.collapse(df=sample_temp)
                # sample_df = sample_df.append(sample_temp, ignore_index=True)
                # sample_df_as_list_of_dicts.extend(totuples(df=sample_temp, text='dict')['dict'])
            except UnicodeDecodeError as e:
                logger.error(
                    f'UnicodeDecodeError when trying to open sample sheet. Study {study} will not be included in report: '
                    f'{str(e)}')
                self.tracker.missing_samplesheets += 1
                self.tracker.stop_timer(study)
                continue

            assays_list = self._sort_assays(study_location)
            for assay in assays_list:
                logger.info('hit interior loop')
                try:
                    assay_temp = pandas.read_csv(os.path.join(study_location, assay), sep="\t", header=0, encoding="utf-8")
                    assay_temp.insert(0, 'Study', study)
                    assay_temp = assay_temp.replace(numpy.nan, '', regex=True)
                    assay_temp = self._dataframe_cleanup(assay_temp)
                    if self.slim:
                        assay_temp = DataFrameUtils.collapse(df=assay_temp)
                    # assay_df = assay_df.append(assay_temp, ignore_index=True)
                    # assay_df_as_list_dicts.extend(totuples(df=assay_temp, text='dict')['dict'])
                    self.tracker.stop_timer(study)
                    yield totuples(df=sample_temp, text='dict')['dict'], totuples(df=assay_temp, text='dict')['dict']
                except Exception as e:
                    logger.error('Error appending assay {0} into larger dataframe: {1}'.format(assay, e))
                    self.tracker.push('assays_causing_errors', assay)
                    self.tracker.stop_timer(study)
                    continue

    def _dataframe_cleanup(self, assay_dataframe) -> DataFrame:
        """
        Pick which cleanup method to use. Uses the same broad assumption as elsewhere that all studies fall into one of two
        categories.
        """
        if self.studytype.count('LC') > 0:
            return DataFrameUtils.LCMS_assay_cleanup(assay_dataframe)
        else:
            return DataFrameUtils.NMR_assay_cleanup(assay_dataframe)

    def _sort_assays(self, study_location, include_all=False) -> List[str]:
        """
        Sort through which assays we want to include in the report. It defaults to all assays in a given study folder,
        but if include_all is set to false it attempts to try and detect which assays are relevant via the filename.

        :param study_location: path on the server of the study in question.
        :param studytype: Used to differentiate assays if include_all is false.
        :param include_all: Flag that decides whether to include all assay files in a study folder, defaults to True.
        :return: A filtered list of assay files
        """
        filtered_assays_list = []
        tokens = {
            'NMR': ['NMR', 'spectroscopy'],
            'LCMS': ['LC', 'LC-MS', 'LCMS', 'spectrometry']
        }
        t = 'LCMS' if self.studytype is not 'NMR' else 'NMR'

        assays_list = [file for file in os.listdir(study_location) if file.startswith('a_') and file.endswith('.txt')]

        if include_all:
            return assays_list
        # if we have only one assay, we already know this is an NMR study, do the assay must be an nmr one.
        if len(assays_list) > 1:
            # attempt to cull any irrelevant assays by checking for tokens in the assay filenames.
            filtered_assays_list = [file for file in assays_list if
                                    any(token.upper() in file.upper() for token in tokens[t])]
        elif len(assays_list) is 1:
            filtered_assays_list = assays_list

        if len(filtered_assays_list) == 0:
            # it might be the case that the assay sheet doesnt include the name of the detection method
            filtered_assays_list = assays_list

        return filtered_assays_list

    def _save(self, result):
        """
        Save the resulting dataframe as a spreadsheet to the server, and if selected, to the google drive.
        """
        token_path = app.config.get('GOOGLE_SHEET_TOKEN')
        # stick this in app.config
        mariana_folder_id = '1i8caTtguyLBvQcBt7Lzqfplmmrr5T1JO'
        ebi_folder_id = '1psQb4OSjAXrEqPmZpt6kQfPZfooHqnBE'

        title = f'{self.studytype} {str(datetime.datetime.now())}'


        if not result.empty:
            try:
                result.to_csv(
                    os.path.join(self.reporting_path, f"{self.studytype}.tsv"), sep="\t", encoding='utf-8', index=False)
            except Exception as e:
                message = f'Problem with writing report to csv file: {str(e)}'
                logger.error(message)
                abort(500, message)
        else:
            message = 'Unexpected error in concatenating dataframes - end result is empty. Check the globals.json file ' \
                  'exists and if so, has been recently generated. Check the spelling of the study type given as a '\
                  'parameter'
            logger.error(message)
            abort(500, message)


        if self.g_drive:
            spreadsheet = {
                'properties': {
                    'title': title
                }
            }
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            credentials = ServiceAccountCredentials.from_json_keyfile_name(token_path, scope)

            gc = gspread.authorize(credentials)
            logger.info(gc.__dict__)
            gc.create(title)
            empty_spreadsheet = gc.open(title)
            logger.info(empty_spreadsheet)
            empty_spreadsheet.add_worksheet(title=title, rows=len(result.index), cols=len(result.columns))
            gspread_dataframe.set_with_dataframe(
                worksheet=empty_spreadsheet.worksheet(title),
                dataframe=result
            )

            drive_service = build('drive', 'v3', credentials=credentials, cache_discovery=False)

            # Now we need to find the file in google drive
            spreadsheet_file = None
            page_token = None
            while True:
                response = drive_service.files().list(q="mimeType='application/vnd.google-apps.spreadsheet'",
                                                      spaces='drive',
                                                      fields='nextPageToken, files(id, name)',
                                                      pageToken=page_token).execute()
                for file in response.get('files', []):
                    # Process change
                    logger.info(f"{file.get('id')}, {file.get('name')}")
                    if file.get('name') == title:
                        spreadsheet_file = file
                        break

                page_token = response.get('nextPageToken', None)
                if page_token is None:
                    break

            if spreadsheet_file is None:
                raise FileNotFoundError

            logger.info(spreadsheet_file.get('parents'))
            previous_parents = ",".join(spreadsheet_file.get('parents'))
            updated_spreadsheet_file = drive_service.files.update(fileId=spreadsheet_file.get('id'),
                                                                  addParents=mariana_folder_id,
                                                                  removeParents=previous_parents,
                                                                  fields="id, parents").execute()



    def _get_data_from_reporting_directory(self):
        """
        Pull out the Study accession numbers from the globals.json file. We have to handle LC-MS data differently as it is
        not all grouped together like say NMR. We need to check which keys in the techniques section are LCMS relevant, and
        then use the keys to pull out the accession numbers we want.

        :param studytype: Type of detection method we want accession numbers for.
        :param reporting_path: Where the global.json report file can be found.
        :return: List of relevant accession numbers.
        """
        json_data = readDatafromFile(self.reporting_path + 'global.json')
        specified_study_data = []
        if str(self.studytype) == 'LCMS':
            keys = [key for key in json_data['data']['techniques'].keys() if key.count('LC') > 0]
            for key in keys:
                specified_study_data.extend(json_data['data']['techniques'][key])
        else:
            try:
                specified_study_data = json_data['data']['techniques'][self.studytype]

            except KeyError as e:
                msg = f'The queried study type {self.studytype} is invalid. Check spelling and punctuation including ' \
                      f'hyphens: {str(e)}'
                logger.error(msg)
                abort(400, msg)
        return specified_study_data

    def _builder_report(self) -> str:
        """Currently only runs if the build process is successful, but this could change if it would be useful.
        It essentially prints out the contents of all the tracking variables and timers held within the tracker object,
        if the user has selected verbose. Otherwise it returns a brief summary.

        :return: Report of the build process as a string
        """
        base_message = f'Successfully wrote report to excel file at {self.reporting_path}{self.studytype}.tsv in ' \
                       f'{str(round(self.tracker.get_duration("total"), 2))} s. There were ' \
                       f'{self.tracker.missing_sample_sheets} studies that were missing sample sheets and so were not '\
                       f'included in the report. There were {str(len(self.tracker.assays_causing_errors))} assay ' \
                       f'sheets which caused errors when processed'
        if self.verbose:
            time_str = 'Output for all timers in tracker: \n'
            timer_message = '\n '.join(self.tracker.report_all_timers())
            total_time = "\n ".join((time_str, timer_message))

            general_report_str = f'Results from AnalyticalMethodBuilder {str(datetime.datetime.now())}: \n'
            general_message = '\n '.join((general_report_str, base_message))

            tracking_variables_str = f'Output from tracking variables \n'
            tracking_message = '\n '.join([
                f'Tracker {key} : {str(val)}'
                for key, val in self.tracker.__dict__.items()
                if key is not '_timers'])
            total_tracking = '\n '.join((tracking_variables_str, tracking_message))

            message = '\n\n '.join(( general_message, total_tracking, total_time))

        else:
            message = base_message

        return message

