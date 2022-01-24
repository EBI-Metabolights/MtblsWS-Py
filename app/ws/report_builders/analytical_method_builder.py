import logging
import os
import pandas
import numpy
from pandas import DataFrame

from flask import current_app as app, abort

from app.ws.utils import readDatafromFile
from app.ws.misc_utilities.dataframe_utils import DataFrameUtils

logger = logging.getLogger('wslog')


def generate_file(original_study_location: str, studytype: str):
    """
    Entry method for generating a report for a given study assay type IE NMR. Calls the build_sheet method, and returns
    a message indicating the outcome of the generation process. Note that the process may fail hard during the
    build_sheet method.

    :param study_location: The location of study MTBLS1. This is overwritten continually with each study we want to
    include in our reporting.
    :param studytype: The kind of study assay to report on IE NMR.
    :return: message indicating the outcome of the file generating process.
    """
    reporting_path = app.config.get('MTBLS_FTP_ROOT') + app.config.get('REPORTING_PATH') + 'global/'

    original_sin, missing_samplesheets, assaysheets_causing_errors = build_sheet(original_study_location, studytype, reporting_path)

    if not original_sin.empty:
        try:
            # original_sin.to_excel(os.path.join(reporting_path, "stats.xlsx"))
            original_sin.to_csv(os.path.join(reporting_path, "{0}.tsv".format(studytype)), sep="\t", encoding='utf-8', index=False)
            message = 'Successfully wrote report to excel file at {path}{file}.tsv . There were {bad_samp} studies that ' \
                      'were missing sample sheets and so were not included in the report. There were {bad_assay} assay'\
                      ' sheets which caused errors when processed'.format(
                            path=reporting_path,
                            file=studytype,
                            bad_samp=missing_samplesheets,
                            bad_assay=str(len(assaysheets_causing_errors)))
        except Exception as e:
            message = 'Problem with writing report to csv file: {0}'.format(e)
            logger.error(message)
            abort(500, message)
    else:
        message = 'Unexpected error in concatenating dataframes - end result is empty. Check the globals.json file ' \
              'exists and if so, has been recently generated. Check the spelling of the study type given as a '\
              'parameter'
        logger.error(message)
        abort(500, message)
    return message


def build_sheet(original_study_location, studytype, reporting_path):
    """
    Pulls all studies that are recorded as being of that detection type (referring to the globals.json report file),
    and iterates over the list, creating a dataframe for each study consisting of the study accession number,
    organism information from the sample sheet and the (relevant) assay sheet. It concatenates all of these dataframes
    together, and attempts to write them to report file under the name of {studytype}.csv.

    There are numerous ways this method can fail, which are accounted for in the implementation, but to offer an easy
    list to refer to when debugging:
    1] The global.json file has been removed from the reporting directory, and has not yet been regenerated. Hit the
    /v2/reports endpoint with query type global to generate the file.
    2] The study type has been misspelled, or punctuation mangled, by the user. IE LC-ms will fail as it will not match
    LC-MS.
    3] There is an issue with the contents of  sample sheet / assay sheet which causes the concatenation to fail. This
    may result in the end dataframe being empty (although this is unlikely).
    4] The sample and assay sheets do not follow expected naming convention (s_File.txt & a_File.txt) and so cannot be
    found.
    5] You do not have permission, for whatever reason, to write to the reporting directory.

    :return: Merged DataFrame object representing the full report.
    """

    specified_study_data = get_data(studytype, reporting_path)
    assaysheets_causing_errors = []
    missing_samplesheets = 0
    sample_df = pandas.DataFrame(
        columns=["Study", "Characteristics.Organism.", "Characteristics.Organism.part.", "Protocol.REF", "Sample.Name"])
    assay_df = get_column_headers_by_detection_type(studytype)

    for study in specified_study_data:
        study_location = original_study_location.replace("MTBLS1", study)
        logger.info(study_location)
        sample_file_list = [file for file in os.listdir(study_location) if
                            file.startswith('s_') and file.endswith('.txt')]
        if len(sample_file_list) is 0:
            logger.error(
                'Sample sheet not found. Either it is not present or does not follow the proper naming convention.')
            missing_samplesheets += 1
            # skip this iteration since we cant find the samplesheet
            continue

        try:
            sample_temp = pandas.read_csv(os.path.join(study_location, sample_file_list[0]), sep="\t", header=0,
                                          encoding='unicode_escape')

            # Get rid of empty numerical values
            sample_temp = sample_temp.replace(numpy.nan, '', regex=True)
            sample_temp.insert(0, 'Study', study)

            # we want to remove any columns we don't want
            sample_temp = DataFrameUtils.sample_cleanup(sample_temp)
            sample_df = sample_df.append(sample_temp, ignore_index=True)
        except UnicodeDecodeError as e:
            logger.error(
                'UnicodeDecodeError when trying to open sample sheet. Study {0} will not be included in report: {1}'.format(
                    study, e))
            missing_samplesheets += 1
            continue

        assays_list = sort_assays(study_location, studytype)
        for assay in assays_list:
            logger.info('hit interior loop')
            try:
                assay_temp = pandas.read_csv(os.path.join(study_location, assay), sep="\t", header=0, encoding="utf-8")
                assay_temp.insert(0, 'Study', study)
                assay_temp = assay_temp.replace(numpy.nan, '', regex=True)
                assay_temp = cleanup(studytype, assay_temp)
                assay_df = assay_df.append(assay_temp, ignore_index=True)
            except Exception as e:
                logger.error('Error appending assay {0} into larger dataframe: {1}'.format(assay, e))
                assaysheets_causing_errors.append(assay)
                continue
    try:
        result = pandas.merge(sample_df, assay_df, on=['Study', 'Sample.Name'])
        return result, missing_samplesheets, assaysheets_causing_errors
    except Exception as e:
        logger.error(e)
        abort(500, e)


def cleanup(studytype, assay_dataframe):
    """
    Pick which cleanup method to use. Uses the same broad assumption as elsewhere that all studies fall into one of two
    categories.
    """
    if studytype.count('LC') > 0:
        return DataFrameUtils.LCMS_assay_cleanup(assay_dataframe)
    else:
        return DataFrameUtils.NMR_assay_cleanup(assay_dataframe)


def sort_assays(study_location, studytype, include_all=True):
    """
    Sort through which assays we want to include in the report. It defaults to all assays in a given study folder,
    but if include_all is set to false it attempts to try and detect which assays are relevant via the filename.

    :param study_location: path on the server of the study in question.
    :param studytype: Used to differentiate assays if include_all is false.
    :param include_all: Flag that decides whether to include all assay files in a study folder, defaults to True.
    """
    filtered_assays_list = []
    assays_list = [file for file in os.listdir(study_location) if file.startswith('a_') and file.endswith('.txt')]
    if include_all:
        return assays_list
    # if we have only one assay, we already know this is an NMR study, do the assay must be an nmr one. If we have
    # more than one assay file, at least one but maybe more of those will be NMR assays, so we need to cull the
    # other assays as they would pollute the resulting table.
    if len(assays_list) > 1:
        filtered_assays_list = [file for file in assays_list if studytype in file.upper()]

    if len(filtered_assays_list) == 0:
        # it might be the case that the assay sheet doesnt include the name of the detection method
        return assays_list

    return filtered_assays_list


def get_data(studytype, reporting_path):
    """
    Pull out the Study accession numbers from the globals.json file. We have to handle LC-MS data differently as it is
    not all grouped together like say NMR. We need to check which keys in the techniques section are LCMS relevant, and
    then use the keys to pull out the accession numbers we want.

    :param studytype: Type of detection method we want accession numbers for.
    :param reporting_path: Where the global.json report file can be found.
    :return: List of relevant accession numbers.
    """
    json_data = readDatafromFile(reporting_path + 'global.json')
    specified_study_data = []
    if str(studytype) == 'LCMS':
        keys = [key for key in json_data['data']['techniques'].keys() if key.count('LC') > 0]
        for key in keys:
            specified_study_data.extend(json_data['data']['techniques'][key])
    else:
        try:
            specified_study_data = json_data['data']['techniques'][studytype]

        except KeyError as e:
            msg = 'The queried study type {type} is invalid. Check spelling and punctuation including hyphens: {e}'.format(type=studytype, e=e)
            logger.error(msg)
            abort(400, msg)
    return specified_study_data


def get_column_headers_by_detection_type(studytype):
    """Return column header based on the detection method in the study. Broadly assumes there are only two categories,
    LCMS and all its variants, and NMR. GC-MS is not accounted for yet, as we currently aren't interested in analysing
    that data. It would only take the selection of the relevant column headers to implement."""
    if studytype.count('LC') > 0:
        return \
            pandas.DataFrame(columns=['Study', 'Sample.Name', 'Protocol.REF.0', 'Parameter.Value.Post.Extraction.',
                                     'Parameter.Value.Derivatization.', 'Extract.Name', 'Protocol.REF.1',
                                     'Parameter.Value.Chromatography.Instrument.', 'Parameter.Value.Column.model.',
                                     'Parameter.Value.Column.type.', 'Labeled.Extract.Name', 'Label', 'Protocol.REF.2',
                                     'Parameter.Value.Scan.polarity.', 'Parameter.Value.Scan.m/z.range.',
                                     'Parameter.Value.Instrument.', 'Parameter.Value.Ion.source.',
                                     'Parameter.Value.Mass.analyzer.', 'MS.Assay.Name', 'Raw.Spectral.Data.File',
                                     'Protocol.REF.3', 'Normalization.Name', 'Derived.Spectral.Data.File',
                                     'Protocol.REF.4',
                                     'Data.Transformation.Name', 'Metabolite.Assignment.File'])
    else:
        return pandas.DataFrame(columns=['Study', 'Sample.Name', 'Protocol.REF.0', 'Protocol.REF.1',
                              'Parameter.Value.NMR.tube.type.', 'Parameter.Value.Solvent.',
                              'Parameter.Value.Sample.pH.', 'Parameter.Value.Temperature.', 'Unit',
                              'Label', 'Protocol.REF.2', 'Parameter.Value.Instrument.',
                              'Parameter.Value.NMR.Probe.', 'Parameter.Value.Number.of.transients.',
                              'Parameter.Value.Pulse.sequence.name.',
                              'Acquisition.Parameter.Data.File', 'Protocol.REF.3', 'NMR.Assay.Name',
                              'Free.Induction.Decay.Data.File', 'Protocol.REF.4',
                              'Derived.Spectral.Data.File', 'Protocol.REF.5',
                              'Data.Transformation.Name', 'Metabolite.Assignment.File'])
