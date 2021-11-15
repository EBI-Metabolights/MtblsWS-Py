import logging
import os
import pandas
import numpy
from pandas import DataFrame

from flask import current_app as app, abort

from app.ws.utils import readDatafromFile

logger = logging.getLogger('wslog')


def generate_file(study_location: str, studytype: str):
    """
    Generates a report for a given study assay type IE NMR. It pulls all studies that are recorded as being of that type
    (referring to the globals.json report file), and iterates over the list, creating a dataframe for each study
    consisting of the study accession number, organism information from the sample sheet and the (relevant) assay sheet.
    It concatenates all of these dataframes together, and attempts to write them to report file under the name of
    {studytype}.csv.

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

    :param study_location: The location of study MTBLS1. This is overwritten continually with each study we want to
    include in our reporting.
    :param studytype: The kind of study assay to report on IE NMR.
    """

    reporting_path = app.config.get('MTBLS_FTP_ROOT') + app.config.get('REPORTING_PATH') + 'global/'
    json_data = readDatafromFile(reporting_path + 'global.json')
    specified_study_data = []

    try:
        specified_study_data = json_data['data']['techniques'][studytype]
    except KeyError as e:
        logger.error('The queried study type {type} is invalid. Check spelling and punctuation including hypens: {e}'.format(type=studytype, e=e))
        abort(400)

    assaysheets_causing_errors = []
    missing_samplesheets = 0
    return_table = []
    original_sin = DataFrame()
    logger.info(specified_study_data)
    for study in specified_study_data:
        logger.info(study)
        study_location = study_location.replace("MTBLS1", study)
        sample_file_list = [file for file in os.listdir(study_location) if file.startswith('s_') and file.endswith('.txt')]
        if len(sample_file_list) is 0:
            logger.error('Sample sheet not found. Either it is not present or does not follow the proper naming convention.')
            missing_samplesheets += 1
            # skip this iteration since we cant find the samplesheet
            continue

        sample_df = pandas.read_csv(os.path.join(study_location, sample_file_list[0]), sep="\t", header=0, encoding='utf-8')

        # Get rid of empty numerical values
        sample_df = sample_df.replace(numpy.nan, '', regex=True)

        # we want two of the columns - organism and organism part which are in columns 1 and 4 respectively (index 0)
        sample_df = sample_df[sample_df.columns[[1, 4]]]
        # logger.info(sample_df)

        assays_list = [file for file in os.listdir(study_location) if file.startswith('a_') and file.endswith('.txt')]

        # if we have only one assay, we already know this is an NMR study, do the assay must be an nmr one. If we have
        # more than one assay file, at least one but maybe more of those will be NMR assays, so we need to cull the
        # other assays as they would pollute the resulting table.
        if len(assays_list) > 1:
            assays_list = [file for file in assays_list if studytype in file.upper()]
        logger.info(assays_list)
        for assay in assays_list:
            logger.info('hit interior loop')
            try:
                assay_df = pandas.read_csv(os.path.join(study_location, assay), sep="\t", header=0, encoding="utf-8")
                assay_df = assay_df.replace(numpy.nan, '', regex=True)
                temp_df = pandas.concat([sample_df, assay_df], axis=1)
                temp_df.insert(0, 'Study', study)
            except Exception as e:
                # not sure what exception to try and catch specifically but seems like a potential point of failure
                logger.error('Error opening assay file {0}: {1}'.format(assay, e))
                assaysheets_causing_errors.append(assay)
                continue

            try:
                #return_table.append(temp_df)
                original_sin = pandas.concat([original_sin, temp_df])
            except Exception as e:
                logger.error('Error concatenating assay {0} into larger dataframe: {1}'.format(assay, e))
                assaysheets_causing_errors.append(assay)
                continue

        logger.info('got out of loop')

    logger.info(original_sin)
    unified = None
    message = ''
    # try:
    #     unified = pandas.concat(return_table)
    # except TypeError as e:
    #     message = "Caught Type Error in unifying all dataframes. Excel file will not be generated.: {0}".format(e)
    #     logger.error(message)
    # except Exception as e:
    #     message = 'Unexpected error in unifying all study dataframes: {0}'.format(e)
    #     logger.error(message)
    if not original_sin.empty:
        try:
            # original_sin.to_excel(os.path.join(reporting_path, "stats.xlsx"))
            original_sin.to_csv(os.path.join(reporting_path, "{0}_stats.csv".format(studytype)), sep="\t", encoding='utf-8', index=False)
            message = 'Successfully wrote report to excel file at {0}. There were {1} studies that were' \
                      ' missing sample sheets and so were not included in the report. There were {2} assay sheets which caused errors when processed'.format(
                            reporting_path,
                            missing_samplesheets,
                            str(len(assaysheets_causing_errors)))
        except Exception as e:
            message = 'Problem with writing report to csv file: {0}'.format(e)
            logger.error(message)
    else:
        logger.error('Unexpected error in concatenating dataframes - end result is empty. Check the globals.json file '
                     'exists and if so, has been recently generated. Check the spelling of the study type given as a '
                     'parameter')
        abort(500)
    return message
