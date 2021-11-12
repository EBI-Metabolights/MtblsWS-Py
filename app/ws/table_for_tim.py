import logging
import os
import pandas
import numpy
from pandas import DataFrame

from app.ws.mtblsWSclient import WsClient
from flask import current_app as app

logger = logging.getLogger('wslog')


def generate_file():
    # apologies for this. If we need this as an actual replicable, often used bit of functionality, wholesale changes
    # need to be made.
    nmr_studies_test = ['MTBLS56']
    nmr_studies = [
        "MTBLS1",
        "MTBLS56",
        "MTBLS563",
        "MTBLS77",
        "MTBLS24",
        "MTBLS25",
        "MTBLS46",
        "MTBLS48",
        "MTBLS100",
        "MTBLS102",
        "MTBLS103",
        "MTBLS104",
        "MTBLS114",
        "MTBLS116",
        "MTBLS123",
        "MTBLS131",
        "MTBLS132",
        "MTBLS133",
        "MTBLS134",
        "MTBLS147",
        "MTBLS149",
        "MTBLS151",
        "MTBLS161",
        "MTBLS166",
        "MTBLS172",
        "MTBLS174",
        "MTBLS177",
        "MTBLS189",
        "MTBLS200",
        "MTBLS237",
        "MTBLS240",
        "MTBLS241",
        "MTBLS242",
        "MTBLS247",
        "MTBLS249",
        "MTBLS256",
        "MTBLS261",
        "MTBLS275",
        "MTBLS326",
        "MTBLS331",
        "MTBLS336",
        "MTBLS337",
        "MTBLS340",
        "MTBLS342",
        "MTBLS346",
        "MTBLS349",
        "MTBLS356",
        "MTBLS361",
        "MTBLS367",
        "MTBLS374",
        "MTBLS382",
        "MTBLS386",
        "MTBLS387",
        "MTBLS395",
        "MTBLS399",
        "MTBLS419",
        "MTBLS422",
        "MTBLS424",
        "MTBLS425",
        "MTBLS430",
        "MTBLS431",
        "MTBLS444",
        "MTBLS455",
        "MTBLS516",
        "MTBLS524",
        "MTBLS539",
        "MTBLS540",
        "MTBLS541",
        "MTBLS543",
        "MTBLS560",
        "MTBLS564",
        "MTBLS576",
        "MTBLS593",
        "MTBLS602",
        "MTBLS619",
        "MTBLS622",
        "MTBLS623",
        "MTBLS626",
        "MTBLS639",
        "MTBLS640",
        "MTBLS643",
        "MTBLS654",
        "MTBLS658",
        "MTBLS667",
        "MTBLS669",
        "MTBLS675",
        "MTBLS678",
        "MTBLS694",
        "MTBLS705",
        "MTBLS720",
        "MTBLS723",
        "MTBLS726",
        "MTBLS781",
        "MTBLS794",
        "MTBLS798",
        "MTBLS806",
        "MTBLS832",
        "MTBLS841",
        "MTBLS862",
        "MTBLS870",
        "MTBLS874",
        "MTBLS876",
        "MTBLS881",
        "MTBLS974",
        "MTBLS981",
        "MTBLS1045",
        "MTBLS1103",
        "MTBLS1209",
        "MTBLS1320",
        "MTBLS1357",
        "MTBLS1396",
        "MTBLS1410",
        "MTBLS1416",
        "MTBLS1437",
        "MTBLS1452",
        "MTBLS1495",
        "MTBLS1497",
        "MTBLS1518",
        "MTBLS1541",
        "MTBLS1544",
        "MTBLS1677",
        "MTBLS1692",
        "MTBLS1824",
        "MTBLS1894",
        "MTBLS2052",
        "MTBLS2060",
        "MTBLS2148",
        "MTBLS2188",
        "MTBLS2327",
        "MTBLS2424",
        "MTBLS2870"
    ]

    nmr_columns = ['Study', 'Characteristics.Organism', 'Characteristics.Organism.part', 'Protocol.REF', 'Sample.Name',
                   'Protocol.REF.0', 'Protocol.REF.1', 'Parameter.Value.NMR.tube.type', 'Parameter.Value.Solvent',
                   'Parameter.Value.Sample.Ph',
                   'Parameter.Value.Temperature', 'Unit', 'Label', 'Protocol.REF.2', 'Parameter.Value.Instrument',
                   'Parameter.Value.NMR.Probe',
                   'Parameter.Value.Number.of.transients', 'Parameter.Value.Pulse.sequence.name', 'Protocol.REF.3',
                   'NMR.Assay.Name',
                   'Free.Induction.Decay.Data.File', 'Protocol.REF.4', 'Derived.Spectral.Data.File', 'Protocol.REF.5',
                   'Data.Transformation.Name',
                   'Metabolite.Assignment.File']

    # did some reading and growing a dataframe row by row is apparently a massive nono as it creates a new dataframe in
    # memory each time, without deleting the old one, so is very wasteful. It's recommended to just do it as a list of
    # rows instead, and then make a dataframe out of that list at the end.
    reporting_path = app.config.get('MTBLS_FTP_ROOT') + app.config.get('REPORTING_PATH') + 'global/'
    wsc = WsClient()
    return_table = []
    # for a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v w, x, y, z, aa
    is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
        wsc.get_permissions('MTBLS1', '4ae0c06f-a5de-41a0-bcf9-7e573c63f515')
    missing_samplesheets = 0

    original_sin = DataFrame()
    for study in nmr_studies:
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
        logger.info(sample_df)

        assays_list = [file for file in os.listdir(study_location) if file.startswith('a_') and file.endswith('.txt')]

        # if we have only one assay, we already know this is an NMR study, do the assay must be an nmr one. If we have
        # more than one assay file, at least one but maybe more of those will be NMR assays, so we need to cull the
        # other assays as they would pollute the resulting table.
        if len(assays_list) > 1:
            assays_list = [file for file in assays_list if 'NMR' in file.upper()]
        logger.info(assays_list)
        for assay in assays_list:
            logger.info('hit interior loop')
            assay_df = pandas.read_csv(os.path.join(study_location, assay), sep="\t", header=0, encoding="utf-8")
            assay_df = assay_df.replace(numpy.nan, '', regex=True)

            temp_df = pandas.concat([sample_df, assay_df], axis=1)
            temp_df.insert(0, 'Study', study)

            return_table.append(temp_df)
            original_sin = pandas.concat([original_sin, temp_df])

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
            original_sin.to_csv(os.path.join(reporting_path, "stats.xlsx"), sep="\t", encoding='utf-8', index=False)
            message = 'Successfully wrote report to excel file at {r_loc}. There were {missing} studies that were' \
                      ' missing sample sheets and so were not included in the report.'.format(
                r_loc=reporting_path,
                missing=missing_samplesheets)
        except Exception as e:
            message = 'Problem with writing report to excel file: {0}'.format(e)
            logger.error(message)
    return {message}
