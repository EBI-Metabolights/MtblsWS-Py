import logging
import os
import pandas

from typing import List
from flask import current_app as app, abort
from app.ws.misc_utilities.dataframe_utils import DataFrameUtils
from app.ws.utils import totuples

logger = logging.getLogger('builder')


class CombinedMafBuilder:
    """
    Builds a combined maf files from a given list of studies.
    """

    def __init__(self, studies_to_combine: List[str], original_study_location: str, method: str):
        self.studies_to_combine = studies_to_combine
        self.original_study_location = original_study_location
        self.method = method
        self.unopenable_maf_register = []
        self.missing_maf_register = []
        self.missing_study_directory_register = []
        self.no_relevant_maf_register = []

    def build(self):
        """
        Entry method to the class. Gets the generator that yields individual maf dicts, and adds that dict to the list.
        We then try and create a new DataFrame object from that list of dicts.
        """
        list_of_mafs = []
        maf_generator = self.get_dataframe()

        for maf_as_dict in maf_generator:
            list_of_mafs.extend(maf_as_dict)

        reporting_path = app.config.get('MTBLS_FTP_ROOT') + app.config.get('REPORTING_PATH') + 'global/'
        combined_maf = None
        try:
            combined_maf = pandas.DataFrame(list_of_mafs)
        except Exception as e:
            logger.error(f'Problem creating dataframe from list of dicts: {str(e)}')
        try:
            combined_maf.to_csv(
                    os.path.join(reporting_path, f'{self.method}_combined_maf.tsv'),
                    sep="\t",
                    encoding='utf-8',
                    index='false'
                )
        except Exception as e:
            # bad practice here catching base exception, but the pandas documentation did not reveal what errors or
            # exceptions to expect
            logger.error(f'Problem writing the combined maf file to csv:{str(e)}')
            abort(500)


    def get_dataframe(self):
        """
        Yield an individual dataframe-as-a-dict. This is a generator method, with the idea being that with such massive files
        we want to limit how many dataframes we are holding in memory at once. We convert the dataframe to a dict in this method,
        and then yield it. This means we only have one dataframe open in memory at a time.

        The method also sorts through each of the maf files found in the study directory, attempting to cast off any
        that might correspond to other analytical methods.
        """
        for i, study_id in enumerate(self.studies_to_combine):
            copy = repr(self.original_study_location).strip("'")
            study_location = copy.replace("MTBLS1", study_id)

            for maf in self.sort_mafs(study_location, study_id):
                maf_temp = None
                try:
                    maf_temp = pandas.read_csv(os.path.join(study_location, maf), sep="\t", header=0, encoding='unicode_escape')
                except pandas.errors.EmptyDataError as e:
                    logger.error(f'EmptyDataError Issue with opening maf file {maf}: {str(e)}')
                    self.unopenable_maf_register.append(maf)
                    continue
                except Exception as e:
                    logger.error(f'Issue with opening maf file {maf}, cause of error unclear: {str(e)}')
                    self.unopenable_maf_register.append(maf)
                    continue

                cleanup_function = getattr(DataFrameUtils, f'{self.method}_maf_cleanup')
                maf_temp = cleanup_function(maf_temp, study_id, maf)
                maf_as_dict = totuples(df=maf_temp, text='dict')['dict']

                yield maf_as_dict


    def sort_mafs(self, study_location: str, study_id: str):
        """
        Sorts through study directory maf files by checking for the presence of 'tokens' in the filename.

        :param study_location: The location in the filesystem of the study, as a string.
        :return: A List of strings representing culled filenames.
        """
        filtered_maf_list = []
        tokens = {
            'NMR': ['NMR', 'spectroscopy'],
            'LCMS': ['LC', 'LC-MS', 'LCMS', 'spectrometry']
        }
        maf_file_list = None

        try:
            maf_file_list = [file for file in os.listdir(study_location) if
                             file.startswith('m_') and file.endswith('.tsv')]
        except FileNotFoundError as e:
            logger.error(f'FileNotFoundError: {str(e)}')
        except NotADirectoryError as e:
            logger.error(f'NotADirectoryError: {str(e)}')
        except Exception as e:
            logger.error(f'Unexpected error: {str(e)}')
        finally:
            if maf_file_list is None:
                self.missing_study_directory_register.append(study_location)
                return []
            elif len(maf_file_list) == 0:
                self.missing_maf_register.append(study_id)
                return maf_file_list

        if len(maf_file_list) == 1:
            filtered_maf_list = maf_file_list
         
        filtered_maf_list = [file for file in maf_file_list if
                             any(token.upper() in file.upper() for token in tokens[self.method])]
        if len(filtered_maf_list) == 0:
            self.no_relevant_maf_register.append(study_id)

        return filtered_maf_list
