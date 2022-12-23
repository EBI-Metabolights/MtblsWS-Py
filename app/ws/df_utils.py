import logging
import os
import re
from typing import List

import numpy as np
import pandas as pd


logger = logging.getLogger('wslog')

def write_tsv(dataframe, file_name):
    try:
        # Remove all ".n" numbers at the end of duplicated column names
        dataframe.rename(columns=lambda x: re.sub(r'\.[0-9]+$', '', x), inplace=True)

        # Write the new row back in the file
        dataframe.to_csv(file_name, sep="\t", encoding='utf-8', index=False)
    except:
        return 'Error: Could not write/update the file ' + file_name

    return 'Success. Update file ' + file_name

def read_tsv_columns(file_name, column_name_patterns: List[str]=None):
    table_df = pd.DataFrame()  # Empty file
    try:
        # Enforce str datatype for all columns we read from ISA-Tab tables
        col_names = pd.read_csv(file_name, sep="\t", nrows=0).columns
        selected_columns = col_names

        if column_name_patterns:
            selected_columns = []
            for column_name_pattern in column_name_patterns:
                    for column in col_names:
                        if re.match(column_name_pattern, column):
                            selected_columns.append(column)

        types_dict = {col: str for col in selected_columns}
        try:
            if os.path.getsize(file_name) == 0:  # Empty file
                logger.error("Could not read file " + file_name)
            else:
                table_df = pd.read_csv(file_name, sep="\t", header=0, encoding='utf-8',
                                       usecols=selected_columns, dtype=types_dict)
        except Exception as e:  # Todo, should check if the file format is Excel. ie. not in the exception handler
            if os.path.getsize(file_name) > 0:
                table_df = pd.read_csv(file_name, sep="\t", header=0, encoding='ISO-8859-1',
                                       usecols=selected_columns, dtype=types_dict)  # Excel format
                logger.info("Tried to open as Excel tsv file 'ISO-8859-1' file " + file_name + ". " + str(e))
    except Exception as e:
        logger.error("Could not read file " + file_name + " " + str(e))

    table_df = table_df.replace(np.nan, '', regex=True)  # Remove NaN
    return table_df


def read_tsv_columns(file_name, column_name_pattern=None):
    table_df = pd.DataFrame()  # Empty file
    try:
        # Enforce str datatype for all columns we read from ISA-Tab tables
        col_names = pd.read_csv(file_name, sep="\t", nrows=0).columns
        selected_columns = col_names
        if column_name_pattern:
            selected_columns = []
            for column in col_names:
                if re.match(column_name_pattern, column):
                    selected_columns.append(column)

        types_dict = {col: str for col in selected_columns}
        try:
            if os.path.getsize(file_name) == 0:  # Empty file
                logger.error("Could not read file " + file_name)
            else:
                table_df = pd.read_csv(file_name, sep="\t", header=0, encoding='utf-8',
                                       usecols=selected_columns, dtype=types_dict)
        except Exception as e:  # Todo, should check if the file format is Excel. ie. not in the exception handler
            if os.path.getsize(file_name) > 0:
                table_df = pd.read_csv(file_name, sep="\t", header=0, encoding='ISO-8859-1',
                                       usecols=selected_columns, dtype=types_dict)  # Excel format
                logger.info("Tried to open as Excel tsv file 'ISO-8859-1' file " + file_name + ". " + str(e))
    except Exception as e:
        logger.error("Could not read file " + file_name + " " + str(e))

    table_df = table_df.replace(np.nan, '', regex=True)  # Remove NaN
    return table_df