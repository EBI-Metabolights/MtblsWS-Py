import logging
import math

import numpy as np
import pandas as pd
from app.config import get_settings

from app.ws.chebi.search.models import CuratedMetabolitesFileColumn
from app.ws.chebi.search.utils import remove_few_characters_for_consistency, safe_split_string

logger = logging.getLogger(__file__)


class CuratedMetaboliteTable(object):
    COMPOUND_INDEX = CuratedMetabolitesFileColumn.COMPOUND_NAME.value
    PRIORITY_INDEX = CuratedMetabolitesFileColumn.PRIORITY.value
    CHEBI_ID_INDEX = CuratedMetabolitesFileColumn.CHEBI_ID.value
    FORMULA_INDEX = CuratedMetabolitesFileColumn.MOLECULAR_FORMULA.value
    SMILES_INDEX = CuratedMetabolitesFileColumn.SMILES.value
    INCHI_INDEX = CuratedMetabolitesFileColumn.INCHI.value

    EMPTY_LIST = []
    _instance = None

    @classmethod
    def get_instance(cls, file_path=None):
        if not CuratedMetaboliteTable._instance:
            cls._instance = CuratedMetaboliteTable(file_path)
        return cls._instance

    def __init__(self, file_path=None):
        self.file_path = file_path
        if not self.file_path:
            self.file_path = get_settings().chebi.pipeline.curated_metabolite_list_file_location
        self.df = None
        self.initialized = False
        self.priority_row_set = set()

    def initialize_df(self):
        if self.initialized:
            return
        try:
            self.df: pd.DataFrame = pd.read_table(self.file_path, engine='python', header=None)
            self.df[self.COMPOUND_INDEX] = self.df[self.COMPOUND_INDEX].str.replace('\"', '', regex=True)
            logger.info(f"Curated table is loaded. Current row count is {len(self.df.index)}.")
            priority_row_list = self.df.index[self.df[self.PRIORITY_INDEX] >= 1].to_list()
            self.priority_row_set = set(priority_row_list)
            self.df = self.df.replace(np.nan, '', regex=True)
            self.initialized = True
        except Exception as e:
            logger.warning(f"Error while reading curated metabolite table file {self.file_path}.")

    def get_matching_rows(self, column_index: int, value: str):
        if self.df is None:
            return self.EMPTY_LIST
        self.initialize_df()
        
        input_value = ''.join(value.split())
        input_value = remove_few_characters_for_consistency(input_value).lower()
        search_column = self.df[column_index]

        def match_row(data):
            if not data or (isinstance(data, float) and math.isnan(data)):
                return np.NaN

            if "|" in data:
                splitted_data = safe_split_string(data)
                for item in splitted_data:
                    trimmed_item = remove_few_characters_for_consistency(item).lower()
                    if trimmed_item == input_value:
                        return data
            else:
                trimmed_data = remove_few_characters_for_consistency(data).lower()
                if trimmed_data == input_value:
                    return data
            return np.NaN

        result = search_column.apply(match_row).dropna()
        result_list = result.index.to_list()
        if not result_list:
            return self.EMPTY_LIST
        if len(result_list) == 1:
            return self.df.loc[result_list[0]].to_list()

        same_name_match = False
        same_name_index = result_list[0]
        for item in result.items():
            if item[1] == input_value:
                same_name_index = item[0]
                same_name_match = True
        if same_name_match:
            return self.df.loc[same_name_index].to_list()

        result_match_index_set = set(result_list)
        priorities = result_match_index_set.intersection(self.priority_row_set)
        if priorities:
            if same_name_index not in priorities:
                same_name_index = next(iter(priorities))
            return self.df.loc[same_name_index].to_list()
        else:
            return self.df.loc[same_name_index].to_list()
