import csv
import glob
import logging
import re
import os
import hashlib
from datetime import datetime
from functools import lru_cache
from typing import List

from app.ws.df_utils import read_tsv_columns
from app.ws.file_reference_evaluator import FileReferenceEvaluator

logger = logging.getLogger('wslog')


class FileClassifier(object):
    UNKNOWN: str = "unknown"

    def __init__(self, app):
        self.app = app
        self.extension_mapper = {}
        self.basename_mapper = {}
        self.name_without_ext_mapper = {}
        self.regex_mapper = {}
        self.compiled_regex_map = {}

        self.load_mapping_from_file("extension", self.extension_mapper)
        self.load_configured_extension_mappings()
        self.load_mapping_from_file("filename_with_extension", self.basename_mapper)
        self.load_mapping_from_file("filename_without_extension", self.name_without_ext_mapper)
        self.load_mapping_from_file("filename_regex", self.regex_mapper)

        for pattern in self.regex_mapper:
            match = re.compile(pattern)
            self.compiled_regex_map[pattern] = match

    @staticmethod
    def load_mapping_from_file(mapping_type, mapper, file=None):
        if not file:
            file = './resources/filename_mappings.tsv'
        with open(file, "r", encoding="utf8") as f:
            tsv_reader = csv.reader(f, delimiter="\t")
            for row in tsv_reader:
                (mapping_type_of_row, key, value) = row
                if mapping_type_of_row == mapping_type:
                    if key in mapper:
                        logger.warning(f"{key} for {mapping_type} is already loaded!")
                    mapper[key] = value

    def _add_to_mapper(self, file_extension_list: List, category: str):
        for item in file_extension_list:
            self.extension_mapper[item.strip('.').lower()] = category

    def load_configured_extension_mappings(self):
        self._add_to_mapper(self.app.config.get('RAW_FILES_LIST'), "raw")
        self._add_to_mapper(self.app.config.get('DERIVED_FILES_LIST'), "derived")
        self._add_to_mapper(self.app.config.get('COMPRESSED_FILES_LIST'), "compressed")

    def classify(self, path):
        """
        Classification of a file has ordered steps:
        1- internal_mapping files
        2- file basename match
        3- filename regex match
        4- double extension match
        5- single extension match
        """
        if not os.path.exists(path):
            return FileClassifier.UNKNOWN

        base_name = os.path.basename(path).lower()
        classification = self.classify_by_designation_extension(path)
        internal_file = self.is_internal_mapping_file(classification, path)
        if internal_file:
            return "internal_mapping"

        # Exact file name match
        if base_name and base_name in self.basename_mapper:
            classification_2 = self.basename_mapper[base_name]
            if classification_2 != FileClassifier.UNKNOWN:
                return classification_2

        for pattern in self.regex_mapper:
            match = self.compiled_regex_map[pattern].match(base_name)
            if match:
                classification_3 = self.regex_mapper[pattern]
                if classification_3 != FileClassifier.UNKNOWN:
                    return classification_3
                break

        classification = self.classify_by_designation_extension(path)
        return classification

    def is_internal_mapping_file(self, classification, path):
        if classification == "internal_mapping":
            return True
        internal_mapping_list = self.app.config.get('INTERNAL_MAPPING_LIST')
        for internal_file in internal_mapping_list:
            if os.sep in internal_file:
                if internal_file in path:
                    return True
        return False

    def apply_exceptions(self, classification, designation, extension):

        if classification == "text":
            new_classification = self._lookup_for_designation(designation)
            if new_classification == "part_of_raw":
                return new_classification

        ignore_file_list = self.app.config.get('IGNORE_FILE_LIST')
        for ignore in ignore_file_list:
            if ignore in designation:
                return 'part_of_raw'

        basename = f"{designation}.{extension}" if extension else designation
        if designation.startswith("~") or basename.endswith("~"):
            return "temp"
        
        return classification

    def classify_by_designation_extension(self, path):
        base_name = os.path.basename(path).lower()
        if base_name.startswith("."):
            name_parts = base_name.lstrip(".").split('.')
            name_parts[0] = f".{name_parts[0]}"
        else:
            name_parts = base_name.split('.')
        part_count = len(name_parts)
        if part_count > 2:
            # if there are more than two extensions, ignore others
            designation_1 = '.'.join(name_parts[:part_count-2])
            ext_1 = '.'.join(name_parts[part_count-2:])
            classification = self._lookup_for_designation_and_ext(designation_1, ext_1)
            # priority is on double extension
            if classification != FileClassifier.UNKNOWN:
                return self.apply_exceptions(classification, designation_1, ext_1)
            designation_2 = '.'.join(name_parts[:part_count-1])
            ext_2 = name_parts[-1]
            classification2 = self._lookup_for_designation_and_ext(designation_2, ext_2)
            if classification2 != FileClassifier.UNKNOWN:
                classification = classification2
            return self.apply_exceptions(classification, designation_1, ext_1)
        elif part_count == 2:
            designation = name_parts[0]
            ext = name_parts[1]
            classification = self._lookup_for_designation_and_ext(designation, ext)
            return self.apply_exceptions(classification, designation, ext)
        else:
            designation = name_parts[0]
            ext = ''
            classification = self._lookup_for_designation_and_ext(designation, ext)
            return self.apply_exceptions(classification, designation, ext)

    def _lookup_for_designation_and_ext(self, designation, extension) -> str:

        if extension and extension in self.extension_mapper:
            return self.extension_mapper[extension]

        if designation and designation in self.name_without_ext_mapper:
            return self.name_without_ext_mapper[designation]
        return FileClassifier.UNKNOWN

    def _lookup_for_designation(self, designation) -> str:

        if designation and designation in self.name_without_ext_mapper:
            return self.name_without_ext_mapper[designation]
        return FileClassifier.UNKNOWN

@lru_cache(1)
def get_file_classifier(app) -> FileClassifier:
    return FileClassifier(app)


@lru_cache(1)
def get_file_reference_evaluator() -> FileReferenceEvaluator:
    return FileReferenceEvaluator()
