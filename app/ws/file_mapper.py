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

        basename = f"{designation}.{extension}" if extension else designation
        if basename.startswith("~") or basename.endswith("~"):
            return "temp"
        if classification == "text":
            new_classification = self._lookup_for_designation(designation)
            if new_classification == "part_of_raw":
                return new_classification

        ignore_file_list = self.app.config.get('IGNORE_FILE_LIST')
        for ignore in ignore_file_list:
            if ignore in designation:
                return 'part_of_raw'

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


class FileReferenceEvaluator(object):
    ACTIVE = 'active'
    NON_ACTIVE = 'unreferenced'

    def __init__(self):
        self.default_mapper = {}
        self.study_reference_cache_map = {}
        self.load_standard_extension_mappings()
    def load_standard_extension_mappings(self):
        file = './resources/classification_mapping.tsv'
        FileClassifier.load_mapping_from_file(file, self.default_mapper, file=file)

    def get_referenced_file_list(self, study_id, path):
        if study_id not in self.study_reference_cache_map:
            self.study_reference_cache_map[study_id] = {"metadata_hash": "", "reference_file_list": set(),
                                                        'build_time': 0}

        metadata_hash = self.calculate_metadata_hash_for_study(study_id, path)
        cached_data = self.study_reference_cache_map[study_id]
        if "metadata_hash" in cached_data:
            old_hash = cached_data["metadata_hash"]
            if metadata_hash == old_hash:
                return cached_data["reference_file_list"]

        file_list = self.calculate_referenced_files(path)
        cached_data["metadata_hash"] = metadata_hash
        cached_data["reference_file_list"] = file_list
        cached_data["build_time"] = datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
        return file_list

    def calculate_metadata_hash_for_study(self, study_id, path):
        files = glob.glob(os.path.join(path, "i_Investigation.txt"))
        files.extend(glob.glob(os.path.join(path, "s_*.txt")))
        files.extend(glob.glob(os.path.join(path, "a_*.txt")))
        files.extend(glob.glob(os.path.join(path, "m_*.tsv")))
        files.sort()
        file_hashes = []  # concatenation of name, last modification time, and size
        for file in files:
            modified = os.path.getmtime(file)
            basename = os.path.basename(file)
            size = os.path.getsize(file)
            file_hashes.append(f"{basename}:{str(modified)}:{size}")
        hash_string = ";".join(file_hashes)
        hash_value = hashlib.md5(str(hash_string).encode('utf-8')).hexdigest()
        logger.debug(f"metadata_hash of {study_id}: {hash_string}")
        return hash_value

    def calculate_referenced_files(self, path):
        file_set = set()
        investigation_file_name = "i_Investigation.txt"
        investigation = os.path.join(path, investigation_file_name)
        if os.path.exists(investigation):
            file_set.add(investigation_file_name)

            sample_pattern = "^Study File Name\ts_.*$"
            lines: List[str] = self.search_lines_in_a_file(investigation, sample_pattern)
            if lines:
                samples = lines[0].split("\t")[1:]
                for sample in samples:
                    file_set.add(sample)

            assays_pattern = "^Study Assay File Name.*$"
            lines: List[str] = self.search_lines_in_a_file(investigation, assays_pattern)
            if lines:
                assays = lines[0].split("\t")[1:]
                for assay in assays:
                    file_set.add(assay)
                    assay_path = os.path.join(path, assay)
                    column_name_pattern = r'^.+ Data File(.\d+)?'
                    df = read_tsv_columns(assay_path, column_name_pattern=column_name_pattern)
                    for col in df:
                        files = df[col].unique()
                        for file in files:
                            if file:
                                file_set.add(file)
                    column_name_pattern = r'^.+ Assignment File(.\d+)?'
                    df = read_tsv_columns(assay_path, column_name_pattern=column_name_pattern)
                    for col in df:
                        files = df[col].unique()
                        for file in files:
                            if file:
                                file_set.add(file)
        return list(file_set)

    @staticmethod
    def search_lines_in_a_file(file, pattern) -> List[str]:
        lines: List[str] = []
        for encoding in ("unicode_escape", "latin-1"):
            with open(file, encoding=encoding) as f:
                try:
                    for line in f:
                        if re.search(pattern, line):
                            lines.append(line.strip())
                    break
                except Exception as e:
                    logger.warning(f'{file} file is not opened with {encoding} mode {str(e)}')
        return lines

    def get_file_status(self, classification, study_id, study_path, file_path: str):
        if classification in self.default_mapper:
            return self.default_mapper[classification]

        referenced_file_list = self.get_referenced_file_list(study_id, study_path)
        file_name = file_path.replace(study_path, '').lstrip(os.sep)
        if file_name in referenced_file_list:
            return FileReferenceEvaluator.ACTIVE
        return FileReferenceEvaluator.NON_ACTIVE


@lru_cache(1)
def get_file_classifier(app) -> FileClassifier:
    return FileClassifier(app)


@lru_cache(1)
def get_file_reference_evaluator() -> FileReferenceEvaluator:
    return FileReferenceEvaluator()
