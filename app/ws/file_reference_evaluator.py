import csv
import glob
import logging
import re
import os
import hashlib
from datetime import datetime
from typing import List

from pydantic import BaseModel

from app.ws.df_utils import read_tsv_columns

logger = logging.getLogger('wslog')

class File(BaseModel):
    name: str = ""
    path: str = ""
    metadata: dict = {}

class ReferenceFile(File):   
    raw_files: List[File] = []
    derived_files: List[File] = []
    derived_data_files: List[File] = []
    metabolites_files: List[File] = []
    file_index_map: dict = {}

class Assay(ReferenceFile):
    pass

class StudyFolder(ReferenceFile):
    study_id: str = ""
    study_path: str = ""
    metadata_path: str = ""
    assay_files: List [File] = []
    investigation_file: File = None
    sample_file: File = None
    referenced_folders: set = set()
    
    
SEARCH_PATTERNS = {"raw_files": [r'^Raw.+ Data File(.\d+)?'], 
                    "derived_files": [r'^Derived.+ Data File(.\d+)?'], 
                    "derived_data_files": [r'^Normalization.+ File(.\d+)?', r'^.+ Decay Data File(.\d+)?', r'^.+ Parameter Data File(.\d+)?'],
                    "metabolites_files": [r'^.+ Assignment File(.\d+)?']}
PAIRED_EXTENSIONS = {".wiff": [".wiff.scan"]}
OPTIONAL_PAIRED_EXTENSIONS = {"": [".peg"]}


class FileReferenceEvaluator(object):
    ACTIVE = 'active'
    NON_ACTIVE = 'unreferenced'

    def __init__(self):
        self.default_mapper = {}
        self.study_reference_cache_map = {}
        self.load_standard_extension_mappings()
        
    def load_standard_extension_mappings(self):
        file = './resources/classification_mapping.tsv'
        self.load_mapping_from_file(file, self.default_mapper, file=file)

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
        hash_value = hashlib.md5(hash_string.encode('utf-8')).hexdigest()
        logger.debug(f"metadata_hash of {study_id}: {hash_string}")
        return hash_value

    def strip_name(self, name):
        if name:
            return name.strip().strip("'").strip('"').strip(os.sep)
        return ""
    def calculate_referenced_files(self, path):
        file_set = set()
        investigation_file_name = "i_Investigation.txt"
        investigation = os.path.join(path, investigation_file_name)
        if os.path.exists(investigation):
            file_set.add(investigation_file_name)

            sample_pattern = r'^Study File Name\t.*$'
            lines: List[str] = self.search_lines_in_a_file(investigation, sample_pattern)
            if lines:
                samples = lines[0].split("\t")[1:]
                for sample in samples:
                    strip_name = self.strip_name(sample)
                    if strip_name:
                        file_set.add(strip_name)

            assays_pattern = r'^Study Assay File Name\t.*$'
            lines: List[str] = self.search_lines_in_a_file(investigation, assays_pattern)
            if lines:
                assays = lines[0].split("\t")[1:]
                for assay in assays:
                    assay_file_name = self.strip_name(assay)
                    if not assay_file_name:
                        logger.warning(f"No assay file name in investigation file. Path: {path}")
                        continue
                    file_set.add(assay_file_name)
                    assay_path = os.path.join(path, assay_file_name)
                    if os.path.exists(assay_path) and os.path.isfile(assay_path):
                        column_name_pattern = r'^.+ Data File(.\d+)?'
                        df = read_tsv_columns(assay_path, column_name_pattern=column_name_pattern)
                        for col in df:
                            files = df[col].unique()
                            for file in files:
                                strip_name = self.strip_name(file)
                                if strip_name:
                                    file_set.add(strip_name)
                        column_name_pattern = r'^.+ Assignment File(.\d+)?'
                        df = read_tsv_columns(assay_path, column_name_pattern=column_name_pattern)
                        for col in df:
                            files = df[col].unique()
                            for file in files:
                                strip_name = self.strip_name(file)
                                if strip_name:
                                    file_set.add(strip_name)
        return list(file_set)

    def get_reference_hierarchy(self, study_id, study_path, metadata_folder=None):
        study_folder = StudyFolder()
        study_folder.study_id = study_id
        study_folder.study_path = study_path
        metadata_folder = metadata_folder if metadata_folder else ""
        metadata_path = os.path.join(study_path, metadata_folder) if metadata_folder else study_path
        study_folder.metadata_path = metadata_path
        
        investigation_file_name = "i_Investigation.txt"
        investigation_file_path = os.path.join(metadata_path, investigation_file_name)
        investigation_file = File(name=investigation_file_name, path=metadata_folder)
        study_folder.file_index_map[investigation_file_name] = investigation_file
        
        if os.path.exists(investigation_file_path):
            study_folder.investigation_file = investigation_file
            sample_pattern = r'^Study File Name\t.*$'
            lines: List[str] = self.search_lines_in_a_file(investigation_file_path, sample_pattern)
            if lines:
                samples = lines[0].split("\t")[1:]
                
                if len(samples) > 1:
                    logger.warning(f"There are multiple sample file in {study_id} investigation file. Path: {metadata_path}")
                for sample in samples:
                    sample_file_name = self.strip_name(sample)
                    if sample_file_name:
                        sample_file = File(name=sample_file_name, path=metadata_folder)
                        study_folder.sample_file = sample_file
                        study_folder.file_index_map[sample_file_name] = sample_file
                        break # skip others
            
            if not study_folder.sample_file:
                logger.warning(f"No sample file definition in {study_id} investigation file. Path: {metadata_path}")
                                        
            assays_pattern = r'^Study Assay File Name\t.*$'
            lines: List[str] = self.search_lines_in_a_file(investigation_file_path, assays_pattern)
            assays_map = {}
            if lines:
                assays = lines[0].split("\t")[1:]
                for assay in assays:
                    assay_file_name = self.strip_name(assay)
                    if not assay_file_name:
                        continue
                    if assay_file_name in assays_map:
                        logger.warning(f"Assay {assay_file_name} file is already defined in {study_id} investigation file. Path: {metadata_path}")
                        continue
                    assay_file = Assay(name=assay_file_name, path=metadata_folder)
                    study_folder.assay_files.append(assay_file)
                    study_folder.file_index_map[assay_file_name] = assay_file
                    assay_path = os.path.join(metadata_path, assay_file_name)
                    if os.path.exists(assay_path):
                        patterns = SEARCH_PATTERNS["raw_files"]
                        self.update_assay_referenced_files(study_folder, assay_path, patterns, assay_file.raw_files, study_folder.file_index_map, study_folder.raw_files, study_folder.referenced_folders)
                        
                        patterns = SEARCH_PATTERNS["derived_files"]
                        self.update_assay_referenced_files(study_folder, assay_path, patterns, assay_file.derived_files, study_folder.file_index_map, study_folder.derived_files, study_folder.referenced_folders)

                        patterns = SEARCH_PATTERNS["derived_data_files"]
                        self.update_assay_referenced_files(study_folder, assay_path, patterns, assay_file.derived_data_files, study_folder.file_index_map, study_folder.derived_data_files, study_folder.referenced_folders)
                                                
                        patterns = SEARCH_PATTERNS["metabolites_files"]
                        self.update_assay_referenced_files(study_folder, assay_path, patterns, assay_file.metabolites_files, study_folder.file_index_map, study_folder.metabolites_files, study_folder.referenced_folders) 
            if not study_folder.assay_files:
                logger.warning(f"No assay file definition in {study_id} investigation file. Path: {metadata_path}")
            
        return study_folder                           

    
    def update_assay_referenced_files(self, study_folder, assay_path, search_pattern_list: List[str], file_list: List[File], unique_files: map, unique_file_list: List, referenced_folders):
        for column_name_pattern in search_pattern_list:
            files = self.get_files_in_assay_columns(assay_path, column_name_pattern)
            for item in files:
                basename = os.path.basename(item)
                dirname = os.path.dirname(item)
                designation, ext = os.path.splitext(basename)
                
                path = os.path.dirname(item)
                file = File(name=basename, path=path)
                file_list.append(file)
                extensions = []
                if ext in PAIRED_EXTENSIONS:
                    extensions = PAIRED_EXTENSIONS[ext]
                    
                extensions.append(ext)
                # add all pairs
                
                referenced_folders.add(path)
                for pair_ext in extensions:
                    pair_basename = f"{designation}{pair_ext}"
                    file = File(name=pair_basename, path=path)
                    file_list.append(file)
                    pair_relative_path = os.path.join(dirname, pair_basename)
                    if pair_relative_path not in unique_files:    
                        unique_files[pair_relative_path] = file
                        unique_file_list.append(file)
                        
                extensions = []
                if ext in OPTIONAL_PAIRED_EXTENSIONS:
                    extensions = OPTIONAL_PAIRED_EXTENSIONS[ext]                
                for pair_ext in extensions:
                    pair_basename = f"{designation}{pair_ext}"
                    file = File(name=pair_basename, path=path)
                    file_list.append(file)
                    
                    pair_relative_path = os.path.join(dirname, pair_basename)
                    pair_abs_path = os.path.join(study_folder.study_path, pair_relative_path)
                    if os.path.exists(pair_abs_path) and os.path.isfile(pair_abs_path):
                        if pair_relative_path not in unique_files:    
                            unique_files[pair_relative_path] = file
                            unique_file_list.append(file)
    
    @staticmethod
    def get_referenced_paths(study_path, hierarchy, ignored_folder_list=[], referenced_folder_extensions=[], referenced_folders_contain_files=[]):
        referenced_paths = set()
        referenced_paths.add(study_path)
        for referenced_folder in hierarchy.referenced_folders:
            if referenced_folder in ignored_folder_list:
                continue
            skip_folder = False
            for sub_file in referenced_folders_contain_files:
                file_path = os.path.join(referenced_folder, sub_file)
                relative_path = file_path.replace(study_path, "").lstrip(os.sep)
                if relative_path in hierarchy.file_index_map:
                    skip_folder = True
                    break
            if skip_folder or not referenced_folder:
                continue   
                
            sub_folders = referenced_folder.split(os.sep)
            for i in range(len(sub_folders)):
                relative_sub_path = os.sep.join(sub_folders[:i+1])
                if relative_sub_path not in hierarchy.file_index_map:
                    sub_path = os.path.join(study_path, relative_sub_path)
                    referenced_paths.add(sub_path)
                else:
                    file, ext = os.path.splitext(os.path.basename(referenced_folder))
                    if ext in referenced_folder_extensions:
                        break
        ordered_referenced_paths = list(referenced_paths)
        ordered_referenced_paths.sort()
        return ordered_referenced_paths
                       
    def get_files_in_assay_columns(self, assay_path, column_name_pattern):
        file_name_list = []
        df = read_tsv_columns(assay_path, column_name_pattern=column_name_pattern)
        for col in df:
            unique_files = df[col].unique()
            files = [self.strip_name(file) for file in unique_files if file]
            file_name_list.extend(files)
        return file_name_list
    
    @staticmethod
    def search_lines_in_a_file(file, pattern) -> List[str]:
        lines: List[str] = []
        fail_count = 0
        for encoding in ("unicode_escape", "latin-1"):
            with open(file, encoding=encoding) as f:
                try:
                    for line in f:
                        if re.search(pattern, line):
                            lines.append(line.strip())
                    if encoding == "latin-1":
                        logger.warning(f'{file} file is opened with {encoding} mode.')
                    break
                except Exception as e:
                    fail_count += 1
        if fail_count > 1:
            logger.error(f'{file} file is not opened. Please review study folder structure.')
        return lines

    def get_file_status_by_study_id(self, classification, study_id, study_path, file_path: str):
        if classification in self.default_mapper:
            return self.default_mapper[classification]

        referenced_file_list = self.get_referenced_file_list(study_id, study_path)
        file_name = file_path.replace(study_path, '').lstrip(os.sep)
        if file_name in referenced_file_list:
            return FileReferenceEvaluator.ACTIVE
        return FileReferenceEvaluator.NON_ACTIVE

    def get_file_status(self, classification, referenced_file_list, study_path, file_path: str):
        if classification in self.default_mapper:
            return self.default_mapper[classification]

        file_name = file_path.replace(study_path, '').lstrip(os.sep)
        if file_name in referenced_file_list:
            return FileReferenceEvaluator.ACTIVE
        return FileReferenceEvaluator.NON_ACTIVE