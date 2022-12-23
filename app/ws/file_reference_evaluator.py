from typing import Dict, Set
import csv
import glob
import logging
import re
import os
import hashlib
from datetime import datetime
from typing import Any, List

from pydantic import BaseModel

from app.ws.df_utils import read_tsv_columns

logger = logging.getLogger('wslog')

class File(BaseModel):
    name: str = ""
    path: str = ""
    metadata: Dict[str, Any] = {}
    is_folder: bool = False
    is_required: bool = True
    
class ReferenceFile(File):   
    raw_files: Dict[str, File] = {}
    derived_files: Dict[str, File] = {}
    supplementary_data_files: Dict[str, File] = {}
    metabolites_files: Dict[str, File] = {}
    file_index_map: Dict[str, File] = {}

class Assay(ReferenceFile):
    pass

class StudyFolder(ReferenceFile):
    study_id: str = ""
    study_path: str = ""
    metadata_path: str = ""
    raw_file_folder: str = "RAW_FILES"
    derived_file_folder: str = "DERIVED_FILES"
    supplementary_data_folder: str = "SUPPLEMENTARY_FILES"
    
    assay_files: Dict[str, File] = {}
    investigation_file: File = None
    sample_file: File = None
    search_path: Set[str] = set()
    referenced_folders_contain_special_files: Dict[str, Set[str]] = {} 
    
    
# search regular expressions to find file columns in assay

SEARCH_PATTERNS = {"raw_files": [r'^Raw.+ Data File(.\d+)?'], 
                    "derived_files": [r'^Derived.+ Data File(.\d+)?'], 
                    "supplementary_data_files": [r'^Normalization.+ File(.\d+)?', r'^.+ Decay Data File(.\d+)?', r'^.+ Parameter Data File(.\d+)?'],
                    "metabolites_files": [r'^.+ Assignment File(.\d+)?']}

# if a file has same extension of key, the files that has same designation name and an extension given in value list are added reference hierarchy   
REQUIRED_PAIRED_EXTENSIONS = {".wiff": [".wiff.scan"]}

# if a file has same extension of key and there is a file that has same designation name and an extension given in value list, this file is also added reference hierarchy   
OPTIONAL_PAIRED_EXTENSIONS = {"": [".peg"]}



class FileReferenceEvaluator(object):
    ACTIVE = 'active'
    NON_ACTIVE = 'unreferenced'

    def __init__(self, referenced_folders_contain_files=None, ignore_folder_list=None):
        self.default_mapper = {}
        self.study_reference_cache_map = {}
        self.referenced_folders_contain_files = referenced_folders_contain_files if referenced_folders_contain_files else ["acqus", "fid", "acqu"]
        self.ignore_folder_list = ignore_folder_list if ignore_folder_list else ["chebi_pipeline_annotations", "audit"]

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
            self.study_reference_cache_map[study_id] = {"metadata_hash": "", 
                                                        "reference_file_list": set(),
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


    def calculate_metadata_hash_for_study(self, study_id: str, path: str) -> str:
        """
        Calculates a hash value using the combination of name, modified time and size of ISA_METADATA files

        Args:
            study_id (str): study accession number
            path (str): path of the input study

        Returns:
            str: calculated MD5 hash value
        """
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

    def strip_name(self, name: str) -> str:
        """ Remove quotes and directory chars from the given string

        Args:
            name (str): any string to remove special prefixes and sufixes

        Returns:
            : the string removed from quotes and directory chars
        """
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
                        
                        column_name_pattern = r'^.+ File(.\d+)?'
                        df = read_tsv_columns(assay_path, column_name_pattern=column_name_pattern)
                        for col in df:
                            files = df[col].unique()
                            for file in files:
                                strip_name = self.strip_name(file)
                                if strip_name:
                                    file_set.add(strip_name)
        return list(file_set)

    def get_reference_hierarchy(self, study_id: str, study_path: str, metadata_folder: str =None) -> StudyFolder:
        """_summary_

        Args:
            study_id (str): study accession number
            study_path (str): path of the study
            metadata_folder (str, optional): folder of ISA_METADATA files. Defaults to None (study_path).

        Returns:
            StudyFolder: All hierarcy of a study
        """
        
        study_folder = StudyFolder()
        study_folder.study_id = study_id
        study_folder.study_path = study_path
        metadata_folder = metadata_folder if metadata_folder else ""
        metadata_path = os.path.join(study_path, metadata_folder) if metadata_folder else study_path
        study_folder.metadata_path = metadata_path
        
        investigation_file_name = "i_Investigation.txt"
        investigation_file_path = os.path.join(metadata_path, investigation_file_name)
        investigation_file = File(name=investigation_file_name, path=metadata_folder)
        
        if os.path.exists(investigation_file_path):
            study_folder.file_index_map[investigation_file_name] = investigation_file
            study_folder.investigation_file = investigation_file
            sample_pattern = r'^Study File Name\t.*$'
            assays_pattern = r'^Study Assay File Name\t.*$'
            results = self.search_lines_in_a_file(investigation_file_path, [sample_pattern, assays_pattern])
            lines = results[sample_pattern]
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
                                        
            lines = results[assays_pattern]
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
                    study_folder.assay_files[assay_file_name] = assay_file
                    study_folder.file_index_map[assay_file_name] = assay_file
                    assay_path = os.path.join(metadata_path, assay_file_name)
                    if os.path.exists(assay_path):
                        patterns = SEARCH_PATTERNS["raw_files"]
                        self._update_assay_references(study_folder, assay_path, patterns, assay_file.raw_files, study_folder.raw_files)
                        
                        patterns = SEARCH_PATTERNS["derived_files"]
                        self._update_assay_references(study_folder, assay_path, patterns, assay_file.derived_files, study_folder.derived_files)

                        patterns = SEARCH_PATTERNS["supplementary_data_files"]
                        self._update_assay_references(study_folder, assay_path, patterns, assay_file.supplementary_data_files, study_folder.supplementary_data_files)
                                                
                        patterns = SEARCH_PATTERNS["metabolites_files"]
                        self._update_assay_references(study_folder, assay_path, patterns, assay_file.metabolites_files, study_folder.metabolites_files) 
            if not study_folder.assay_files:
                logger.warning(f"No assay file definition in {study_id} investigation file. Path: {metadata_path}")
            referenced_folders = self._get_referenced_folders_contain_special_files(study_path, study_folder, self.ignore_folder_list, self.referenced_folders_contain_files)    
            study_folder.referenced_folders_contain_special_files = referenced_folders
            study_folder.search_path = study_folder.search_path.difference(set(referenced_folders.keys()))

            for referenced_folder in referenced_folders:
                parent_dir = os.path.dirname(referenced_folder)
                if parent_dir and parent_dir not in study_folder.search_path:
                    study_folder.search_path.add(parent_dir)
        return study_folder                           

    
    def _update_assay_references(self, study_folder: StudyFolder, assay_path: str, search_pattern_list: List[str], 
                                 file_list: Dict[str, File], unique_file_list: Dict[str, File]):
        for column_name_pattern in search_pattern_list:
            files = self.get_files_in_assay_columns(assay_path, column_name_pattern)
            for item in files:
                basename = os.path.basename(item)
                path = os.path.dirname(item)
                study_folder.search_path.add(path)
                # add all pairs
                designation, ext = os.path.splitext(basename)
                required_extensions = [ext]
                if ext in REQUIRED_PAIRED_EXTENSIONS:
                    required_extensions.extend(REQUIRED_PAIRED_EXTENSIONS[ext])
                
                optional_extensions = []
                if ext in OPTIONAL_PAIRED_EXTENSIONS:
                    optional_extensions = OPTIONAL_PAIRED_EXTENSIONS[ext] 
                                    
                self.add_files_with_extensions(study_folder, path, designation, 
                                               file_list, unique_file_list, required_extensions, optional=False)
                self.add_files_with_extensions(study_folder, path, designation, 
                                               file_list, unique_file_list, optional_extensions, optional=True)

    def add_files_with_extensions(self, study_folder, path, designation, file_list, unique_file_list, extensions, optional=False):
        for pair_ext in extensions:
            pair_basename = f"{designation}{pair_ext}"
            file = File(name=pair_basename, path=path)
            file.is_required = False
            pair_relative_path = os.path.join(path, pair_basename)
            file_list[pair_relative_path] = file
            add_to_list = True
            if optional:
                pair_abs_path = os.path.join(study_folder.study_path, pair_relative_path)
                if not os.path.exists(pair_abs_path) or not os.path.isfile(pair_abs_path):
                    add_to_list = False
            if add_to_list and pair_relative_path not in study_folder.file_index_map:    
                    study_folder.file_index_map[pair_relative_path] = file
                    unique_file_list[pair_relative_path] = file

    @staticmethod
    def _get_referenced_folders_contain_special_files(study_path, hierarchy, ignored_folder_list=[], referenced_folders_contain_files=[]):
        skiped_folders = {}
        for referenced_folder in hierarchy.search_path:
            if not referenced_folder or referenced_folder in ignored_folder_list:
                continue
            relative_path = ""
            for sub_file in referenced_folders_contain_files:
                file_path = os.path.join(referenced_folder, sub_file)
                relative_path = file_path.replace(study_path, "").lstrip(os.sep)
                if relative_path in hierarchy.file_index_map:
                    if not referenced_folder in skiped_folders:
                        skiped_folders[referenced_folder] = set()
                    skiped_folders[referenced_folder].add(relative_path)

        return skiped_folders  
            

    @staticmethod
    def get_referenced_paths(study_path, hierarchy, ignored_folder_list=[], referenced_folder_extensions=[], referenced_folders_contain_files=[]):
        referenced_paths = set()
        referenced_paths.add(study_path)
        for referenced_folder in hierarchy.search_path:
            if not referenced_folder or referenced_folder in ignored_folder_list:
                continue
            skip_folder = False
            relative_path = ""
            for sub_file in referenced_folders_contain_files:
                file_path = os.path.join(referenced_folder, sub_file)
                relative_path = file_path.replace(study_path, "").lstrip(os.sep)
                if relative_path in hierarchy.file_index_map:
                    skip_folder = True
                    break
            if skip_folder:
                continue   
                
            sub_folders = referenced_folder.split(os.sep)
            for i in range(len(sub_folders)):
                relative_sub_path = os.sep.join(sub_folders[:i+1])
                if relative_sub_path not in hierarchy.file_index_map:
                    sub_path = os.path.join(study_path, relative_sub_path)
                    referenced_paths.add(sub_path)
                else:
                    _, ext = os.path.splitext(os.path.basename(referenced_folder))
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
    def search_lines_in_a_file(file: str, pattern: str) -> List[str]:
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

    @staticmethod
    def search_lines_in_a_file(file: str, patterns: List[str]) -> List[str]:
        lines: Dict[str, List[str]] = {}
        for pattern  in patterns:
            lines[pattern] = []
        fail_count = 0
        for encoding in ("unicode_escape", "latin-1"):
            with open(file, encoding=encoding) as f:
                try:
                    for line in f:
                        for pattern in patterns:
                            if re.search(pattern, line):
                                lines[pattern].append(line.strip())
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