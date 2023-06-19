import fnmatch
import glob
import os
import pathlib
from typing import Dict, List

from pydantic import BaseModel

STOP_FOLDER_EXTENSIONS = {".raw", ".d", ".fid"}

STOP_FOLDER_SAMPLE_FILES = {".raw": "_FUNC001.DAT", ".d": "SyncHelper", ".fid": "fid"}
DEFAULT_SAMPLE_FILE_NAME = "_history"
SKIP_FOLDER_NAMES= {"audit", "chebi_pipeline_annotations", "__MACOSX"}
SKIP_FOLDER_CONTAINS_FILE_NAME = "fid"
SKIP_FOLDER_CONTAINS_FILE_NAME_PATTERN = "acqu*"

SKIP_FILE_EXTENSIONS =  {".json"}

METADATA_FILE_PREFIXES = {"a_", "i_", "s_", "m_"}



class FileDescriptor(BaseModel):
    relative_path: str = ""
    modified_time: float = 0
    is_dir: bool = False
    extension: str = ""
    is_stop_folder: bool = False
    sub_filename:str = ""


def get_all_metadata_files(study_metadata_files_path):
    metadata_files = []
    patterns = ["a_*.txt", "s_*.txt", "i_*.txt", "m_*.tsv"]
    for pattern in patterns:
        metadata_files.extend(glob.glob(os.path.join(study_metadata_files_path, pattern), recursive=False))
    return metadata_files

def find_study_data_files(study_metadata_path, filtered_subfolder=None, search_pattern=None, match_files=True, match_folders=True):

        if filtered_subfolder:
            data_files_path = os.path.join(study_metadata_path, filtered_subfolder)
        else:
            data_files_path = study_metadata_path
        search_path = os.path.join(study_metadata_path, search_pattern)
        
        source_file_descriptors: Dict[str, FileDescriptor] = {}

        study_source_path_item = pathlib.Path(data_files_path)
        source_folders_iter = []
        if os.path.exists(study_metadata_path):
            source_folders_iter = get_study_folder_files(
                data_files_path, source_file_descriptors, study_source_path_item, pattern=search_path
            )
        
            [x for x in source_folders_iter]
            
        search_results =  []
        for desc in source_file_descriptors:
            if match_files and not source_file_descriptors[desc].is_dir:
                search_results.append(f"{filtered_subfolder}/{desc}" if filtered_subfolder else desc)
            if match_folders and source_file_descriptors[desc].is_dir:
                search_results.append(f"{filtered_subfolder}/{desc}" if filtered_subfolder else desc)
        
        search_results.sort()

        result = [{"name": str(file)} for file in search_results]
        return result
    

def get_study_folder_files(root_path, file_descriptors: Dict[str, FileDescriptor], root: pathlib.Path, list_all_files=False, pattern=None, recursive=True, exclude_list=None, include_metadata_files=False):
    if not exclude_list:
        exclude_list = []
    relative_root_path = str(root).replace(f"{root_path}", "").lstrip("/")
    if str(root_path) != str(root):
        if not relative_root_path or relative_root_path in exclude_list:
            yield root
    for item in root.iterdir():
        relative_path = str(item).replace(f"{root_path}", "").lstrip("/")
        if relative_path in exclude_list:
            continue
        name = str(item)
        if not list_all_files:
            if item.name and item.name[0] == ".":
                continue
        if item.is_dir():            
            if not list_all_files:
                if item.name in SKIP_FOLDER_NAMES:
                    continue
            sub_filename = ""
            is_stop_folder = False
            if str(item) != root_path:
                if os.path.exists(f"{item}/{SKIP_FOLDER_CONTAINS_FILE_NAME}"):
                    files = glob.iglob(f"{item}/{SKIP_FOLDER_CONTAINS_FILE_NAME_PATTERN}")
                    if files:
                        is_stop_folder = True
                        sub_filename = SKIP_FOLDER_CONTAINS_FILE_NAME


                        
            ext = item.suffix.lower()
            relative_path = str(item).replace(f"{root_path}", "").lstrip("/")

            m_time = os.path.getmtime(item)
            if ext in STOP_FOLDER_EXTENSIONS or is_stop_folder:
                if pattern and not fnmatch.fnmatch(name, pattern):
                    continue
                if ext in STOP_FOLDER_SAMPLE_FILES:
                    sub_filename = STOP_FOLDER_SAMPLE_FILES[ext]
                else:
                    sub_filename = DEFAULT_SAMPLE_FILE_NAME
                file_descriptors[relative_path] = FileDescriptor(relative_path=relative_path, is_dir=True, modified_time=m_time, extension=ext, is_stop_folder=True, sub_filename=sub_filename)
                yield item
            else:
                if pattern and fnmatch.fnmatch(name, pattern):
                    file_descriptors[relative_path] = FileDescriptor(relative_path=relative_path, is_dir=True, modified_time=m_time, extension=ext, is_stop_folder=False)
                if recursive:
                    yield from get_study_folder_files(root_path, file_descriptors, item, list_all_files=list_all_files, pattern=pattern, recursive=recursive, exclude_list=exclude_list, include_metadata_files=include_metadata_files)
                else:
                    yield item
        else:
            if pattern and not fnmatch.fnmatch(name, pattern):
                continue
            ext = item.suffix
            if not list_all_files:
                if ext in SKIP_FILE_EXTENSIONS:
                    continue
                if not include_metadata_files:
                    if len(item.name) > 2 and item.name[:2] in METADATA_FILE_PREFIXES:
                        continue

                
            relative_path = str(item).replace(f"{root_path}/", "")
            m_time = os.path.getmtime(item)
            file_descriptors[relative_path] = FileDescriptor(relative_path=relative_path, is_dir=False, modified_time=m_time, extension=ext)
                                
            yield item