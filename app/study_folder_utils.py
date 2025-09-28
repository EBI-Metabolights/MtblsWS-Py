import enum
import fnmatch
import glob
import os
import pathlib
from typing import Dict, List, Union

from pydantic import BaseModel

from app import application_path

STOP_FOLDER_EXTENSIONS = {".raw", ".d", ".fid"}

STOP_FOLDER_SAMPLE_FILES = {".raw": "_FUNC001.DAT", ".d": "SyncHelper", ".fid": "fid"}
DEFAULT_SAMPLE_FILE_NAME = "_history"
# SKIP_FOLDER_NAMES= {"audit", "chebi_pipeline_annotations", "__MACOSX", "AUDIT_FILES", "INTERNAL_FILES"}
SKIP_FOLDER_CONTAINS_ANY = ["fid*", "ser*", "pdata"]
SKIP_FOLDER_CONTAINS_FILE_NAME_PATTERN = "acqu*"

SKIP_FILE_EXTENSIONS =  {".__MACOSX"}

METADATA_FILE_PREFIXES = {"a_", "i_", "s_", "m_"}
MANAGED_FOLDERS = {"FILES", "FILES/RAW_FILES", "FILES/DERIVED_FILES", "FILES/SUPPLEMENTARY_FILES"}


def convert_relative_to_real_path(relative_path: str) -> str:
    if not relative_path or relative_path.startswith(os.sep):
        return relative_path
    if relative_path == ".":
        return application_path
    removed_prefix = relative_path.replace(f".{os.sep}", "", 1) if relative_path.startswith(f".{os.sep}") else relative_path
    return os.path.join(application_path, removed_prefix)
    
class FileDifference(enum.StrEnum):
    NEW =  "NEW"
    DELETED = "DELETED"
    MODIFIED = "MODIFIED"
    

class FileDescriptor(BaseModel):
    file_difference: Union[None, FileDifference] = None
    name: str = ""
    parent_relative_path: str = ""
    relative_path: str = ""
    modified_time: float = 0
    is_dir: bool = False
    extension: str = ""
    is_stop_folder: bool = False
    sub_filename:str = ""
    is_empty: bool = False
    file_size: int = 0


def get_all_metadata_files(study_metadata_files_path: Union[None, str] = None):
    metadata_files = []
    if not os.path.exists(study_metadata_files_path):
        return metadata_files
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
 
def get_all_study_metadata_and_data_files(study_metadata_path: str, exclude_list: List[str]=None, include_metadata_files: bool=True, add_sub_folders: bool = True, list_all_files:bool=False) -> Dict[str, FileDescriptor]:
        data_files_path = study_metadata_path
        file_descriptors: Dict[str, FileDescriptor] = {}

        study_source_path_item = pathlib.Path(data_files_path)
        source_folders_iter = []
        if os.path.exists(study_metadata_path):
            source_folders_iter = get_study_folder_files(
                data_files_path, file_descriptors, study_source_path_item, pattern=None, recursive=True, exclude_list=exclude_list, 
                include_metadata_files=include_metadata_files, add_sub_folders=add_sub_folders, list_all_files=list_all_files
            )
        
            [x for x in source_folders_iter]
            
        # search_results =  []
        # for desc in file_descriptors:
        #     search_results.append(desc)
        # search_results = [ file_descriptors[x] for x in file_descriptors]
        # search_results.sort(key=lambda x: x.relative_path)

        return file_descriptors
       

def get_study_folder_files(root_path: str, file_descriptors: Dict[str, FileDescriptor],
                           root: pathlib.Path, list_all_files=False, pattern=None, 
                           recursive=True, exclude_list=None, include_metadata_files=False, 
                           add_sub_folders: bool=True):
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
            # if not list_all_files:
            #     if item.name in SKIP_FOLDER_NAMES:
            #         continue
            sub_filename = ""
            referenced_sub_files = set()
            is_stop_folder = False
            if relative_path and str(relative_path) not in MANAGED_FOLDERS:
                search = glob.iglob(f"{item}/{SKIP_FOLDER_CONTAINS_FILE_NAME_PATTERN}")
                
                files = [x for x in search if "." not in os.path.basename(x)]
                
                if files:
                    referenced_sub_files = referenced_sub_files.union(set(files))
                    for parameters_file in SKIP_FOLDER_CONTAINS_ANY:
                        items = glob.iglob(f"{item}/{parameters_file}")
                        expected_files = [x for x in items if "." not in os.path.basename(x) and "_" not in os.path.basename(x)]
                        if expected_files:
                            referenced_sub_files = referenced_sub_files.union(set(expected_files))
                            sub_filename = parameters_file
                            is_stop_folder = True
                                    
            ext = item.suffix.lower()
            relative_path = str(item).replace(f"{root_path}", "").lstrip("/")

            m_time = os.path.getmtime(item)
            if ext in STOP_FOLDER_EXTENSIONS or is_stop_folder:
                if pattern and not fnmatch.fnmatch(name, pattern):
                    continue
                if is_stop_folder:
                    sub_filename = ""
                else:
                    if ext in STOP_FOLDER_SAMPLE_FILES:
                        sub_filename = STOP_FOLDER_SAMPLE_FILES[ext]
                    else:
                        sub_filename = DEFAULT_SAMPLE_FILE_NAME
                    
                file_descriptors[relative_path] = FileDescriptor(relative_path=relative_path, is_dir=True, modified_time=m_time, extension=ext, is_stop_folder=True, sub_filename=sub_filename)
                if is_stop_folder:
                    for subfile in referenced_sub_files:
                        relative_path = str(subfile).replace(f"{root_path}", "").lstrip("/")
                        file_descriptors[relative_path] = FileDescriptor(relative_path=relative_path, is_dir=False, modified_time=m_time, extension="", is_stop_folder=False, sub_filename="")
                yield item
            else:
                if not pattern or (pattern and fnmatch.fnmatch(name, pattern)):
                    is_empty = True
                    for _ in root.iterdir():
                        is_empty = False
                        break
                    if add_sub_folders or is_empty:
                        file_descriptors[relative_path] = FileDescriptor(relative_path=relative_path, is_dir=True, modified_time=m_time, extension=ext, is_stop_folder=False, is_empty=is_empty)
                    
                if recursive:
                    yield from get_study_folder_files(root_path, file_descriptors, item, list_all_files=list_all_files, pattern=pattern, recursive=recursive, exclude_list=exclude_list, include_metadata_files=include_metadata_files)
                else:
                    yield item
        else:
            if pattern and not fnmatch.fnmatch(name, pattern):
                continue
            ext = item.suffix.lower()
            if not list_all_files:
                if ext in SKIP_FILE_EXTENSIONS:
                    continue
                if not include_metadata_files:
                    if len(item.name) > 2 and item.name[:2] in METADATA_FILE_PREFIXES and (item.name.endswith(".txt") or item.name.endswith(".tsv")):
                        continue

            relative_path = str(item).replace(f"{root_path}/", "")
            if item.is_symlink() and not item.resolve().exists():
                continue
            if not item.exists():
                continue
            file_size = 0
            if not item.is_symlink() or (item.is_symlink() and item.resolve().exists()):
                
                file_size = os.path.getsize(item)
                m_time = os.path.getmtime(item)
                is_empty = True if item.stat().st_size == 0 else False
            file_descriptors[relative_path] = FileDescriptor(relative_path=relative_path, is_dir=False, modified_time=m_time, extension=ext, is_empty=is_empty, file_size=file_size)
                                
            yield item