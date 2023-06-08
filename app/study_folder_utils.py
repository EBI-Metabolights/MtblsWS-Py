import glob
import os
import pathlib
from typing import Dict

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
   
def get_study_folder_files(root_path, file_descriptors: Dict[str, FileDescriptor], root: pathlib.Path, list_all_files=False):
    for item in root.iterdir():
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
            relative_path = str(item).replace(f"{root_path}/", "")
            dirname = item.parent
            m_time = os.path.getmtime(item)
            relative_parent_path = str(item).replace(f"{root_path}/", "")
            if relative_parent_path and relative_parent_path not in file_descriptors:
                parent_dir_m_time = os.path.getmtime(dirname)
                parent_ext = dirname.suffix
                file_descriptors[relative_parent_path] = FileDescriptor(relative_path=relative_parent_path, is_dir=True, modified_time=parent_dir_m_time, extension=parent_ext)
            if ext in STOP_FOLDER_EXTENSIONS or is_stop_folder:
                if ext in STOP_FOLDER_SAMPLE_FILES:
                    sub_filename = STOP_FOLDER_SAMPLE_FILES[ext]
                else:
                    sub_filename = DEFAULT_SAMPLE_FILE_NAME
                file_descriptors[relative_path] = FileDescriptor(relative_path=relative_path, is_dir=True, modified_time=m_time, extension=ext, is_stop_folder=True, sub_filename=sub_filename)
                yield item
            else:
                yield from get_study_folder_files(root_path, file_descriptors, item, list_all_files=list_all_files)
        else:
            ext = item.suffix
            if not list_all_files:
                if ext in SKIP_FILE_EXTENSIONS:
                    continue
                if len(item.name) > 2 and item.name[:2] in METADATA_FILE_PREFIXES:
                    continue

                
            relative_path = str(item).replace(f"{root_path}/", "")
            m_time = os.path.getmtime(item)
            file_descriptors[relative_path] = FileDescriptor(relative_path=relative_path, is_dir=False, modified_time=m_time, extension=ext)
                                
            yield item