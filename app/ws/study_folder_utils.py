import datetime
import logging
import os
import pathlib
from typing import Dict, List, Set, Union

from isatools import isatab
from isatools.model import Investigation, Study, Assay
import pandas as pd
from pydantic import BaseModel, ConfigDict
from app.config import get_settings
from app.study_folder_utils import FileDescriptor, get_study_folder_files

logger = logging.getLogger("wslog")


class LiteFileMetadata(BaseModel):
    createdAt: str = ""
    directory: bool = False
    file: str
    status: str = "unreferenced"
    timestamp: str = ""
    type: str = "unknown"
    model_config = ConfigDict(from_attributes=True)


def sortFileMetadataList(items: List[LiteFileMetadata]):
    items.sort(key=metadata_sort)

def metadata_sort(item: LiteFileMetadata):
    if not item:
        return ""
    if item.directory:
        return item.file.upper()
    else:
        # ~~ is to ensure files are after folders
        return "~~" + item.file.upper()
    
class FileMetadata(LiteFileMetadata):
    relative_path: str = ""
    extension: str = ""
    is_stop_folder: bool = False
    is_empty: bool = False
    model_config = ConfigDict(from_attributes=True)
    


class FileSearchResult(BaseModel):
    study: List[FileMetadata] = []
    private: List[str] = []
    uploadPath: str = ""
    obfuscationCode: str = ""
    latest: List[FileMetadata] = []
    model_config = ConfigDict(from_attributes=True)


class LiteFileSearchResult(BaseModel):
    study: List[LiteFileMetadata] = []
    private: List[str] = []
    uploadPath: str = ""
    obfuscationCode: str = ""
    latest: List[LiteFileMetadata] = []
    model_config = ConfigDict(from_attributes=True)


class StudyFolderIndex(BaseModel):
    study: List[FileMetadata] = []
    model_config = ConfigDict(from_attributes=True)


def get_directory_files(
    root_path: str, subpath: str, recursive=False, search_pattern="**/*", exclude_list=None, include_metadata_files=True
):
    source_file_descriptors: Dict[str, FileDescriptor] = {}
    if not exclude_list:
        exclude_list = []
    root_path_item = pathlib.Path(root_path)

    source_folders_iter = []
    filtered_path = root_path_item
    if subpath:
        subpath_item = pathlib.Path(subpath)
        filtered_path = root_path_item / subpath_item
        search_path = root_path_item / pathlib.Path(search_pattern)
    else:
        filtered_path = root_path_item
        search_path = root_path_item.parent / pathlib.Path(search_pattern)
    if os.path.exists(root_path):
        source_folders_iter = get_study_folder_files(
            root_path_item,
            source_file_descriptors,
            filtered_path,
            pattern=str(search_path),
            recursive=recursive,
            exclude_list=exclude_list,
            include_metadata_files=include_metadata_files,
        )

        [x for x in source_folders_iter]
    return source_file_descriptors

def evaluate_files(
    source_file_descriptors: Dict[str, FileDescriptor], referenced_file_set: Set[str]
) -> LiteFileSearchResult:
    file_search_result = evaluate_files_in_detail(source_file_descriptors, referenced_file_set)
    return LiteFileSearchResult.model_validate(file_search_result)

def evaluate_files_in_detail(
    source_file_descriptors: Dict[str, FileDescriptor], referenced_file_set: Set[str]
) -> FileSearchResult:
    file_search_result = FileSearchResult()
    results = file_search_result.study
    settings = get_settings()
    derived_file_extensions = set(settings.file_filters.derived_files_list)
    raw_file_extensions = set(settings.file_filters.raw_files_list)
    for key, file in source_file_descriptors.items():
        name = os.path.basename(file.relative_path)
        datetime.datetime.fromtimestamp(file.modified_time)
        modified_time = datetime.datetime.fromtimestamp(file.modified_time)
        long_datetime = modified_time.strftime("%Y-%m-%d %H:%M:%S")
        timestamp = modified_time.strftime("%Y%m%d%H%M%S")
        metadata = FileMetadata(file=name, directory=file.is_dir, createdAt=long_datetime, timestamp=timestamp)
        # if  file.is_stop_folder:
        #     metadata.directory = False
        # else:
        #     if file.is_dir:
        #         metadata.type = "directory"
        if file.is_stop_folder:
            metadata.type = "raw"
        if file.is_dir:
            metadata.type = "directory"
        if file.relative_path in referenced_file_set:
            metadata.status = "active"
        file_extension = file.extension
        if file_extension == ".tsv" and name.startswith("m_"):
            metadata.type = "metadata_maf"
        elif file_extension == ".txt" and name.startswith("s_"):
            metadata.type = "metadata_sample"
        elif file_extension == ".txt" and name.startswith("a_"):
            metadata.type = "metadata_assay"
        elif file_extension == ".txt" and name.startswith("i_"):
            metadata.type = "metadata_investigation"
        elif file_extension in derived_file_extensions:
            metadata.type = "derived"
        elif file_extension in raw_file_extensions:
            metadata.type = "raw"
        elif file_extension in (".xls", ".xlsx", ".xlsm", ".csv", ".tsv"):
            metadata.type = "spreadsheet"
        elif file_extension in (".png", ".tiff", ".tif", ".jpeg", ".mpg", ".jpg"):
            metadata.type = "image"
        elif file_extension in  (".rar", ".7z", ".z", ".g7z", ".arj", ".bz2", ".war", ".tar", ".zip"):
            metadata.type = "compressed"

        if file.relative_path.startswith(
            settings.study.audit_files_symbolic_link_name
        ) or file.relative_path.startswith(settings.study.internal_files_symbolic_link_name):
            metadata.type = "audit"
        metadata.relative_path = file.relative_path
        metadata.is_stop_folder = file.is_stop_folder
        metadata.extension = file_extension
        metadata.is_empty = file.is_empty
        results.append(metadata)
    return file_search_result


def get_referenced_file_set(study_id, metadata_path: str) -> List[str]:
    referenced_files: Set[str] = set()
    try:
        investigation_file_name = get_settings().study.investigation_file_name
        investigation_file_path = os.path.join(metadata_path, investigation_file_name)
        investigation: Union[None, Investigation] = None
        if not os.path.exists(investigation_file_path):
            return []
        try:
            with open(investigation_file_path, encoding="utf-8", errors="ignore") as fp:
                # loading tables also load Samples and Assays
                investigation = isatab.load(fp, skip_load_tables=True)
        except Exception as ex:
            logger.warning(f"Error while loading investigation file '{study_id}")
            with open(investigation_file_path, encoding="latin-1") as fp:
                # loading tables also load Samples and Assays
                investigation = isatab.load(fp, skip_load_tables=True)  
        referenced_files.add(investigation_file_name)
        if investigation and investigation.studies and investigation.studies[0]:
            study: Study = investigation.studies[0]
            referenced_files.add(study.filename)
            if study.assays:
                for item in study.assays:
                    assay: Assay = item
                    referenced_files.add(assay.filename)
                    assay_file_path = os.path.join(metadata_path, assay.filename)
                    if os.path.exists(assay_file_path):
                        try:
                            df = None
                            try:
                                with open(assay_file_path, encoding="utf-8", errors="ignore") as fp:
                                    df: pd.DataFrame = pd.read_csv(fp, delimiter="\t", header=0, dtype=str)
                            except Exception as ex:
                                logger.warning(f"Error while loading assay file '{study_id} {assay.filename}")
                                with open(assay_file_path, encoding="latin-1", errors="ignore") as fp:
                                    df: pd.DataFrame = pd.read_csv(fp, delimiter="\t", header=0, dtype=str)
                            if df is not None:
                                df = df.fillna("")
                            referenced_file_columns: List[str] = []
                            for column in df.columns:
                                if " Data File" in column or "Metabolite Assignment File" in column:
                                    referenced_file_columns.append(column)
                                    file_names = df[column].unique()
                                    for item in file_names:
                                        if item:
                                            referenced_files.add(item)
                        except Exception as ex:
                            logger.error(f"Error reading assay file of {study_id} {assay.filename}. Skipping...")
                        # df: pd.DataFrame = pd.read_csv(assay_file_path, delimiter="\t", header=0, names=referenced_file_columns, dtype=str)
                        # if df is not None:
                        #     df = df.fillna("")
                        # for column in referenced_file_columns:
                        #     file_names = df[column].unique()
                        #     for item in file_names:
                        #         if item:
                        #             referenced_files.add(item)
    except Exception as exc:
        logger.error(f"Error reading investigation file of {study_id}")
    file_list = list(referenced_files)
    file_list.sort()
    return file_list
