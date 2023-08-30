from typing import List
from pydantic import BaseModel


class FileFilters(BaseModel):
    deleted_samples_prefix_tag: str
    folder_exclusion_list: List[str]
    empty_exclusion_list: List[str]
    ignore_file_list: List[str]
    raw_files_list: List[str]
    derived_files_list: List[str]
    compressed_files_list: List[str]
    internal_mapping_list: List[str]
    rsync_exclude_list: List[str]
    derived_data_folder_list: List[str]
