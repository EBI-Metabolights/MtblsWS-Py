from typing import Union, List
from pydantic import BaseModel


class ValidationTaskDescription(BaseModel):
    task_id: str = ""
    last_update_time: Union[int, float] = 0
    last_update_time_str: str = ""
    last_status: str = ""
    task_done_time: Union[int, float] = 0
    task_done_time_str: str = ""



class ValidationDetail(BaseModel):
    message: str = ""
    section: str = ""
    val_sequence: str = ""
    status: str = ""
    metadata_file: str = ""
    value: str = ""
    description: str = ""
    val_override: str = ""
    val_message: str = ""
    comment: str = ""

def validation_message_sorter(item: ValidationDetail):
    if item.status:
        if item.status == "error":
            return 0
        elif item.status == "warning":
            return 10
        elif item.status == "info":
            return 100
        elif item.status == "success":
            return 100
    return 1000     


class ValidationSection(BaseModel):
    section: str = ""
    has_more_items: bool = False
    details: List[ValidationDetail] = []
    
class ValidationReportContent(BaseModel):
    version: str = "1.0"
    status: str = "not ready"
    timing: float = 0.0
    last_update_time: str = ""
    last_update_timestamp: Union[int, float] = 0
    validations: List[ValidationSection] = []
    
class ValidationReportFile(BaseModel):
    validation: ValidationReportContent = ValidationReportContent()