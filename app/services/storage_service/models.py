from enum import Enum

from pydantic import BaseModel, Field


class SyncCalculationStatus(str, Enum):
    NO_TASK = 'NO_TASK'
    UNKNOWN = 'UNKNOWN'
    PENDING = 'PENDING'
    SYNC_NEEDED = "SYNC_NEEDED"
    SYNC_NOT_NEEDED = "SYNC_NOT_NEEDED"
    CALCULATING = "CALCULATING"
    NOT_FOUND = "NOT_FOUND"
    CALCULATION_FAILURE = "CALCULATION_FAILURE"


class SyncTaskStatus(str, Enum):
    NO_TASK = 'NO_TASK'
    UNKNOWN = 'UNKNOWN'
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    START_FAILURE = 'START_FAILURE'
    SYNC_FAILURE = 'SYNC_FAILURE'
    COMPLETED_SUCCESS = 'COMPLETED_SUCCESS'


class JobResultModel(BaseModel):
    description: str = Field('')
    last_update_time: str = Field('')


class SyncTaskResult(JobResultModel):
    status: SyncTaskStatus = Field(SyncTaskStatus.NO_TASK)


class SyncCalculationTaskResult(JobResultModel):
    status: SyncCalculationStatus = Field(SyncCalculationStatus.NO_TASK)


class CommandOutput(BaseModel):
    execution_status: bool = Field(False)
    execution_output: str = Field(None)