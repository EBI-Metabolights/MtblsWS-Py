from enum import Enum

from pydantic import BaseModel, Field


class SyncCalculationStatus(str, Enum):
    SYNC_NEEDED = "SYNC_NEEDED"
    SYNC_NOT_NEEDED = "SYNC_NOT_NEEDED"
    CALCULATING = "CALCULATING"


class SyncTaskStatus(str, Enum):
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    START_FAILURE = 'START_FAILURE'
    SYNC_FAILURE = 'SYNC_FAILURE'
    COMPLETED_SUCCESS = 'COMPLETED_SUCCESS'


class JobResultModel(BaseModel):
    description: str = Field(None)
    last_update_time: str = Field(None)


class SyncTaskResult(JobResultModel):
    status: SyncTaskStatus = Field(None)


class SyncCalculationTaskResult(JobResultModel):
    status: SyncCalculationStatus = Field(None)