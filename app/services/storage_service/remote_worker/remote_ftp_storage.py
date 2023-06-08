import os
import os.path
import random
import shutil
from datetime import datetime, timezone
from distutils.dir_util import copy_tree
from typing import List

from dirsync import sync

from app.services.storage_service.exceptions import StorageServiceException
from app.services.storage_service.models import SyncTaskResult, SyncCalculationTaskResult, SyncCalculationStatus, \
    SyncTaskStatus
from app.services.storage_service.mounted.local_file_manager import MountedVolumeFileManager
from app.services.storage_service.remote_worker.remote_file_manager import RemoteFileManager
from app.services.storage_service.unmounted.unmounted_storage import UnmountedStorage
from app.utils import MetabolightsException


class RemoteFtpStorage(UnmountedStorage):

    def __init__(self, name, app, remote_folder):
        manager_name = name + '_remote_file_manager'
        
        remote_file_manager: RemoteFileManager = RemoteFileManager(manager_name, mounted_root_folder=remote_folder)
        self.remote_file_manager: RemoteFileManager = remote_file_manager

        super(RemoteFtpStorage, self).__init__(name=name, app=app, remote_file_manager=self.remote_file_manager)