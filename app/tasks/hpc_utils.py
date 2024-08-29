
from app.config import get_settings
from app.config.model.hpc_cluster import HpcClusterSettings
from app.tasks.hpc_client import HpcClient
from app.tasks.lsf_client import LsfClient
from app.tasks.slurm_client import SlurmClient


def get_new_hpc_client(submit_with_ssh: bool=True) -> HpcClient:
    config: HpcClusterSettings = get_settings().hpc_cluster
    if config.datamover.workload_manager.lower() == "lsf":
        return LsfClient(submit_with_ssh=submit_with_ssh)
    return SlurmClient(submit_with_ssh=submit_with_ssh)