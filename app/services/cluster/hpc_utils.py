
from app.config import get_settings
from app.services.cluster.hpc_client import HpcClient
from app.services.cluster.lsf_client import LsfClient
from app.services.cluster.slurm_client import SlurmClient


def get_new_hpc_datamover_client() -> HpcClient:
    setttings = get_settings()
    cluster_settings = setttings.hpc_cluster.datamover
    if cluster_settings.workload_manager.lower() == "lsf":
        return LsfClient(cluster_settings)
    return SlurmClient(cluster_settings)

def get_new_hpc_compute_client() -> HpcClient:
    setttings = get_settings()
    cluster_settings = setttings.hpc_cluster.compute
    if cluster_settings.workload_manager.lower() == "lsf":
        return LsfClient(cluster_settings)
    return SlurmClient(cluster_settings)