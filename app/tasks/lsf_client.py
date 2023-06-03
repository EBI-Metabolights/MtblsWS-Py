


from app.ws.settings.hpc_cluster import HpcClusterSettings
from app.ws.settings.utils import get_cluster_settings


class LsfClient(object):
    
    def __init__(self, cluster_settings: HpcClusterSettings) -> None:
        self.cluster_settings = cluster_settings
        
        if not cluster_settings:
            self.cluster_settings = get_cluster_settings()
            
    def submit_datamover_job(self, command: str, job_name: str=None, output_file=None, error_file=None, account=None) -> int:
        bsub_command = self.submit_job(command, self.cluster_settings.cluster_lsf_bsub_datamover_queue, job_name, output_file, error_file, account)


    def submit_job(self, command: str, queue=None, job_name: str=None, output_file=None, error_file=None, account=None) -> int:
        initial_command = self.build_ssh_command()
        bsub_command = self.build_sub_command(command, queue, job_name, output_file, error_file, account)
        
    
    def build_sub_command(self, command: str, queue=None, job_name: str=None, output_file=None, error_file=None, account=None):
        bsub_command = [self.cluster_settings.job_submit_command]
        
        bsub_command.append("-q")
        if queue:
            bsub_command.append(queue)
        else:
            bsub_command.append(self.cluster_settings.cluster_lsf_bsub_default_queue)
        
        bsub_command.append("-J")
        if job_name:
            bsub_command.append(job_name)
        else:
            bsub_command.append(job_name)
    
    
    def build_ssh_command(self):
        command = []
        command.append(self.cluster_settings.cluster_lsf_host_ssh_command)
        command.append("-o")
        command.append("StrictHostKeyChecking=no")
        command.append("-o")
        command.append("LogLevel=quiet")
        command.append("-o")
        command.append("UserKnownHostsFile=/dev/null")
        command.append(f"{self.cluster_settings.cluster_lsf_host_user}@{self.cluster_settings.cluster_lsf_host}")
        return command
        
        
        
        