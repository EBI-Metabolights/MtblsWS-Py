


import logging
import os
import subprocess
from typing import Tuple
import uuid
from app.config import get_settings

from app.utils import MetabolightsException

from jinja2 import Environment, PackageLoader, select_autoescape

logger = logging.getLogger('wslog')
env = Environment(
    loader=PackageLoader("resources.templates", "scripts"),
    autoescape=select_autoescape(['html', 'xml'])
)

  
class BashClient(object):
    
    @staticmethod
    def execute_command(command: str) -> Tuple[str, str]:
        try:
            logger.info(f" A command is executing  : '{command}'")
            print(f" A command is executing  : '{command}'")
            job_status = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, check=True)
            stderr = ""
            stdout = ""
            if job_status.stdout:
                stdout = job_status.stdout.decode("utf-8")
            if job_status.stderr:
                stderr = job_status.stderr.decode("utf-8")            
            return stdout, stderr
        except Exception as e:
            logger.error(f"Could not execute command '{command}'. {str(e)}")     
            raise e   
        
    @staticmethod
    def execute_command_and_nowait(command: str) -> None:
        try:
            logger.info(f" A command is executing  : '{command}'")
            print(f" A command is executing  : '{command}'")
            subprocess.run(command, shell=True, check=False)
        except Exception as e:
            logger.error(f"Could not execute command '{command}'. {str(e)}")     
            raise e   
        
    @staticmethod
    def build_ssh_command(hostname:str, username: str=None):
        command = []
        command.append("ssh")
        command.append("-o")
        command.append("StrictHostKeyChecking=no")
        command.append("-o")
        command.append("LogLevel=quiet")
        command.append("-o")
        command.append("UserKnownHostsFile=/dev/null")
        if not username:
            command.append(f"{hostname}")
        else:
            command.append(f"{username}@{hostname}")
        return " ".join(command)
    
    @staticmethod
    def prepare_script_from_template(script_template_name: str, **kwargs):
        template = env.get_template(script_template_name)
        content = template.render(kwargs) 
        basename = os.path.basename(script_template_name).replace(".j2", "").replace(".", "_")
        
        temp_file_name =  f"{basename}_{str(uuid.uuid4())}.sh"
        file_input_path = os.path.join(get_settings().server.temp_directory_path, temp_file_name)
        
        with open(file_input_path, "w") as f:
            f.writelines(content)
        return file_input_path