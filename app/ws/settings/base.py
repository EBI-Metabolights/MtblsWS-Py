from pydantic import BaseSettings



class MetabolightsBaseSettings(BaseSettings):
    class Config:
        # read and set security settings variables from this env_file
        env_file = ".env"
