

import datetime
from typing import Union
from app.config import get_settings
from app.ws.study.identifier import Identifier, IdentifierPrefix


class DefaultIdentifier(Identifier):
    def __init__(self, prefix: str, pattern: str):
        super().__init__(prefix, pattern)
    
    def get_id(self, unique_id: Union[str, int], creation_time: Union[None, datetime.datetime]=None) -> str:
        return f"{self.prefix}{unique_id}"

class SubmissionIdentifier(Identifier):
    def __init__(self, prefix: str, pattern: str):
        super().__init__(prefix, pattern)
    
    def get_id(self, unique_id: Union[str, int], creation_time: Union[None, datetime.datetime]=None) -> str:
      if creation_time:
        return f"{self.prefix}{creation_time.strftime('%Y%m%d')}{unique_id}"  
      return f"{self.prefix}{unique_id}"

app_settings = get_settings()
accession_prefix = app_settings.study.accession_number_prefix

default_mtbls_identifier = DefaultIdentifier(app_settings.study.accession_number_prefix, app_settings.study.accession_number_regex)
default_provisional_identifier = SubmissionIdentifier(app_settings.study.provisional_id_prefix, app_settings.study.provisional_id_regex)
default_mxd_identifier = DefaultIdentifier("MXD", r"^(MXD)(\d+)-(\d+)$")
