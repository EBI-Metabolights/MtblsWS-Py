
import abc
import datetime
import enum
import re
from typing import List, Union

class IdentifierPrefix(str, enum.Enum):
  MTBLS = "MTBLS"
  SUBMISSION = "REQ"
  MXD = "MXD"

class Identifier(abc.ABC):
    def __init__(self, prefix: str, pattern: str):
      self.prefix = prefix
      self.pattern = pattern
    
    @abc.abstractmethod
    def get_id(self, unique_id: Union[str, int], creation_time: Union[None, datetime.datetime]=None) -> str:
      pass
    
    def get_prefix(self) -> str:
      return self.prefix
    
    def get_pattern(self) -> str:
      return self.pattern
      
    def validate_format(self, identifier: str) -> bool:
      result = self.get_id_parts(identifier)
      if not result or len(result) < 2:
        return False
      return True

    def get_id_parts(self, identifier: str) -> List[Union[str, int]]:
      if not identifier:
        return []
      result = re.match(self.get_pattern(), identifier)
      if not result:
        return []
      return list(result.groups())
    
    
    
