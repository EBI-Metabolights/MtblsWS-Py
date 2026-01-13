from abc import ABC, abstractmethod


class AbstractBuilder(ABC):
    def save(self):
        raise NotImplementedError

    @abstractmethod
    def build(self): ...

    @abstractmethod
    def process(self): ...
