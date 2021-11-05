from importlib import import_module
from inspect import isclass

from app.ws.model_classes.isa_wrapper import IsaApiWrapper
from app.ws.model_classes.permissions import PermissionsObj
from app.ws.model_classes.validation_parameters import AbstractValParams
from app.ws.validation_dir.validators.abstract_validator import AbstractValidator


class ValidationFactory:

    def __init__(self, section):
        self.section = section
        self.validators = []


    def load(self, perms: PermissionsObj, val_params: AbstractValParams, isa_wrapper: IsaApiWrapper):
        """
        Load either the basic validation and a single section, or all sections. Checks if a given class is a subclass
        of the base class as we don't want to actually load the base class.

        """
        module = import_module('app.ws.validation_dir.validators')
        if self.section is 'all':
            for validation_class in dir(module):
                validator = getattr(module, validation_class)
                if isclass(validator) and issubclass(validator, AbstractValidator):
                    val_instance = validator(perms=perms, val_params = val_params, isa_wrapper=isa_wrapper)
                    self.validators.append(val_instance)
