from abc import ABC
import os

from rdr_service.importer import import_from_string


class Provider(ABC):
    environment_variable_name: str = None

    @classmethod
    def get_provider(cls, name: str = None, default=None):
        name = name or os.environ.get(cls.environment_variable_name)
        if name:
            try:
                return import_from_string(name)
            except ValueError:
                pass
        return default
