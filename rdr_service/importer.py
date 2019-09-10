import importlib


def import_from_string(value: str):
    module_name, _, item_name = value.rpartition('.')
    if not module_name:
        raise ValueError('missing module specification `{}`'.format(value))
    module = importlib.import_module(module_name)
    return getattr(module, item_name)
