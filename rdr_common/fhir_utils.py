class SimpleFhirR4Reader(object):
  """
  A lightweight accessor for a FHIR R4 resource. Not intended to replace the fhirclient library.
  """

  def __init__(self, fhir_data_structure):
    self._data = fhir_data_structure

  def get(self, *lookup_path, **dict_lookup_keys):
    obj = self._data
    if dict_lookup_keys:
      lookup_path = lookup_path + (dict_lookup_keys,)
    for key in lookup_path:
      obj = _lookup_obj_key(obj, key)
    if isinstance(obj, (list, dict)):
      return SimpleFhirR4Reader(obj)
    return obj

  def __getattr__(self, item):
    obj = self.get(item)
    if isinstance(obj, (list, dict)):
      return SimpleFhirR4Reader(obj)
    return obj


def _lookup_obj_key(obj, key):
  if isinstance(obj, dict):
    return obj[key]
  if isinstance(obj, list):
    if isinstance(key, dict):  # dict key based lookup
      for x in obj:
        if _dict_has_values(x, **key):
          return x


def _dict_has_values(obj, **queries):
  for key, value in queries.items():
    if obj.get(key) != value:
      return False
  return True
