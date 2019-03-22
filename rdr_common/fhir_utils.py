class SimpleFhirR4Reader(object):
  """
  A lightweight accessor for a FHIR R4 resource. Not intended to replace the fhirclient library.

  This class does not attempt any validation or impose any structure. It is only intended to aid in
  reading from FHIR-like data structures.

  When performing list lookups, return the first item in the list that matches the given parameters.
  """

  def __init__(self, fhir_data_structure):
    self._obj = fhir_data_structure

  def get(self, *lookup_path, **dict_lookup_keys):
    obj = self._obj
    if dict_lookup_keys:
      lookup_path = lookup_path + (dict_lookup_keys,)
    for key in lookup_path:
      obj = _lookup_obj_key(obj, key)
    if isinstance(obj, (list, dict)):
      return SimpleFhirR4Reader(obj)
    return obj

  def __getattr__(self, item):
    try:
      return self.get(item)
    except (ValueError, KeyError):
      raise AttributeError("{!r} can't be found in {!r}".format(item, self._obj))

  def __getitem__(self, item):
    if isinstance(item, slice):
      if isinstance(self._obj, list):
        return self._obj[item]
      raise TypeError("{!r} can't be found in {!r}".format(item, self._obj))
    try:
      return self.get(item)
    except ValueError:
      if isinstance(self._obj, list):
        raise IndexError("{!r} can't be found in {!r}".format(item, self._obj))
      raise KeyError("{!r} can't be found in {!r}".format(item, self._obj))


def _lookup_obj_key(obj, key):
  if isinstance(obj, dict):
    if callable(key):
      return filter(key, obj.items())
    return obj[key]
  if isinstance(obj, list):
    if isinstance(key, dict):  # dict key based lookup
      for x in obj:
        if _dict_has_values(x, **key):
          return x
    if isinstance(key, int):
      return obj[key]
    if callable(key):
      return filter(key, obj)
  raise ValueError("could not lookup '{!r}' from {!r}".format(key, obj))


def _dict_has_values(obj, **queries):
  for key, value in queries.items():
    try:
      if obj[key] != value:
        return False
    except KeyError:
      return False
  return True
