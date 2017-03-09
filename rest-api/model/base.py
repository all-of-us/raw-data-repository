"""Defines the declarative base. Import this and extend from Base for all tables.

Also defines helpers for FHIR parsing.
"""

import collections

from sqlalchemy.ext.declarative import declarative_base
from dictalchemy import DictableModel
from fhirclient.models.domainresource import DomainResource
from fhirclient.models.fhirabstractbase import FHIRValidationError
from werkzeug.exceptions import BadRequest

# Due to the use of DictableModel, all model objects that extend from Base
# define asdict() and fromdict() methods, which can be used in tests and when
# copying rows from the database (e.g. creating history rows.)
#
# asdict() produces a dictionary from the fields of the model object;
# see https://pythonhosted.org/dictalchemy/#using-asdict. Subclasses can define
# asdict_with_children() methods to provide a convenience wrapper around asdict(),
# supplying fields for child collections in a follow parameter.
#
# fromdict() populates fields in the model object based on an input dictionary;
# see https://pythonhosted.org/dictalchemy/#using-fromdict. fromdict() does not
# populate fields that contain lists.
#
# Subclasses should define to_client_json and from_client_json for client request (de)serialization.
Base = declarative_base(cls=DictableModel)

def get_column_name(model_type, field_name):
  return getattr(model_type, field_name).property.columns[0].name 

_FhirProperty = collections.namedtuple(
    'FhirProperty',
    ('name', 'json_name', 'fhir_type', 'is_list', 'of_many', 'not_optional'))


def FhirProperty(name, fhir_type, json_name=None, is_list=False, required=False):
  """Helper for declaring FHIR propertly tuples which fills in common default values.

  By default, JSON name is the camelCase version of the Python snake_case name.

  The tuples are documented in FHIRAbstractBase as:
  ("name", "json_name", type, is_list, "of_many", not_optional)
  """
  if json_name is None:
    components = name.split('_')
    json_name = components[0] + ''.join(c.capitalize() for c in components[1:])
  of_many = None  # never used?
  return _FhirProperty(name, json_name, fhir_type, is_list, of_many, required)


class FhirMixin(object):
  """Derive from this to simplify declaring custom FHIR resource or element classes.

  This aids in (de)serialization of JSON, including validation of field presence and types.

  Subclasses should derive from DomainResource or (for nested fields) BackboneElement, and fill in
  two class-level fields: resource_name (an arbitrary string) and _PROPERTIES.
  """
  _PROPERTIES = None  # Subclasses declar a list of calls to FP (producing tuples).

  def __init__(self, jsondict=None):
    for proplist in self._PROPERTIES:
      setattr(self, proplist[0], None)
    try:
      super(FhirMixin, self).__init__(jsondict=jsondict, strict=True)
    except FHIRValidationError, e:
      if isinstance(self, DomainResource):
        # Only convert FHIR exceptions to BadError at the top level. For nested objects, FHIR
        # repackages exceptions itself.
        raise BadRequest(e.message)
      else:
        raise

  def __str__(self):
    """Returns an object description to be used in validation error messages."""
    return self.resource_name

  def elementProperties(self):
    js = super(FhirMixin, self).elementProperties()
    js.extend(self._PROPERTIES)
    return js
