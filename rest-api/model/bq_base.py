#
# BigQuery table schema support
#
import collections
import importlib
import itertools
import json
import operator
from enum import Enum, EnumMeta


# BigQuery exceptions
class BQException(Exception):
  """ Base class for BQ exceptions """
  pass

class BQInvalidModeException(BQException):
  """
  BigQuery error in update operation: Provided Schema does not match Table
    - Cannot add required columns to an existing schema
  """
  pass

class BQModeChangedException(BQException):
  """
  BigQuery error in update operation: Provided Schema does not match Table
    - Field [FIELD] has changed mode from REPEATED to NULLABLE
  """
  pass

class BQSchemaStructureException(BQException):
  """
  BigQuery error in update operation
    - Precondition Failed
  Note: This occurs if new fields are not added to the end of the schema.
  """
  pass

class BQInvalidSchemaException(BQException):
  """
  Error while reading data, error message
    - parsing error in row starting at position [INT] - No such field: [FIELD]
  """
  pass

class BQDuplicateFieldException(BQException):
  """
  BigQuery error in update operation
    - Field new_field already exists in schema
  """
  pass

# all BQ schemas should be listed here.
BQ_SCHEMAS = [
  ('model.bq_participant_summary', 'BQParticipantSummary'),
]


class BQFieldTypeEnum(Enum):
  """
  BigQuery column types
  """
  STRING = 1
  BYTES = 2
  INTEGER = 3
  FLOAT = 4
  BOOLEAN = 5
  TIMESTAMP = 6
  DATE = 7
  TIME = 8
  DATETIME = 9
  ARRAY = 10
  RECORD = 11


class BQFieldModeEnum(Enum):
  """
  BigQuery mode types
  """
  NULLABLE = 1
  REQUIRED = 2
  REPEATED = 3


class BQField(object):
  """
  A BigQuery table schema field
  """
  _fld_name = None
  _fld_type = None
  _fld_mode = None
  _fld_enum = None
  _fld_descr = None
  _counter = itertools.count()

  def __init__(self, fld_name, fld_type, fld_mode=BQFieldModeEnum.REQUIRED, fld_enum=None, fld_descr=None):
    # https://stackoverflow.com/questions/350799/how-does-django-know-the-order-to-render-form-fields
    self._count = BQField._counter.next()

    if not isinstance(fld_name, basestring):
      raise TypeError('field name must be a string')
    if not isinstance(fld_type, Enum):
      raise TypeError('field type must be a BQFieldTypeEnum value')
    if not isinstance(fld_mode, Enum):
      raise TypeError('field mode must be a BQFieldModeEnum value')
    if fld_enum:
      if not isinstance(fld_enum, EnumMeta):
        raise TypeError('field enum must be an instance of Enum')
      pass
    self._fld_name = fld_name
    self._fld_type = fld_type
    self._fld_mode = fld_mode
    self._fld_enum = fld_enum
    self._fld_descr = fld_descr


  def to_dict(self):
    """
    Return a dictionary of the field information.
    :return: dict
    """
    data = collections.OrderedDict()
    data['name'] = self._fld_name
    data['type'] = getattr(self._fld_type, 'name')
    data['mode'] = getattr(self._fld_mode, 'name')
    if self._fld_enum:
      data['description'] = '{0}.{1}'.format(self._fld_enum.__module__, self._fld_enum.__name__)
    elif self._fld_descr:
      data['description'] = self._fld_descr
    return data

  @staticmethod
  def serialize(o):
    """
    serialize object for converting to json
    :param o: object
    :return: dict
    """
    return o.to_dict()

  def __repr__(self):
    field = self.to_dict()
    return json.dumps(field)


class BQRecordField(BQField):
  """
  A BigQuery Record field which holds a set of BQFields.
  """
  def __init__(self, fld_name, fld_descr=None):
    super(BQRecordField, self).\
      __init__(fld_name, BQFieldTypeEnum.RECORD, BQFieldModeEnum.REPEATED, fld_descr=fld_descr)

  def del_prop(self, name):

    if hasattr(self, name):
      setattr(self, name, None)

  def to_dict(self):
    """
    Return a dictionary of the field information.
    :return: dict
    """
    # get our field information
    data = super(BQRecordField, self).to_dict()
    data['description'] = '{0}.{1}'.format(self.__module__, self.__class__.__name__)
    # get list of BQField objects in correct order.
    fields = self.to_list()
    if len(fields) > 0:
      data['fields'] = fields

    return data

  def to_list(self):
    """
    Return a list of field object for this record object
    :return: list
    """
    fields = list()
    for key in dir(self):
      field = getattr(self, key)
      if isinstance(field, BQField):
        fields.append(field)
    # sort the BQField objects in the original order they are defined.
    fields.sort(key=operator.attrgetter('_count'))

    return fields


class BQSchema(object):
  """
  A BigQuery dataset object schema.
  Note: Field properties of python object derived from this should the underscore naming
        convention. Property names must exactly match BigQuery field names.
  """
  def __init__(self, *args):

    if args is not None and len(args) is not 0 and args[0] is not None:
      self._add_fields(self, args[0])

  def _cmp_schema(self, o1, o2):
    """
    Recursively compare schemas. This is normally used to compare a local schema with
    a remote BQ schema.  For this to succeed, schema property names for field object
    must exactly match BQ field names.
    :param o1: BQSchema object
    :param o2: BQSchema object
    :return:
    """
    if len(dir(o1)) != len(dir(o2)):
      return False
    pairs = zip(o1.to_list(), o2.to_list())
    for l1, l2 in pairs:
      if isinstance(l1, BQRecordField):
        if not self._cmp_schema(l1, l2):
          return False
      else:
        if l1.to_dict() != l2.to_dict():
          return False

    return True

  def __eq__(self, other):
    return self._cmp_schema(self, other)

  def __ne__(self, other):
    return not self._cmp_schema(self, other)

  def _add_fields(self, obj, fields):
    """
    Recursively add fields to object
    :param obj: object to add fields to.
    :param fields: list of field dicts
    :return: object
    """
    for field in fields:
      fld_name = field['name']
      fld_type = BQFieldTypeEnum[field['type']]
      fld_mode = BQFieldModeEnum[field['mode']]
      fld_descr = field['description'] if 'description' in field else None
      enum = None

      if fld_descr and fld_type != BQFieldTypeEnum.RECORD:
        try:
          mod_path, mod_name = fld_descr.rsplit('.', 1)
          mod_name = 'crap'
          enum = getattr(importlib.import_module(mod_path, mod_name), mod_name)
          fld_descr = None
        except AttributeError:
          pass

      if fld_type == BQFieldTypeEnum.RECORD:
        # Note: instantiating the subclass in descr here causes local and remote
        #       comparisons to fail.  This is because the instantiated subclass
        #       will always have any new fields as properties, regardless to
        #       what fields the remote BQ server has.
        rec_obj = BQRecordField(fld_name, fld_descr=fld_descr)
        self.__dict__[fld_name] = self._add_fields(rec_obj, field['fields'])
      else:
        obj.__dict__[fld_name] = BQField(fld_name, fld_type, fld_mode, enum, fld_descr)

    return obj

  def to_list(self):
    """
    Return a list of schema fields
    :return: list
    """
    fields = list()
    for key in dir(self):
      field = getattr(self, key)
      if isinstance(field, BQField):
        fields.append(field)

    # sort the BQField objects in the original order they are defined.
    fields.sort(key=operator.attrgetter('_count'))
    return fields

  def to_json(self):
    """
    Return the json representation of the schema.
    :return: json string
    """
    return json.dumps(self.to_list(), indent=2, default=BQField.serialize)

  def __repr__(self):
    self.to_json()


class BQTable(object):
  """
  https://cloud.google.com/bigquery/docs/managing-table-schemas
  Rules for changing the table structure:
    1) New fields added to an existing table must be set to "NULLABLE".
    2) New fields may only be added to the bottom of the field list.
    3) Existing fields can not be removed.
  """
  __tablename__ = None
  class __schema__(BQSchema):
    pass

  def get_name(self):
    return self.__tablename__

  def get_schema(self):
    return self.__schema__()

class BQView(object):
  __viewname__ = None
  sql = None


class BQRecordSet(object):
  """
  Represents a BigQuery data set.
  """

  _bq_job = None

  def __int__(self, bq_job):
    """
    :param bq_job: A BigQueryJob object.
    """
    self._bq_job = bq_job
    pass


class BQRow(object):
  """
  Represents a single BigQuery data row.
  """
