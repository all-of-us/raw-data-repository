from base64 import urlsafe_b64decode, urlsafe_b64encode
import collections
import json
import logging
import datetime
import random

from fhirclient.models.domainresource import DomainResource
from fhirclient.models.fhirabstractbase import FHIRValidationError
from protorpc import messages
from query import Operator, PropertyType, FieldFilter, Results
from sqlalchemy import or_, and_
from sqlalchemy.exc import IntegrityError
from werkzeug.exceptions import BadRequest, NotFound, PreconditionFailed, ServiceUnavailable

import api_util
import dao.database_factory
from model.utils import get_property_type

# Maximum number of times we will attempt to insert an entity with a random ID before
# giving up.
MAX_INSERT_ATTEMPTS = 20

# Range of possible values for random IDs.
_MIN_ID = 100000000
_MAX_ID = 999999999

_COMPARABLE_PROPERTY_TYPES = [PropertyType.DATE, PropertyType.DATETIME, PropertyType.INTEGER]

_OPERATOR_PREFIX_MAP = {
  "lt": Operator.LESS_THAN,
  "le": Operator.LESS_THAN_OR_EQUALS,
  "gt": Operator.GREATER_THAN,
  "ge": Operator.GREATER_THAN_OR_EQUALS,
  "ne": Operator.NOT_EQUALS
}

class BaseDao(object):
  """A data access object base class; defines common methods for inserting and retrieving
  objects using SQLAlchemy.

  Extend directly from BaseDao if entities cannot be updated after being
  inserted; extend from UpdatableDao if they can be updated.

  order_by_ending is a list of field names to always order by (in ascending order, possibly after
  another sort field) when query() is invoked. It should always end in the primary key.
  If not specified, query() is not supported.
  """
  def __init__(self, model_type, backup=False, order_by_ending=None, db=None):
    self.model_type = model_type
    if not db:
      if backup:
        db = dao.database_factory.get_backup_database()
      else:
        db = dao.database_factory.get_database()
    self._database = db
    self.order_by_ending = order_by_ending

  def session(self):
    return self._database.session()

  def _validate_model(self, session, obj):
    """Override to validate a model before any db write (insert or update)."""
    pass

  def _validate_insert(self, session, obj):
    """Override to validate a new model before inserting it (not applied to updates)."""
    self._validate_model(session, obj)

  def insert_with_session(self, session, obj):
    """Adds the object into the session to be inserted."""
    self._validate_insert(session, obj)
    session.add(obj)
    return obj

  def insert(self, obj):
    """Inserts an object into the database. The calling object may be mutated
    in the process."""
    with self.session() as session:
      return self.insert_with_session(session, obj)

  def get_id(self, obj):
    """Returns the ID (for single primary key column tables) or a list of IDs (for multiple
    primary key column tables). Must be overridden by subclasses."""
    raise NotImplementedError

  def get_with_session(self, session, obj_id, for_update=False, options=None):
    """Gets an object by ID for this type using the specified session. Returns None if not found."""
    query = session.query(self.model_type)
    if for_update:
      query = query.with_for_update()
    if options:
      query = query.options(options)
    return query.get(obj_id)

  def get(self, obj_id):
    """Gets an object with the specified ID for this type from the database.

    Returns None if not found.
    """
    with self.session() as session:
      return self.get_with_session(session, obj_id)

  def get_with_children(self, obj_id):
    """Subclasses may override this to eagerly loads any child objects (using subqueryload)."""
    return self.get(obj_id)

  def get_all(self):
    """Fetches all entities from the database. For use on small tables."""
    with self.session() as session:
      return session.query(self.model_type).all()

  def make_query_filter(self, field_name, value):
    """Attempts to make a query filter for the model property with the specified name, matching
    the specified value. If no such property exists, None is returned.
    """
    prop = getattr(self.model_type, field_name, None)
    if prop:
      property_type = get_property_type(prop)
      filter_value = None
      operator = Operator.EQUALS
      # If we're dealing with a comparable property type, look for a prefix that indicates an
      # operator other than EQUALS and strip it off
      if property_type in _COMPARABLE_PROPERTY_TYPES:
        for prefix, op in _OPERATOR_PREFIX_MAP.iteritems():
          if isinstance(value, (str, unicode)) and value.startswith(prefix):
            operator = op
            value = value[len(prefix):]
            break
      filter_value = self._parse_value(prop, property_type, value)
      return FieldFilter(field_name, operator, filter_value)
    else:
      return None

  def _parse_value(self, prop, property_type, value):
    if value is None:
      return None
    try:
      if property_type == PropertyType.DATE:
        return api_util.parse_date(value).date()
      elif property_type == PropertyType.DATETIME:
        return api_util.parse_date(value)
      elif property_type == PropertyType.ENUM:
        return prop.property.columns[0].type.enum_type(value)
      elif property_type == PropertyType.INTEGER:
        return int(value)
      else:
        return value
    except ValueError:
      raise BadRequest('Invalid value for property of type %s: %r.' % (property_type, value))

  def _from_json_value(self, prop, value):
    property_type = get_property_type(prop)
    result = self._parse_value(prop, property_type, value)
    return result

  def query(self, query_def):
    if not self.order_by_ending:
      raise BadRequest("Can't query on type %s -- no order by ending speciifed" % self.model_type)

    with self.session() as session:
      query, field_names = self._make_query(session, query_def)
      items = query.all()

      total = None
      if query_def.include_total:
        total = self._count_query(session, query_def)

      if not items:
        return Results([], total=total)

    if len(items) > query_def.max_results:
      # Items, pagination token, and more are available
      page = items[0:query_def.max_results]
      token = self._make_pagination_token(items[query_def.max_results - 1].asdict(), field_names)
      return Results(page, token, more_available=True, total=total)
    else:
      token = (self._make_pagination_token(items[-1].asdict(), field_names)
               if query_def.always_return_token
               else None)
      return Results(items, token, more_available=False, total=total)

  def _make_pagination_token(self, item_dict, field_names):
    vals = [item_dict.get(field_name) for field_name in field_names]
    vals_json = json.dumps(vals, default=json_serial)
    return urlsafe_b64encode(vals_json)

  def _initialize_query(self, session, query_def):
    """Creates the initial query, before the filters, order by, and limit portions are added
    from the query definition. Clients can subclass to manipulate the initial query criteria
    or validate the query definition."""
    #pylint: disable=unused-argument
    return session.query(self.model_type)

  def _count_query(self, session, query_def):
    query = self._initialize_query(session, query_def)
    query = self._set_filters(query, query_def.field_filters)
    return query.count()

  def _make_query(self, session, query_def):
    query = self._initialize_query(session, query_def)
    query = self._set_filters(query, query_def.field_filters)
    order_by_field_names = []
    order_by_fields = []
    first_descending = False
    if query_def.order_by:
      query = self._add_order_by(query, query_def.order_by, order_by_field_names, order_by_fields)
      first_descending = not query_def.order_by.ascending
    query = self._add_order_by_ending(query, order_by_field_names, order_by_fields)
    if query_def.pagination_token:
      # Add a query filter based on the pagination token.
      query = self._add_pagination_filter(query, query_def, order_by_fields,
                                          first_descending)
    # Return one more than max_results, so that we know if there are more results.
    query = query.limit(query_def.max_results + 1)
    if query_def.offset:
      query = query.offset(query_def.offset)
    return query, order_by_field_names

  def _set_filters(self, query, filters):
    for field_filter in filters:
      try:
        f = getattr(self.model_type, field_filter.field_name)
      except AttributeError:
        raise BadRequest(
            'No field named %r found on %r.' % (field_filter.field_name, self.model_type))
      query = self._add_filter(query, field_filter, f)
    return query

  def _add_filter(self, query, field_filter, f):
    if field_filter.value is None:
      return query.filter(f.is_(None))
    query = {Operator.EQUALS: query.filter(f == field_filter.value),
             Operator.LESS_THAN: query.filter(f < field_filter.value),
             Operator.GREATER_THAN: query.filter(f > field_filter.value),
             Operator.LESS_THAN_OR_EQUALS: query.filter(f <= field_filter.value),
             Operator.GREATER_THAN_OR_EQUALS: query.filter(f >= field_filter.value),
             Operator.NOT_EQUALS: query.filter(f != field_filter.value)}.get(field_filter.operator)
    if not query:
      raise BadRequest('Invalid operator: %r.' % field_filter.operator)
    return query

  def _add_pagination_filter(self, query, query_def, fields, first_descending):
    """Adds a pagination filter for the decoded values in the pagination token based on
    the sort order."""
    decoded_vals = self._decode_token(query_def, fields)
    # SQLite does not support tuple comparisons, so make an or-of-ands statements that is
    # equivalent.
    or_clauses = []
    if first_descending:
      if decoded_vals[0] is not None:
        or_clauses.append(fields[0] < decoded_vals[0])
        or_clauses.append(fields[0].is_(None))
    else:
      if decoded_vals[0] is None:
        or_clauses.append(fields[0].isnot(None))
      else:
        or_clauses.append(fields[0] > decoded_vals[0])
    for i in range(1, len(fields)):
      and_clauses = []
      for j in range(0, i):
        and_clauses.append(fields[j] == decoded_vals[j])
      if decoded_vals[i] is None:
        and_clauses.append(fields[i].isnot(None))
      else:
        and_clauses.append(fields[i] > decoded_vals[i])
      or_clauses.append(and_(*and_clauses))
    return query.filter(or_(*or_clauses))

  def _decode_token(self, query_def, fields):
    pagination_token = query_def.pagination_token
    try:
      decoded_vals = json.loads(urlsafe_b64decode(pagination_token.encode("ascii")))
    except:
      raise BadRequest('Invalid pagination token: %r.' % pagination_token)
    if not type(decoded_vals) is list or len(decoded_vals) != len(fields):
      raise BadRequest('Invalid pagination token: %r.' % pagination_token)
    for i in range(0, len(fields)):
      decoded_vals[i] = self._from_json_value(fields[i], decoded_vals[i])
    return decoded_vals

  def _add_order_by(self, query, order_by, field_names, fields):
    """Adds a single order by field, as the primary sort order."""
    try:
      f = getattr(self.model_type, order_by.field_name)
    except AttributeError:
      raise BadRequest('No field named %r found on %r.' % (order_by.field_name, self.model_type))
    field_names.append(order_by.field_name)
    fields.append(f)
    if order_by.ascending:
      return query.order_by(f)
    else:
      return query.order_by(f.desc())

  def _get_order_by_ending(self, query):
    #pylint: disable=unused-argument
    return self.order_by_ending

  def _add_order_by_ending(self, query, field_names, fields):
    """Adds the order by ending."""
    for order_by_field in self.order_by_ending:
      if order_by_field in field_names:
        continue
      try:
        f = getattr(self.model_type, order_by_field)
      except AttributeError:
        raise BadRequest('No field named %r found on %r.' % (order_by_field, self.model_type))
      field_names.append(order_by_field)
      fields.append(f)
      query = query.order_by(f)
    return query

  def _get_random_id(self):
    return random.randint(_MIN_ID, _MAX_ID)

  def _insert_with_random_id(self, obj, fields):
    """Attempts to insert an entity with randomly assigned ID(s) repeatedly until success
    or a maximum number of attempts are performed."""
    all_tried_ids = []
    for _ in range(0, MAX_INSERT_ATTEMPTS):
      tried_ids = {}
      for field in fields:
        rand_id = self._get_random_id()
        tried_ids[field] = rand_id
        setattr(obj, field, rand_id)
      all_tried_ids.append(tried_ids)
      try:
        with self.session() as session:
          return self.insert_with_session(session, obj)
      except IntegrityError, e:
        # SQLite and MySQL variants of the error message, respectively.
        if 'UNIQUE constraint failed' in e.message or 'Duplicate entry' in e.message:
          logging.warning('Failed insert with %s: %s', tried_ids, e.message)
        else:
          raise
    # We were unable to insert a participant (unlucky). Throw an error.
    logging.warning(
        'Giving up after %d insert attempts, tried %s.' % (MAX_INSERT_ATTEMPTS, all_tried_ids))
    raise ServiceUnavailable('Giving up after %d insert attempts.' % MAX_INSERT_ATTEMPTS)

  def count(self):
    with self.session() as session:
      return session.query(self.model_type).count()

  def to_client_json(self, model):
    # pylint: disable=unused-argument
    """Converts the given model to a JSON object to be returned to API clients.

    Subclasses must implement this unless their model store a model.resource attribute.
    """
    try:
      return json.loads(model.resource)
    except AttributeError:
      raise NotImplementedError()

  def from_client_json(self):
    """Subclasses must implement this to parse API request bodies into model objects.

    Subclass args:
      resource: JSON object.
      participant_id: For subclasses which are children of participants only, the numeric ID.
      client_id: An informative string ID of the caller (who is creating/modifying the resource).
      id_: For updates, ID of the model to modify.
      expected_version: For updates, require this to match the existing model's version.
    """
    raise NotImplementedError()


class UpsertableDao(BaseDao):
  """A DAO that allows upserts of its entities (without any checking to see if the
  entities already exist or have a particular expected version.
  """

  def _validate_upsert(self, session, obj):
    """Override to validate a new model before upserting it (not applied to inserts)."""
    self._validate_model(session, obj)

  def _do_upsert(self, session, obj):
    """Perform the upsert of the specified object. Subclasses can override to alter things."""
    session.merge(obj)

  def upsert_with_session(self, session, obj):
    """Upserts the object in the database with the specified session."""
    self._validate_upsert(session, obj)
    self._do_upsert(session, obj)

  def upsert(self, obj):
    """Upserts the object in the database (creating the object if it does not exist, and replacing
    it if it does.)"""
    with self.session() as session:
      return self.upsert_with_session(session, obj)

class UpdatableDao(BaseDao):
  """A DAO that allows updates to entities.

  Extend from UpdatableDao if entities can be updated after being inserted.

  All model objects using this DAO must define a "version" field.
  """
  def _validate_update(self, session, obj, existing_obj):
    """Validates that an update is OK before performing it. (Not applied on insert.)

    By default, validates that the object already exists, and if an expected version ID is provided,
    that it matches.
    """
    if not existing_obj:
      raise NotFound('%s with id %s does not exist' % (self.model_type.__name__, id))
    if existing_obj.version != obj.version:
      raise PreconditionFailed('Expected version was %s; stored version was %s' % \
                               (obj.version, existing_obj.version))
    self._validate_model(session, obj)

  def _validate_patch_update(self, session, model, resource, expected_version):
    #pylint: disable=unused-argument
    if expected_version != model.version:
      raise PreconditionFailed('Expected version was %s; stored version was %s' % \
                               (expected_version, model.version))

  # pylint: disable=unused-argument
  def _do_update(self, session, obj, existing_obj):
    """Perform the update of the specified object. Subclasses can override to alter things."""
    session.merge(obj)

  def get_for_update(self, session, obj_id):
    return self.get_with_session(session, obj_id, for_update=True)

  def update_with_session(self, session, obj):
    """Updates the object in the database with the specified session. Will fail if the object
    doesn't exist already, or if obj.version does not match the version of the existing object."""
    existing_obj = self.get_for_update(session, self.get_id(obj))
    self._validate_update(session, obj, existing_obj)
    self._do_update(session, obj, existing_obj)

  def patch_update_with_session(self, session, model, resource, expected_version):
    """Updates the object in the database with the specified session. Will fail if the object
    doesn't exist already, or if obj.version does not match the version of the existing object."""
    self._validate_patch_update(session, model, resource, expected_version)
    self._do_update_with_patch(session, model, resource)

  def update(self, obj):
    """Updates the object in the database. Will fail if the object doesn't exist already, or
    if obj.version does not match the version of the existing object.
    May modify the passed in object."""
    with self.session() as session:
      return self.update_with_session(session, obj)

  def update_with_patch(self, obj, resource, expected_version):
    """creates an atomic patch request on an object. It will fail if the object
    doesn't exist already, or if obj.version does not match the version of the existing object.
    May modify the passed in object."""
    with self.session() as session:
      return self.patch_update_with_session(session, obj, resource, expected_version)

def json_serial(obj):
  """JSON serializer for objects not serializable by default json code"""
  if isinstance(obj, datetime.datetime) or isinstance(obj, datetime.date):
    return obj.isoformat()
  if isinstance(obj, messages.Enum):
    return str(obj)
  raise TypeError("Type not serializable")


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
