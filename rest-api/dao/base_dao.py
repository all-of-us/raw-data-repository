import logging
import datetime
import random

import api_util
import dao.database_factory
import json
from contextlib import contextmanager
from werkzeug.exceptions import BadRequest, NotFound, PreconditionFailed, ServiceUnavailable
from sqlalchemy.exc import IntegrityError
from query import Operator, PropertyType, FieldFilter, Results
from base64 import urlsafe_b64decode, urlsafe_b64encode
from sqlalchemy import or_, and_
from protorpc import messages

# Maximum number of times we will attempt to insert an entity with a random ID before
# giving up.
MAX_INSERT_ATTEMPTS = 20

# Range of possible values for random IDs.
_MIN_ID = 100000000
_MAX_ID = 999999999

_PROPERTY_TYPE_MAP = {
  "String": PropertyType.STRING,
  "Date": PropertyType.DATE,
  "DateTime": PropertyType.DATETIME,
  "Enum": PropertyType.ENUM,
  "Integer": PropertyType.INTEGER,
  "SmallInteger": PropertyType.INTEGER
}

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
  def __init__(self, model_type, order_by_ending=None):
    self.model_type = model_type
    self._database = dao.database_factory.get_database()
    self.order_by_ending = order_by_ending

  @contextmanager
  def session(self):
    sess = self._database.make_session()
    try:
      yield sess
      sess.commit()
    except Exception:
      sess.rollback()
      raise
    finally:
      sess.close()

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

  def get_with_session(self, session, obj_id):
    """Gets an object by ID for this type using the specified session. Returns None if not found."""
    return session.query(self.model_type).get(obj_id)

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

  def _get_property_type(self, prop):
    property_classname = prop.property.columns[0].type.__class__.__name__
    property_type = _PROPERTY_TYPE_MAP.get(property_classname)
    if not property_type:
      raise BadRequest("Unrecognized property of type %s" % property_classname)
    return property_type

  def make_query_filter(self, field_name, value):
    """Attempts to make a query filter for the model property with the specified name, matching
    the specified value. If no such property exists, None is returned.
    """
    prop = getattr(self.model_type, field_name, None)
    if prop:
      property_type = self._get_property_type(prop)
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
      raise BadRequest("Invalid value for property of type %s: %s" % (property_type, value))

  def _from_json_value(self, prop, value):
    property_type = self._get_property_type(prop)
    result = self._parse_value(prop, property_type, value)
    return result

  def query(self, query_def):
    if not self.order_by_ending:
      raise BadRequest("Can't query on type %s -- no order by ending speciifed" % self.model_type)
    with self.session() as session:
      query, field_names = self._make_query(session, query_def)
      items = query.all()
    if items:
      if len(items) > query_def.max_results:
        # Items, pagination token, and more are available
        return Results(items[0:query_def.max_results],
                       self._make_pagination_token(items[query_def.max_results - 1].asdict(),
                                                   field_names),
                       True)
      else:
        if query_def.always_return_token:
          # Items and pagination token, but no more available
          return Results(items,
                         self._make_pagination_token(items[len(items) - 1].asdict(), field_names),
                         False)
        else:
          # Items but no pagination token, and no more available
          return Results(items, None, False)
    else:
      # No items, no pagination token, and no more available
      return Results([], None, False)

  def _make_pagination_token(self, item_dict, field_names):
    vals = [item_dict.get(field_name) for field_name in field_names]
    vals_json = json.dumps(vals, default=json_serial)
    return urlsafe_b64encode(vals_json)

  def _make_query(self, session, query_def):
    query = session.query(self.model_type)
    for field_filter in query_def.field_filters:
      try:
        f = getattr(self.model_type, field_filter.field_name)
      except AttributeError:
        raise BadRequest("No field named %s found on %s", (field_filter.field_name,
                                                           self.model_type))
      query = self._add_filter(query, field_filter, f)
    order_by_field_names = []
    order_by_fields = []
    first_descending = False
    if query_def.order_by:
      query = self._add_order_by(query, query_def.order_by, order_by_field_names, order_by_fields)
      first_descending = not query_def.order_by.ascending
    query = self._add_order_by_ending(query, order_by_field_names, order_by_fields)
    if query_def.pagination_token:
      # Add a query filter based on the pagination token.
      query = self._add_pagination_filter(query, query_def.pagination_token, order_by_fields,
                                          first_descending)
    # Return one more than max_results, so that we know if there are more results.
    query = query.limit(query_def.max_results + 1)

    return query, order_by_field_names

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
      raise BadRequest("Invalid operator: %s" % field_filter.operator)
    return query

  def _add_pagination_filter(self, query, pagination_token, fields, first_descending):
    """Adds a pagination filter for the decoded values in the pagination token based on
    the sort order."""
    decoded_vals = self._decode_token(pagination_token, fields)
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

  def _decode_token(self, pagination_token, fields):
    try:
      decoded_vals = json.loads(urlsafe_b64decode(pagination_token.encode("ascii")))
    except:
      raise BadRequest("Invalid pagination token: %s", pagination_token)
    if not type(decoded_vals) is list or len(decoded_vals) != len(fields):
      raise BadRequest("Invalid pagination token: %s" % pagination_token)
    for i in range(0, len(fields)):
      decoded_vals[i] = self._from_json_value(fields[i], decoded_vals[i])
    return decoded_vals

  def _add_order_by(self, query, order_by, field_names, fields):
    """Adds a single order by field, as the primary sort order."""
    try:
      f = getattr(self.model_type, order_by.field_name)
    except AttributeError:
      raise BadRequest("No field named %s found on %s", (order_by.field_name, self.model_type))
    field_names.append(order_by.field_name)
    fields.append(f)
    if order_by.ascending:
      return query.order_by(f)
    else:
      return query.order_by(f.desc())

  def _add_order_by_ending(self, query, field_names, fields):
    """Adds the order by ending."""
    for order_by_field in self.order_by_ending:
      if order_by_field in field_names:
        continue
      try:
        f = getattr(self.model_type, order_by_field)
      except AttributeError:
        raise BadRequest("No field named %s found on %s", (order_by_field,
                                                           self.model_type))
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
      except IntegrityError:
        logging.warning('Failed insert with %s.', tried_ids, exc_info=True)
    # We were unable to insert a participant (unlucky). Throw an error.
    logging.warning(
        'Giving up after %d insert attempts, tried %s.' % (MAX_INSERT_ATTEMPTS, all_tried_ids))
    raise ServiceUnavailable('Giving up after %d insert attempts.' % MAX_INSERT_ATTEMPTS)

  def count(self):
    with self.session() as session:
      return session.query(self.model_type).count()


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

  # pylint: disable=unused-argument
  def _do_update(self, session, obj, existing_obj):
    """Perform the update of the specified object. Subclasses can override to alter things."""
    session.merge(obj)

  def update_with_session(self, session, obj):
    """Updates the object in the database with the specified session and (optionally)
    expected version ID."""
    existing_obj = self.get_with_session(session, self.get_id(obj))
    self._validate_update(session, obj, existing_obj)
    self._do_update(session, obj, existing_obj)

  def update(self, obj):
    """Updates the object in the database. Will fail if the object doesn't exist already, or
    if obj.version does not match the version of the existing object.
    May modify the passed in object."""
    with self.session() as session:
      return self.update_with_session(session, obj)

def json_serial(obj):
  """JSON serializer for objects not serializable by default json code"""
  if isinstance(obj, datetime.datetime) or isinstance(obj, datetime.date):
    return obj.isoformat()
  if isinstance(obj, messages.Enum):
    return str(obj)
  raise TypeError("Type not serializable")
