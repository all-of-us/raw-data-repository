import logging
import random

import dao.database_factory
from contextlib import contextmanager
from werkzeug.exceptions import BadRequest, NotFound, PreconditionFailed, ServiceUnavailable
from sqlalchemy.exc import IntegrityError
from query import Operator, PropertyType
from base64 import urlsafe_b64decode, urlsafe_b64encode
from singletons import get_cache
from sqlalchemy import or_, and_

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

_COMPARABLE_PROPERTY_TYPES = [ PropertyType.DATE, PropertyType.DATETIME, PropertyType.INTEGER ]

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

  cache_ttl_seconds can be specified to enable in-memory caching of results on get() calls only.
  Updates do not invalidate the cache on other servers, so beware.

  order_by_ending is a list of OrderBy objects to always use when query() is invoked, which should
  always end in the primary key; if not specified, query() is not supported.
  """
  def __init__(self, model_type, cache_ttl_seconds=None, order_by_ending=None):
    self.model_type = model_type
    self._database = dao.database_factory.get_database()
    self.order_by_ending = order_by_ending
    if cache_ttl_seconds:
      self._cache = get_cache(self.model_type, cache_ttl_seconds)
    else:
      self._cache = None

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

  def get_with_session(self, session, obj_id, check_cache=True):
    """Gets an object by ID for this type using the specified session. Returns None if not found."""
    if self._cache is not None and check_cache:
      result = self._cache.get(obj_id)
      if result:
        return result
    result = session.query(self.model_type).get(obj_id)
    if result and self._cache is not None:
      self._cache[obj_id] = result
    return result

  def get(self, obj_id):
    """Gets an object with the specified ID for this type from the database.

    Returns None if not found.
    """
    if self._cache is not None:
      result = self._cache.get(obj_id)
      if result:
        return result
    with self.session() as session:
      return self.get_with_session(session, obj_id, check_cache=False)

  def get_with_children(self, obj_id):
    """Subclasses may override this to eagerly loads any child objects (using subqueryload)."""
    return self.get(obj_id)

  def make_query_filter(self, field_name, value):
    """Attempts to make a query filter for the model property with the specified name, matching
    the specified value. If no such property exists, None is returned.
    """
    prop = getattr(self.model_type, field_name, None)
    if prop:
      property_type = _PROPERTY_TYPE_MAP.get(prop.__class__.__name__)
      if not property_type:
        raise BadRequest("Unrecognized filter on property of type %s" % prop.__class__.__name__)
      filter_value = None
      operator = Operator.EQUALS
      # If we're dealing with a comparable property type, look for a prefix that indicates an
      # operator other than EQUALS and strip it off
      if property_type in _COMPARABLE_PROPERTY_TYPES:
        for prefix, op in _OPERATOR_PREFIX_MAP.iteritems():
          if value.startswith(prefix):
            operator = op
            value = value[len(op)]
            break
      try:
        if property_type == PropertyType.DATE:
          filter_value = api_util.parse_date(value).date()
        elif property_type == PropertyType.DATETIME:
          filter_value = api_util.parse_date(value)
        elif property_type == PropertyType.ENUM:
          filter_value = prop._enum_type(value)
        elif property_type == PropertyType.INTEGER:
          filter_value = int(value)
        else:
          filter_value = value
      except ValueError:
        raise BadRequest("Invalid value for %s of type %s: %s" % (field_name, property_type,
                                                                  value))
      return FieldFilter(field_name, operator, filter_value)
    else:
      return None

  def query(self, query_def):
    if not order_by_ending:
      raise BadRequest("Can't query on type %s -- no order by ending speciifed" % self.model_type)
    with self.session() as session:
      query, fields = self._make_query(session, query_def)
      items = query.all()
    if items:
      if len(items) > query_def.max_results:
        return Results(items[0:query_def.max_results],
                       _make_pagination_token(items[query_def.max_results - 1].asdict(), fields))
      else:
        return Results(items, None)
    else:
      return Results(None, None)

  def _make_pagination_token(self, item_dict, fields):
    vals = [item_dict.get(field) for field in fields]
    vals_json = json.dumps(vals)
    return urlsafe_b64encode(vals_json)

  def _make_query(self, session, query_def):
    query = session.query(self.model_type)
    for field_filter in query_def.field_filters:
      try:
        f = getattr(self.model_type, field_filter.field_name)
      except AttributeError:
        raise BadRequest("No field named %s found on %s", (field_filter.field_name,
                                                           self.model_type))
      if field_filter.field_operator == Operator.EQUALS:
        query = query.filter(f == field_filter.value)
      elif field_filter.field_operator == Operator.LESS_THAN:
        query = query.filter(f < field_filter.value)
      elif field_filter.field_operator == Operator.GREATER_THAN:
        query = query.filter(f > field_filter.value)
      elif field_filter.field_operator == Operator.LESS_THAN_OR_EQUALS:
        query = query.filter(f <= field_filter.value)
      elif field_filter.field_operator == Operator.GREATER_THAN_OR_EQUALS:
        query = query.filter(f >= field_filter.value)
      elif field_filter.field_operator == Operator.NOT_EQUALS:
        query = query.filter(f != field_filter.value)
      else:
        raise BadRequest("Invalid operator: %s" % field_filter.field_operator)
    if query_def.ancestor_id:
      # For now, we only support participant IDs for ancestors.
      query = query.filter(self.model_type.participantId == query_def.ancestor_id)
    order_fields = []
    field_names = set()
    field_ascending = []
    fields = []
    if query_def.order_by:
      self._add_order_by(query_def.order_by, order_fields, field_names, field_ascending, fields)
    self._add_order_by(self.order_by_ending, order_fields, field_names, field_ascending, fields)
    query = query.order_by(order_fields)
    # Return one more than max_results, so that we know if there are more results.
    query = query.limit(query_def.max_results + 1)
    if query_def.pagination_token:
      # Add a query filter based on the pagination token.
      query = self._add_pagination_filter(query, query_def.pagination_token, fields,
                                          field_ascending)
    return query, fields

  def _add_pagination_filter(self, query, pagination_token, fields, field_ascending):
    """Adds a pagination filter for the decoded values in the pagination token based on
    the sort order. Example:

    ParticipantSummary.lastName > 'Jones' or
    (ParticipantSummary.lastName == 'Jones' and Participant.firstName > 'Bob') or
    (ParticipantSummary.lastName == 'Jones' and ParticipantSummary.firstName == 'Bob' and
     ParticipantSummary.dateOfBirth > <date>) or
    (ParticipantSummary.lastName == 'Jones' and ParticipantSummary.firstName == 'Bob' and
     ParticiapntSummary.dateOfBirth == <date> and ParticipantSummary.participantId > 123)
    """
    try:
      decoded_vals = json.loads(urlsafe_b64decode(query_def.pagination_token))
    except:
      raise BadRequest("Invalid pagination token: %s", query_def.pagination_token)
    if not type(decoded_vals) is list or len(decoded_vals) != len(order_by_fields):
      raise BadRequest("Invalid pagination token: %s" % query_def.pagination_token)
    or_clauses = []
    for i in range(0, len(fields)):
      or_clause_parts = []
      for j in range(0, i):
        or_clause_parts.append(fields[j] == decoded_vals[j])
      if field_ascending[i]:
        or_clause_parts.append(fields[i] > decoded_vals[i])
      else:
        or_clause_parts.append(fields[i] < decoded_vals[i])
      if len(or_clause_parts) == 1:
        or_clauses.append(or_clause_parts[0])
      else:
        or_clauses.append(and_(or_clause_parts))
    if len(or_clauses) == 1:
      return query.filter(or_clauses[0])
    else:
      return query.filter(or_(or_clauses))

  def _add_order_by(self, order_by, order_fields, field_names, field_ascending, fields):
    for order_by_field in order_by:
      if order_by_field.field_name in field_names:
        continue
      try:
        f = getattr(self.model_type, order_by_field.field_name)
      except AttributeError:
        raise BadRequest("No field named %s found on %s", (order_by_field.field_name,
                                                             self.model_type))
      field_names.add(order_by_field.field_name)
      field_ascending.append(order_by_field.ascending)
      fields.append(f)
      if order_by_field.ascending:
        order_fields.append(f)
      else:
        order_fields.append(f.desc())
  
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
    if self._cache is not None:
      del self._cache[self.get_id(obj)]

  def update(self, obj):
    """Updates the object in the database. Will fail if the object doesn't exist already, or
    if obj.version does not match the version of the existing object.
    May modify the passed in object."""
    with self.session() as session:
      return self.update_with_session(session, obj)
