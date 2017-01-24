"""Base object for Datastore data access objects."""
import uuid
import copy
import api_util

from query import PropertyType, Operator, Results
from google.appengine.ext import ndb
from werkzeug.exceptions import Conflict
from werkzeug.exceptions import PreconditionFailed
from werkzeug.exceptions import NotFound

PROPERTY_TYPE_MAP = {
  "ComputedProperty": PropertyType.STRING,
  "StringProperty": PropertyType.STRING,
  "DateProperty": PropertyType.DATE,
  "DateTimeProperty": PropertyType.DATE,
  "EnumProperty": PropertyType.ENUM
}

class DataAccessObject(object):
  """Base class for data access objects.

  DataAccessObjects handle translating models to and from their JSON
  representation, persisting model objects to the datastore and appending to the
  history table.

  Models are serialized to JSON with the to_dict() function.  Unfortunately, the
  to_dict() function doesn't always convert to JSON serializable values. If the
  model has any fields need to be converted to JSON serializable values, do that
  by overriding properties_to_json().

  Models are parsed from a JSON dictionary by passing to the model's populate
  function.  Unfortunately the populate function can't take string
  representations for non-string properties.  Convert any non-string properties
  by overriding properties_from_json().

  properties_from_json() and properties_to_json() can also make changes to the
  structure of the JSON dictionary if the json fields don't map directly to the
  model fields.
  """
  def __init__(self, model_type, ancestor_type=None, keep_history=True):
    self.model_type = model_type
    self.ancestor_type = ancestor_type
    self.model_name = model_type.__name__
    self.keep_history = keep_history

    if self.keep_history:
      history_props = {
          'date': ndb.DateTimeProperty(auto_now_add=True),
          'obj': ndb.StructuredProperty(model_type, repeated=False),
          'client_id': ndb.StringProperty(),
      }
      self.history_model = type(
          self.model_name + 'History', (ndb.Model,), history_props)

  def to_json(self, m):
    properties_obj = copy.deepcopy(m.to_dict())
    dict_ = self.properties_to_json(properties_obj)
    last_modified = dict_.get('last_modified')
    if last_modified:
      del dict_['last_modified']
      dict_['meta'] = {'versionId': self.make_version_id(last_modified)}
    return dict_

  def from_json(self, dict_, ancestor_id=None, id_=None):
    assert bool(ancestor_id) == bool(self.ancestor_type), "Requires an ancestor_id"
    dict_ = copy.deepcopy(dict_)

    key_path = []
    if self.ancestor_type and ancestor_id:
      key_path.append(self.ancestor_type)
      key_path.append(ancestor_id)
    if id_:
      key_path.append(self.model_type)
      key_path.append(id_)

    key = None
    if key_path:
      key = ndb.Key(flat=key_path)

    m = self.model_type(key=key)
    dict_ = self.properties_from_json(dict_, ancestor_id, id_)
    # Do not populate meta fields received in the request.
    if dict_.get('meta'):
      del dict_['meta']

    m.populate(**dict_)
    return m

  # pylint: disable=unused-argument
  def properties_from_json(self, dict_, ancestor_id, id_):
    """Converts json fields so they can be assigned to ndb properties.

    Overriding this method is required unless the ndb model is all
    StringProperties.

    After this function is called the returned dictionary is passed to the
    constructor of the ndb model.  Each field must be converted so that it can
    be assigned to the ndb properties.  This base class implementation is a
    pass-through.

    Args:
      dict_: The json dictionary.  For convienience, this object is a deep copy
        of the original json, so it can be manipulated in place.
      ancestor_id: The ancestor id.
      id_: The object's id.

    Returns:
      A dictionary that can be passed to the contstructor of the ndb model.
    """
    return dict_

  def properties_to_json(self, dict_):
    """Converts ndb properties to their string representations for json.

    As a first step in converting ndb models to json, to_dict() is called.  This
    is sufficient if all the ndb properties are strings.  If not, subclasses
    should override this method to convert each field to its string
    representation. This base class implementation is a pass-through.

    Args:
      dict_: A deep copy of the dict as returned from ndb.Model.to_dict().
        For simple transformations fo the fields, this object can be modified
        and returned.

    Returns:
      A json representation of the model.
    """
    return dict_

  def load_if_present(self, id_, ancestor_id=None):
    assert bool(ancestor_id) == bool(self.ancestor_type), "Requires an ancestor_id"
    key = self._make_key(id_, ancestor_id)
    return key.get()

  def load(self, id_, ancestor_id=None):
    m = self.load_if_present(id_, ancestor_id)
    if not m:
      raise NotFound('{} with id {}:{} not found.'.format(
          self.model_name, ancestor_id, id_))
    return m

  @ndb.transactional
  def insert(self, model, date=None, client_id=None):
    if model.key.get():
      raise Conflict('{} with key {} already exists'.format(
          self.model_name, model.key))
    return self.store(model, date, client_id)

  @ndb.transactional
  def update(self, model, expected_version_id, date=None, client_id=None):
    if not expected_version_id:
      raise PreconditionFailed('If-Match header missing when updating resource')
    existing_obj = model.key.get()
    if not existing_obj:
      raise NotFound('{} with key {} does not exist'.format(
          self.model_name, model.key))
    if existing_obj.last_modified:
      version_id = self.make_version_id(existing_obj.last_modified)
      if version_id != expected_version_id:
        raise PreconditionFailed('If-Match header was {}; stored version was {}'.format(
            expected_version_id, version_id))
    model.last_modified = None
    return self.store(model, date, client_id)

  @ndb.transactional
  def replace(self, model, date=None, client_id=None):
    existing_obj = model.key.get()
    if not existing_obj:
      raise NotFound('{} with key {} does not exist'.format(self.model_name, model.key))
    return self.store(model, date, client_id)

  @ndb.transactional
  def make_history(self, model, date=None, client_id=None):
    h = self.history_model(parent=model.key, obj=model)
    if date:
      h.populate(date=date)
    if client_id:
      h.populate(client_id=client_id)
    return h

  @ndb.transactional
  def store(self, model, date=None, client_id=None):
    if self.keep_history:
      self.make_history(model, date, client_id).put()
    return model.put()

  def get_all_history(self, ancestor_key, now=None):
    assert self.keep_history
    result = self.history_model.query(ancestor=ancestor_key).fetch()
    if now:
      return [hist_obj for hist_obj in result if hist_obj.date <= now]
    return result

  def children(self, parent):
    """Gets all objects that have parent as an ancestor."""
    return self.model_type.query(ancestor=parent.key).fetch()

  def last_history(self, obj):
    """Gets the history object associated with the last update to obj."""
    assert self.keep_history
    query = self.history_model.query(ancestor=obj.key).order(-self.history_model.date)
    hists = query.fetch(limit=1)
    return hists and hists[0] or None

  def _make_key(self, id_, ancestor_id=None):
    """Generates a key for this type given an and and ancestor id (for child objects)"""
    assert bool(ancestor_id) == bool(self.ancestor_type), "Requires an ancestor_id"
    if ancestor_id:
      return ndb.Key(self.ancestor_type, ancestor_id, self.model_type, id_)
    else:
      return ndb.Key(self.model_type, id_)

  def allocate_id(self):
    """Creates a new id for this object.

    Override this to use something other than a uuid.
    """
    return str(uuid.uuid4())

  def make_version_id(self, last_modified):
    return 'W/"{}"'.format(api_util.unix_time_millis(last_modified))

  def is_string_property(self, prop):
    property_type = PROPERTY_TYPE_MAP.get(prop.__class__.__name__)
    assert property_type, "Property class {} had invalid property type".format(
        prop.__class__.__name__)
    return property_type == PropertyType.STRING

  def get_search_property_and_value(self, field_name, value=None):
    prop = getattr(self.model_type, field_name, None)
    assert prop, "Property {}.{} not found".format(self.model_name, field_name)
    if self.is_string_property(prop):
      search_property = getattr(self.model_type, field_name + 'Search', None)
      if search_property:
        if value:
          return (search_property, api_util.searchable_representation(value))
        else:
          return (search_property, None)
    return (prop, value)

  def query(self, query_definition):
    if query_definition.ancestor_id:
      ancestor_key = ndb.Key(self.ancestor_type, query_definition.ancestor_id)
      query = self.model_type.query(ancestor=ancestor_key)
    else:
      query = self.model_type.query()
    for field_filter in query_definition.field_filters:
      (search_property, search_value) = self.get_search_property_and_value(
          field_filter.field_name, field_filter.value)
      operator = field_filter.operator
      if operator == Operator.EQUALS:
        query = query.filter(search_property == search_value)
      elif operator == Operator.LESS_THAN:
        query = query.filter(search_property < search_value)
      elif operator == Operator.GREATER_THAN:
        query = query.filter(search_property > search_value)
      elif operator == Operator.LESS_THAN_OR_EQUALS:
        query = query.filter(search_property <= search_value)
      elif operator == Operator.GREATER_THAN_OR_EQUALS:
        query = query.filter(search_property >= search_value)
      else:
        assert false, "Invalid operator: {}".format(operator)
    if query_definition.order_by:
      order_property = self.get_search_property_and_value(
          query_definition.order_by.field_name, None)[0]
      if query_definition.order_by.ascending:
        query = query.order(order_property)
      else:
        query = query.order(-order_property)
    cursor = None
    if query_definition.pagination_token:
      cursor = ndb.Cursor(urlsafe=query_definition.pagination_token)
    fetch_results = query.fetch_page(query_definition.max_results, start_cursor=cursor)
    result_token = None
    if fetch_results[2]:
      result_token = fetch_results[1].urlsafe()
    return Results(fetch_results[0], result_token)
