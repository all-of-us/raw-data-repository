"""Base object for Datastore data access objects."""

import copy

from google.appengine.ext import ndb
from werkzeug.exceptions import NotFound

class DataAccessObject(object):
  """Base class for data access objects.

  DataAccessObjects handle translating models to and from their JSON
  representation, persisting model objects to the datastore and appending to the
  history table.

  Models are serialized to JSON with the to_dict() function.  Unfortunately, the
  to_dict() function doesn't always convert to JSON serializable values. If the
  model has any fields need to be converted to JSON serializable values, do that
  by overriding fields_to_json().

  Models are parsed from a JSON dictionary by passing to the model's populate
  function.  Unfortunately the populate function can't take string
  representations for non-string properties.  Convert any non-string properties
  by overriding fields_from_json().

  fields_from_json() and fields_to_json() can also make changes to the structure
  of the JSON dictionary if the json fields don't map directly to the model
  fields.
  """
  def __init__(self, model_type, ancestor_type=None):
    self.model_type = model_type
    self.ancestor_type = ancestor_type
    self.model_name = model_type.__name__

    history_props = {
        "date": ndb.DateTimeProperty(auto_now_add=True),
        "obj": ndb.StructuredProperty(model_type, repeated=False),
    }
    self.history_model = type(
        self.model_name + 'History', (ndb.Model,), history_props)

  def to_json(self, m):
    json_obj = copy.deepcopy(m.to_dict())
    return self.fields_to_json(json_obj)

  def from_json(self, dict, ancestor_id=None, id=None):
    dict = copy.deepcopy(dict)

    key_path = []
    if self.ancestor_type:
      key_path.append(self.ancestor_type)
      key_path.append(ancestor_id)
    if id:
      key_path.append(self.model_type)
      key_path.append(id)

    key = None
    if key_path:
      key = ndb.Key(flat=key_path)

    m = self.model_type(key=key)
    dict = self.fields_from_json(dict, ancestor_id, id)
    m.populate(**dict)
    return m

  def fields_from_json(self, dict, ancestor_id=None, id=None):
    return dict

  def fields_to_json(self, dict):
    return dict

  def load(self, id, ancestor_id=None):
    if ancestor_id:
      key = ndb.Key(self.ancestor_type, ancestor_id, self.model_type, id)
    else:
      key = ndb.Key(self.model_type, id)

    m = key.get()
    if not m:
      raise NotFound('{} with id {}:{} not found.'.format(
          self.model_name, ancestor_id, id))
    return m

  def store(self, model):
    self.history_model(obj=model).put()
    model.put()
