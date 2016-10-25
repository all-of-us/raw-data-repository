"""Base object for Datastore data access objects."""
import api_util
import uuid
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
  by overriding properties_to_json().

  Models are parsed from a JSON dictionary by passing to the model's populate
  function.  Unfortunately the populate function can't take string
  representations for non-string properties.  Convert any non-string properties
  by overriding properties_from_json().

  properties_from_json() and properties_to_json() can also make changes to the
  structure of the JSON dictionary if the json fields don't map directly to the
  model fields.
  """
  def __init__(self, model_type, ancestor_type=None):
    self.model_type = model_type
    self.ancestor_type = ancestor_type
    self.model_name = model_type.__name__

    history_props = {
        'date': ndb.DateTimeProperty(auto_now_add=True),
        'obj': ndb.StructuredProperty(model_type, repeated=False),
    }
    self.history_model = type(
        self.model_name + 'History', (ndb.Model,), history_props)

  def to_json(self, m):
    properties_obj = copy.deepcopy(m.to_dict())
    return self.properties_to_json(properties_obj)

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

    m.populate(**dict_)
    return m

  def properties_from_json(self, dict_, ancestor_id, id_):
    """Convert json fields to so they  can be assigned to ndb properties.

    Overriding this method id required unless the ndb model is all
    StringProperties.

    After this function is called the returned dictionary is passed to the
    constructor of the ndb model.  Each field must be converted so that it can
    be assigned to the ndb properties.

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
    """Convert ndb properties to their string representations for json.

    As a first step in converting ndb models to json, to_dict() is called.  This
    is sufficient if all the ndb properties are strings.  If not, subclasses
    should override this method to convert each field to its string
    representation.

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
    return self._make_key(id_, ancestor_id).get()

  def load(self, id_, ancestor_id=None):
    m = self.load_if_present(id_, ancestor_id)
    if not m:
      raise NotFound('{} with id {}:{} not found.'.format(
          self.model_name, ancestor_id, id_))
    return m

  def store(self, model, date=None):
    h = self.history_model(parent=model.key, obj=model)
    if date:
      h.populate(date=date)
    h.put()
    model.put()

  def get_all_history(self, ancestor_key):
    return self.history_model.query(ancestor=ancestor_key).fetch()

  def _make_key(self, id_, ancestor_id):
    if ancestor_id:
      return ndb.Key(self.ancestor_type, ancestor_id, self.model_type, id_)
    else:
      return ndb.Key(self.model_type, id_)

  def allocate_id(self):
    """Creates a new id for this object.

    Override this to use something other than a uuid.
    """
    return str(uuid.uuid4())
