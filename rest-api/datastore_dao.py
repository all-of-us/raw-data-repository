'''The definition of the participant object and DB marshalling.
'''

import pprint

from data_access_object import DataAccessObject
from protorpc import message_types
from protorpc import messages
from google.appengine.ext import ndb
from endpoints_proto_datastore.ndb import EndpointsModel
from werkzeug.exceptions import NotFound


class DatastoreDAO(object):
  def __init__(self, model_class):
    self.model_class = model_class

  def update(self, model):
    # Get the existing entity from the datastore by the drc_internal_id.  We
    # just need the key so we can update it.
    from_db = self.get(model)
    # This fills in all unset fields in entity with thier corresponding values
    # from participant.
    model.EntityKeySet(from_db.entityKey)
    # Write back the merged entity.
    model.put()
    return model

  def get(self, key):
    return key.get()

  def list(request_obj):
    query = self.model_class.query()
    for field_name, val in reqeust_obj.to_dict().iteritems():
      if val is not None:
        query = query.filter(getattr(self.model_class, field_name) == val)

    return Participant.ToMessageCollection(query.fetch())
