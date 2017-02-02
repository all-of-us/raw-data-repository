'''The definition of the Biobank order object and DB marshalling.
'''

import api_util

import data_access_object
import singletons

from google.appengine.ext import ndb

class BiobankOrderIdentifier(ndb.Model):
  """An identifier for a Biobank order"""
  system = ndb.StringProperty()
  value = ndb.StringProperty()

class BiobankOrderSample(ndb.Model):
  """A sample in a Biobank order"""
  test = ndb.StringProperty()
  description = ndb.StringProperty()
  processingRequired = ndb.BooleanProperty()
  collected = ndb.DateTimeProperty()
  processed = ndb.DateTimeProperty()
  finalized = ndb.DateTimeProperty()

class BiobankOrderNotes(ndb.Model):
  """Notes associated with a Biobank order"""
  collected = ndb.StringProperty()
  processed = ndb.StringProperty()
  finalized = ndb.StringProperty()

class BiobankOrder(ndb.Model):
  """The Biobank order resource definition"""
  id = ndb.StringProperty()
  subject = ndb.StringProperty()
  created = ndb.DateTimeProperty()
  identifier = ndb.StructuredProperty(BiobankOrderIdentifier, repeated=True)
  sourceSite = ndb.StructuredProperty(BiobankOrderIdentifier, repeated=False)
  samples = ndb.LocalStructuredProperty(BiobankOrderSample, repeated=True)
  notes = ndb.LocalStructuredProperty(BiobankOrderNotes, repeated=False)
  last_modified = ndb.DateTimeProperty(auto_now=True)
  
class BiobankOrderDAO(data_access_object.DataAccessObject):  
  DATE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

  def __init__(self):
    import participant
    super(BiobankOrderDAO, self).__init__(BiobankOrder, participant.Participant)

  def properties_from_json(self, dict_, ancestor_id, id_):
    if id_:
      dict_['id'] = id_
    api_util.parse_json_date(dict_, 'created')
    for sample_dict in dict_['samples']:
      api_util.parse_json_date(sample_dict, 'collected')
      api_util.parse_json_date(sample_dict, 'processed')
      api_util.parse_json_date(sample_dict, 'finalized')
    return dict_

  def properties_to_json(self, dict_):
    api_util.format_json_date(dict_, 'created',
                              date_format=BiobankOrderDAO.DATE_TIME_FORMAT)
    for sample_dict in dict_['samples']:
      api_util.format_json_date(sample_dict, 'collected',
                                date_format=BiobankOrderDAO.DATE_TIME_FORMAT)
      api_util.format_json_date(sample_dict, 'processed',
                                date_format=BiobankOrderDAO.DATE_TIME_FORMAT)
      api_util.format_json_date(sample_dict, 'finalized',
                                date_format=BiobankOrderDAO.DATE_TIME_FORMAT)
    return dict_

  def find_by_identifier(self, identifier):
    query = BiobankOrder.query(BiobankOrder.identifier.system == identifier.system,
                               BiobankOrder.identifier.value == identifier.value)
    results = query.fetch()
    if len(results) == 0:
      return None
    return results[0]

def DAO():
  return singletons.get(BiobankOrderDAO)
