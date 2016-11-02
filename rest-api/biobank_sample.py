import api_util
import data_access_object

from participant import Participant

from google.appengine.ext import ndb

class BiobankSample(ndb.Model):
   """A sample taken from a participant"""
   family_id = ndb.StringProperty()
   sample_id = ndb.StringProperty()
   event_name = ndb.StringProperty()
   storage_status = ndb.StringProperty()
   type = ndb.StringProperty()
   treatments = ndb.StringProperty()
   expected_volume = ndb.StringProperty()
   quantity = ndb.StringProperty()   
   container_type = ndb.StringProperty()
   collection_date = ndb.DateTimeProperty()
   disposal_status = ndb.StringProperty()
   disposed_date = ndb.DateTimeProperty()
   parent_sample_id = ndb.StringProperty()
   confirmed_date = ndb.DateTimeProperty()
   
class BiobankSamples(ndb.Model):
  """An inventory of samples"""
  samples = ndb.LocalStructuredProperty(BiobankSample, repeated=True)
  
class BiobankSamplesPipelineRun(ndb.Model):
  in_progress = ndb.BooleanProperty()
  complete = ndb.BooleanProperty()
  date = ndb.DateTimeProperty(auto_now=True)
  
class BiobankSamplesDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(BiobankSamplesDAO, self).__init__(BiobankSamples, Participant, False)

  def properties_from_json(self, dict_, ancestor_id, id_):        
    for sample_dict in dict_['samples']:
      api_util.parse_json_date(sample_dict, 'collection_date')
      api_util.parse_json_date(sample_dict, 'disposed_date')
      api_util.parse_json_date(sample_dict, 'confirmed_date')
    return dict_    

DAO = BiobankSamplesDAO()
