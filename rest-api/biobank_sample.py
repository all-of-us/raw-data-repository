import api_util
import data_access_object

from participant import Participant

from google.appengine.ext import ndb

class BiobankSample(ndb.Model):
   """A sample taken from a participant"""
   familyId = ndb.StringProperty()
   sampleId = ndb.StringProperty()
   eventName = ndb.StringProperty()
   storageStatus = ndb.StringProperty()
   type = ndb.StringProperty()
   treatments = ndb.StringProperty()
   expectedVolume = ndb.StringProperty()
   quantity = ndb.StringProperty()   
   containerType = ndb.StringProperty()
   collectionDate = ndb.DateTimeProperty()
   disposalStatus = ndb.StringProperty()
   disposedDate = ndb.DateTimeProperty()
   parentSampleId = ndb.StringProperty()
   confirmedDate = ndb.DateTimeProperty()
   
class BiobankSamples(ndb.Model):
  """An inventory of samples"""
  samples = ndb.LocalStructuredProperty(BiobankSample, repeated=True)
    
class BiobankSamplesDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(BiobankSamplesDAO, self).__init__(BiobankSamples, Participant, False)

  def properties_from_json(self, dict_, ancestor_id, id_):        
    for sample_dict in dict_['samples']:
      api_util.parse_json_date(sample_dict, 'collectionDate')
      api_util.parse_json_date(sample_dict, 'disposedDate')
      api_util.parse_json_date(sample_dict, 'confirmedDate')
    return dict_    

DAO = BiobankSamplesDAO()
