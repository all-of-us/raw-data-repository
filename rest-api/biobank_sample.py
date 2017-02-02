import api_util
import data_access_object
import singletons

from participant_summary import DAO as summaryDAO
from google.appengine.ext import ndb

# The ID (scoped within a participant) for all biobank samples.
# (There is only one per participant.)
SINGLETON_SAMPLES_ID = '1'

class BiobankSample(ndb.Model):
  """A sample taken from a participant"""
  familyId = ndb.StringProperty()
  sampleId = ndb.StringProperty()
  storageStatus = ndb.StringProperty()
  type = ndb.StringProperty()
  testCode = ndb.StringProperty()
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
  last_modified = ndb.DateTimeProperty(auto_now=True)

class BiobankSamplesDAO(data_access_object.DataAccessObject):
  def __init__(self):
    import participant
    super(BiobankSamplesDAO, self).__init__(BiobankSamples, participant.Participant, False)

  def get_samples_for_participant(self, participant_id):
    return self.load_if_present(SINGLETON_SAMPLES_ID, participant_id)    

  def properties_from_json(self, dict_, ancestor_id, id_):
    for sample_dict in dict_['samples']:
      api_util.parse_json_date(sample_dict, 'collectionDate')
      api_util.parse_json_date(sample_dict, 'disposedDate')
      api_util.parse_json_date(sample_dict, 'confirmedDate')
    return dict_
    
  @ndb.transactional
  def store(self, model, date=None, client_id=None):
    super(BiobankSamplesDAO, self).store(model, date, client_id)
    import field_config.participant_summary_config
    participant_id = model.key.parent().id()
    summaryDAO().update_with_incoming_data(participant_id, model,
                                         field_config.participant_summary_config.CONFIG)

def DAO():
  return singletons.get(BiobankSamplesDAO)
