from dao.base_dao import BaseDao
from dao.participant_dao import ParticipantDao
from model.biobank_stored_sample import BiobankStoredSample


class BiobankStoredSampleDao(BaseDao):
  def __init__(self):
    super(BiobankStoredSampleDao, self).__init__(BiobankStoredSample, use_log_position=True)

  def get_id(self, obj):
    return obj.biobankStoredSampleId

  def _validate_insert(self, session, obj):
    ParticipantDao().validate_participant_reference(session, obj)

  #def store(self, model):
  #  update participant summary
