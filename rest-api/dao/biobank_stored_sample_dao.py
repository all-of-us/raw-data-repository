from dao.base_dao import BaseDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.biobank_stored_sample import BiobankStoredSample


class BiobankStoredSampleDao(BaseDao):
  def __init__(self):
    super(BiobankStoredSampleDao, self).__init__(BiobankStoredSample, use_log_position=True)

  def get_id(self, obj):
    return obj.biobankStoredSampleId

  def _validate_insert(self, session, obj):
    ParticipantDao().validate_participant_reference(session, obj)

  def insert_with_session(self, session, obj):
    super(BiobankStoredSampleDao, self).insert_with_session(session, obj)
    ParticipantSummaryDao().update_from_biobank_stored_sample(session, obj)
