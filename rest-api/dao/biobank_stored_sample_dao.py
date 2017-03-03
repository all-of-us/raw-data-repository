from dao.base_dao import BaseDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.biobank_stored_sample import BiobankStoredSample
from model.log_position import LogPosition

from werkzeug.exceptions import BadRequest


class BiobankStoredSampleDao(BaseDao):
  """For testing only.

  BiobankStoredSample creation and associated ParticipantSummary updates are managed through SQL in
  the samples import and reconciliation pipelines.
  """
  def __init__(self):
    super(BiobankStoredSampleDao, self).__init__(BiobankStoredSample)

  def get_id(self, obj):
    return obj.biobankStoredSampleId
