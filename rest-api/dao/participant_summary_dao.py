from dao.base_dao import UpdatableDao
from model.participant_summary import ParticipantSummary

class ParticipantSummaryDao(UpdatableDao):
  
  def __init__(self):
    super(ParticipantSummaryDao, self).__init__(ParticipantSummary)

  def get_id(self, obj):
    return obj.participantId
  