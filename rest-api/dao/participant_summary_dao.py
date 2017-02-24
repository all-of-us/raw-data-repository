import config
from dao.base_dao import UpdatableDao
from model.participant_summary import ParticipantSummary


class ParticipantSummaryDao(UpdatableDao):
  def __init__(self):
    super(ParticipantSummaryDao, self).__init__(ParticipantSummary)

  def get_id(self, obj):
    return obj.participantId

  def update_from_biobank_stored_sample(self, session, sample):
    """Updates the count of samples (for baseline tests only) received for a participant."""
    if sample.testCode in config.getSettingList(config.BASELINE_SAMPLE_TEST_CODES, []):
      summary = self.get(sample.participantId)
      summary.numBaselineSamplesArrived += 1
      session.merge(summary)
