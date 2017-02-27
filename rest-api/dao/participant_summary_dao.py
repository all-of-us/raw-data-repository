import config
from dao.base_dao import UpdatableDao
from model.participant_summary import ParticipantSummary


class ParticipantSummaryDao(UpdatableDao):
  def __init__(self):
    super(ParticipantSummaryDao, self).__init__(ParticipantSummary)

  def get_id(self, obj):
    return obj.participantId

  def update_from_biobank_stored_samples(self, session, participant_id, all_samples):
    """Updates the count of samples (for baseline tests only) received for a participant."""
    summary = self.get_with_session(session, participant_id)
    baseline_tests = set(config.getSettingList(config.BASELINE_SAMPLE_TEST_CODES, []))
    summary.numBaselineSamplesArrived = sum(
        [1 for s in all_samples if s.testCode in baseline_tests])
