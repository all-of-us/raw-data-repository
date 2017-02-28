from dao.base_dao import BaseDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.biobank_stored_sample import BiobankStoredSample
from model.log_position import LogPosition

from werkzeug.exceptions import BadRequest


class BiobankStoredSampleDao(BaseDao):
  def __init__(self):
    super(BiobankStoredSampleDao, self).__init__(BiobankStoredSample)

  def get_id(self, obj):
    return obj.biobankStoredSampleId

  def _validate_insert(self, session, obj):
    raise NotImplementedError('Inserting individual samples is not supported.')

  def _validate_sample_ids_unused(self, session, participant_id, sample_ids):
    """Validates that none of the given sample IDs conflict with other participants."""
    conflicts = (session.query(BiobankStoredSample)
        .filter(BiobankStoredSample.participantId != participant_id)
        .filter(BiobankStoredSample.biobankStoredSampleId in sample_ids)
        .all())
    if conflicts:
      conflict_names = ['P%d/%d' % (s.participantId, s.biobankStoredSampleId) for s in conflicts]
      raise BadRequest(
          'Incoming sample IDs for P%d conflict with: %s.' % (participant_id, conflict_names))

  def _list_existing_samples(self, session, participant_id, new_sample_ids):
    """Lists all pre-existing samples on a participant, excluding new/updated samples."""
    return (session.query(BiobankStoredSample)
        .filter(BiobankStoredSample.participantId == participant_id)
        .filter(BiobankStoredSample.biobankStoredSampleId not in new_sample_ids)
        .all())

  def insert_or_update(self, session, participant_id, sample_list):
    """Sets the given participants samples list to match the given list. Updates ParticipantSummary.

    All the given samples' participantId fields are set to participant_id (they need not be set
    beforehand).

    Existing samples with matching biobankStoredSampleId and participantId will be replaced. New
    samples will be inserted, and existing samples on the participant with different IDs will be
    untouched.
    """
    if not sample_list:
      return
    for sample in sample_list:
      sample.participantId = participant_id
      sample.logPosition = LogPosition()
    ParticipantDao().validate_participant_reference(session, sample_list[0])

    new_sample_ids = [sample.biobankStoredSampleId for sample in sample_list]
    self._validate_sample_ids_unused(session, participant_id, new_sample_ids)
    untouched_existing_samples = self._list_existing_samples(
        session, participant_id, new_sample_ids)
    # Add & flush parents. Committing children at the same time as or before parents causes errors.
    session.add_all([s for s in sample_list if s.parentSampleId is None])
    session.flush()
    session.add_all(sample_list)
    ParticipantSummaryDao().update_from_biobank_stored_samples(
        session, participant_id, sample_list + untouched_existing_samples)
