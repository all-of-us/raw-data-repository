from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.model.participant_incentives import ParticipantIncentives


class ParticipantIncentivesDao(UpdatableDao):
    validate_version_match = False

    def __init__(self):
        super(ParticipantIncentivesDao, self).__init__(
            ParticipantIncentives, order_by_ending=['id'])

    def get_by_participant(self, participant_ids):
        if type(participant_ids) is not list:
            participant_ids = [participant_ids]

        with self.session() as session:
            return session.query(
                ParticipantIncentives
            ).filter(
                ParticipantIncentives.participant_id.in_(participant_ids)
            )
