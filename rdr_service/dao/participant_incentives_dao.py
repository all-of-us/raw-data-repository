from rdr_service.api_util import format_json_bool
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.model.participant_incentives import ParticipantIncentives
from rdr_service.model.utils import to_client_participant_id


class ParticipantIncentivesDao(UpdatableDao):
    validate_version_match = False

    def __init__(self):
        super(ParticipantIncentivesDao, self).__init__(
            ParticipantIncentives, order_by_ending=['id'])
        self.site_dao = SiteDao()

    def get_id(self, obj):
        pass

    def to_client_json(self, model):
        obj = self.get_by_incentive_id(model.id)
        obj = self.convert_json_obj(obj)
        return obj

    def from_client_json(self, resource):
        incentive = self.model_type(**resource)
        return incentive

    def get_by_participant(self, participant_ids):
        if type(participant_ids) is not list:
            participant_ids = [participant_ids]

        with self.session() as session:
            return session.query(
                ParticipantIncentives.id.label('incentiveId'),
                ParticipantIncentives.participantId,
                ParticipantIncentives.site,
                ParticipantIncentives.createdBy,
                ParticipantIncentives.dateGiven,
                ParticipantIncentives.incentiveType,
                ParticipantIncentives.amount,
                ParticipantIncentives.occurrence,
                ParticipantIncentives.giftcardType,
                ParticipantIncentives.notes,
                ParticipantIncentives.cancelled,
                ParticipantIncentives.cancelledBy,
                ParticipantIncentives.cancelledDate
            ).filter(
                ParticipantIncentives.participantId.in_(participant_ids)
            ).all()

    def get_by_incentive_id(self, incentive_id):
        with self.session() as session:
            return session.query(
                ParticipantIncentives.id.label('incentiveId'),
                ParticipantIncentives.participantId,
                ParticipantIncentives.site,
                ParticipantIncentives.createdBy,
                ParticipantIncentives.incentiveType,
                ParticipantIncentives.dateGiven,
                ParticipantIncentives.amount,
                ParticipantIncentives.occurrence,
                ParticipantIncentives.giftcardType,
                ParticipantIncentives.notes,
                ParticipantIncentives.cancelled,
                ParticipantIncentives.cancelledBy,
                ParticipantIncentives.cancelledDate
            ).filter(
                ParticipantIncentives.id == incentive_id
            ).one_or_none()

    def convert_json_obj(self, obj):
        obj = obj._asdict() or obj.asdict()

        bool_fields = ['cancelled']
        for field in bool_fields:
            format_json_bool(obj, field_name=field)

        obj['participantId'] = to_client_participant_id(obj['participantId'])

        site = self.site_dao.get(obj['site'])
        obj['site'] = site.googleGroup

        for key, val in obj.items():
            if val is None:
                obj[key] = 'UNSET'

        return obj
