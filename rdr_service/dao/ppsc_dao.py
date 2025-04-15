from typing import List, Dict

from sqlalchemy import and_, case, or_
from sqlalchemy.sql import functions
from sqlalchemy.orm import aliased

from rdr_service.dao.base_dao import BaseDao, UpsertableDao
from rdr_service.model.ppsc import Participant, Site, NPHOptInEvent, ProfileUpdatesEvent, ParticipantStatusEvent, \
    ConsentEvent
from rdr_service.model.rex import ParticipantMapping
from rdr_service.model.study_nph import EligibleParticipants


class ParticipantDao(BaseDao):

    def __init__(self):
        super().__init__(Participant)

    def to_client_json(self, participant: Participant) -> str:
        return f'Participant P{participant.id} was created successfully'

    def get_participant_by_participant_id(self, *, participant_id: int):
        with self.session() as session:
            return session.query(Participant).filter(Participant.id == participant_id).all()

    def get_participant_by_biobank_id(self, *, biobank_id: int):
        with self.session() as session:
            return session.query(Participant).filter(Participant.biobank_id == biobank_id).all()

    def get_all_participants_from_list(self, *, participant_ids: List[int]):
        with self.session() as session:
            return session.query(Participant).filter(Participant.id.in_(participant_ids)).all()


class SiteDao(UpsertableDao):

    def __init__(self):
        super().__init__(Site)

    def to_client_json(self, obj: Site, action_type: str) -> str:
        return f'Site {obj.site_identifier} was {action_type} successfully'

    def get_site_by_identifier(self, *, site_identifier: str):
        with self.session() as session:
            return session.query(Site).filter(site_identifier == Site.site_identifier).first()


class PPSCDefaultBaseDao(BaseDao):

    def __init__(self, model_type):
        super().__init__(model_type)

    def from_client_json(self):
        pass

    def to_client_json(self, payload):
        return f"Event Record Created for: {payload['participantId']}"

    def get_id(self, obj):
        return obj.id

    def insert_bulk(self, batch: List[Dict]) -> None:
        with self.session() as session:
            session.bulk_insert_mappings(
                self.model_type,
                batch
            )


class PPSCNphOptEventInDao(BaseDao):

    def __init__(self):
        super().__init__(NPHOptInEvent)

    def from_client_json(self):
        pass

    def get_id(self, obj):
        return obj.id

    def get_eligible_participant_records(self):
        with self.session() as session:
            profile_updates_alias = aliased(ProfileUpdatesEvent)
            lastest_nph_ppi_data = session.query(
                ProfileUpdatesEvent.participant_id,
                case(
                    (ProfileUpdatesEvent.data_element_name == 'piiname_first',
                     ProfileUpdatesEvent.data_element_value),
                    else_=None
                ).label('first_name'),
                case(
                    (ProfileUpdatesEvent.data_element_name == 'piiname_last',
                     ProfileUpdatesEvent.data_element_value),
                    else_=None
                ).label('last_name'),
                case(
                    (ProfileUpdatesEvent.data_element_name == 'piicontactinformation_email',
                     ProfileUpdatesEvent.data_element_value),
                    else_=None
                ).label('email'),
                case(
                    (ProfileUpdatesEvent.data_element_name == 'piicontactinformation_phone',
                    ProfileUpdatesEvent.data_element_value),
                    else_=None
                ).label('phone'),
                case(
                    (ProfileUpdatesEvent.data_element_name == 'streetaddress_piizip',
                     ProfileUpdatesEvent.data_element_value),
                    else_=None
                ).label('zip_code'),
                case(
                    (ProfileUpdatesEvent.data_element_name == 'language_preference',
                     ProfileUpdatesEvent.data_element_value),
                    else_=None
                ).label('language_preference')
            ).join(
                # nph_opt_in_event
                self.model_type,
                and_(
                    self.model_type.participant_id == ProfileUpdatesEvent.participant_id,
                    self.model_type.data_element_value == 'submitted_yes'
                )
            ).join(
                ConsentEvent,
                and_(
                    ConsentEvent.participant_id == ProfileUpdatesEvent.participant_id,
                    ConsentEvent.event_type_name.ilike('%Primary Consent%'),
                    ConsentEvent.data_element_name == 'activity_status',
                    ConsentEvent.data_element_value.ilike('%Yes%')
                )
            ).outerjoin(
                profile_updates_alias,
                and_(
                    profile_updates_alias.participant_id == ProfileUpdatesEvent.participant_id,
                    profile_updates_alias.data_element_name == ProfileUpdatesEvent.data_element_name,
                    profile_updates_alias.event_authored_time > ProfileUpdatesEvent.event_authored_time
                )
            ).outerjoin(
              EligibleParticipants,
              EligibleParticipants.primary_participant_id == ProfileUpdatesEvent.participant_id
            ).outerjoin(
                ParticipantStatusEvent,
                and_(
                    ParticipantStatusEvent.participant_id == ProfileUpdatesEvent.participant_id,
                    ParticipantStatusEvent.event_type_name.ilike('%Test Account%')
                )
            ).outerjoin(
                ParticipantMapping,
                ParticipantMapping.primary_participant_id == ProfileUpdatesEvent.participant_id
            ).filter(
                ProfileUpdatesEvent.data_element_name.in_(
                    ['piiname_first',
                     'piiname_last',
                     'piicontactinformation_email',
                     'piicontactinformation_phone',
                     'streetaddress_piizip',
                     'language_preference']
                ),
                profile_updates_alias.id.is_(None),
                EligibleParticipants.id.is_(None),
                ParticipantStatusEvent.id.is_(None),
                ParticipantMapping.id.is_(None)
            ).group_by(
                ProfileUpdatesEvent.participant_id,
                case(
                    (ProfileUpdatesEvent.data_element_name == 'piiname_first',
                     ProfileUpdatesEvent.data_element_value),
                    else_=None
                ),
                case(
                    (ProfileUpdatesEvent.data_element_name == 'piiname_last',
                     ProfileUpdatesEvent.data_element_value),
                    else_=None
                ),
                case(
                    (ProfileUpdatesEvent.data_element_name == 'piicontactinformation_email',
                     ProfileUpdatesEvent.data_element_value),
                    else_=None
                ),
                case(
                    (ProfileUpdatesEvent.data_element_name == 'piicontactinformation_phone',
                     ProfileUpdatesEvent.data_element_value),
                    else_=None
                ).label('phone'),
                case(
                    (ProfileUpdatesEvent.data_element_name == 'streetaddress_piizip',
                     ProfileUpdatesEvent.data_element_value),
                    else_=None
                ).label('zip_code'),
                case(
                    (ProfileUpdatesEvent.data_element_name == 'language_preference',
                     ProfileUpdatesEvent.data_element_value),
                    else_=None
                ).label('language_preference')
            ).subquery()

            return session.query(
                lastest_nph_ppi_data.c.participant_id,
                functions.max(lastest_nph_ppi_data.c.first_name).label('first_name'),
                functions.max(lastest_nph_ppi_data.c.last_name).label('last_name'),
                functions.max(lastest_nph_ppi_data.c.email).label('email'),
                functions.max(lastest_nph_ppi_data.c.phone).label('phone'),
                functions.max(lastest_nph_ppi_data.c.zip_code).label('zip_code'),
                functions.max(lastest_nph_ppi_data.c.language_preference).label('language_preference')
            ).group_by(
                lastest_nph_ppi_data.c.participant_id
            ).having(
                or_(
                    functions.max(lastest_nph_ppi_data.c.phone).isnot(None),
                    functions.max(lastest_nph_ppi_data.c.email).isnot(None),
                )
            ).distinct().all()

    def insert_bulk(self, batch: List[Dict]) -> None:
        with self.session() as session:
            session.bulk_insert_mappings(
                self.model_type,
                batch
            )

