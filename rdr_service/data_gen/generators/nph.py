from rdr_service.dao import database_factory
from rdr_service.model.study_nph import Participant, Site, PairingEvent, ParticipantEventActivity, Activity, \
    PairingEventType


class NphDataGenerator:
    def __init__(self):
        self.session = database_factory.get_database().make_session()
        self._next_unique_participant_id = 100000000
        self._next_unique_biobank_id = 1000000000
        self._next_unique_research_id = 10000

    def _commit_to_database(self, model):
        self.session.add(model)
        self.session.commit()

    def unique_participant_id(self):
        next_participant_id = self._next_unique_participant_id
        self._next_unique_participant_id += 1
        return next_participant_id

    def unique_biobank_id(self):
        next_biobank_id = self._next_unique_biobank_id
        self._next_unique_biobank_id += 1
        return next_biobank_id

    def unique_research_id(self):
        next_research_id = self._next_unique_research_id
        self._next_unique_research_id += 1
        return next_research_id

    @staticmethod
    def _participant(**kwargs):
        return Participant(**kwargs)

    def create_database_participant(self, **kwargs):

        fields = {
            "id": self.unique_participant_id(),
            "biobank_id": self.unique_biobank_id(),
            "research_id": self.unique_research_id()
        }
        fields.update(kwargs)

        participant = self._participant(**fields)
        self._commit_to_database(participant)
        return participant

    @staticmethod
    def _site(**kwargs):
        return Site(**kwargs)

    def create_database_site(self, **kwargs):
        site = self._site(**kwargs)
        self._commit_to_database(site)
        return site

    @staticmethod
    def _participant_event_activity(**kwargs):
        return ParticipantEventActivity(**kwargs)

    def create_database_participant_event_activity(self, **kwargs):
        pea = self._participant_event_activity(**kwargs)
        self._commit_to_database(pea)
        return pea

    @staticmethod
    def _pairing_event_type(**kwargs):
        return PairingEventType(**kwargs)

    def create_database_pairing_event_type(self, **kwargs):
        pairing_type = self._pairing_event_type(**kwargs)
        self._commit_to_database(pairing_type)
        return pairing_type

    @staticmethod
    def _pairing_event(**kwargs):
        return PairingEvent(**kwargs)

    def create_database_pairing_event(self, participant_id, **kwargs):
        event_id = kwargs.get('event_id')
        if event_id is None:
            pea = self.create_database_participant_event_activity(activity_id=2, participant_id=participant_id)
            event_id = pea.id

        # Todo: Add more default fields as needed
        fields = {
            "participant_id": participant_id,
            "event_id": event_id,
            "event_type_id": 1,
        }
        fields.update(kwargs)
        pairing_event = self._pairing_event(**fields)
        self._commit_to_database(pairing_event)
        return pairing_event

    @staticmethod
    def _activity(**kwargs):
        return Activity(**kwargs)

    def create_database_activity(self, **kwargs):
        activity = self._activity(**kwargs)
        self._commit_to_database(activity)
        return activity
