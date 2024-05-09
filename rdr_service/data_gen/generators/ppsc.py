from datetime import datetime

from rdr_service import clock
from rdr_service.dao import database_factory
from rdr_service.model.ppsc import Participant, Activity

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TIME = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)


class PPSCBaseDataGenerator:
    def __init__(self):
        self.session = database_factory.get_database().make_session()

    def _commit_to_database(self, model):
        self.session.add(model)
        self.session.commit()


class PPSCDataGenerator(PPSCBaseDataGenerator):
    def __init__(self):
        super().__init__()
        self._next_unique_participant_id = 100000000
        self._next_unique_biobank_id = 1100000000
        self._next_unique_research_id = 10000

    def unique_participant_id(self):
        next_participant_id = self._next_unique_participant_id
        self._next_unique_participant_id += 1
        return next_participant_id

    def unique_biobank_id(self):
        next_biobank_id = self._next_unique_biobank_id
        self._next_unique_biobank_id += 1
        return next_biobank_id

    @staticmethod
    def create_participant(**kwargs):
        return Participant(**kwargs)

    def create_database_participant(self, **kwargs):
        participant = {
            "id": self.unique_participant_id(),
            "biobank_id": self.unique_biobank_id(),
            "registered_date": clock.CLOCK.now()
        }
        participant.update(kwargs)
        participant = self.create_participant(**participant)
        self._commit_to_database(participant)
        return participant

    @staticmethod
    def _activity(**kwargs):
        return Activity(**kwargs)

    def create_database_activity(self, **kwargs):
        activity = self._activity(**kwargs)
        self._commit_to_database(activity)
        return activity
