from datetime import datetime

from rdr_service.dao import database_factory
from rdr_service.model.rex import ParticipantMapping
from rdr_service.model.study_nph import Participant, Site, PairingEvent, ParticipantEventActivity, Activity, \
    PairingEventType, ConsentEvent, ConsentEventType, EnrollmentEventType, EnrollmentEvent, WithdrawalEvent, \
    DeactivationEvent, ParticipantOpsDataElement, OrderedSample, StoredSample, Order, StudyCategory, DietEvent
from rdr_service.ancillary_study_resources.nph import enums
from rdr_service.model.study_nph_sms import SmsJobRun, SmsSample, SmsBlocklist, SmsN0, SmsN1Mc1

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TIME = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)


class NphBaseGenerator:
    def __init__(self):
        self.session = database_factory.get_database().make_session()

    def _commit_to_database(self, model):
        self.session.add(model)
        self.session.commit()


class NphDataGenerator(NphBaseGenerator):
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
    def _rex_participant_mapping(**kwargs):
        return ParticipantMapping(**kwargs)

    def create_database_rex_participant_mapping(self, **kwargs):

        fields = {
            "primary_study_id": 1,
            "ancillary_study_id": 2,
            "primary_participant_id": kwargs.get("primary_participant_id"),
            "ancillary_participant_id": kwargs.get("ancillary_participant_id"),
        }
        fields.update(kwargs)

        participant_mapping = self._rex_participant_mapping(**fields)
        self._commit_to_database(participant_mapping)
        return participant_mapping

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
        if not event_id:
            pea = self.create_database_participant_event_activity(
                activity_id=2,
                participant_id=participant_id,
                resource={}
            )
            event_id = pea.id

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
    def _consent_event_type(**kwargs):
        return ConsentEventType(**kwargs)

    def create_database_consent_event_type(self, **kwargs):
        consent_event_type = self._consent_event_type(**kwargs)
        self._commit_to_database(consent_event_type)
        return consent_event_type

    @staticmethod
    def _enrollment_event_type(**kwargs):
        return EnrollmentEventType(**kwargs)

    def create_database_enrollment_event_type(self, **kwargs):
        event_type = self._enrollment_event_type(**kwargs)
        self._commit_to_database(event_type)
        return event_type

    @staticmethod
    def _enrollment_event(**kwargs):
        return EnrollmentEvent(**kwargs)

    def create_database_enrollment_event(self, participant_id, **kwargs):
        event_id = kwargs.get('event_id')
        if not event_id:
            pea = self.create_database_participant_event_activity(
                activity_id=enums.Activity.ENROLLMENT.number,
                participant_id=participant_id,
                resource={}
            )
            event_id = pea.id

        fields = {
            "participant_id": participant_id,
            "event_id": event_id,
            "event_type_id": 1,
            "event_authored_time": TIME
        }

        fields.update(kwargs)
        enrollment_event = self._enrollment_event(**fields)
        self._commit_to_database(enrollment_event)
        return enrollment_event

    @staticmethod
    def _consent_event(**kwargs):
        return ConsentEvent(**kwargs)

    def create_database_consent_event(self, participant_id, **kwargs):
        event_id = kwargs.get('event_id')
        if not event_id:
            pea = self.create_database_participant_event_activity(
                activity_id=3,
                participant_id=participant_id,
                resource={}
            )
            event_id = pea.id

        fields = {
            "event_authored_time": TIME,
            "participant_id": participant_id,
            "event_id": event_id,
            "event_type_id": kwargs.get("event_type_id", 1),
            "opt_in": enums.ConsentOptInTypes.PERMIT,
        }
        fields.update(kwargs)
        consent_event = self._consent_event(**fields)
        self._commit_to_database(consent_event)
        return consent_event

    @staticmethod
    def _activity(**kwargs):
        return Activity(**kwargs)

    def create_database_activity(self, **kwargs):
        activity = self._activity(**kwargs)
        self._commit_to_database(activity)
        return activity

    @staticmethod
    def _withdrawal_event(**kwargs):
        return WithdrawalEvent(**kwargs)

    def create_database_withdrawal_event(self, **kwargs):
        withdrawal_event = self._withdrawal_event(**kwargs)
        self._commit_to_database(withdrawal_event)
        return withdrawal_event

    @staticmethod
    def _deactivated_event(**kwargs):
        return DeactivationEvent(**kwargs)

    def create_database_deactivated_event(self, **kwargs):
        deactivated_event = self._deactivated_event(**kwargs)
        self._commit_to_database(deactivated_event)
        return deactivated_event

    @staticmethod
    def _ops_data_element(**kwargs):
        return ParticipantOpsDataElement(**kwargs)

    def create_database_participant_ops_data_element(self, **kwargs):
        ops_data_element = self._ops_data_element(**kwargs)
        self._commit_to_database(ops_data_element)
        return ops_data_element

    @staticmethod
    def _diet_event(**kwargs):
        return DietEvent(**kwargs)

    def create_database_diet_event(self, **kwargs):
        event_id = kwargs.get('event_id')
        if not event_id:
            pea = self.create_database_participant_event_activity(
                activity_id=6,
                participant_id=kwargs.get('participant_id'),
                resource={}
            )
            event_id = pea.id

        fields = {
            "event_authored_time": TIME,
            "participant_id": kwargs.get('participant_id'),
            "event_id": event_id,
        }
        fields.update(kwargs)
        diet_event = self._diet_event(**kwargs)
        self._commit_to_database(diet_event)
        return diet_event


class NphSmsDataGenerator(NphBaseGenerator):
    @staticmethod
    def _sms_job_run(**kwargs):
        return SmsJobRun(**kwargs)

    def create_database_sms_job_run(self, **kwargs):
        obj = self._sms_job_run(**kwargs)
        self._commit_to_database(obj)
        return obj

    @staticmethod
    def _order(**kwargs):
        return Order(**kwargs)

    def create_database_order(self, **kwargs):
        obj = self._order(**kwargs)
        self._commit_to_database(obj)
        return obj

    @staticmethod
    def _study_category(**kwargs):
        return StudyCategory(**kwargs)

    def create_database_study_category(self, **kwargs):
        obj = self._study_category(**kwargs)
        self._commit_to_database(obj)
        return obj

    @staticmethod
    def _ordered_sample(**kwargs):
        return OrderedSample(**kwargs)

    def create_database_ordered_sample(self, **kwargs):
        obj = self._ordered_sample(**kwargs)
        self._commit_to_database(obj)
        return obj

    @staticmethod
    def _stored_sample(**kwargs):
        return StoredSample(**kwargs)

    def create_database_stored_sample(self, **kwargs):
        obj = self._stored_sample(**kwargs)
        self._commit_to_database(obj)
        return obj

    @staticmethod
    def _sms_sample(**kwargs):
        return SmsSample(**kwargs)

    def create_database_sms_sample(self, **kwargs):
        obj = self._sms_sample(**kwargs)
        self._commit_to_database(obj)
        return obj

    @staticmethod
    def _sms_blocklist(**kwargs):
        return SmsBlocklist(**kwargs)

    def create_database_sms_blocklist(self, **kwargs):
        obj = self._sms_blocklist(**kwargs)
        self._commit_to_database(obj)
        return obj

    @staticmethod
    def _sms_n0(**kwargs):
        return SmsN0(**kwargs)

    def create_database_sms_n0(self, **kwargs):
        obj = self._sms_n0(**kwargs)
        self._commit_to_database(obj)
        return obj

    @staticmethod
    def _sms_n1_mcac(**kwargs):
        return SmsN1Mc1(**kwargs)

    def create_database_sms_n1_mcac(self, **kwargs):
        obj = self._sms_n1_mcac(**kwargs)
        self._commit_to_database(obj)
        return obj

