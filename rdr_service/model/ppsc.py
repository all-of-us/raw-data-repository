from sqlalchemy import Column, Integer, BigInteger, String, ForeignKey, event
from sqlalchemy.dialects.mysql import TINYINT, JSON

from rdr_service.model.base import model_insert_listener, model_update_listener, PPSCBase
from rdr_service.model.utils import UTCDateTime


class Participant(PPSCBase):
    __tablename__ = "participant"

    id = Column("id", BigInteger, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    disable_flag = Column(TINYINT, default=0)
    disable_reason = Column(String(1024))
    biobank_id = Column(BigInteger, nullable=False, unique=True, index=True)
    registered_date = Column(UTCDateTime, nullable=False)


event.listen(Participant, "before_insert", model_insert_listener)
event.listen(Participant, "before_update", model_update_listener)


class Activity(PPSCBase):
    __tablename__ = "activity"

    id = Column("id", Integer, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    name = Column(String(128))
    rdr_note = Column(String(1024), nullable=True)
    rule_codes = Column(JSON, nullable=True)


event.listen(Activity, "before_insert", model_insert_listener)
event.listen(Activity, "before_update", model_update_listener)


class ParticipantEventActivity(PPSCBase):
    __tablename__ = "participant_event_activity"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    ignore_reason = Column(String(512))
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    activity_id = Column(Integer, ForeignKey("activity.id"))
    resource = Column(JSON, nullable=True)


event.listen(ParticipantEventActivity, "before_insert", model_insert_listener)
event.listen(ParticipantEventActivity, "before_update", model_update_listener)


class EnrollmentEventType(PPSCBase):
    __tablename__ = "enrollment_event_type"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    name = Column(String(128))
    source_name = Column(String(128))
    rule_codes = Column(JSON, nullable=True)
    version = Column(String(128), nullable=True)


event.listen(EnrollmentEventType, "before_insert", model_insert_listener)
event.listen(EnrollmentEventType, "before_update", model_update_listener)


class EnrollmentEvent(PPSCBase):
    __tablename__ = "enrollment_event"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    ignore_reason = Column(String(512))
    event_authored_time = Column(UTCDateTime)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    event_id = Column(BigInteger, ForeignKey("participant_event_activity.id"))
    event_type_id = Column(BigInteger, ForeignKey("enrollment_event_type.id"))


event.listen(EnrollmentEvent, "before_insert", model_insert_listener)
event.listen(EnrollmentEvent, "before_update", model_update_listener)


class ConsentEvent(PPSCBase):
    __tablename__ = "consent_event"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime, index=True)
    modified = Column(UTCDateTime)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    event_id = Column(BigInteger, ForeignKey("participant_event_activity.id"))
    event_type_name = Column(String(128), index=True)
    event_authored_time = Column(UTCDateTime, index=True)
    data_element_name = Column(String(512), index=True)
    data_element_value = Column(String(512), index=True)
    ignore_flag = Column(TINYINT, default=0)
    ignore_reason = Column(String(512))
    is_correction_flag = Column(TINYINT, default=0)
    dev_note = Column(String(512))


event.listen(ConsentEvent, "before_insert", model_insert_listener)
event.listen(ConsentEvent, "before_update", model_update_listener)


class ProfileUpdatesEvent(PPSCBase):
    __tablename__ = "profile_updates_event"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime, index=True)
    modified = Column(UTCDateTime)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    event_id = Column(BigInteger, ForeignKey("participant_event_activity.id"))
    event_type_name = Column(String(128), index=True)
    event_authored_time = Column(UTCDateTime, index=True)
    data_element_name = Column(String(512), index=True)
    data_element_value = Column(String(512), index=True)
    ignore_flag = Column(TINYINT, default=0)
    ignore_reason = Column(String(512))
    is_correction_flag = Column(TINYINT, default=0)
    dev_note = Column(String(512))


event.listen(ProfileUpdatesEvent, "before_insert", model_insert_listener)
event.listen(ProfileUpdatesEvent, "before_update", model_update_listener)


class SurveyCompletionEvent(PPSCBase):
    __tablename__ = "survey_completion_event"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime, index=True)
    modified = Column(UTCDateTime)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    event_id = Column(BigInteger, ForeignKey("participant_event_activity.id"))
    event_type_name = Column(String(128), index=True)
    event_authored_time = Column(UTCDateTime, index=True)
    data_element_name = Column(String(512), index=True)
    data_element_value = Column(String(512), index=True)
    ignore_flag = Column(TINYINT, default=0)
    ignore_reason = Column(String(512))
    is_correction_flag = Column(TINYINT, default=0)
    dev_note = Column(String(512))


event.listen(SurveyCompletionEvent, "before_insert", model_insert_listener)
event.listen(SurveyCompletionEvent, "before_update", model_update_listener)


class WithdrawalEvent(PPSCBase):
    __tablename__ = "withdrawal_event"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime, index=True)
    modified = Column(UTCDateTime)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    event_id = Column(BigInteger, ForeignKey("participant_event_activity.id"))
    event_type_name = Column(String(128), index=True)
    event_authored_time = Column(UTCDateTime, index=True)
    data_element_name = Column(String(512), index=True)
    data_element_value = Column(String(512), index=True)
    ignore_flag = Column(TINYINT, default=0)
    ignore_reason = Column(String(512))
    is_correction_flag = Column(TINYINT, default=0)
    dev_note = Column(String(512))


event.listen(WithdrawalEvent, "before_insert", model_insert_listener)
event.listen(WithdrawalEvent, "before_update", model_update_listener)


class DeactivationEvent(PPSCBase):
    __tablename__ = "deactivation_event"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime, index=True)
    modified = Column(UTCDateTime)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    event_id = Column(BigInteger, ForeignKey("participant_event_activity.id"))
    event_type_name = Column(String(128), index=True)
    event_authored_time = Column(UTCDateTime, index=True)
    data_element_name = Column(String(512), index=True)
    data_element_value = Column(String(512), index=True)
    ignore_flag = Column(TINYINT, default=0)
    ignore_reason = Column(String(512))
    is_correction_flag = Column(TINYINT, default=0)
    dev_note = Column(String(512))


event.listen(DeactivationEvent, "before_insert", model_insert_listener)
event.listen(DeactivationEvent, "before_update", model_update_listener)


class ParticipantStatusEvent(PPSCBase):
    __tablename__ = "participant_status_event"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime, index=True)
    modified = Column(UTCDateTime)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    event_id = Column(BigInteger, ForeignKey("participant_event_activity.id"))
    event_type_name = Column(String(128), index=True)
    event_authored_time = Column(UTCDateTime, index=True)
    data_element_name = Column(String(512), index=True)
    data_element_value = Column(String(512), index=True)
    ignore_flag = Column(TINYINT, default=0)
    ignore_reason = Column(String(512))
    is_correction_flag = Column(TINYINT, default=0)
    dev_note = Column(String(512))


event.listen(ParticipantStatusEvent, "before_insert", model_insert_listener)
event.listen(ParticipantStatusEvent, "before_update", model_update_listener)


class SiteAttributionEvent(PPSCBase):
    __tablename__ = "site_attribution_event"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime, index=True)
    modified = Column(UTCDateTime)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    event_id = Column(BigInteger, ForeignKey("participant_event_activity.id"))
    event_type_name = Column(String(128), index=True)
    event_authored_time = Column(UTCDateTime, index=True)
    data_element_name = Column(String(512), index=True)
    data_element_value = Column(String(512), index=True)
    ignore_flag = Column(TINYINT, default=0)
    ignore_reason = Column(String(512))
    is_correction_flag = Column(TINYINT, default=0)
    dev_note = Column(String(512))


event.listen(SiteAttributionEvent, "before_insert", model_insert_listener)
event.listen(SiteAttributionEvent, "before_update", model_update_listener)
