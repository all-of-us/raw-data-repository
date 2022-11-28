from protorpc import messages
from sqlalchemy import Boolean, Column, Date, event, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.participant import Participant
from rdr_service.model.utils import Enum, UTCDateTime


class ConsentOtherErrors:
    # Potential values populated in the consent_file table other_errors string field
    MISSING_CONSENT_CHECK_MARK = 'missing consent check mark'
    NON_VETERAN_CONSENT_FOR_VETERAN = 'non-veteran consent for veteran participant'
    VETERAN_CONSENT_FOR_NON_VETERAN = 'veteran consent for non-veteran participant'
    INVALID_PRINTED_NAME = 'invalid printed name'
    SENSITIVE_EHR_EXPECTED = 'non-sensitive ehr consent given when sensitive version expected'
    NONSENSITIVE_EHR_EXPECTED = 'sensitive ehr consent given when non-sensitive version expected'
    INITIALS_MISSING_ON_SENSITIVE_EHR = 'missing expected initials on sensitive ehr'


class ConsentType(messages.Enum):
    PRIMARY = 1
    CABOR = 2
    EHR = 3
    GROR = 4
    UNKNOWN = 5
    PRIMARY_UPDATE = 6
    WEAR = 7
    PRIMARY_RECONSENT = 8
    EHR_RECONSENT = 9
    ETM = 10


class ConsentSyncStatus(messages.Enum):
    NEEDS_CORRECTING = 1
    READY_FOR_SYNC = 2
    OBSOLETE = 3
    SYNC_COMPLETE = 4

    LEGACY = 5
    DELAYING_SYNC = 6
    UNKNOWN = 7


class ConsentFile(Base):
    __tablename__ = 'consent_file'
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    last_checked = Column(UTCDateTime)
    participant_id = Column(Integer, ForeignKey(Participant.participantId))
    type = Column(Enum(ConsentType))

    file_exists = Column(Boolean)
    is_signature_valid = Column(Boolean, default=False)
    is_signing_date_valid = Column(Boolean, default=False)

    signature_str = Column(String(200), nullable=True)
    is_signature_image = Column(Boolean, default=False)
    signing_date = Column(Date, nullable=True)
    printed_name = Column(String(200), nullable=True)
    expected_sign_date = Column(Date, nullable=True)

    file_upload_time = Column(UTCDateTime, nullable=True)
    file_path = Column(String(250), nullable=True)

    sync_time = Column(UTCDateTime, nullable=True)

    other_errors = Column(String(200), nullable=True)
    sync_status = Column(Enum(ConsentSyncStatus))

    consent_response_id = Column(Integer, ForeignKey('consent_response.id'), nullable=True)
    """Id of a record linking the consent to the QuestionnaireResponse that provided the consent."""

    consent_response = relationship('ConsentResponse')
    consent_error_report = relationship('ConsentErrorReport')

# DA-2611:  Track which consent_file records with validation errors already had error reports generated.
# Table schema may expand to include other details as warranted
class ConsentErrorReport(Base):
    __tablename__ = 'consent_error_report'
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    consent_file_id = Column(Integer, ForeignKey(ConsentFile.id), nullable=False)
    notes = Column(String(2048))
    """ Additional details about the report, e.g. the error description and/or corresponding PTSC SD ticket number """


event.listen(ConsentFile, "before_insert", model_insert_listener)
event.listen(ConsentFile, "before_update", model_update_listener)
event.listen(ConsentErrorReport, "before_insert", model_insert_listener)
event.listen(ConsentErrorReport, "before_update", model_insert_listener)
