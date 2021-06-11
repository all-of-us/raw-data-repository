from protorpc import messages
from sqlalchemy import Boolean, Column, Date, ForeignKey, Integer, String

from rdr_service.model.base import Base
from rdr_service.model.participant import Participant
from rdr_service.model.utils import Enum, UTCDateTime


class ConsentType(messages.Enum):
    PRIMARY = 1
    CABOR = 2
    EHR = 3
    GROR = 4


class ConsentSyncStatus(messages.Enum):
    NEEDS_CORRECTING = 1
    READY_FOR_SYNC = 2


class ConsentFile(Base):
    __tablename__ = 'consent_file'
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    participant_id = Column(Integer, ForeignKey(Participant.participantId))
    type = Column(Enum(ConsentType))

    file_exists = Column(Boolean)
    is_signature_valid = Column(Boolean, default=False)
    is_signing_date_valid = Column(Boolean, default=False)

    signature_str = Column(String(200), nullable=True)
    is_signature_image = Column(Boolean, default=False)
    signing_date = Column(Date, nullable=True)
    expected_sign_date = Column(Date, nullable=True)

    file_upload_time = Column(UTCDateTime, nullable=True)

    other_errors = Column(String(200), nullable=True)
    sync_status = Column(Enum(ConsentSyncStatus))

