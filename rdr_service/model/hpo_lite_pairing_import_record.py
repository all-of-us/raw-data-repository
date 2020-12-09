from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship

from rdr_service.model.base import Base
from rdr_service.model.participant import Participant
from rdr_service.model.organization import Organization
from rdr_service.model.utils import UTCDateTime


class HpoLitePairingImportRecord(Base):
    __tablename__ = "hpo_lite_pairing_import_record"
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime, nullable=False)
    participantId = Column("participant_id", Integer, ForeignKey(Participant.participantId), nullable=False)
    pairedDate = Column("paired_date", UTCDateTime)
    orgId = Column("org_id", Integer, ForeignKey(Organization.organizationId), nullable=False)
    uploadingUserId = Column("uploading_user_id", Integer, ForeignKey("api_user.id"), nullable=False)

    participant = relationship("Participant", foreign_keys='HpoLitePairingImportRecord.participantId')
    org = relationship("Organization", foreign_keys='HpoLitePairingImportRecord.orgId')
    uploadingUser = relationship("ApiUser", foreign_keys='HpoLitePairingImportRecord.uploadingUserId', lazy='joined')
