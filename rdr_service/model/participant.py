from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    ForeignKey,
    Index,
    Integer,
    String,
    UnicodeText,
    UniqueConstraint,
)
from sqlalchemy import BLOB  # pylint: disable=unused-import
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship

from rdr_service.model.base import Base
from rdr_service.model.utils import Enum, UTCDateTime, UTCDateTime6
from rdr_service.participant_enums import SuspensionStatus, WithdrawalReason, WithdrawalStatus
from rdr_service.model.field_types import BlobUTF8


class ParticipantBase(object):
    """Mixin with shared columns for Participant and ParticipantHistory"""

    # Randomly assigned internal ID. We tack 'P' on the front whenever we use this externally.
    participantId = Column("participant_id", Integer, primary_key=True, autoincrement=False)
    # Assigned ID from PTSC. Recieved in request to create a new Participant.
    externalId = Column("external_id", BigInteger)

    # Incrementing version, starts at 1 and is incremented on each update.
    version = Column("version", Integer, nullable=False)

    # Randomly assigned ID used with Biobank. Prefixed with 'B' whenever we use this externally.
    biobankId = Column("biobank_id", Integer, nullable=False)

    lastModified = Column("last_modified", UTCDateTime6, nullable=False)
    signUpTime = Column("sign_up_time", UTCDateTime, nullable=False)

    # One or more HPO IDs in FHIR JSON. (The primary link is separately stored as hpoId.)
    providerLink = Column("provider_link", BlobUTF8)

    # Both HealthPro and PTC can mutate participants; we use clientId to track
    # which system did it. An client ID of example@example.com means we created fake data for this
    # participant.
    clientId = Column("client_id", String(80))
    # The originating resource for participant, this (unlike clientId) will not change.
    participantOrigination = Column("participant_origination", String(80), nullable=False)

    # Default values for withdrawal and suspension are managed through the DAO (instead of column
    # defaults here) to simplify insert v. update semantics.
    # Withdrawal is permanent, and indicates we should neither contact the participant nor use their
    # data in the future.
    withdrawalStatus = Column("withdrawal_status", Enum(WithdrawalStatus), nullable=False)

    # The time at which the participants set their withdrawal status to NO_USE.
    withdrawalTime = Column("withdrawal_time", UTCDateTime)
    withdrawalAuthored = Column("withdrawal_authored", UTCDateTime)
    withdrawalReason = Column("withdrawal_reason", Enum(WithdrawalReason))
    withdrawalReasonJustification = Column("withdrawal_reason_justification", UnicodeText)
    # Suspension may be temporary, and indicates we should not contact the participant but may
    # continue using their data.
    suspensionStatus = Column("suspension_status", Enum(SuspensionStatus), nullable=False)

    # The time at which the participant set their suspension status to NO_CONTACT.
    suspensionTime = Column("suspension_time", UTCDateTime)
    # If a participant is deemed to be a "ghost" i.e. not real or empty participant obj.
    isGhostId = Column("is_ghost_id", Boolean)
    # The date the participant was marked as ghost
    dateAddedGhost = Column("date_added_ghost", UTCDateTime)

    @declared_attr
    def hpoId(cls):
        return Column("hpo_id", Integer, ForeignKey("hpo.hpo_id"), nullable=False)

    @declared_attr
    def organizationId(cls):
        return Column("organization_id", Integer, ForeignKey("organization.organization_id"))

    @declared_attr
    def siteId(cls):
        return Column("site_id", Integer, ForeignKey("site.site_id"))


class Participant(ParticipantBase, Base):
    __tablename__ = "participant"
    participantSummary = relationship(
        "ParticipantSummary", uselist=False, back_populates="participant", cascade="all, delete-orphan"
    )
    __table_args__ = (UniqueConstraint("external_id"),)


Index("participant_biobank_id", Participant.biobankId, unique=True)
Index("participant_hpo_id", Participant.hpoId)
Index(
    "participant_withdrawl_sign_up_hpo",
    Participant.participantId,
    Participant.withdrawalStatus,
    Participant.signUpTime,
    Participant.hpoId,
    Participant.isGhostId,
)


class ParticipantHistory(ParticipantBase, Base):
    __tablename__ = "participant_history"
    version = Column("version", Integer, primary_key=True)
