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
from sqlalchemy.sql import expression

from rdr_service.model.base import Base
from rdr_service.model.utils import Enum, UTCDateTime, UTCDateTime6
from rdr_service.participant_enums import SuspensionStatus, WithdrawalReason, WithdrawalStatus
from rdr_service.model.field_types import BlobUTF8


class ParticipantBase(object):
    """Mixin with shared columns for Participant and ParticipantHistory"""

    participantId = Column("participant_id", Integer, primary_key=True, autoincrement=False)
    """
    PMI-specific ID generated by the RDR and used for tracking/linking participant data.
    10-character string beginning with P.
    """
    # Assigned ID from PTSC. Received in request to create a new Participant.
    externalId = Column("external_id", BigInteger)
    """Vibrent's internal ID for participants"""
    # 7 digits number unique research id
    researchId = Column("research_id", Integer)
    version = Column("version", Integer, nullable=False)
    """
    Incrementing version, starts at 1 and is incremented on each update. The history table will have multiple versions
    ranging from 1 to the number of times the record has been updated. Each of these different versions will show
    the values that have changed.
    """

    # Randomly assigned ID used with Biobank. Prefixed with 'B' whenever we use this externally.
    biobankId = Column("biobank_id", Integer, nullable=False)
    """
    PMI-specific ID generated by the RDR and used exclusively for communicating with the biobank.
    Human-readable 10-character string beginning with B.
    """

    lastModified = Column("last_modified", UTCDateTime6, nullable=False)
    """The date and time the participant was last modified"""
    signUpTime = Column("sign_up_time", UTCDateTime, nullable=False)
    """The time at which the participant initially signed up for All of Us"""

    providerLink = Column("provider_link", BlobUTF8)
    """
    List of "provider link" objects indicating that this participant is known to one or more HPO.
    The primary link is separately stored as hpoId.
    """

    clientId = Column("client_id", String(80))
    """
    Both HealthPro and PTC can mutate participants; we use clientId to track
    which system did it. An client ID of example@example.com means we created fake data for this
    participant.
    """
    participantOrigin = Column("participant_origin", String(80), nullable=False)
    """
    The originating resource for participant, this (unlike clientId) will not change.
    @rdr_dictionary_show_unique_values
    """

    # Default values for withdrawal and suspension are managed through the DAO (instead of column
    # defaults here) to simplify insert v. update semantics.
    withdrawalStatus = Column("withdrawal_status", Enum(WithdrawalStatus), nullable=False)
    """
    Indicates whether the participant has withdrawn from the study, and does not want their data used in future;
    (No use indicates data cannot be used)
    """

    # The time at which the participants set their withdrawal status to NO_USE.
    withdrawalTime = Column("withdrawal_time", UTCDateTime)
    """
    The date and time at which the participant withdrew from the study
    (48 hour delay from the real time the participant clicked the withdraw button)
    """
    withdrawalAuthored = Column("withdrawal_authored", UTCDateTime)
    """
    The actual time at which the participant chose to stop participating in the Participant Portal
    """
    withdrawalReason = Column("withdrawal_reason", Enum(WithdrawalReason))
    """Specific to administrative withdrawals; reason admin withdrew the participant"""
    withdrawalReasonJustification = Column("withdrawal_reason_justification", UnicodeText)
    """
    Related to administrative withdrawals, used when withdrawal_reason is not unset;
    allows for additional explanation related to withdrawal reason
    """
    suspensionStatus = Column("suspension_status", Enum(SuspensionStatus), nullable=False)
    """
    Indicates whether the participant has indicated they do not want to be contacted anymore;
    also shouldn't have any EHR data transferred after the given suspension date.
    Suspension may be temporary, and indicates we should not contact the participant but may
    continue using their data.
    """

    # The time at which the participant set their suspension status to NO_CONTACT.
    suspensionTime = Column("suspension_time", UTCDateTime)
    """
    The date and time at which the participant has indicated they do not want to be contacted anymore;
    also shouldn't have any EHR data transferred after the given suspension date.
    """
    isGhostId = Column("is_ghost_id", Boolean)
    """If a participant is deemed to be a "ghost" i.e. not real or empty participant obj. 1 = ghost"""
    # The date the participant was marked as ghost
    dateAddedGhost = Column("date_added_ghost", UTCDateTime)
    """
    The date the RDR marked a participant id as a ghost account based on a .csv file ingested by the RDR that identifies
    ghost participants (based on is_ghost field). .csv file is generated by Vibrent (SS) and uses their standard
    algorithm for ghost participants.
    """

    isTestParticipant = Column(
        "is_test_participant",
        Boolean, nullable=False,
        default=False,
        server_default=expression.false()
    )

    @declared_attr
    def hpoId(cls):
        return Column("hpo_id", Integer, ForeignKey("hpo.hpo_id"), nullable=False)

    @declared_attr
    def organizationId(cls):
        return Column("organization_id", Integer, ForeignKey("organization.organization_id"))

    @declared_attr
    def siteId(cls):
        """
        Reference to a physical location pairing level below organization.
        """
        return Column("site_id", Integer, ForeignKey("site.site_id"))

    @declared_attr
    def enrollmentSiteId(cls):
        return Column("enrollment_site_id", Integer, ForeignKey("site.site_id"))


class Participant(ParticipantBase, Base):
    __tablename__ = "participant"
    participantSummary = relationship(
        "ParticipantSummary", uselist=False, back_populates="participant", cascade="all, delete-orphan"
    )
    __table_args__ = (UniqueConstraint("external_id"), UniqueConstraint("research_id"),)

    organization = relationship("Organization", foreign_keys='Participant.organizationId', viewonly=True)
    """
    Organ doc string on actual class
    """


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
    """An indicator for the version of participant history"""
