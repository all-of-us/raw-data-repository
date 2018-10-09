from model.base import Base
from model.utils import Enum, UTCDateTime
from participant_enums import WithdrawalStatus, SuspensionStatus
from sqlalchemy import Column, Integer, BLOB, ForeignKey, Index, String, UnicodeText
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship


class ParticipantBase(object):
  """Mixin with shared columns for Participant and ParticipantHistory"""

  # Randomly assigned internal ID. We tack 'P' on the front whenever we use this externally.
  participantId = Column('participant_id', Integer, primary_key=True, autoincrement=False)

  # Incrementing version, starts at 1 and is incremented on each update.
  version = Column('version', Integer, nullable=False)

  # Randomly assigned ID used with Biobank. Prefixed with 'B' whenever we use this externally.
  biobankId = Column('biobank_id', Integer, nullable=False)

  lastModified = Column('last_modified', UTCDateTime, nullable=False)
  signUpTime = Column('sign_up_time', UTCDateTime, nullable=False)

  # One or more HPO IDs in FHIR JSON. (The primary link is separately stored as hpoId.)
  providerLink = Column('provider_link', BLOB)

  # Both HealthPro and PTC can mutate participants; we use clientId to track
  # which system did it. An client ID of example@example.com means we created fake data for this
  # participant.
  clientId = Column('client_id', String(80))

  # Default values for withdrawal and suspension are managed through the DAO (instead of column
  # defaults here) to simplify insert v. update semantics.
  # Withdrawal is permanent, and indicates we should neither contact the participant nor use their
  # data in the future.
  withdrawalStatus = Column('withdrawal_status', Enum(WithdrawalStatus), nullable=False)

  # The time at which the participants set their withdrawal status to NO_USE.
  withdrawalTime = Column('withdrawal_time', UTCDateTime)

  withdrawalReason = Column('withdrawal_reason', String(80))
  withdrawalReasonJustification = Column('withdrawal_reason_justification', UnicodeText)
  # Suspension may be temporary, and indicates we should not contact the participant but may
  # continue using their data.
  suspensionStatus = Column('suspension_status', Enum(SuspensionStatus), nullable=False)

  # The time at which the participant set their suspension status to NO_CONTACT.
  suspensionTime = Column('suspension_time', UTCDateTime)

  @declared_attr
  def hpoId(cls):
    return Column('hpo_id', Integer, ForeignKey('hpo.hpo_id'), nullable=False)

  @declared_attr
  def organizationId(cls):
    return Column('organization_id', Integer, ForeignKey('organization.organization_id'))

  @declared_attr
  def siteId(cls):
    return Column('site_id', Integer, ForeignKey('site.site_id'))


class Participant(ParticipantBase, Base):
  __tablename__ = 'participant'
  participantSummary = relationship('ParticipantSummary', uselist=False,
                                    back_populates='participant', cascade='all, delete-orphan')


Index('participant_biobank_id', Participant.biobankId, unique=True)
Index('participant_hpo_id', Participant.hpoId)


class ParticipantHistory(ParticipantBase, Base):
  __tablename__ = 'participant_history'
  version = Column('version', Integer, primary_key=True)
