import json

from model.base import Base
from model.utils import Enum, to_client_participant_id, to_client_biobank_id
from participant_enums import WithdrawalStatus, SuspensionStatus
from sqlalchemy import Column, Integer, DateTime, BLOB, ForeignKey, Index, String
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

  lastModified = Column('last_modified', DateTime, nullable=False)
  signUpTime = Column('sign_up_time', DateTime, nullable=False)
  providerLink = Column('provider_link', BLOB)

  # Both HealthPro and PTC can mutate participants; we use clientId to track
  # which system did it. An client ID of example@example.com means we created fake data for this
  # participant.
  clientId = Column('client_id', String(80))

  # Withdrawal from the study of the participant's own accord.
  withdrawalStatus = Column('withdrawal_status', Enum(WithdrawalStatus), nullable=False)
  suspensionStatus = Column('suspension_status', Enum(SuspensionStatus), nullable=False)

  @declared_attr
  def hpoId(cls):
    return Column('hpo_id', Integer, ForeignKey('hpo.hpo_id'), nullable=False)

  def to_client_json(self):
    return {
        'participantId': to_client_participant_id(self.participantId),
        'biobankId': to_client_biobank_id(self.biobankId),
        'lastModified': self.lastModified.isoformat(),
        'signUpTime': self.signUpTime.isoformat(),
        'providerLink': json.loads(self.providerLink),
        'withdrawalStatus': self.withdrawalStatus.number,
        'suspensionStatus': self.suspensionStatus.number,
    }


class Participant(ParticipantBase, Base):
  __tablename__ = 'participant'
  participantSummary = relationship("ParticipantSummary", uselist=False,
                                    back_populates="participant", cascade='all, delete-orphan')

  @staticmethod
  def from_client_json(resource_json, id_=None, expected_version=None, client_id=None):
    withdrawal_value = resource_json.get('withdrawalStatus')
    suspension_value = resource_json.get('suspensionStatus')
    # biobankId, lastModified, signUpTime are set by DAO.
    return Participant(
        participantId=id_,
        version=expected_version,
        providerLink=json.dumps(resource_json.get('providerLink')),
        clientId=client_id,
        withdrawalStatus=withdrawal_value and WithdrawalStatus(withdrawal_value),
        suspensionStatus=suspension_value and SuspensionStatus(suspension_value))


Index('participant_biobank_id', Participant.biobankId, unique=True)
Index('participant_hpo_id', Participant.hpoId)


class ParticipantHistory(ParticipantBase, Base):
  __tablename__ = 'participant_history'
  version = Column('version', Integer, primary_key=True)
