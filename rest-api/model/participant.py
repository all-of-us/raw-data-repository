import clock

from model.base import Base
from model.model_utils import make_history
from sqlalchemy import Column, Integer, DateTime, BLOB, UniqueConstraint, ForeignKey, Index
from sqlalchemy.ext.declarative import declared_attr

class ParticipantBase(object):
  """Mixin with shared columns for Participant and ParticipantHistory"""

  # We tack 'P' on the front whenever we use this externally
  id = Column('id', Integer, primary_key=True)

  # Incrementing version, starts at 1 and is incremented on each update.
  version = Column('version', Integer, nullable=False)

  # We tack 'B' on the front whenever we use this externally
  biobankId = Column('biobank_id', Integer, nullable=False)

  lastModified = Column('last_modified', DateTime, default=clock.CLOCK.now,
                        onupdate=clock.CLOCK.now, nullable=False)
  signUpTime = Column('sign_up_time', DateTime, default=clock.CLOCK.now, nullable=False)
  providerLink = Column('provider_link', BLOB)

  @declared_attr
  def hpoId(cls):
    return Column('hpo_id', Integer, ForeignKey('hpo.id'), nullable=False)

class Participant(ParticipantBase, Base):
  """Model object for participants"""
  __tablename__ = 'participant'  
  
Index('participant_biobank_id', Participant.biobankId, unique=True)  
Index('participant_hpo_id', Participant.hpoId)

class ParticipantHistory(ParticipantBase, Base):
  """History for participants"""
  __tablename__ = 'participant_history'
  version = Column(Integer, nullable=False, primary_key=True)


