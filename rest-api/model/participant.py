import clock

from model.base import Base
from model.model_utils import make_history
from sqlalchemy import Column, Integer, DateTime, BLOB, UniqueConstraint, ForeignKey
from sqlalchemy.ext.declarative import declared_attr

"""Shared columns for Participant and ParticipantHistory"""
class ParticipantBase(object):
  # We tack 'P' on the front whenever we use this externally
  id = Column(Integer, primary_key=True)

  # Incrementing version, starts at 1 and is incremented on each update.
  version = Column(Integer, nullable=False)

  # We tack 'B' on the front whenever we use this externally
  biobankId = Column('biobank_id', Integer, nullable=False)

  lastModified = Column('last_modified', DateTime, default=clock.CLOCK.now,
                        onupdate=clock.CLOCK.now, nullable=False)
  signUpTime = Column('sign_up_time', DateTime, default=clock.CLOCK.now, nullable=False)
  providerLink = Column('provider_link', BLOB)

  @declared_attr
  def hpoId(cls):
    return Column('hpo_id', Integer, ForeignKey('hpo.id'), nullable=False)

"""Model object for participants"""
class Participant(ParticipantBase, Base):
  __tablename__ = 'participant'
  __table_args__ = (
    UniqueConstraint('biobank_id'),
  )

"""History for participants"""
class ParticipantHistory(ParticipantBase, Base):
  __tablename__ = 'participant_history'
  version = Column(Integer, nullable=False, primary_key=True)


