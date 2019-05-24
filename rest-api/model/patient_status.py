from model.base import Base, model_insert_listener, model_update_listener, ModelMixin
from model.utils import Enum, UTCDateTime
from participant_enums import PatientStatusFlag
from sqlalchemy import Column, DateTime, Integer, ForeignKey, UniqueConstraint, Text, event, String


class PatientStatus(Base, ModelMixin):
  """
  Site patient status
  """
  __tablename__ = 'patient_status'

  # Primary Key
  id = Column('id', Integer, primary_key=True, autoincrement=True, nullable=False)
  # have mysql set the creation data for each new order
  created = Column('created', DateTime, nullable=True)
  # have mysql always update the modified data when the record is changed
  modified = Column('modified', DateTime, nullable=True)
  participantId = Column('participant_id', Integer, ForeignKey('participant.participant_id'), nullable=False)
  patientStatus = Column('patient_status', Enum(PatientStatusFlag), nullable=False)
  hpoId = Column('hpo_id', Integer, ForeignKey('hpo.hpo_id'), nullable=False)
  organizationId = Column('organization_id', Integer, ForeignKey('organization.organization_id'),
                                nullable=False, index=True)
  siteId = Column('site_id', Integer, ForeignKey('site.site_id'), nullable=False)
  comment = Column('comment', Text, nullable=True)
  authored = Column('authored', UTCDateTime)
  user = Column('user', String(80), nullable=False)

  __table_args__ = (
    UniqueConstraint('participant_id', 'organization_id', name='uidx_patient_status'),
  )

event.listen(PatientStatus, 'before_insert', model_insert_listener)
event.listen(PatientStatus, 'before_update', model_update_listener)
