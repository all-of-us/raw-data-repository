from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, event

from rdr_service.model.base import Base, ModelMixin, model_insert_listener, model_update_listener
from rdr_service.model.utils import Enum, UTCDateTime
from rdr_service.participant_enums import PatientStatusFlag


class PatientStatus(Base, ModelMixin):
    """
  Site patient status
  """

    __tablename__ = "patient_status"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", DateTime, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", DateTime, nullable=True)
    participantId = Column("participant_id", Integer, ForeignKey("participant.participant_id"), nullable=False)
    """Participant ID that the status is for"""
    patientStatus = Column("patient_status", Enum(PatientStatusFlag), nullable=False)
    """Coming from HealthPro, when a participant is enrolling or is there for physical measurements & biospecimens"""
    hpoId = Column("hpo_id", Integer, ForeignKey("hpo.hpo_id"), nullable=False)
    """An identifier for the HPO marked as primary for this participant, if any"""
    organizationId = Column(
        "organization_id", Integer, ForeignKey("organization.organization_id"), nullable=False, index=True
    )
    """An organization a participant is paired with or "unset" if none"""
    siteId = Column("site_id", Integer, ForeignKey("site.site_id"), nullable=False)
    """Reference to a physical location pairing level below organization"""
    comment = Column("comment", Text, nullable=True)
    authored = Column("authored", UTCDateTime)
    """
    The exact time a patient status was entered for HealthPro, to support enrollment information sharing across sites
    """
    user = Column("user", String(80), nullable=False)
    """PMI Ops email that sent the data from the site"""

    __table_args__ = (UniqueConstraint("participant_id", "organization_id", name="uidx_patient_status"),)


event.listen(PatientStatus, "before_insert", model_insert_listener)
event.listen(PatientStatus, "before_update", model_update_listener)
