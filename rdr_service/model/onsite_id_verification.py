from sqlalchemy import Column, Integer, String, JSON, ForeignKey, event
from sqlalchemy.ext.declarative import declared_attr

from rdr_service.model.utils import Enum, UTCDateTime6
from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.participant_enums import OnSiteVerificationType, OnSiteVerificationVisitType


class OnsiteIdVerification(Base):
    """participant onsite ID verification histories"""

    __tablename__ = "onsite_id_verification"
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime6, nullable=True)
    """The create time for this record."""
    modified = Column("modified", UTCDateTime6, nullable=True)
    """The last modified time for this record."""

    participantId = Column("participant_id", Integer, ForeignKey("participant.participant_id"), nullable=False)
    """
    Participant id for the on site participant
    """
    userEmail = Column("user_email", String(200))
    """
    Email address for the on site participant
    """
    @declared_attr
    def siteId(cls):
        """The site id for the on site verification event"""
        return Column("site_id", Integer, ForeignKey("site.site_id"))

    verifiedTime = Column("verified_time", UTCDateTime6, nullable=False)
    """
    On site verification event time
    """
    verificationType = Column("verification_type", Enum(OnSiteVerificationType), nullable=False)
    """
    Indicates the on site verification types
    :ref:`Enumerated values <verification_type>`
    """
    visitType = Column("visit_type", Enum(OnSiteVerificationVisitType), nullable=False)
    """
    Indicates the on site verification visit types
    :ref:`Enumerated values <visit_type>`
    """
    resource = Column("resource", JSON)
    """Original resource value; whole payload request that was sent from the requester"""


event.listen(OnsiteIdVerification, "before_insert", model_insert_listener)
event.listen(OnsiteIdVerification, "before_update", model_update_listener)
