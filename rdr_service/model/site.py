from sqlalchemy import Column, Date, Float, ForeignKey, Integer, String, UnicodeText

from rdr_service.model.base import Base
from rdr_service.model.utils import Enum
from .site_enums import DigitalSchedulingStatus, EnrollingStatus, ObsoleteStatus, SiteStatus


class Site(Base):
    __tablename__ = "site"
    siteId = Column("site_id", Integer, primary_key=True)
    siteName = Column("site_name", String(255), nullable=False)
    # The Google group for the site; this is a unique key used externally.
    googleGroup = Column("google_group", String(255), nullable=False, unique=True)
    mayolinkClientNumber = Column("mayolink_client_number", Integer)
    organizationId = Column("organization_id", Integer, ForeignKey("organization.organization_id"))
    # Deprecated; this is being replaced by organizationId.
    hpoId = Column("hpo_id", Integer, ForeignKey("hpo.hpo_id"))
    siteType = Column("site_type", String(255))
    siteStatus = Column("site_status", Enum(SiteStatus))
    enrollingStatus = Column("enrolling_status", Enum(EnrollingStatus))
    digitalSchedulingStatus = Column("digital_scheduling_status", Enum(DigitalSchedulingStatus))
    scheduleInstructions = Column("schedule_instructions", String(2048))
    scheduleInstructions_ES = Column("schedule_instructions_es", String(2048))
    launchDate = Column("launch_date", Date)
    notes = Column("notes", UnicodeText)
    notes_ES = Column("notes_es", UnicodeText)
    latitude = Column("latitude", Float)
    longitude = Column("longitude", Float)
    timeZoneId = Column("time_zone_id", String(1024))
    directions = Column("directions", UnicodeText)
    physicalLocationName = Column("physical_location_name", String(1024))
    address1 = Column("address_1", String(1024))
    address2 = Column("address_2", String(1024))
    city = Column("city", String(255))
    state = Column("state", String(2))
    zipCode = Column("zip_code", String(10))
    phoneNumber = Column("phone_number", String(80))
    adminEmails = Column("admin_emails", String(4096))
    link = Column("link", String(255))
    isObsolete = Column("is_obsolete", Enum(ObsoleteStatus))
    resourceId = Column('resource_id', String(255))
