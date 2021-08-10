from sqlalchemy import Column, Date, Float, ForeignKey, Integer, String, UnicodeText

from rdr_service.model.base import Base
from rdr_service.model.utils import Enum
from .site_enums import DigitalSchedulingStatus, EnrollingStatus, ObsoleteStatus, SiteStatus


class Site(Base):
    __tablename__ = "site"
    siteId = Column("site_id", Integer, primary_key=True)
    """Identifier for the site"""
    siteName = Column("site_name", String(255), nullable=False)
    """The name of the site"""
    # The Google group for the site; this is a unique key used externally.
    googleGroup = Column("google_group", String(255), nullable=False, unique=True)
    mayolinkClientNumber = Column("mayolink_client_number", Integer)
    organizationId = Column("organization_id", Integer, ForeignKey("organization.organization_id"))
    """Organization of the site"""
    hpoId = Column("hpo_id", Integer, ForeignKey("hpo.hpo_id"))
    """Deprecated; this is being replaced by organizationId."""
    siteType = Column("site_type", String(255))
    siteStatus = Column("site_status", Enum(SiteStatus))
    enrollingStatus = Column("enrolling_status", Enum(EnrollingStatus))
    digitalSchedulingStatus = Column("digital_scheduling_status", Enum(DigitalSchedulingStatus))
    """Can participant schedule appointments online"""
    scheduleInstructions = Column("schedule_instructions", String(4096))
    """Contains the script from the communications to schedule an appointment with the site in English"""
    scheduleInstructions_ES = Column("schedule_instructions_es", String(4096))
    """Contains the script from the communications to schedule an appointment with the site in Spanish"""
    launchDate = Column("launch_date", Date)
    notes = Column("notes", UnicodeText)
    notes_ES = Column("notes_es", UnicodeText)

    latitude = Column("latitude", Float)
    """The latitude of the site"""
    longitude = Column("longitude", Float)
    """The longitude of the site"""

    timeZoneId = Column("time_zone_id", String(1024))
    """The time zone of the site"""
    directions = Column("directions", UnicodeText)
    """Directions to the site"""
    physicalLocationName = Column("physical_location_name", String(1024))
    """Name of the physical location (e.g. hospital name)"""

    address1 = Column("address_1", String(1024))
    """The address of the site"""
    address2 = Column("address_2", String(1024))
    """The second line of the address of the site"""
    city = Column("city", String(255))
    """The city of the site"""
    state = Column("state", String(2))
    """The state for the site"""
    zipCode = Column("zip_code", String(10))
    """The postal zip code of the site"""

    phoneNumber = Column("phone_number", String(80))
    """Phone number of the site"""
    adminEmails = Column("admin_emails", String(4096))
    """Administrator emails for each site"""
    link = Column("link", String(255))
    """The link associated with joining at each site"""
    isObsolete = Column("is_obsolete", Enum(ObsoleteStatus))
    """Whether or not the site has been inactivated (1 if obsolete)"""
    resourceId = Column('resource_id', String(255))
